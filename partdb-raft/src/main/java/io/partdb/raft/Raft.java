package io.partdb.raft;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.IntUnaryOperator;

public final class Raft {

    private record PendingRead(long index, byte[] context, String requester, Set<String> acks) {}

    private final String id;
    private Membership membership;
    private Set<String> peers;
    private Set<String> votingPeers;
    private int quorum;
    private boolean isLearner;
    private Long pendingConfigIndex;
    private final int electionTimeoutMin;
    private final int electionTimeoutMax;
    private final int heartbeatInterval;
    private final int maxEntriesPerAppend;
    private final IntUnaryOperator random;
    private final RaftStorage storage;
    private final Unstable unstable;

    private long term;
    private String votedFor;

    private Role role;
    private String leaderId;
    private long commitIndex;
    private long lastApplied;

    private Map<String, Long> nextIndex;
    private Map<String, Long> matchIndex;
    private List<PendingRead> pendingReads;

    private Set<String> votesReceived;
    private Set<String> preVotesReceived;

    private long tickCount;
    private long electionTicks;
    private long heartbeatTicks;
    private long electionTimeout;
    private long leaderContactTicks;
    private Map<String, Long> lastHeardFrom;

    public Raft(String id, Membership membership, RaftConfig config,
                RaftStorage storage, IntUnaryOperator random) {
        if (!membership.isVoter(id) && !membership.isLearner(id)) {
            throw new IllegalArgumentException("Node must be member of cluster");
        }
        this.id = id;
        this.membership = membership;
        this.isLearner = membership.isLearner(id);
        this.quorum = membership.voters().size() / 2 + 1;

        var allPeers = new LinkedHashSet<>(membership.voters());
        allPeers.addAll(membership.learners());
        allPeers.remove(id);
        this.peers = Set.copyOf(allPeers);

        var voting = new LinkedHashSet<>(membership.voters());
        voting.remove(id);
        this.votingPeers = Set.copyOf(voting);

        this.electionTimeoutMin = config.electionTimeoutMin();
        this.electionTimeoutMax = config.electionTimeoutMax();
        this.heartbeatInterval = config.heartbeatInterval();
        this.maxEntriesPerAppend = config.maxEntriesPerAppend();
        this.random = random;
        this.storage = storage;
        this.unstable = new Unstable(storage.lastIndex() + 1);
        this.role = Role.FOLLOWER;
        this.term = 0;
        this.commitIndex = 0;
        this.lastApplied = 0;
        resetElectionTimeout();
    }

    public Ready step(RaftEvent event) {
        var builder = new ReadyBuilder();

        switch (event) {
            case RaftEvent.Tick() -> tick(builder);
            case RaftEvent.Propose(var data) -> propose(data, builder);
            case RaftEvent.Receive(var from, var msg) -> receive(from, msg, builder);
            case RaftEvent.ReadIndex(var context) -> handleReadIndexEvent(context, builder);
            case RaftEvent.ChangeConfig(var change) -> proposeConfigChange(change, builder);
        }

        prepareEntriesToApply(builder);

        return builder.build();
    }

    public void advance(long persistedIndex, long persistedTerm) {
        lastApplied = Math.max(lastApplied, persistedIndex);
        unstable.stableTo(persistedIndex, persistedTerm);
    }

    public void restore(HardState hardState, long snapIndex) {
        this.term = hardState.term();
        this.votedFor = hardState.votedFor();
        this.commitIndex = Math.max(hardState.commit(), snapIndex);
        this.lastApplied = snapIndex;
    }

    public boolean isLeader() {
        return role == Role.LEADER;
    }

    public Role role() {
        return role;
    }

    public Optional<String> leaderId() {
        return Optional.ofNullable(leaderId);
    }

    public long term() {
        return term;
    }

    public long commitIndex() {
        return commitIndex;
    }

    public long lastApplied() {
        return lastApplied;
    }

    public Membership membership() {
        return membership;
    }

    private long lastIndex() {
        if (unstable.hasEntries()) {
            return unstable.lastIndex();
        }
        return storage.lastIndex();
    }

    private long lastTerm() {
        if (unstable.hasEntries()) {
            return unstable.lastTerm();
        }
        long last = storage.lastIndex();
        return last > 0 ? storage.term(last) : 0;
    }

    private long term(long index) {
        Long t = unstable.term(index);
        if (t != null) {
            return t;
        }
        return storage.term(index);
    }

    private LogEntry get(long index) {
        LogEntry entry = unstable.get(index);
        if (entry != null) {
            return entry;
        }
        var entries = storage.entries(index, index + 1, Long.MAX_VALUE);
        return entries.isEmpty() ? null : entries.getFirst();
    }

    private List<LogEntry> slice(long from, long to) {
        if (from >= to) {
            return List.of();
        }

        var result = new ArrayList<LogEntry>();
        long storageLastIndex = storage.lastIndex();

        if (from <= storageLastIndex) {
            result.addAll(storage.entries(from, Math.min(to, storageLastIndex + 1), Long.MAX_VALUE));
        }

        if (to > storageLastIndex + 1) {
            result.addAll(unstable.slice(Math.max(from, storageLastIndex + 1), to));
        }

        return result;
    }

    private boolean hasEntryMatching(long index, long prevTerm) {
        if (index == 0) {
            return true;
        }
        return term(index) == prevTerm;
    }

    private long lastIncludedIndex() {
        long first = storage.firstIndex();
        return first > 1 ? first - 1 : 0;
    }

    private long lastIncludedTerm() {
        long index = lastIncludedIndex();
        return index > 0 ? storage.term(index) : 0;
    }

    private void appendEntry(LogEntry entry) {
        unstable.append(entry);
    }

    private void tick(ReadyBuilder builder) {
        tickCount++;
        leaderContactTicks++;
        switch (role) {
            case LEADER -> tickHeartbeat(builder);
            case FOLLOWER, PRE_CANDIDATE, CANDIDATE -> tickElection(builder);
        }
    }

    private void tickHeartbeat(ReadyBuilder builder) {
        heartbeatTicks++;
        if (heartbeatTicks >= heartbeatInterval) {
            heartbeatTicks = 0;
            broadcastAppend(builder);
        }
        if (!hasRecentQuorumContact()) {
            becomeFollower(term, null, builder);
        }
    }

    private boolean hasRecentQuorumContact() {
        if (quorum == 1) {
            return true;
        }
        int reachable = 1;
        for (String peer : votingPeers) {
            if (tickCount - lastHeardFrom.getOrDefault(peer, 0L) < electionTimeout) {
                reachable++;
            }
        }
        return reachable >= quorum;
    }

    private void tickElection(ReadyBuilder builder) {
        if (isLearner) {
            return;
        }
        electionTicks++;
        if (electionTicks >= electionTimeout) {
            switch (role) {
                case FOLLOWER, PRE_CANDIDATE -> startPreVote(builder);
                case CANDIDATE -> startElection(builder);
                case LEADER -> throw new IllegalStateException("Leader should not tick election");
            }
        }
    }

    private void propose(byte[] data, ReadyBuilder builder) {
        if (role != Role.LEADER) {
            return;
        }

        var entry = new LogEntry.Data(lastIndex() + 1, term, data);
        appendEntry(entry);
        builder.persist(entry);

        if (quorum == 1) {
            commitIndex = entry.index();
            persistHardState(builder);
        } else {
            broadcastAppend(builder);
        }
    }

    private void proposeConfigChange(ConfigChange change, ReadyBuilder builder) {
        if (role != Role.LEADER) {
            return;
        }

        if (hasPendingConfigChange()) {
            return;
        }

        Membership newMembership;
        try {
            newMembership = applyConfigChange(change);
        } catch (IllegalArgumentException e) {
            return;
        }

        var entry = new LogEntry.Config(lastIndex() + 1, term, newMembership);
        appendEntry(entry);
        builder.persist(entry);
        pendingConfigIndex = entry.index();

        if (quorum == 1) {
            commitIndex = entry.index();
            persistHardState(builder);
        } else {
            broadcastAppend(builder);
        }
    }

    private boolean hasPendingConfigChange() {
        return pendingConfigIndex != null && pendingConfigIndex > commitIndex;
    }

    private Membership applyConfigChange(ConfigChange change) {
        return switch (change) {
            case ConfigChange.AddLearner(var nodeId) -> membership.addLearner(nodeId);
            case ConfigChange.PromoteVoter(var nodeId) -> membership.promoteVoter(nodeId);
            case ConfigChange.DemoteToLearner(var nodeId) -> membership.demoteToLearner(nodeId);
            case ConfigChange.RemoveNode(var nodeId) -> membership.removeNode(nodeId);
        };
    }

    private void updateMembershipState(Membership newMembership) {
        this.membership = newMembership;
        this.isLearner = newMembership.isLearner(id);
        this.quorum = newMembership.voters().size() / 2 + 1;

        var allPeers = new LinkedHashSet<>(newMembership.voters());
        allPeers.addAll(newMembership.learners());
        allPeers.remove(id);
        this.peers = Set.copyOf(allPeers);

        var voting = new LinkedHashSet<>(newMembership.voters());
        voting.remove(id);
        this.votingPeers = Set.copyOf(voting);

        if (role == Role.LEADER) {
            for (String peer : peers) {
                if (!nextIndex.containsKey(peer)) {
                    nextIndex.put(peer, lastIndex() + 1);
                    matchIndex.put(peer, 0L);
                }
            }
            nextIndex.keySet().retainAll(peers);
            matchIndex.keySet().retainAll(peers);
        }
    }

    private void stepDownAfterConfigApplied() {
        role = Role.FOLLOWER;
        leaderId = null;
        pendingReads = null;
        lastHeardFrom = null;
        resetElectionTimeout();
    }

    private void receive(String from, RaftMessage msg, ReadyBuilder builder) {
        boolean checkTerm = switch (msg) {
            case RaftMessage.PreVote _,
                 RaftMessage.PreVoteResponse _,
                 RaftMessage.ReadIndex _,
                 RaftMessage.ReadIndexResponse _ -> false;
            default -> true;
        };

        if (checkTerm && msg.term() > term) {
            becomeFollower(msg.term(), null, builder);
        }

        switch (msg) {
            case RaftMessage.PreVote pv -> handlePreVote(from, pv, builder);
            case RaftMessage.PreVoteResponse pvr -> handlePreVoteResponse(from, pvr, builder);
            case RaftMessage.RequestVote rv -> handleRequestVote(from, rv, builder);
            case RaftMessage.RequestVoteResponse rvr -> handleRequestVoteResponse(from, rvr, builder);
            case RaftMessage.AppendEntries ae -> handleAppendEntries(from, ae, builder);
            case RaftMessage.AppendEntriesResponse aer -> handleAppendEntriesResponse(from, aer, builder);
            case RaftMessage.InstallSnapshot is -> handleInstallSnapshot(from, is, builder);
            case RaftMessage.InstallSnapshotResponse isr -> handleInstallSnapshotResponse(from, isr);
            case RaftMessage.ReadIndex ri -> handleReadIndex(from, ri, builder);
            case RaftMessage.ReadIndexResponse rir -> handleReadIndexResponse(rir, builder);
        }
    }

    private void startPreVote(ReadyBuilder builder) {
        if (quorum == 1) {
            startElection(builder);
            return;
        }

        role = Role.PRE_CANDIDATE;
        leaderId = null;
        preVotesReceived = new HashSet<>();
        preVotesReceived.add(id);
        resetElectionTimeout();

        var request = new RaftMessage.PreVote(term + 1, id, lastIndex(), lastTerm());
        for (String peer : votingPeers) {
            builder.send(peer, request);
        }
    }

    private void startElection(ReadyBuilder builder) {
        term++;
        votedFor = id;
        role = Role.CANDIDATE;
        leaderId = null;
        votesReceived = new HashSet<>();
        votesReceived.add(id);
        resetElectionTimeout();

        persistHardState(builder);

        if (quorum == 1) {
            becomeLeader(builder);
            return;
        }

        var request = new RaftMessage.RequestVote(
            term, id, lastIndex(), lastTerm()
        );

        for (String peer : votingPeers) {
            builder.send(peer, request);
        }
    }

    private void becomeLeader(ReadyBuilder builder) {
        role = Role.LEADER;
        leaderId = id;
        heartbeatTicks = 0;

        nextIndex = new HashMap<>();
        matchIndex = new HashMap<>();
        lastHeardFrom = new HashMap<>();
        pendingReads = new ArrayList<>();
        for (String peer : peers) {
            nextIndex.put(peer, lastIndex() + 1);
            matchIndex.put(peer, 0L);
            lastHeardFrom.put(peer, tickCount);
        }

        var noOp = new LogEntry.NoOp(lastIndex() + 1, term);
        appendEntry(noOp);
        builder.persist(noOp);

        if (quorum == 1) {
            commitIndex = noOp.index();
            persistHardState(builder);
        } else {
            broadcastAppend(builder);
        }
    }

    private void becomeFollower(long newTerm, String leader, ReadyBuilder builder) {
        boolean termChanged = newTerm > term;
        if (termChanged) {
            term = newTerm;
            votedFor = null;
            persistHardState(builder);
        }
        if (role != Role.FOLLOWER || termChanged) {
            role = Role.FOLLOWER;
            leaderId = leader;
            pendingReads = null;
            lastHeardFrom = null;
        } else if (leader != null) {
            leaderId = leader;
        }
        resetElectionTimeout();
    }

    private void handlePreVote(String from, RaftMessage.PreVote pv, ReadyBuilder builder) {
        boolean voteGranted = false;

        if (pv.term() >= term) {
            boolean leaderActive = leaderContactTicks < electionTimeout;
            if (isLogUpToDate(pv.lastLogTerm(), pv.lastLogIndex()) && !leaderActive) {
                voteGranted = true;
            }
        }

        builder.send(from, new RaftMessage.PreVoteResponse(term, voteGranted));
    }

    private void handlePreVoteResponse(String from, RaftMessage.PreVoteResponse pvr, ReadyBuilder builder) {
        if (role != Role.PRE_CANDIDATE || pvr.term() < term) {
            return;
        }

        if (pvr.voteGranted()) {
            preVotesReceived.add(from);
            if (preVotesReceived.size() >= quorum) {
                startElection(builder);
            }
        }
    }

    private void handleRequestVote(String from, RaftMessage.RequestVote rv, ReadyBuilder builder) {
        boolean voteGranted = false;

        if (rv.term() >= term) {
            if (isLogUpToDate(rv.lastLogTerm(), rv.lastLogIndex()) &&
                (votedFor == null || votedFor.equals(from))) {
                voteGranted = true;
                votedFor = from;
                persistHardState(builder);
                resetElectionTimeout();
            }
        }

        builder.send(from, new RaftMessage.RequestVoteResponse(term, voteGranted));
    }

    private void handleRequestVoteResponse(String from, RaftMessage.RequestVoteResponse rvr, ReadyBuilder builder) {
        if (role != Role.CANDIDATE || rvr.term() != term) {
            return;
        }

        if (rvr.voteGranted()) {
            votesReceived.add(from);
            if (votesReceived.size() >= quorum) {
                becomeLeader(builder);
            }
        }
    }

    private void handleAppendEntries(String from, RaftMessage.AppendEntries ae, ReadyBuilder builder) {
        if (ae.term() < term) {
            builder.send(from, new RaftMessage.AppendEntriesResponse(term, false, 0));
            return;
        }

        leaderContactTicks = 0;
        becomeFollower(ae.term(), ae.leaderId(), builder);

        if (!hasEntryMatching(ae.prevLogIndex(), ae.prevLogTerm())) {
            builder.send(from, new RaftMessage.AppendEntriesResponse(term, false, 0));
            return;
        }

        long matchIdx = ae.prevLogIndex();
        for (LogEntry entry : ae.entries()) {
            long existingTerm = term(entry.index());
            if (existingTerm != entry.term()) {
                appendEntry(entry);
                builder.persist(entry);
            }
            matchIdx = entry.index();
        }

        if (ae.leaderCommit() > commitIndex) {
            commitIndex = Math.min(ae.leaderCommit(), matchIdx);
            persistHardState(builder);
        }

        builder.send(from, new RaftMessage.AppendEntriesResponse(term, true, matchIdx));
    }

    private void handleAppendEntriesResponse(String from, RaftMessage.AppendEntriesResponse aer, ReadyBuilder builder) {
        if (role != Role.LEADER || aer.term() != term) {
            return;
        }

        lastHeardFrom.put(from, tickCount);

        if (aer.success()) {
            if (aer.matchIndex() > matchIndex.getOrDefault(from, 0L)) {
                nextIndex.put(from, aer.matchIndex() + 1);
                matchIndex.put(from, aer.matchIndex());
                advanceCommitIndex(builder);
            }
            checkPendingReads(from, builder);
        } else {
            long next = nextIndex.getOrDefault(from, 1L);
            nextIndex.put(from, Math.max(1, next - 1));
            sendAppend(from, builder);
        }
    }

    private void handleInstallSnapshot(String from, RaftMessage.InstallSnapshot is, ReadyBuilder builder) {
        if (is.term() < term) {
            builder.send(from, new RaftMessage.InstallSnapshotResponse(term));
            return;
        }

        leaderContactTicks = 0;
        becomeFollower(is.term(), is.leaderId(), builder);

        if (is.lastIncludedIndex() <= commitIndex) {
            builder.send(from, new RaftMessage.InstallSnapshotResponse(term));
            return;
        }

        unstable.acceptSnapshot(new Snapshot(
            is.lastIncludedIndex(),
            is.lastIncludedTerm(),
            is.membership(),
            is.data()
        ));
        commitIndex = is.lastIncludedIndex();
        lastApplied = is.lastIncludedIndex();
        persistHardState(builder);

        updateMembershipState(is.membership());

        builder.setIncomingSnapshot(new Snapshot(
            is.lastIncludedIndex(),
            is.lastIncludedTerm(),
            is.membership(),
            is.data()
        ));
        builder.send(from, new RaftMessage.InstallSnapshotResponse(term));
    }

    private void handleInstallSnapshotResponse(String from, RaftMessage.InstallSnapshotResponse isr) {
        if (role != Role.LEADER || isr.term() != term) {
            return;
        }
        lastHeardFrom.put(from, tickCount);
        nextIndex.put(from, lastIncludedIndex() + 1);
        matchIndex.put(from, lastIncludedIndex());
    }

    private void handleReadIndexEvent(byte[] context, ReadyBuilder builder) {
        if (role == Role.LEADER) {
            startReadIndexConfirmation(null, context, builder);
        } else if (leaderId != null) {
            builder.send(leaderId, new RaftMessage.ReadIndex(term, context));
        }
    }

    private void handleReadIndex(String from, RaftMessage.ReadIndex ri, ReadyBuilder builder) {
        if (role != Role.LEADER) {
            builder.send(from, new RaftMessage.ReadIndexResponse(term, 0, ri.context()));
            return;
        }
        startReadIndexConfirmation(from, ri.context(), builder);
    }

    private void handleReadIndexResponse(RaftMessage.ReadIndexResponse rir, ReadyBuilder builder) {
        if (rir.readIndex() > 0) {
            builder.addReadState(rir.readIndex(), rir.context());
        }
    }

    private void startReadIndexConfirmation(String requester, byte[] context, ReadyBuilder builder) {
        if (quorum == 1) {
            if (requester == null) {
                builder.addReadState(commitIndex, context);
            } else {
                builder.send(requester, new RaftMessage.ReadIndexResponse(term, commitIndex, context));
            }
            return;
        }

        var acks = new HashSet<String>();
        acks.add(id);
        pendingReads.add(new PendingRead(commitIndex, context, requester, acks));
        broadcastAppend(builder);
    }

    private void checkPendingReads(String from, ReadyBuilder builder) {
        if (pendingReads == null || pendingReads.isEmpty()) {
            return;
        }

        if (!membership.isVoter(from)) {
            return;
        }

        var it = pendingReads.iterator();
        while (it.hasNext()) {
            var pending = it.next();
            pending.acks().add(from);
            if (pending.acks().size() >= quorum) {
                if (pending.requester() == null) {
                    builder.addReadState(pending.index(), pending.context());
                } else {
                    builder.send(pending.requester(),
                        new RaftMessage.ReadIndexResponse(term, pending.index(), pending.context()));
                }
                it.remove();
            }
        }
    }

    private void broadcastAppend(ReadyBuilder builder) {
        for (String peer : peers) {
            sendAppend(peer, builder);
        }
    }

    private void sendAppend(String peer, ReadyBuilder builder) {
        long next = nextIndex.getOrDefault(peer, lastIndex() + 1);
        long prevIndex = next - 1;

        if (prevIndex < lastIncludedIndex()) {
            sendSnapshot(peer, builder);
            return;
        }

        long prevTerm = term(prevIndex);
        List<LogEntry> entries = slice(next, next + maxEntriesPerAppend);

        builder.send(peer, new RaftMessage.AppendEntries(
            term, id, prevIndex, prevTerm, entries, commitIndex
        ));
    }

    private void sendSnapshot(String peer, ReadyBuilder builder) {
        builder.setSnapshotToSend(peer, lastIncludedIndex(), lastIncludedTerm());
    }

    private void advanceCommitIndex(ReadyBuilder builder) {
        long oldCommit = commitIndex;
        for (long n = lastIndex(); n > commitIndex; n--) {
            if (term(n) != term) {
                continue;
            }

            int count = 1;
            for (String peer : votingPeers) {
                if (matchIndex.getOrDefault(peer, 0L) >= n) {
                    count++;
                }
            }

            if (count >= quorum) {
                commitIndex = n;
                break;
            }
        }
        if (commitIndex > oldCommit) {
            persistHardState(builder);
        }
    }

    private void prepareEntriesToApply(ReadyBuilder builder) {
        for (long i = lastApplied + 1; i <= commitIndex; i++) {
            switch (get(i)) {
                case LogEntry.Data entry -> builder.apply(entry.index(), entry.term(), entry.data());
                case LogEntry.Config entry -> {
                    Membership previous = membership;
                    updateMembershipState(entry.membership());
                    builder.addMembershipChange(entry.index(), previous, entry.membership());

                    if (pendingConfigIndex != null && pendingConfigIndex == entry.index()) {
                        pendingConfigIndex = null;
                    }

                    if (role == Role.LEADER && !entry.membership().isVoter(id)) {
                        stepDownAfterConfigApplied();
                    }
                }
                case LogEntry.NoOp _ -> {}
                case null -> throw new IllegalStateException(
                    "Missing log entry at index " + i + " (firstIndex=" + storage.firstIndex() + ")"
                );
            }
        }
    }

    private void resetElectionTimeout() {
        electionTicks = 0;
        int jitter = electionTimeoutMax - electionTimeoutMin;
        electionTimeout = electionTimeoutMin + (jitter > 0 ? random.applyAsInt(jitter) : 0);
    }

    private boolean isLogUpToDate(long lastLogTerm, long lastLogIndex) {
        return lastLogTerm > lastTerm() ||
            (lastLogTerm == lastTerm() && lastLogIndex >= lastIndex());
    }

    private void persistHardState(ReadyBuilder builder) {
        builder.setHardState(new HardState(term, votedFor, commitIndex));
    }
}
