package io.partdb.raft;

import io.partdb.bytes.Bytes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.IntUnaryOperator;

public final class RaftNode {

    private record PendingRead(long index, Bytes context, String requester, Set<String> acks) {}

    private final String id;
    private RaftMembership membership;
    private Set<String> peers;
    private Set<String> votingPeers;
    private int quorum;
    private boolean isLearner;
    private Long pendingMembershipChangeIndex;
    private final int electionTimeoutMin;
    private final int electionTimeoutMax;
    private final int heartbeatInterval;
    private final int maxEntriesPerAppend;
    private final IntUnaryOperator random;
    private final RaftLogReader log;
    private final UnstableLog unstable;

    private long term;
    private String votedFor;

    private RaftRole role;
    private String leaderId;
    private long commitIndex;
    private long lastApplied;

    private Map<String, Long> nextIndex;
    private Map<String, Long> matchIndex;
    private List<PendingRead> pendingReads = List.of();

    private Set<String> votesReceived;
    private Set<String> preVotesReceived;

    private long tickCount;
    private long electionTicks;
    private long heartbeatTicks;
    private long electionTimeout;
    private long leaderContactTicks;
    private Map<String, Long> lastHeardFrom;

    private RaftNode(String id, RaftMembership membership, RaftOptions options,
                 RaftLogReader log, IntUnaryOperator electionJitter, RaftHardState hardState, long snapshotIndex) {
        if (!membership.isMember(id)) {
            throw new IllegalArgumentException("Node must be member of cluster");
        }
        this.id = id;
        this.membership = membership;
        this.isLearner = membership.isLearner(id);
        this.quorum = membership.quorumSize();
        this.peers = membership.peerIdsExcluding(id);
        this.votingPeers = membership.votingPeerIdsExcluding(id);

        this.electionTimeoutMin = options.electionTimeoutMin();
        this.electionTimeoutMax = options.electionTimeoutMax();
        this.heartbeatInterval = options.heartbeatInterval();
        this.maxEntriesPerAppend = options.maxEntriesPerAppend();
        this.random = electionJitter;
        this.log = log;
        this.unstable = new UnstableLog(log.lastIndex() + 1);
        this.role = RaftRole.FOLLOWER;
        this.term = hardState.term();
        this.votedFor = hardState.votedFor();
        this.commitIndex = Math.max(hardState.commit(), snapshotIndex);
        this.lastApplied = snapshotIndex;
        resetElectionTimeout();
    }

    public static Builder builder(String id, RaftMembership membership, RaftOptions options, RaftLogReader log) {
        return new Builder(id, membership, options, log);
    }

    public RaftEffects step(RaftInput event) {
        var accumulator = new RaftEffectAccumulator();

        switch (event) {
            case RaftInput.Tick() -> tick(accumulator);
            case RaftInput.EntryProposed(var data) -> propose(data, accumulator);
            case RaftInput.MessageReceived(var from, var msg) -> receive(from, msg, accumulator);
            case RaftInput.ReadIndexRequested(var context) -> handleReadIndexEvent(context, accumulator);
            case RaftInput.MembershipChangeProposed(var change) -> proposeMembershipChange(change, accumulator);
            case RaftInput.EntriesPersisted(var index, var entryTerm) -> unstable.stableTo(index, entryTerm);
            case RaftInput.SnapshotPersisted() -> unstable.snapshotStabilized();
            case RaftInput.Applied(var index) -> lastApplied = Math.max(lastApplied, index);
            case RaftInput.ReplayCommitted() -> {}
        }

        prepareEntriesToApply(accumulator);

        return accumulator.finish();
    }

    public boolean isLeader() {
        return role == RaftRole.LEADER;
    }

    public RaftRole role() {
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

    public RaftMembership membership() {
        return membership;
    }

    public RaftStatus status() {
        return new RaftStatus(id, role, term, leaderId(), commitIndex, lastApplied, membership);
    }

    private long lastIndex() {
        if (unstable.hasEntries()) {
            return unstable.lastIndex();
        }
        return log.lastIndex();
    }

    private long lastTerm() {
        if (unstable.hasEntries()) {
            return unstable.lastTerm();
        }
        long last = log.lastIndex();
        return last > 0 ? log.term(last) : 0;
    }

    private long term(long index) {
        Long t = unstable.term(index);
        if (t != null) {
            return t;
        }
        return log.term(index);
    }

    private RaftLogEntry get(long index) {
        RaftLogEntry entry = unstable.get(index);
        if (entry != null) {
            return entry;
        }
        var entries = log.entries(index, index + 1, Long.MAX_VALUE);
        return entries.isEmpty() ? null : entries.getFirst();
    }

    private List<RaftLogEntry> slice(long from, long to) {
        if (from >= to) {
            return List.of();
        }

        var result = new ArrayList<RaftLogEntry>();
        long storageLastIndex = log.lastIndex();

        if (from <= storageLastIndex) {
            result.addAll(log.entries(from, Math.min(to, storageLastIndex + 1), Long.MAX_VALUE));
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
        long first = log.firstIndex();
        return first > 1 ? first - 1 : 0;
    }

    private long lastIncludedTerm() {
        long index = lastIncludedIndex();
        return index > 0 ? log.term(index) : 0;
    }

    private void appendEntry(RaftLogEntry entry) {
        unstable.append(entry);
    }

    private void tick(RaftEffectAccumulator accumulator) {
        tickCount++;
        leaderContactTicks++;
        switch (role) {
            case LEADER -> tickHeartbeat(accumulator);
            case FOLLOWER, PRE_CANDIDATE, CANDIDATE -> tickElection(accumulator);
        }
    }

    private void tickHeartbeat(RaftEffectAccumulator accumulator) {
        heartbeatTicks++;
        if (heartbeatTicks >= heartbeatInterval) {
            heartbeatTicks = 0;
            broadcastAppend(accumulator);
        }
        if (!hasRecentQuorumContact()) {
            becomeFollower(term, null, accumulator);
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

    private void tickElection(RaftEffectAccumulator accumulator) {
        if (!membership.isVoter(id)) {
            return;
        }
        electionTicks++;
        if (electionTicks >= electionTimeout) {
            switch (role) {
                case FOLLOWER, PRE_CANDIDATE -> startPreVote(accumulator);
                case CANDIDATE -> startElection(accumulator);
                case LEADER -> throw new IllegalStateException("Leader should not tick election");
            }
        }
    }

    private void propose(Bytes data, RaftEffectAccumulator accumulator) {
        if (role != RaftRole.LEADER) {
            return;
        }

        var entry = new RaftLogEntry.Data(lastIndex() + 1, term, data);
        appendEntry(entry);
        accumulator.persist(entry);

        if (quorum == 1) {
            commitIndex = entry.index();
            persistHardState(accumulator);
        } else {
            broadcastAppend(accumulator);
        }
    }

    private void proposeMembershipChange(MembershipChange change, RaftEffectAccumulator accumulator) {
        if (role != RaftRole.LEADER) {
            return;
        }

        if (hasPendingMembershipChange()) {
            return;
        }

        RaftMembership newConfiguration;
        try {
            newConfiguration = applyMembershipChange(change);
        } catch (IllegalArgumentException e) {
            return;
        }

        var entry = new RaftLogEntry.Config(lastIndex() + 1, term, newConfiguration);
        appendEntry(entry);
        accumulator.persist(entry);
        pendingMembershipChangeIndex = entry.index();

        if (quorum == 1) {
            commitIndex = entry.index();
            persistHardState(accumulator);
        } else {
            broadcastAppend(accumulator);
        }
    }

    private boolean hasPendingMembershipChange() {
        return pendingMembershipChangeIndex != null && pendingMembershipChangeIndex > commitIndex;
    }

    private RaftMembership applyMembershipChange(MembershipChange change) {
        return membership.apply(change);
    }

    private void replaceConfiguration(RaftMembership newConfiguration) {
        this.membership = newConfiguration;
        this.isLearner = newConfiguration.isLearner(id);
        this.quorum = newConfiguration.quorumSize();
        this.peers = newConfiguration.peerIdsExcluding(id);
        this.votingPeers = newConfiguration.votingPeerIdsExcluding(id);

        if (role == RaftRole.LEADER) {
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

    private void stepDownAfterMembershipChange() {
        role = RaftRole.FOLLOWER;
        leaderId = null;
        pendingReads = List.of();
        lastHeardFrom = null;
        resetElectionTimeout();
    }

    private void receive(String from, RaftMessage msg, RaftEffectAccumulator accumulator) {
        if (!membership.isMember(from)) {
            return;
        }

        boolean checkTerm = switch (msg) {
            case RaftMessage.PreVote _,
                 RaftMessage.PreVoteResponse _,
                 RaftMessage.ReadIndexRequested _,
                 RaftMessage.ReadIndexResponse _ -> false;
            default -> true;
        };

        if (checkTerm && msg.term() > term) {
            becomeFollower(msg.term(), null, accumulator);
        }

        switch (msg) {
            case RaftMessage.PreVote pv -> handlePreVote(from, pv, accumulator);
            case RaftMessage.PreVoteResponse pvr -> handlePreVoteResponse(from, pvr, accumulator);
            case RaftMessage.RequestVote rv -> handleRequestVote(from, rv, accumulator);
            case RaftMessage.RequestVoteResponse rvr -> handleRequestVoteResponse(from, rvr, accumulator);
            case RaftMessage.AppendEntries ae -> handleAppendEntries(from, ae, accumulator);
            case RaftMessage.AppendEntriesResponse aer -> handleAppendEntriesResponse(from, aer, accumulator);
            case RaftMessage.InstallSnapshot is -> handleInstallSnapshot(from, is, accumulator);
            case RaftMessage.InstallSnapshotResponse isr -> handleInstallSnapshotResponse(from, isr);
            case RaftMessage.ReadIndexRequested ri -> handleReadIndex(from, ri, accumulator);
            case RaftMessage.ReadIndexResponse rir -> handleReadIndexResponse(from, rir, accumulator);
        }
    }

    private void startPreVote(RaftEffectAccumulator accumulator) {
        if (!membership.isVoter(id)) {
            return;
        }
        if (quorum == 1) {
            startElection(accumulator);
            return;
        }

        role = RaftRole.PRE_CANDIDATE;
        leaderId = null;
        preVotesReceived = new HashSet<>();
        preVotesReceived.add(id);
        resetElectionTimeout();

        var request = new RaftMessage.PreVote(term + 1, id, lastIndex(), lastTerm());
        for (String peer : votingPeers) {
            accumulator.send(peer, request);
        }
    }

    private void startElection(RaftEffectAccumulator accumulator) {
        if (!membership.isVoter(id)) {
            return;
        }
        term++;
        votedFor = id;
        role = RaftRole.CANDIDATE;
        leaderId = null;
        votesReceived = new HashSet<>();
        votesReceived.add(id);
        resetElectionTimeout();

        persistHardState(accumulator);

        if (quorum == 1) {
            becomeLeader(accumulator);
            return;
        }

        var request = new RaftMessage.RequestVote(
            term, id, lastIndex(), lastTerm()
        );

        for (String peer : votingPeers) {
            accumulator.send(peer, request);
        }
    }

    private void becomeLeader(RaftEffectAccumulator accumulator) {
        role = RaftRole.LEADER;
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

        var noOp = new RaftLogEntry.NoOp(lastIndex() + 1, term);
        appendEntry(noOp);
        accumulator.persist(noOp);

        if (quorum == 1) {
            commitIndex = noOp.index();
            persistHardState(accumulator);
        } else {
            broadcastAppend(accumulator);
        }
    }

    private void becomeFollower(long newTerm, String leader, RaftEffectAccumulator accumulator) {
        boolean termChanged = newTerm > term;
        if (termChanged) {
            term = newTerm;
            votedFor = null;
            persistHardState(accumulator);
        }
        if (role != RaftRole.FOLLOWER || termChanged) {
            role = RaftRole.FOLLOWER;
            leaderId = leader;
            pendingReads = List.of();
            lastHeardFrom = null;
        } else if (leader != null) {
            leaderId = leader;
        }
        resetElectionTimeout();
    }

    private void handlePreVote(String from, RaftMessage.PreVote pv, RaftEffectAccumulator accumulator) {
        boolean voteGranted = false;

        if (pv.term() >= term) {
            boolean leaderActive = leaderContactTicks < electionTimeout;
            if (isLogUpToDate(pv.lastLogTerm(), pv.lastLogIndex()) && !leaderActive) {
                voteGranted = true;
            }
        }

        accumulator.send(from, new RaftMessage.PreVoteResponse(term, voteGranted));
    }

    private void handlePreVoteResponse(String from, RaftMessage.PreVoteResponse pvr, RaftEffectAccumulator accumulator) {
        if (role != RaftRole.PRE_CANDIDATE || pvr.term() < term) {
            return;
        }

        if (pvr.voteGranted() && membership.isVoter(from)) {
            preVotesReceived.add(from);
            if (preVotesReceived.size() >= quorum) {
                startElection(accumulator);
            }
        }
    }

    private void handleRequestVote(String from, RaftMessage.RequestVote rv, RaftEffectAccumulator accumulator) {
        boolean voteGranted = false;

        if (rv.term() >= term) {
            if (isLogUpToDate(rv.lastLogTerm(), rv.lastLogIndex()) &&
                (votedFor == null || votedFor.equals(from))) {
                voteGranted = true;
                votedFor = from;
                persistHardState(accumulator);
                resetElectionTimeout();
            }
        }

        accumulator.send(from, new RaftMessage.RequestVoteResponse(term, voteGranted));
    }

    private void handleRequestVoteResponse(String from, RaftMessage.RequestVoteResponse rvr, RaftEffectAccumulator accumulator) {
        if (role != RaftRole.CANDIDATE || rvr.term() != term) {
            return;
        }

        if (rvr.voteGranted() && membership.isVoter(from)) {
            votesReceived.add(from);
            if (votesReceived.size() >= quorum) {
                becomeLeader(accumulator);
            }
        }
    }

    private void handleAppendEntries(String from, RaftMessage.AppendEntries ae, RaftEffectAccumulator accumulator) {
        if (ae.term() < term) {
            accumulator.send(from, new RaftMessage.AppendEntriesResponse(term, false, 0));
            return;
        }

        leaderContactTicks = 0;
        becomeFollower(ae.term(), ae.leaderId(), accumulator);

        if (!hasEntryMatching(ae.prevLogIndex(), ae.prevLogTerm())) {
            accumulator.send(from, new RaftMessage.AppendEntriesResponse(term, false, 0));
            return;
        }

        long matchIdx = ae.prevLogIndex();
        for (RaftLogEntry entry : ae.entries()) {
            long existingTerm = term(entry.index());
            if (existingTerm != entry.term()) {
                appendEntry(entry);
                accumulator.persist(entry);
            }
            matchIdx = entry.index();
        }

        if (ae.leaderCommit() > commitIndex) {
            commitIndex = Math.min(ae.leaderCommit(), matchIdx);
            persistHardState(accumulator);
        }

        accumulator.send(from, new RaftMessage.AppendEntriesResponse(term, true, matchIdx));
    }

    private void handleAppendEntriesResponse(String from, RaftMessage.AppendEntriesResponse aer, RaftEffectAccumulator accumulator) {
        if (role != RaftRole.LEADER || aer.term() != term) {
            return;
        }

        lastHeardFrom.put(from, tickCount);

        if (aer.success()) {
            if (aer.matchIndex() > matchIndex.getOrDefault(from, 0L)) {
                nextIndex.put(from, aer.matchIndex() + 1);
                matchIndex.put(from, aer.matchIndex());
                advanceCommitIndex(accumulator);
            }
            checkPendingReads(from, accumulator);
        } else {
            long next = nextIndex.getOrDefault(from, 1L);
            nextIndex.put(from, Math.max(1, next - 1));
            sendAppend(from, accumulator);
        }
    }

    private void handleInstallSnapshot(String from, RaftMessage.InstallSnapshot is, RaftEffectAccumulator accumulator) {
        if (is.term() < term) {
            accumulator.send(from, new RaftMessage.InstallSnapshotResponse(term));
            return;
        }

        leaderContactTicks = 0;
        becomeFollower(is.term(), is.leaderId(), accumulator);

        if (is.lastIncludedIndex() <= commitIndex) {
            accumulator.send(from, new RaftMessage.InstallSnapshotResponse(term));
            return;
        }

        unstable.acceptSnapshot(new RaftSnapshot(
            is.lastIncludedIndex(),
            is.lastIncludedTerm(),
            is.membership(),
            is.data()
        ));
        commitIndex = is.lastIncludedIndex();
        lastApplied = is.lastIncludedIndex();
        persistHardState(accumulator);

        replaceConfiguration(is.membership());

        accumulator.setIncomingSnapshot(new RaftSnapshot(
            is.lastIncludedIndex(),
            is.lastIncludedTerm(),
            is.membership(),
            is.data()
        ));
        accumulator.send(from, new RaftMessage.InstallSnapshotResponse(term));
    }

    private void handleInstallSnapshotResponse(String from, RaftMessage.InstallSnapshotResponse isr) {
        if (role != RaftRole.LEADER || isr.term() != term) {
            return;
        }
        lastHeardFrom.put(from, tickCount);
        nextIndex.put(from, lastIncludedIndex() + 1);
        matchIndex.put(from, lastIncludedIndex());
    }

    private void handleReadIndexEvent(Bytes context, RaftEffectAccumulator accumulator) {
        if (role == RaftRole.LEADER) {
            startReadIndexConfirmation(null, context, accumulator);
        } else if (leaderId != null) {
            accumulator.send(leaderId, new RaftMessage.ReadIndexRequested(term, context));
        }
    }

    private void handleReadIndex(String from, RaftMessage.ReadIndexRequested ri, RaftEffectAccumulator accumulator) {
        if (role != RaftRole.LEADER) {
            accumulator.send(from, new RaftMessage.ReadIndexResponse(term, 0, ri.context()));
            return;
        }
        startReadIndexConfirmation(from, ri.context(), accumulator);
    }

    private void handleReadIndexResponse(String from, RaftMessage.ReadIndexResponse rir, RaftEffectAccumulator accumulator) {
        if (!membership.isVoter(from) || !Objects.equals(from, leaderId)) {
            return;
        }
        if (rir.term() > term) {
            becomeFollower(rir.term(), from, accumulator);
        }
        if (rir.term() != term) {
            return;
        }
        if (rir.readIndex() > 0) {
            accumulator.addReadState(rir.readIndex(), rir.context());
        }
    }

    private void startReadIndexConfirmation(String requester, Bytes context, RaftEffectAccumulator accumulator) {
        if (role != RaftRole.LEADER) {
            return;
        }

        if (quorum == 1) {
            if (requester == null) {
                accumulator.addReadState(commitIndex, context);
            } else {
                accumulator.send(requester, new RaftMessage.ReadIndexResponse(term, commitIndex, context));
            }
            return;
        }

        var acks = new HashSet<String>();
        acks.add(id);
        pendingReads.add(new PendingRead(commitIndex, context, requester, acks));
        broadcastAppend(accumulator);
    }

    private void checkPendingReads(String from, RaftEffectAccumulator accumulator) {
        if (pendingReads.isEmpty()) {
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
                    accumulator.addReadState(pending.index(), pending.context());
                } else {
                    accumulator.send(pending.requester(),
                        new RaftMessage.ReadIndexResponse(term, pending.index(), pending.context()));
                }
                it.remove();
            }
        }
    }

    private void broadcastAppend(RaftEffectAccumulator accumulator) {
        for (String peer : peers) {
            sendAppend(peer, accumulator);
        }
    }

    private void sendAppend(String peer, RaftEffectAccumulator accumulator) {
        long next = nextIndex.getOrDefault(peer, lastIndex() + 1);
        long prevIndex = next - 1;

        if (prevIndex < lastIncludedIndex()) {
            requestSnapshot(peer, accumulator);
            return;
        }

        long prevTerm = term(prevIndex);
        List<RaftLogEntry> entries = slice(next, next + maxEntriesPerAppend);

        accumulator.send(peer, new RaftMessage.AppendEntries(
            term, id, prevIndex, prevTerm, entries, commitIndex
        ));
    }

    private void requestSnapshot(String peer, RaftEffectAccumulator accumulator) {
        accumulator.setSnapshotNeeded(peer, lastIncludedIndex(), lastIncludedTerm());
    }

    private void advanceCommitIndex(RaftEffectAccumulator accumulator) {
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
            persistHardState(accumulator);
        }
    }

    private void prepareEntriesToApply(RaftEffectAccumulator accumulator) {
        for (long i = lastApplied + 1; i <= commitIndex; i++) {
            accumulator.advanceAppliedThrough(i);
            switch (get(i)) {
                case RaftLogEntry.Data entry -> accumulator.apply(entry.index(), entry.term(), entry.data());
                case RaftLogEntry.Config entry -> {
                    RaftMembership previous = membership;
                    replaceConfiguration(entry.membership());
                    accumulator.addMembershipTransition(entry.index(), previous, entry.membership());

                    if (pendingMembershipChangeIndex != null && pendingMembershipChangeIndex == entry.index()) {
                        pendingMembershipChangeIndex = null;
                    }

                    if (role == RaftRole.LEADER && !entry.membership().isVoter(id)) {
                        stepDownAfterMembershipChange();
                    }
                }
                case RaftLogEntry.NoOp _ -> {}
                case null -> throw new IllegalStateException(
                    "Missing log entry at index " + i + " (firstIndex=" + log.firstIndex() + ")"
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

    private void persistHardState(RaftEffectAccumulator accumulator) {
        accumulator.setHardState(new RaftHardState(term, votedFor, commitIndex));
    }

    public static final class Builder {
        private final String id;
        private final RaftMembership membership;
        private final RaftOptions options;
        private final RaftLogReader log;
        private IntUnaryOperator electionJitter = _ -> 0;
        private RaftHardState hardState = RaftHardState.INITIAL;
        private long snapshotIndex;

        private Builder(String id, RaftMembership membership, RaftOptions options, RaftLogReader log) {
            this.id = id;
            this.membership = membership;
            this.options = options;
            this.log = log;
        }

        public Builder electionJitter(IntUnaryOperator electionJitter) {
            this.electionJitter = Objects.requireNonNull(electionJitter, "electionJitter must not be null");
            return this;
        }

        public Builder restoredFrom(RaftHardState hardState, long snapshotIndex) {
            this.hardState = Objects.requireNonNull(hardState, "hardState must not be null");
            if (snapshotIndex < 0) {
                throw new IllegalArgumentException("snapshotIndex must be non-negative");
            }
            this.snapshotIndex = snapshotIndex;
            return this;
        }

        public RaftNode build() {
            return new RaftNode(id, membership, options, log, electionJitter, hardState, snapshotIndex);
        }
    }
}
