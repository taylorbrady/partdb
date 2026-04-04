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
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.IntUnaryOperator;

public final class Raft {

    private record PendingRead(long index, Bytes context, String requester, Set<String> acks) {}

    private final String id;
    private RaftConfiguration configuration;
    private Set<String> peers;
    private Set<String> votingPeers;
    private int quorum;
    private boolean isLearner;
    private Long pendingConfigurationChangeIndex;
    private final int electionTimeoutMin;
    private final int electionTimeoutMax;
    private final int heartbeatInterval;
    private final int maxEntriesPerAppend;
    private final IntUnaryOperator random;
    private final RaftLogView logView;
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

    private Raft(String id, RaftConfiguration configuration, RaftConfig config,
                 RaftLogView logView, IntUnaryOperator random) {
        if (!configuration.isMember(id)) {
            throw new IllegalArgumentException("Node must be member of cluster");
        }
        this.id = id;
        this.configuration = configuration;
        this.isLearner = configuration.isLearner(id);
        this.quorum = configuration.quorumSize();
        this.peers = configuration.peerIdsExcluding(id);
        this.votingPeers = configuration.votingPeerIdsExcluding(id);

        this.electionTimeoutMin = config.electionTimeoutMin();
        this.electionTimeoutMax = config.electionTimeoutMax();
        this.heartbeatInterval = config.heartbeatInterval();
        this.maxEntriesPerAppend = config.maxEntriesPerAppend();
        this.random = random;
        this.logView = logView;
        this.unstable = new UnstableLog(logView.lastIndex() + 1);
        this.role = RaftRole.FOLLOWER;
        this.term = 0;
        this.commitIndex = 0;
        this.lastApplied = 0;
        resetElectionTimeout();
    }

    public static Builder builder(String id, RaftConfiguration configuration, RaftConfig config, RaftLogView logView) {
        return new Builder(id, configuration, config, logView);
    }

    public RaftReady step(RaftEvent event) {
        var accumulator = new RaftStepAccumulator();

        switch (event) {
            case RaftEvent.Tick() -> tick(accumulator);
            case RaftEvent.Propose(var data) -> propose(data, accumulator);
            case RaftEvent.Receive(var from, var msg) -> receive(from, msg, accumulator);
            case RaftEvent.ReadIndex(var context) -> handleReadIndexEvent(context, accumulator);
            case RaftEvent.ChangeConfiguration(var change) -> proposeConfigurationChange(change, accumulator);
        }

        prepareEntriesToApply(accumulator);

        return accumulator.finish();
    }

    public void acknowledgeEntryPersistence(long persistedIndex, long persistedTerm) {
        unstable.stableTo(persistedIndex, persistedTerm);
    }

    public void acknowledgeSnapshotPersistence() {
        unstable.snapshotStabilized();
    }

    public void acknowledgeApplication(long appliedIndex) {
        lastApplied = Math.max(lastApplied, appliedIndex);
    }

    public void restore(RaftPersistentState hardState, long snapIndex) {
        this.term = hardState.term();
        this.votedFor = hardState.votedFor();
        this.commitIndex = Math.max(hardState.commit(), snapIndex);
        this.lastApplied = snapIndex;
    }

    public RaftReady.Application recoverCommittedApplication() {
        if (lastApplied >= commitIndex) {
            return RaftReady.Application.EMPTY;
        }

        var accumulator = new RaftStepAccumulator();
        prepareEntriesToApply(accumulator);
        lastApplied = commitIndex;
        return accumulator.finish().application();
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

    public RaftConfiguration configuration() {
        return configuration;
    }

    private long lastIndex() {
        if (unstable.hasEntries()) {
            return unstable.lastIndex();
        }
        return logView.lastIndex();
    }

    private long lastTerm() {
        if (unstable.hasEntries()) {
            return unstable.lastTerm();
        }
        long last = logView.lastIndex();
        return last > 0 ? logView.term(last) : 0;
    }

    private long term(long index) {
        Long t = unstable.term(index);
        if (t != null) {
            return t;
        }
        return logView.term(index);
    }

    private LogEntry get(long index) {
        LogEntry entry = unstable.get(index);
        if (entry != null) {
            return entry;
        }
        var entries = logView.entries(index, index + 1, Long.MAX_VALUE);
        return entries.isEmpty() ? null : entries.getFirst();
    }

    private List<LogEntry> slice(long from, long to) {
        if (from >= to) {
            return List.of();
        }

        var result = new ArrayList<LogEntry>();
        long storageLastIndex = logView.lastIndex();

        if (from <= storageLastIndex) {
            result.addAll(logView.entries(from, Math.min(to, storageLastIndex + 1), Long.MAX_VALUE));
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
        long first = logView.firstIndex();
        return first > 1 ? first - 1 : 0;
    }

    private long lastIncludedTerm() {
        long index = lastIncludedIndex();
        return index > 0 ? logView.term(index) : 0;
    }

    private void appendEntry(LogEntry entry) {
        unstable.append(entry);
    }

    private void tick(RaftStepAccumulator accumulator) {
        tickCount++;
        leaderContactTicks++;
        switch (role) {
            case LEADER -> tickHeartbeat(accumulator);
            case FOLLOWER, PRE_CANDIDATE, CANDIDATE -> tickElection(accumulator);
        }
    }

    private void tickHeartbeat(RaftStepAccumulator accumulator) {
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

    private void tickElection(RaftStepAccumulator accumulator) {
        if (!configuration.isVoter(id)) {
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

    private void propose(Bytes data, RaftStepAccumulator accumulator) {
        if (role != RaftRole.LEADER) {
            return;
        }

        var entry = new LogEntry.Data(lastIndex() + 1, term, data);
        appendEntry(entry);
        accumulator.persist(entry);

        if (quorum == 1) {
            commitIndex = entry.index();
            persistHardState(accumulator);
        } else {
            broadcastAppend(accumulator);
        }
    }

    private void proposeConfigurationChange(ConfigurationChange change, RaftStepAccumulator accumulator) {
        if (role != RaftRole.LEADER) {
            return;
        }

        if (hasPendingConfigurationChange()) {
            return;
        }

        RaftConfiguration newConfiguration;
        try {
            newConfiguration = applyConfigurationChange(change);
        } catch (IllegalArgumentException e) {
            return;
        }

        var entry = new LogEntry.Config(lastIndex() + 1, term, newConfiguration);
        appendEntry(entry);
        accumulator.persist(entry);
        pendingConfigurationChangeIndex = entry.index();

        if (quorum == 1) {
            commitIndex = entry.index();
            persistHardState(accumulator);
        } else {
            broadcastAppend(accumulator);
        }
    }

    private boolean hasPendingConfigurationChange() {
        return pendingConfigurationChangeIndex != null && pendingConfigurationChangeIndex > commitIndex;
    }

    private RaftConfiguration applyConfigurationChange(ConfigurationChange change) {
        return configuration.apply(change);
    }

    private void replaceConfiguration(RaftConfiguration newConfiguration) {
        this.configuration = newConfiguration;
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

    private void stepDownAfterConfigurationChange() {
        role = RaftRole.FOLLOWER;
        leaderId = null;
        pendingReads = List.of();
        lastHeardFrom = null;
        resetElectionTimeout();
    }

    private void receive(String from, RaftMessage msg, RaftStepAccumulator accumulator) {
        if (!configuration.isMember(from)) {
            return;
        }

        boolean checkTerm = switch (msg) {
            case RaftMessage.PreVote _,
                 RaftMessage.PreVoteResponse _,
                 RaftMessage.ReadIndex _,
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
            case RaftMessage.ReadIndex ri -> handleReadIndex(from, ri, accumulator);
            case RaftMessage.ReadIndexResponse rir -> handleReadIndexResponse(from, rir, accumulator);
        }
    }

    private void startPreVote(RaftStepAccumulator accumulator) {
        if (!configuration.isVoter(id)) {
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

    private void startElection(RaftStepAccumulator accumulator) {
        if (!configuration.isVoter(id)) {
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

    private void becomeLeader(RaftStepAccumulator accumulator) {
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

        var noOp = new LogEntry.NoOp(lastIndex() + 1, term);
        appendEntry(noOp);
        accumulator.persist(noOp);

        if (quorum == 1) {
            commitIndex = noOp.index();
            persistHardState(accumulator);
        } else {
            broadcastAppend(accumulator);
        }
    }

    private void becomeFollower(long newTerm, String leader, RaftStepAccumulator accumulator) {
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

    private void handlePreVote(String from, RaftMessage.PreVote pv, RaftStepAccumulator accumulator) {
        boolean voteGranted = false;

        if (pv.term() >= term) {
            boolean leaderActive = leaderContactTicks < electionTimeout;
            if (isLogUpToDate(pv.lastLogTerm(), pv.lastLogIndex()) && !leaderActive) {
                voteGranted = true;
            }
        }

        accumulator.send(from, new RaftMessage.PreVoteResponse(term, voteGranted));
    }

    private void handlePreVoteResponse(String from, RaftMessage.PreVoteResponse pvr, RaftStepAccumulator accumulator) {
        if (role != RaftRole.PRE_CANDIDATE || pvr.term() < term) {
            return;
        }

        if (pvr.voteGranted() && configuration.isVoter(from)) {
            preVotesReceived.add(from);
            if (preVotesReceived.size() >= quorum) {
                startElection(accumulator);
            }
        }
    }

    private void handleRequestVote(String from, RaftMessage.RequestVote rv, RaftStepAccumulator accumulator) {
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

    private void handleRequestVoteResponse(String from, RaftMessage.RequestVoteResponse rvr, RaftStepAccumulator accumulator) {
        if (role != RaftRole.CANDIDATE || rvr.term() != term) {
            return;
        }

        if (rvr.voteGranted() && configuration.isVoter(from)) {
            votesReceived.add(from);
            if (votesReceived.size() >= quorum) {
                becomeLeader(accumulator);
            }
        }
    }

    private void handleAppendEntries(String from, RaftMessage.AppendEntries ae, RaftStepAccumulator accumulator) {
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
        for (LogEntry entry : ae.entries()) {
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

    private void handleAppendEntriesResponse(String from, RaftMessage.AppendEntriesResponse aer, RaftStepAccumulator accumulator) {
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

    private void handleInstallSnapshot(String from, RaftMessage.InstallSnapshot is, RaftStepAccumulator accumulator) {
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
            is.configuration(),
            is.data()
        ));
        commitIndex = is.lastIncludedIndex();
        lastApplied = is.lastIncludedIndex();
        persistHardState(accumulator);

        replaceConfiguration(is.configuration());

        accumulator.setIncomingSnapshot(new RaftSnapshot(
            is.lastIncludedIndex(),
            is.lastIncludedTerm(),
            is.configuration(),
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

    private void handleReadIndexEvent(Bytes context, RaftStepAccumulator accumulator) {
        if (role == RaftRole.LEADER) {
            startReadIndexConfirmation(null, context, accumulator);
        } else if (leaderId != null) {
            accumulator.send(leaderId, new RaftMessage.ReadIndex(term, context));
        }
    }

    private void handleReadIndex(String from, RaftMessage.ReadIndex ri, RaftStepAccumulator accumulator) {
        if (role != RaftRole.LEADER) {
            accumulator.send(from, new RaftMessage.ReadIndexResponse(term, 0, ri.context()));
            return;
        }
        startReadIndexConfirmation(from, ri.context(), accumulator);
    }

    private void handleReadIndexResponse(String from, RaftMessage.ReadIndexResponse rir, RaftStepAccumulator accumulator) {
        if (!configuration.isVoter(from) || !Objects.equals(from, leaderId)) {
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

    private void startReadIndexConfirmation(String requester, Bytes context, RaftStepAccumulator accumulator) {
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

    private void checkPendingReads(String from, RaftStepAccumulator accumulator) {
        if (pendingReads.isEmpty()) {
            return;
        }

        if (!configuration.isVoter(from)) {
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

    private void broadcastAppend(RaftStepAccumulator accumulator) {
        for (String peer : peers) {
            sendAppend(peer, accumulator);
        }
    }

    private void sendAppend(String peer, RaftStepAccumulator accumulator) {
        long next = nextIndex.getOrDefault(peer, lastIndex() + 1);
        long prevIndex = next - 1;

        if (prevIndex < lastIncludedIndex()) {
            requestSnapshotTransfer(peer, accumulator);
            return;
        }

        long prevTerm = term(prevIndex);
        List<LogEntry> entries = slice(next, next + maxEntriesPerAppend);

        accumulator.send(peer, new RaftMessage.AppendEntries(
            term, id, prevIndex, prevTerm, entries, commitIndex
        ));
    }

    private void requestSnapshotTransfer(String peer, RaftStepAccumulator accumulator) {
        accumulator.setSnapshotTransfer(peer, lastIncludedIndex(), lastIncludedTerm());
    }

    private void advanceCommitIndex(RaftStepAccumulator accumulator) {
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

    private void prepareEntriesToApply(RaftStepAccumulator accumulator) {
        for (long i = lastApplied + 1; i <= commitIndex; i++) {
            accumulator.advanceAppliedThrough(i);
            switch (get(i)) {
                case LogEntry.Data entry -> accumulator.apply(entry.index(), entry.term(), entry.data());
                case LogEntry.Config entry -> {
                    RaftConfiguration previous = configuration;
                    replaceConfiguration(entry.configuration());
                    accumulator.addConfigurationTransition(entry.index(), previous, entry.configuration());

                    if (pendingConfigurationChangeIndex != null && pendingConfigurationChangeIndex == entry.index()) {
                        pendingConfigurationChangeIndex = null;
                    }

                    if (role == RaftRole.LEADER && !entry.configuration().isVoter(id)) {
                        stepDownAfterConfigurationChange();
                    }
                }
                case LogEntry.NoOp _ -> {}
                case null -> throw new IllegalStateException(
                    "Missing log entry at index " + i + " (firstIndex=" + logView.firstIndex() + ")"
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

    private void persistHardState(RaftStepAccumulator accumulator) {
        accumulator.setPersistentState(new RaftPersistentState(term, votedFor, commitIndex));
    }

    public static final class Builder {
        private final String id;
        private final RaftConfiguration configuration;
        private final RaftConfig config;
        private final RaftLogView logView;
        private IntUnaryOperator random = bound -> ThreadLocalRandom.current().nextInt(bound);

        private Builder(String id, RaftConfiguration configuration, RaftConfig config, RaftLogView logView) {
            this.id = id;
            this.configuration = configuration;
            this.config = config;
            this.logView = logView;
        }

        Builder random(IntUnaryOperator random) {
            this.random = random;
            return this;
        }

        public Raft build() {
            return new Raft(id, configuration, config, logView, random);
        }
    }
}
