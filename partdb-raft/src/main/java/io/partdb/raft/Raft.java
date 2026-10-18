package io.partdb.raft;

import io.partdb.bytes.Bytes;
import io.partdb.raft.Message.AppendAccepted;
import io.partdb.raft.Message.AppendEntries;
import io.partdb.raft.Message.AppendRejected;
import io.partdb.raft.Message.Heartbeat;
import io.partdb.raft.Message.HeartbeatResponse;
import io.partdb.raft.Message.PreVote;
import io.partdb.raft.Message.PreVoteResponse;
import io.partdb.raft.Message.ReadIndexRequest;
import io.partdb.raft.Message.ReadIndexResponse;
import io.partdb.raft.Message.RequestVote;
import io.partdb.raft.Message.RequestVoteResponse;
import io.partdb.raft.Message.SnapshotInstalled;
import io.partdb.raft.Message.TimeoutNow;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.random.RandomGenerator;

public final class Raft {

    private sealed interface RoleState permits Follower, PreCandidate, Candidate, Leader {}

    private record Follower() implements RoleState {}

    private record PreCandidate(Map<NodeId, Boolean> votes) implements RoleState {}

    private record Candidate(Map<NodeId, Boolean> votes) implements RoleState {}

    private record PendingRead(NodeId requester, long id, long index, long round, Set<NodeId> acks) {}

    private record Transfer(long id, NodeId target, long startedAt) {}

    private static final class Leader implements RoleState {
        private final Map<NodeId, Progress> progress = new TreeMap<>();
        private final List<PendingRead> pendingReads = new ArrayList<>();
        private final List<PendingRead> deferredReads = new ArrayList<>();
        private final Set<NodeId> forcedAppends = new TreeSet<>();
        private long readRound;
        private boolean readRoundStarted;
        private boolean heartbeatDue;
        private boolean commitAdvanced;
    }

    private final NodeId id;
    private final Options options;
    private final StableLog log;
    private final RandomGenerator random;
    private final UnstableLog unstable;
    private final Map<Long, Long> forwardedReads = new LinkedHashMap<>();
    private final NavigableMap<Long, Membership> configurations = new TreeMap<>();

    private final List<Envelope> messages = new ArrayList<>();
    private final List<Outcome> outcomes = new ArrayList<>();
    private final Set<NodeId> snapshotRequests = new TreeSet<>();

    private Membership membership;
    private RoleState role = new Follower();
    private long term;
    private NodeId votedFor;
    private NodeId leaderId;
    private Transfer transfer;
    private long commitIndex;
    private long appliedIndex;
    private long ticks;
    private long electionElapsed;
    private long heartbeatElapsed;
    private long electionTimeout;
    private HardState lastHardState;

    private Raft(
        NodeId id,
        Membership membership,
        Options options,
        StableLog log,
        HardState hardState,
        long applied,
        RandomGenerator random
    ) {
        this.id = Objects.requireNonNull(id, "id must not be null");
        this.options = Objects.requireNonNull(options, "options must not be null");
        this.log = Objects.requireNonNull(log, "log must not be null");
        this.random = Objects.requireNonNull(random, "random must not be null");
        Objects.requireNonNull(membership, "membership must not be null");
        Objects.requireNonNull(hardState, "hardState must not be null");

        long snapshotIndex = log.firstIndex() - 1;
        if (applied < snapshotIndex || applied > log.lastIndex()) {
            throw new IllegalArgumentException(
                "applied index " + applied + " is outside the log [" + snapshotIndex + ", " + log.lastIndex() + "]");
        }
        if (hardState.commit() > log.lastIndex()) {
            throw new IllegalArgumentException("commit index " + hardState.commit() + " is beyond the log end " + log.lastIndex());
        }

        this.unstable = new UnstableLog(log.lastIndex() + 1);
        this.term = hardState.term();
        this.votedFor = hardState.votedFor().orElse(null);
        this.appliedIndex = applied;
        this.commitIndex = Math.max(hardState.commit(), applied);
        recoverConfigurations(membership, snapshotIndex);
        this.lastHardState = hardState;
        resetElectionTimeout();
    }

    public static Raft recover(
        NodeId id,
        Membership membership,
        Options options,
        StableLog log,
        HardState hardState,
        long applied,
        RandomGenerator random
    ) {
        return new Raft(id, membership, options, log, hardState, applied, random);
    }

    public NodeId id() {
        return id;
    }

    public Role role() {
        return switch (role) {
            case Follower _ -> Role.FOLLOWER;
            case PreCandidate _ -> Role.PRE_CANDIDATE;
            case Candidate _ -> Role.CANDIDATE;
            case Leader _ -> Role.LEADER;
        };
    }

    public long term() {
        return term;
    }

    public Optional<NodeId> leaderId() {
        return Optional.ofNullable(leaderId);
    }

    public long commitIndex() {
        return commitIndex;
    }

    public long appliedIndex() {
        return appliedIndex;
    }

    public long lastIndex() {
        return unstable.lastIndex().orElseGet(log::lastIndex);
    }

    public Membership membership() {
        return membership;
    }

    public Membership membershipAt(long index) {
        var configuration = configurations.floorEntry(index);
        if (configuration == null) {
            throw new IllegalArgumentException("membership at " + index + " precedes the log start " + configurations.firstKey());
        }
        return configuration.getValue();
    }

    public void step(Input input) {
        switch (input) {
            case Input.Tick() -> tick();
            case Input.Propose(long request, Bytes command) -> propose(request, command);
            case Input.ReadIndex(long request) -> readIndex(request);
            case Input.ChangeMembership(long request, MembershipChange change) -> changeMembership(request, change);
            case Input.TransferLeadership(long request, NodeId target) -> transferLeadership(request, target);
            case Input.StepDown(long request) -> stepDown(request);
            case Input.Receive(NodeId from, Message message) -> receive(from, message);
            case Input.InstallSnapshot(NodeId from, long snapshotTerm, SnapshotMetadata snapshot) ->
                installSnapshot(from, snapshotTerm, snapshot);
            case Input.Persisted(long index, long entryTerm) -> persisted(index, entryTerm);
            case Input.SnapshotSent(NodeId peer) -> snapshotSent(peer);
            case Input.SnapshotFailed(NodeId peer) -> snapshotFailed(peer);
            case Input.Unreachable(NodeId peer) -> unreachable(peer);
        }
    }

    public Ready ready() {
        var committed = takeCommitted();
        if (role instanceof Leader leader) {
            flush(leader);
        }
        pruneConfigurations();

        var hardState = new HardState(term, Optional.ofNullable(votedFor), commitIndex);
        var changedHardState = hardState.equals(lastHardState) ? Optional.<HardState>empty() : Optional.of(hardState);
        lastHardState = hardState;

        var ready = new Ready(
            changedHardState,
            unstable.takeSnapshot(),
            unstable.takeEntries(),
            messages,
            committed,
            outcomes,
            snapshotRequests
        );
        messages.clear();
        outcomes.clear();
        snapshotRequests.clear();
        return ready;
    }

    private void recoverConfigurations(Membership base, long snapshotIndex) {
        configurations.put(snapshotIndex, base);
        if (log.lastIndex() > snapshotIndex) {
            for (LogEntry entry : log.entries(snapshotIndex + 1, log.lastIndex() + 1, Long.MAX_VALUE)) {
                if (entry instanceof LogEntry.Configuration(long index, _, var next)) {
                    configurations.put(index, next);
                }
            }
        }
        membership = configurations.lastEntry().getValue();
    }

    private void pruneConfigurations() {
        Long base = configurations.floorKey(firstIndex() - 1);
        if (base != null) {
            configurations.headMap(base, false).clear();
        }
    }

    private void append(LogEntry entry) {
        unstable.append(entry);
        configurations.tailMap(entry.index(), true).clear();
        if (entry instanceof LogEntry.Configuration(long index, _, var next)) {
            configurations.put(index, next);
        }
        refreshMembership();
    }

    private void refreshMembership() {
        var current = configurations.lastEntry().getValue();
        if (current.equals(membership)) {
            return;
        }
        membership = current;
        if (transfer != null && !current.isVoter(transfer.target())) {
            finishTransfer(new Rejection.Aborted());
        }
        if (!(role instanceof Leader leader)) {
            return;
        }
        long nextIndex = lastIndex() + 1;
        leader.progress.keySet().removeIf(member -> !member.equals(id) && !current.isMember(member));
        for (NodeId member : current.members()) {
            leader.progress.computeIfAbsent(member, _ -> new Progress(nextIndex, options.maxInflightAppends()));
        }
        maybeCommit(leader);
    }

    private boolean membershipPending() {
        return configurations.lastKey() > commitIndex;
    }

    private void tick() {
        ticks++;
        expireForwardedReads();
        if (transfer != null && ticks - transfer.startedAt() >= 2L * options.electionTicks()) {
            finishTransfer(new Rejection.Aborted());
        }
        if (role instanceof Leader leader) {
            tickLeader(leader);
        } else {
            tickElection();
        }
    }

    private void tickElection() {
        electionElapsed++;
        if (electionElapsed >= electionTimeout && promotable()) {
            campaign(true, false);
        }
    }

    private void tickLeader(Leader leader) {
        heartbeatElapsed++;
        electionElapsed++;
        if (electionElapsed >= options.electionTicks()) {
            electionElapsed = 0;
            if (!quorumActive(leader)) {
                becomeFollower(term, null);
                return;
            }
            leader.progress.forEach((member, progress) -> {
                if (!member.equals(id)) {
                    progress.resetActive();
                }
            });
            if (transfer != null) {
                finishTransfer(new Rejection.Aborted());
            }
        }
        if (heartbeatElapsed >= options.heartbeatTicks()) {
            heartbeatElapsed = 0;
            leader.heartbeatDue = true;
        }
    }

    private boolean quorumActive(Leader leader) {
        long active = membership.voters().stream()
            .filter(voter -> voter.equals(id) || isActive(leader, voter))
            .count();
        return active >= membership.quorum();
    }

    private static boolean isActive(Leader leader, NodeId member) {
        var progress = leader.progress.get(member);
        return progress != null && progress.recentActive();
    }

    private void campaign(boolean preVote, boolean leadershipTransfer) {
        changeLeader(null);
        electionElapsed = 0;
        resetElectionTimeout();

        var votes = new TreeMap<NodeId, Boolean>();
        votes.put(id, true);
        long voteTerm;
        if (preVote) {
            role = new PreCandidate(votes);
            voteTerm = term + 1;
        } else {
            term++;
            votedFor = id;
            role = new Candidate(votes);
            voteTerm = term;
        }

        if (tally(votes) == VoteResult.WON) {
            if (preVote) {
                campaign(false, leadershipTransfer);
            } else {
                becomeLeader();
            }
            return;
        }

        long lastIndex = lastIndex();
        long lastTerm = termOf(lastIndex);
        for (NodeId voter : membership.voters()) {
            if (!voter.equals(id)) {
                send(voter, preVote
                    ? new PreVote(voteTerm, lastIndex, lastTerm)
                    : new RequestVote(voteTerm, lastIndex, lastTerm, leadershipTransfer));
            }
        }
    }

    private enum VoteResult { WON, LOST, PENDING }

    private VoteResult tally(Map<NodeId, Boolean> votes) {
        long granted = 0;
        long rejected = 0;
        for (NodeId voter : membership.voters()) {
            Boolean vote = votes.get(voter);
            if (vote == null) {
                continue;
            }
            if (vote) {
                granted++;
            } else {
                rejected++;
            }
        }
        if (granted >= membership.quorum()) {
            return VoteResult.WON;
        }
        if (rejected >= membership.quorum()) {
            return VoteResult.LOST;
        }
        return VoteResult.PENDING;
    }

    private void becomeFollower(long newTerm, NodeId leader) {
        if (role instanceof Leader current) {
            rejectLeaderReads(current, Optional.ofNullable(leader));
        }
        if (newTerm > term) {
            term = newTerm;
            votedFor = null;
        }
        role = new Follower();
        changeLeader(leader);
        electionElapsed = 0;
        resetElectionTimeout();
    }

    private void becomeLeader() {
        var leader = new Leader();
        long next = lastIndex() + 1;
        for (NodeId member : membership.members()) {
            leader.progress.put(member, new Progress(next, options.maxInflightAppends()));
        }
        var self = leader.progress.computeIfAbsent(id, _ -> new Progress(next, options.maxInflightAppends()));
        self.maybeUpdate(unstable.offset() - 1);
        self.markActive();

        role = leader;
        changeLeader(id);
        electionElapsed = 0;
        heartbeatElapsed = 0;
        append(new LogEntry.NoOp(next, term));
    }

    private void changeLeader(NodeId leader) {
        if (!Objects.equals(leaderId, leader)) {
            rejectForwardedReads(Optional.ofNullable(leader));
        }
        leaderId = leader;
        if (transfer != null && leader != null) {
            if (leader.equals(transfer.target())) {
                finishTransfer(null);
            } else if (leader.equals(id)) {
                finishTransfer(new Rejection.Aborted());
            } else {
                finishTransfer(new Rejection.NotLeader(Optional.of(leader)));
            }
        }
    }

    private void finishTransfer(Rejection rejection) {
        outcomes.add(rejection == null ? new Outcome.Transferred(transfer.id()) : new Outcome.Rejected(transfer.id(), rejection));
        transfer = null;
    }

    private void resetElectionTimeout() {
        electionTimeout = options.electionTicks() + random.nextInt(options.electionTicks());
    }

    private boolean inLease() {
        return leaderId != null && electionElapsed < options.electionTicks();
    }

    private void receive(NodeId from, Message message) {
        if (message.term() > term) {
            boolean voteRequest = message instanceof PreVote || message instanceof RequestVote;
            boolean transferVote = message instanceof RequestVote vote && vote.leadershipTransfer();
            if (voteRequest && !transferVote && inLease()) {
                if (message instanceof PreVote) {
                    send(from, new PreVoteResponse(term, false));
                }
                return;
            }
            boolean keepTerm = message instanceof PreVote || message instanceof PreVoteResponse response && response.granted();
            if (!keepTerm) {
                becomeFollower(message.term(), message instanceof AppendEntries || message instanceof Heartbeat ? from : null);
            }
        } else if (message.term() < term) {
            switch (message) {
                case AppendEntries _, Heartbeat _ -> send(from, new HeartbeatResponse(term, 0));
                case PreVote _ -> send(from, new PreVoteResponse(term, false));
                default -> {}
            }
            return;
        }

        switch (message) {
            case PreVote vote -> handlePreVote(from, vote);
            case PreVoteResponse response -> handlePreVoteResponse(from, response);
            case RequestVote vote -> handleRequestVote(from, vote);
            case RequestVoteResponse response -> handleRequestVoteResponse(from, response);
            case AppendEntries append -> handleAppendEntries(from, append);
            case Heartbeat heartbeat -> handleHeartbeat(from, heartbeat);
            case ReadIndexRequest read -> handleForwardedRead(from, read);
            case ReadIndexResponse response -> handleReadIndexResponse(from, response);
            case TimeoutNow _ -> handleTimeoutNow(from);
            case AppendAccepted accepted -> {
                if (role instanceof Leader leader) {
                    handleAppendAccepted(leader, from, accepted);
                }
            }
            case AppendRejected rejected -> {
                if (role instanceof Leader leader) {
                    handleAppendRejected(leader, from, rejected);
                }
            }
            case HeartbeatResponse response -> {
                if (role instanceof Leader leader) {
                    handleHeartbeatResponse(leader, from, response);
                }
            }
            case SnapshotInstalled installed -> {
                if (role instanceof Leader leader) {
                    handleSnapshotInstalled(leader, from, installed);
                }
            }
        }
    }

    private void handlePreVote(NodeId from, PreVote vote) {
        boolean granted = vote.term() > term && isUpToDate(vote.lastLogIndex(), vote.lastLogTerm());
        send(from, new PreVoteResponse(granted ? vote.term() : term, granted));
    }

    private void handlePreVoteResponse(NodeId from, PreVoteResponse response) {
        if (!(role instanceof PreCandidate(var votes)) || (response.granted() && response.term() != term + 1)) {
            return;
        }
        votes.put(from, response.granted());
        switch (tally(votes)) {
            case WON -> campaign(false, false);
            case LOST -> becomeFollower(term, null);
            case PENDING -> {}
        }
    }

    private void handleRequestVote(NodeId from, RequestVote vote) {
        boolean canVote = from.equals(votedFor) || (votedFor == null && leaderId == null);
        boolean granted = canVote && isUpToDate(vote.lastLogIndex(), vote.lastLogTerm());
        if (granted) {
            votedFor = from;
            electionElapsed = 0;
        }
        send(from, new RequestVoteResponse(term, granted));
    }

    private void handleRequestVoteResponse(NodeId from, RequestVoteResponse response) {
        if (!(role instanceof Candidate(var votes))) {
            return;
        }
        votes.put(from, response.granted());
        switch (tally(votes)) {
            case WON -> becomeLeader();
            case LOST -> becomeFollower(term, null);
            case PENDING -> {}
        }
    }

    private boolean isUpToDate(long candidateLastIndex, long candidateLastTerm) {
        long lastIndex = lastIndex();
        long lastTerm = termOf(lastIndex);
        return candidateLastTerm > lastTerm || (candidateLastTerm == lastTerm && candidateLastIndex >= lastIndex);
    }

    private void followLeader(NodeId from) {
        if (!(role instanceof Follower) || !from.equals(leaderId)) {
            becomeFollower(term, from);
        }
        electionElapsed = 0;
    }

    private void handleAppendEntries(NodeId from, AppendEntries append) {
        if (role instanceof Leader) {
            return;
        }
        followLeader(from);

        if (append.prevLogIndex() < commitIndex) {
            send(from, new AppendAccepted(term, commitIndex));
            return;
        }
        if (!matchesTerm(append.prevLogIndex(), append.prevLogTerm())) {
            long hintIndex = Math.min(append.prevLogIndex(), lastIndex());
            long conflict = conflictIndex(hintIndex, append.prevLogTerm());
            send(from, new AppendRejected(term, append.prevLogIndex(), conflict, termOf(conflict)));
            return;
        }

        long lastNewIndex = append.prevLogIndex() + append.entries().size();
        boolean conflicted = false;
        for (LogEntry entry : append.entries()) {
            if (conflicted || !matchesTerm(entry.index(), entry.term())) {
                conflicted = true;
                append(entry);
            }
        }
        commitTo(Math.min(append.commitIndex(), lastNewIndex));
        send(from, new AppendAccepted(term, lastNewIndex));
    }

    private void handleHeartbeat(NodeId from, Heartbeat heartbeat) {
        if (role instanceof Leader) {
            return;
        }
        followLeader(from);
        commitTo(Math.min(heartbeat.commitIndex(), lastIndex()));
        send(from, new HeartbeatResponse(term, heartbeat.round()));
    }

    private void installSnapshot(NodeId from, long snapshotTerm, SnapshotMetadata snapshot) {
        if (snapshotTerm < term) {
            send(from, new HeartbeatResponse(term, 0));
            return;
        }
        if (snapshotTerm > term) {
            becomeFollower(snapshotTerm, from);
        }
        if (role instanceof Leader) {
            return;
        }
        followLeader(from);

        if (snapshot.index() <= commitIndex) {
            send(from, new SnapshotInstalled(term, commitIndex));
            return;
        }
        if (matchesTerm(snapshot.index(), snapshot.term())) {
            commitTo(snapshot.index());
            send(from, new SnapshotInstalled(term, commitIndex));
            return;
        }

        unstable.restore(snapshot);
        commitIndex = snapshot.index();
        appliedIndex = snapshot.index();
        configurations.clear();
        configurations.put(snapshot.index(), snapshot.membership());
        refreshMembership();
        send(from, new SnapshotInstalled(term, snapshot.index()));
    }

    private boolean promotable() {
        return membership.isVoter(id) || membershipAt(commitIndex).isVoter(id);
    }

    private void handleTimeoutNow(NodeId from) {
        if (from.equals(leaderId) && promotable()) {
            campaign(false, true);
        }
    }

    private void commitTo(long index) {
        if (index > commitIndex) {
            commitIndex = index;
        }
    }

    private void handleAppendAccepted(Leader leader, NodeId from, AppendAccepted accepted) {
        var progress = leader.progress.get(from);
        if (progress == null) {
            return;
        }
        progress.markActive();
        boolean updated = progress.maybeUpdate(accepted.matchIndex());
        switch (progress.state()) {
            case PROBE -> progress.becomeReplicate();
            case REPLICATE -> progress.acknowledged(accepted.matchIndex());
            case SNAPSHOT -> {
                if (progress.match() >= progress.pendingSnapshot()) {
                    progress.becomeProbe();
                    progress.becomeReplicate();
                }
            }
        }
        if (updated) {
            maybeCommit(leader);
        }
        if (transfer != null && from.equals(transfer.target()) && progress.match() == lastIndex()) {
            send(from, new TimeoutNow(term));
        }
    }

    private void handleAppendRejected(Leader leader, NodeId from, AppendRejected rejected) {
        var progress = leader.progress.get(from);
        if (progress == null) {
            return;
        }
        progress.markActive();
        long hint = rejected.hintTerm() > 0
            ? conflictIndex(rejected.hintIndex(), rejected.hintTerm())
            : rejected.hintIndex();
        if (progress.maybeDecrease(rejected.rejectedIndex(), hint) && progress.state() == Progress.State.REPLICATE) {
            progress.becomeProbe();
        }
    }

    private void handleHeartbeatResponse(Leader leader, NodeId from, HeartbeatResponse response) {
        var progress = leader.progress.get(from);
        if (progress == null) {
            return;
        }
        progress.markActive();
        progress.resume();
        if (progress.match() < lastIndex()) {
            leader.forcedAppends.add(from);
        }
        acknowledgeReads(leader, from, response.round());
    }

    private void handleSnapshotInstalled(Leader leader, NodeId from, SnapshotInstalled installed) {
        var progress = leader.progress.get(from);
        if (progress == null) {
            return;
        }
        progress.markActive();
        boolean updated = progress.maybeUpdate(installed.matchIndex());
        if (progress.state() == Progress.State.SNAPSHOT) {
            progress.becomeProbe();
        }
        if (updated) {
            maybeCommit(leader);
        }
    }

    private void snapshotSent(NodeId peer) {
        if (role instanceof Leader leader && leader.progress.get(peer) instanceof Progress progress
            && progress.state() == Progress.State.SNAPSHOT) {
            progress.becomeProbe();
            progress.pauseProbe();
        }
    }

    private void snapshotFailed(NodeId peer) {
        if (role instanceof Leader leader && leader.progress.get(peer) instanceof Progress progress
            && progress.state() == Progress.State.SNAPSHOT) {
            progress.abortSnapshot();
            progress.pauseProbe();
        }
    }

    private void unreachable(NodeId peer) {
        if (role instanceof Leader leader && leader.progress.get(peer) instanceof Progress progress
            && progress.state() == Progress.State.REPLICATE) {
            progress.becomeProbe();
        }
    }

    private void maybeCommit(Leader leader) {
        long[] matches = membership.voters().stream()
            .mapToLong(voter -> leader.progress.get(voter) instanceof Progress progress ? progress.match() : 0)
            .sorted()
            .toArray();
        long candidate = matches[matches.length - membership.quorum()];
        if (candidate > commitIndex && termOf(candidate) == term) {
            commitIndex = candidate;
            leader.commitAdvanced = true;
            releaseDeferredReads(leader);
            if (!membership.isVoter(id) && !membershipPending()) {
                handOff(leader);
            }
        }
    }

    private void handOff(Leader leader) {
        flush(leader);
        membership.voters().stream()
            .max(Comparator.comparingLong(voter -> leader.progress.get(voter) instanceof Progress progress ? progress.match() : 0))
            .ifPresent(successor -> send(successor, new TimeoutNow(term)));
        becomeFollower(term, null);
    }

    private void persisted(long index, long entryTerm) {
        unstable.stableTo(index, entryTerm);
        if (role instanceof Leader leader && termOf(index) == entryTerm) {
            var self = leader.progress.get(id);
            if (self != null && self.maybeUpdate(index)) {
                maybeCommit(leader);
            }
        }
    }

    private Optional<Rejection> rejectUnlessLeader() {
        if (!(role instanceof Leader)) {
            return Optional.of(new Rejection.NotLeader(Optional.ofNullable(leaderId)));
        }
        if (transfer != null) {
            return Optional.of(new Rejection.TransferInProgress());
        }
        return Optional.empty();
    }

    private void propose(long request, Bytes command) {
        var rejection = rejectUnlessLeader();
        if (rejection.isPresent()) {
            outcomes.add(new Outcome.Rejected(request, rejection.get()));
            return;
        }
        var entry = new LogEntry.Command(lastIndex() + 1, term, command);
        append(entry);
        outcomes.add(new Outcome.Appended(request, entry.index(), term));
    }

    private void changeMembership(long request, MembershipChange change) {
        var rejection = rejectUnlessLeader()
            .or(() -> membershipPending() || !committedInCurrentTerm() ? Optional.of(new Rejection.ChangeInProgress()) : Optional.empty())
            .or(() -> membership.check(change).map(Rejection.Invalid::new));
        if (rejection.isPresent()) {
            outcomes.add(new Outcome.Rejected(request, rejection.get()));
            return;
        }
        var entry = new LogEntry.Configuration(lastIndex() + 1, term, membership.apply(change));
        append(entry);
        outcomes.add(new Outcome.Appended(request, entry.index(), term));
    }

    private void transferLeadership(long request, NodeId target) {
        var rejection = rejectUnlessLeader()
            .or(() -> membership.isVoter(target) ? Optional.empty() : Optional.of(new Rejection.Invalid(target + " is not a voter")));
        if (rejection.isPresent()) {
            outcomes.add(new Outcome.Rejected(request, rejection.get()));
            return;
        }
        if (target.equals(id)) {
            outcomes.add(new Outcome.Transferred(request));
            return;
        }
        var leader = (Leader) role;
        transfer = new Transfer(request, target, ticks);
        electionElapsed = 0;
        var progress = leader.progress.get(target);
        if (progress != null && progress.match() == lastIndex()) {
            send(target, new TimeoutNow(term));
        }
    }

    private void stepDown(long request) {
        var rejection = rejectUnlessLeader();
        if (rejection.isPresent()) {
            outcomes.add(new Outcome.Rejected(request, rejection.get()));
            return;
        }
        var leader = (Leader) role;
        Comparator<NodeId> readiness = Comparator.comparing((NodeId voter) -> leader.progress.get(voter).recentActive())
            .thenComparingLong(voter -> leader.progress.get(voter).match())
            .thenComparing(Comparator.reverseOrder());
        membership.voters().stream()
            .filter(voter -> !voter.equals(id) && leader.progress.containsKey(voter))
            .max(readiness)
            .ifPresentOrElse(
                successor -> transferLeadership(request, successor),
                () -> outcomes.add(new Outcome.Rejected(request, new Rejection.Invalid("no other voter can take over leadership"))));
    }

    private List<LogEntry> takeCommitted() {
        if (appliedIndex >= commitIndex) {
            return List.of();
        }
        var committed = slice(appliedIndex + 1, commitIndex + 1, options.maxApplyBytes());
        appliedIndex = committed.getLast().index();
        return committed;
    }

    private void flush(Leader leader) {
        boolean heartbeat = leader.heartbeatDue || leader.readRoundStarted;
        for (var entry : leader.progress.entrySet()) {
            NodeId peer = entry.getKey();
            if (peer.equals(id)) {
                continue;
            }
            var progress = entry.getValue();
            if (heartbeat) {
                send(peer, new Heartbeat(term, Math.min(progress.match(), commitIndex), leader.readRound));
            }
            boolean sent = false;
            while (sendAppend(peer, progress, false)) {
                sent = true;
            }
            if (!sent && (leader.commitAdvanced || leader.forcedAppends.contains(peer))) {
                sendAppend(peer, progress, true);
            }
        }
        leader.forcedAppends.clear();
        leader.heartbeatDue = false;
        leader.readRoundStarted = false;
        leader.commitAdvanced = false;
    }

    private boolean sendAppend(NodeId peer, Progress progress, boolean force) {
        if (progress.state() == Progress.State.SNAPSHOT || (!force && progress.isPaused())) {
            return false;
        }
        long prevIndex = progress.next() - 1;
        if (prevIndex < firstIndex() - 1) {
            requestSnapshot(peer, progress);
            return false;
        }
        var entries = slice(progress.next(), lastIndex() + 1, options.maxAppendBytes());
        if (entries.isEmpty() && !force) {
            return false;
        }
        send(peer, new AppendEntries(term, prevIndex, termOf(prevIndex), entries, commitIndex));
        progress.sent(entries.isEmpty() ? prevIndex : entries.getLast().index(), !entries.isEmpty());
        return !entries.isEmpty();
    }

    private void requestSnapshot(NodeId peer, Progress progress) {
        if (!progress.recentActive()) {
            return;
        }
        progress.becomeSnapshot(firstIndex() - 1);
        snapshotRequests.add(peer);
    }

    private void readIndex(long request) {
        if (role instanceof Leader leader) {
            confirmRead(leader, null, request);
        } else if (leaderId != null) {
            forwardedReads.put(request, ticks);
            send(leaderId, new ReadIndexRequest(term, request));
        } else {
            outcomes.add(new Outcome.Rejected(request, new Rejection.NotLeader(Optional.empty())));
        }
    }

    private void handleForwardedRead(NodeId from, ReadIndexRequest read) {
        if (role instanceof Leader leader) {
            confirmRead(leader, from, read.id());
        } else {
            send(from, new ReadIndexResponse(term, read.id(), OptionalLong.empty()));
        }
    }

    private void handleReadIndexResponse(NodeId from, ReadIndexResponse response) {
        if (!from.equals(leaderId) || forwardedReads.remove(response.id()) == null) {
            return;
        }
        outcomes.add(response.readIndex().isPresent()
            ? new Outcome.ReadReady(response.id(), response.readIndex().getAsLong())
            : new Outcome.Rejected(response.id(), new Rejection.NotLeader(Optional.empty())));
    }

    private void confirmRead(Leader leader, NodeId requester, long request) {
        if (!committedInCurrentTerm()) {
            leader.deferredReads.add(new PendingRead(requester, request, 0, 0, Set.of()));
            return;
        }
        if (membership.quorum() == 1 && membership.isVoter(id)) {
            completeRead(requester, request, commitIndex);
            return;
        }
        if (!leader.readRoundStarted) {
            leader.readRound++;
            leader.readRoundStarted = true;
        }
        var acks = new HashSet<NodeId>();
        acks.add(id);
        leader.pendingReads.add(new PendingRead(requester, request, commitIndex, leader.readRound, acks));
    }

    private void acknowledgeReads(Leader leader, NodeId from, long round) {
        if (!membership.isVoter(from)) {
            return;
        }
        var iterator = leader.pendingReads.iterator();
        while (iterator.hasNext()) {
            var pending = iterator.next();
            if (pending.round() > round) {
                continue;
            }
            pending.acks().add(from);
            long acknowledged = pending.acks().stream().filter(membership::isVoter).count();
            if (acknowledged >= membership.quorum()) {
                completeRead(pending.requester(), pending.id(), pending.index());
                iterator.remove();
            }
        }
    }

    private void releaseDeferredReads(Leader leader) {
        if (leader.deferredReads.isEmpty() || !committedInCurrentTerm()) {
            return;
        }
        var deferred = List.copyOf(leader.deferredReads);
        leader.deferredReads.clear();
        for (var read : deferred) {
            confirmRead(leader, read.requester(), read.id());
        }
    }

    private void completeRead(NodeId requester, long request, long index) {
        if (requester == null) {
            outcomes.add(new Outcome.ReadReady(request, index));
        } else {
            send(requester, new ReadIndexResponse(term, request, OptionalLong.of(index)));
        }
    }

    private void rejectRead(NodeId requester, long request, Optional<NodeId> leader) {
        if (requester == null) {
            outcomes.add(new Outcome.Rejected(request, new Rejection.NotLeader(leader)));
        } else {
            send(requester, new ReadIndexResponse(term, request, OptionalLong.empty()));
        }
    }

    private void rejectLeaderReads(Leader leader, Optional<NodeId> newLeader) {
        for (var pending : leader.pendingReads) {
            rejectRead(pending.requester(), pending.id(), newLeader);
        }
        for (var deferred : leader.deferredReads) {
            rejectRead(deferred.requester(), deferred.id(), newLeader);
        }
        leader.pendingReads.clear();
        leader.deferredReads.clear();
    }

    private void rejectForwardedReads(Optional<NodeId> newLeader) {
        for (long read : forwardedReads.keySet()) {
            outcomes.add(new Outcome.Rejected(read, new Rejection.NotLeader(newLeader)));
        }
        forwardedReads.clear();
    }

    private void expireForwardedReads() {
        var iterator = forwardedReads.entrySet().iterator();
        while (iterator.hasNext()) {
            var read = iterator.next();
            if (ticks - read.getValue() >= 2L * options.electionTicks()) {
                outcomes.add(new Outcome.Rejected(read.getKey(), new Rejection.TimedOut()));
                iterator.remove();
            }
        }
    }

    private boolean committedInCurrentTerm() {
        return termOf(commitIndex) == term;
    }

    private long firstIndex() {
        return unstable.snapshot().map(snapshot -> snapshot.index() + 1).orElseGet(log::firstIndex);
    }

    private long termOf(long index) {
        if (index == 0) {
            return 0;
        }
        var unstableTerm = unstable.term(index);
        if (unstableTerm.isPresent()) {
            return unstableTerm.getAsLong();
        }
        if (index >= unstable.offset() || index < firstIndex() - 1 || index > log.lastIndex()) {
            return 0;
        }
        return log.term(index);
    }

    private boolean matchesTerm(long index, long expectedTerm) {
        if (index == 0) {
            return expectedTerm == 0;
        }
        return index <= lastIndex() && index >= firstIndex() - 1 && termOf(index) == expectedTerm;
    }

    private long conflictIndex(long index, long maxTerm) {
        long first = firstIndex();
        for (; index > 0; index--) {
            if (index < first - 1 || termOf(index) <= maxTerm) {
                return index;
            }
        }
        return 0;
    }

    private List<LogEntry> slice(long fromIndex, long toIndex, long maxBytes) {
        if (fromIndex >= toIndex) {
            return List.of();
        }
        var result = new ArrayList<LogEntry>();
        long stableEnd = Math.min(toIndex, unstable.offset());
        if (fromIndex < stableEnd) {
            var stable = log.entries(fromIndex, stableEnd, maxBytes);
            result.addAll(stable);
            if (result.size() < stableEnd - fromIndex) {
                return result;
            }
        }
        long bytes = result.stream().mapToLong(LogEntry::payloadSize).sum();
        for (LogEntry entry : unstable.slice(Math.max(fromIndex, unstable.offset()), toIndex)) {
            if (!result.isEmpty() && bytes + entry.payloadSize() > maxBytes) {
                break;
            }
            result.add(entry);
            bytes += entry.payloadSize();
        }
        return result;
    }

    private void send(NodeId to, Message message) {
        messages.add(new Envelope(id, to, message));
    }
}
