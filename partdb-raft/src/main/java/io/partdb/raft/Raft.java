package io.partdb.raft;

import io.partdb.bytes.Bytes;
import io.partdb.raft.RaftEffects.ProposalResult;
import io.partdb.raft.RaftEffects.ReadResult;
import io.partdb.raft.RaftMessage.AppendAccepted;
import io.partdb.raft.RaftMessage.AppendEntries;
import io.partdb.raft.RaftMessage.AppendRejected;
import io.partdb.raft.RaftMessage.Heartbeat;
import io.partdb.raft.RaftMessage.HeartbeatResponse;
import io.partdb.raft.RaftMessage.InstallSnapshot;
import io.partdb.raft.RaftMessage.InstallSnapshotResponse;
import io.partdb.raft.RaftMessage.PreVote;
import io.partdb.raft.RaftMessage.PreVoteResponse;
import io.partdb.raft.RaftMessage.ReadIndexResponse;
import io.partdb.raft.RaftMessage.RequestVote;
import io.partdb.raft.RaftMessage.RequestVoteResponse;
import io.partdb.raft.RaftMessage.TimeoutNow;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.random.RandomGenerator;

public final class Raft {

    private sealed interface RoleState permits Follower, PreCandidate, Candidate, Leader {}

    private record Follower() implements RoleState {}

    private record PreCandidate(Map<String, Boolean> votes) implements RoleState {}

    private record Candidate(Map<String, Boolean> votes) implements RoleState {}

    private record PendingRead(String requester, long id, long index, long round, Set<String> acks) {}

    private static final class Leader implements RoleState {
        private final Map<String, Progress> progress = new TreeMap<>();
        private final List<PendingRead> pendingReads = new ArrayList<>();
        private final List<PendingRead> deferredReads = new ArrayList<>();
        private final Set<String> forcedAppends = new TreeSet<>();
        private String transferTarget;
        private long readRound;
        private boolean readRoundStarted;
        private boolean heartbeatDue;
        private boolean commitAdvanced;
    }

    private final String id;
    private final RaftOptions options;
    private final RaftLog log;
    private final RandomGenerator random;
    private final UnstableLog unstable;
    private final Map<Long, Long> forwardedReads = new LinkedHashMap<>();

    private final List<Envelope> messages = new ArrayList<>();
    private final List<ProposalResult> proposals = new ArrayList<>();
    private final List<ReadResult> reads = new ArrayList<>();
    private final Set<String> snapshotRequests = new TreeSet<>();

    private RaftMembership membership;
    private RoleState role = new Follower();
    private long term;
    private String votedFor;
    private String leaderId;
    private long commitIndex;
    private long appliedIndex;
    private long pendingMembershipIndex;
    private long ticks;
    private long electionElapsed;
    private long heartbeatElapsed;
    private long electionTimeout;
    private RaftHardState lastHardState;

    private Raft(
        String id,
        RaftMembership membership,
        RaftOptions options,
        RaftLog log,
        RaftHardState hardState,
        long applied,
        RandomGenerator random
    ) {
        this.id = Validation.nodeId(id, "id");
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
        this.membership = recoverMembership(membership, snapshotIndex, applied);
        this.lastHardState = hardState;
        resetElectionTimeout();
    }

    public static Raft recover(
        String id,
        RaftMembership membership,
        RaftOptions options,
        RaftLog log,
        RaftHardState hardState,
        long applied,
        RandomGenerator random
    ) {
        return new Raft(id, membership, options, log, hardState, applied, random);
    }

    public String id() {
        return id;
    }

    public RaftRole role() {
        return switch (role) {
            case Follower _ -> RaftRole.FOLLOWER;
            case PreCandidate _ -> RaftRole.PRE_CANDIDATE;
            case Candidate _ -> RaftRole.CANDIDATE;
            case Leader _ -> RaftRole.LEADER;
        };
    }

    public long term() {
        return term;
    }

    public Optional<String> leaderId() {
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

    public RaftMembership membership() {
        return membership;
    }

    public void step(RaftInput input) {
        switch (input) {
            case RaftInput.Tick() -> tick();
            case RaftInput.Propose(long proposal, Bytes command) -> propose(proposal, command);
            case RaftInput.Receive(String from, RaftMessage message) -> receive(from, message);
            case RaftInput.ReadIndex(long read) -> readIndex(read);
            case RaftInput.ChangeMembership(long proposal, MembershipChange change) -> changeMembership(proposal, change);
            case RaftInput.TransferLeadership(String target) -> transferLeadership(target);
            case RaftInput.Persisted(long index, long entryTerm) -> persisted(index, entryTerm);
            case RaftInput.SnapshotSent(String peer) -> snapshotSent(peer);
            case RaftInput.SnapshotFailed(String peer) -> snapshotFailed(peer);
            case RaftInput.Unreachable(String peer) -> unreachable(peer);
        }
    }

    public RaftEffects ready() {
        var committed = takeCommitted();
        if (role instanceof Leader leader) {
            flush(leader);
        }

        var hardState = new RaftHardState(term, Optional.ofNullable(votedFor), commitIndex);
        var changedHardState = hardState.equals(lastHardState) ? Optional.<RaftHardState>empty() : Optional.of(hardState);
        lastHardState = hardState;

        var effects = new RaftEffects(
            changedHardState,
            unstable.takeSnapshot(),
            unstable.takeEntries(),
            messages,
            committed,
            proposals,
            reads,
            snapshotRequests
        );
        messages.clear();
        proposals.clear();
        reads.clear();
        snapshotRequests.clear();
        return effects;
    }

    private RaftMembership recoverMembership(RaftMembership base, long snapshotIndex, long applied) {
        var recovered = base;
        if (applied > snapshotIndex) {
            for (RaftLogEntry entry : log.entries(snapshotIndex + 1, applied + 1, Long.MAX_VALUE)) {
                if (entry instanceof RaftLogEntry.Membership(_, _, var membership)) {
                    recovered = membership;
                }
            }
        }
        return recovered;
    }

    private void tick() {
        ticks++;
        expireForwardedReads();
        if (role instanceof Leader leader) {
            tickLeader(leader);
        } else {
            tickElection();
        }
    }

    private void tickElection() {
        electionElapsed++;
        if (electionElapsed >= electionTimeout && membership.isVoter(id)) {
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
            leader.transferTarget = null;
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

    private static boolean isActive(Leader leader, String member) {
        var progress = leader.progress.get(member);
        return progress != null && progress.recentActive();
    }

    private void campaign(boolean preVote, boolean leadershipTransfer) {
        changeLeader(null);
        electionElapsed = 0;
        resetElectionTimeout();

        var votes = new TreeMap<String, Boolean>();
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
        for (String voter : membership.voters()) {
            if (!voter.equals(id)) {
                send(voter, preVote
                    ? new PreVote(voteTerm, lastIndex, lastTerm)
                    : new RequestVote(voteTerm, lastIndex, lastTerm, leadershipTransfer));
            }
        }
    }

    private enum VoteResult { WON, LOST, PENDING }

    private VoteResult tally(Map<String, Boolean> votes) {
        long granted = 0;
        long rejected = 0;
        for (String voter : membership.voters()) {
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

    private void becomeFollower(long newTerm, String leader) {
        if (role instanceof Leader current) {
            rejectLeaderReads(current);
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
        for (String member : membership.members()) {
            leader.progress.put(member, new Progress(next, options.maxInflightAppends()));
        }
        var self = leader.progress.computeIfAbsent(id, _ -> new Progress(next, options.maxInflightAppends()));
        self.maybeUpdate(unstable.offset() - 1);
        self.markActive();

        role = leader;
        changeLeader(id);
        electionElapsed = 0;
        heartbeatElapsed = 0;
        pendingMembershipIndex = lastIndex();
        unstable.append(new RaftLogEntry.NoOp(next, term));
    }

    private void changeLeader(String leader) {
        if (!Objects.equals(leaderId, leader)) {
            rejectForwardedReads();
        }
        leaderId = leader;
    }

    private void resetElectionTimeout() {
        electionTimeout = options.electionTicks() + random.nextInt(options.electionTicks());
    }

    private boolean inLease() {
        return leaderId != null && electionElapsed < options.electionTicks();
    }

    private void receive(String from, RaftMessage message) {
        if (!membership.isMember(from)) {
            return;
        }
        if (message.term() > term) {
            boolean voteRequest = message instanceof PreVote || message instanceof RequestVote;
            boolean transfer = message instanceof RequestVote vote && vote.leadershipTransfer();
            if (voteRequest && !transfer && inLease()) {
                if (message instanceof PreVote) {
                    send(from, new PreVoteResponse(term, false));
                }
                return;
            }
            boolean keepTerm = message instanceof PreVote || message instanceof PreVoteResponse response && response.granted();
            if (!keepTerm) {
                becomeFollower(message.term(), leaderMessage(message) ? from : null);
            }
        } else if (message.term() < term) {
            switch (message) {
                case AppendEntries _, Heartbeat _, InstallSnapshot _ -> send(from, new HeartbeatResponse(term, 0));
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
            case InstallSnapshot snapshot -> handleInstallSnapshot(from, snapshot);
            case RaftMessage.ReadIndex read -> handleForwardedRead(from, read);
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
            case InstallSnapshotResponse response -> {
                if (role instanceof Leader leader) {
                    handleInstallSnapshotResponse(leader, from, response);
                }
            }
        }
    }

    private static boolean leaderMessage(RaftMessage message) {
        return message instanceof AppendEntries || message instanceof Heartbeat || message instanceof InstallSnapshot;
    }

    private void handlePreVote(String from, PreVote vote) {
        boolean granted = vote.term() > term && isUpToDate(vote.lastLogIndex(), vote.lastLogTerm());
        send(from, new PreVoteResponse(granted ? vote.term() : term, granted));
    }

    private void handlePreVoteResponse(String from, PreVoteResponse response) {
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

    private void handleRequestVote(String from, RequestVote vote) {
        boolean canVote = from.equals(votedFor) || (votedFor == null && leaderId == null);
        boolean granted = canVote && isUpToDate(vote.lastLogIndex(), vote.lastLogTerm());
        if (granted) {
            votedFor = from;
            electionElapsed = 0;
        }
        send(from, new RequestVoteResponse(term, granted));
    }

    private void handleRequestVoteResponse(String from, RequestVoteResponse response) {
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

    private void followLeader(String from) {
        if (!(role instanceof Follower) || !from.equals(leaderId)) {
            becomeFollower(term, from);
        }
        electionElapsed = 0;
    }

    private void handleAppendEntries(String from, AppendEntries append) {
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
        for (RaftLogEntry entry : append.entries()) {
            if (conflicted || !matchesTerm(entry.index(), entry.term())) {
                conflicted = true;
                unstable.append(entry);
            }
        }
        commitTo(Math.min(append.commitIndex(), lastNewIndex));
        send(from, new AppendAccepted(term, lastNewIndex));
    }

    private void handleHeartbeat(String from, Heartbeat heartbeat) {
        if (role instanceof Leader) {
            return;
        }
        followLeader(from);
        commitTo(Math.min(heartbeat.commitIndex(), lastIndex()));
        send(from, new HeartbeatResponse(term, heartbeat.round()));
    }

    private void handleInstallSnapshot(String from, InstallSnapshot install) {
        if (role instanceof Leader) {
            return;
        }
        followLeader(from);

        var snapshot = install.snapshot();
        if (snapshot.index() <= commitIndex) {
            send(from, new InstallSnapshotResponse(term, commitIndex));
            return;
        }
        if (matchesTerm(snapshot.index(), snapshot.term())) {
            commitTo(snapshot.index());
            send(from, new InstallSnapshotResponse(term, commitIndex));
            return;
        }

        unstable.restore(snapshot);
        commitIndex = snapshot.index();
        appliedIndex = snapshot.index();
        membership = snapshot.membership();
        send(from, new InstallSnapshotResponse(term, snapshot.index()));
    }

    private void handleTimeoutNow(String from) {
        if (from.equals(leaderId) && membership.isVoter(id)) {
            campaign(false, true);
        }
    }

    private void commitTo(long index) {
        if (index > commitIndex) {
            commitIndex = index;
        }
    }

    private void handleAppendAccepted(Leader leader, String from, AppendAccepted accepted) {
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
        if (from.equals(leader.transferTarget) && progress.match() == lastIndex()) {
            send(from, new TimeoutNow(term));
        }
    }

    private void handleAppendRejected(Leader leader, String from, AppendRejected rejected) {
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

    private void handleHeartbeatResponse(Leader leader, String from, HeartbeatResponse response) {
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

    private void handleInstallSnapshotResponse(Leader leader, String from, InstallSnapshotResponse response) {
        var progress = leader.progress.get(from);
        if (progress == null) {
            return;
        }
        progress.markActive();
        boolean updated = progress.maybeUpdate(response.matchIndex());
        if (progress.state() == Progress.State.SNAPSHOT) {
            progress.becomeProbe();
        }
        if (updated) {
            maybeCommit(leader);
        }
    }

    private void snapshotSent(String peer) {
        if (role instanceof Leader leader && leader.progress.get(peer) instanceof Progress progress
            && progress.state() == Progress.State.SNAPSHOT) {
            progress.becomeProbe();
            progress.pauseProbe();
        }
    }

    private void snapshotFailed(String peer) {
        if (role instanceof Leader leader && leader.progress.get(peer) instanceof Progress progress
            && progress.state() == Progress.State.SNAPSHOT) {
            progress.abortSnapshot();
            progress.pauseProbe();
        }
    }

    private void unreachable(String peer) {
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
        }
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

    private void propose(long proposal, Bytes command) {
        if (!(role instanceof Leader leader) || leader.transferTarget != null) {
            proposals.add(new ProposalResult.Rejected(proposal));
            return;
        }
        var entry = new RaftLogEntry.Command(lastIndex() + 1, term, command);
        unstable.append(entry);
        proposals.add(new ProposalResult.Accepted(proposal, entry.index(), term));
    }

    private void changeMembership(long proposal, MembershipChange change) {
        if (!(role instanceof Leader leader)
            || leader.transferTarget != null
            || pendingMembershipIndex > appliedIndex
            || !committedInCurrentTerm()) {
            proposals.add(new ProposalResult.Rejected(proposal));
            return;
        }
        RaftMembership next;
        try {
            next = membership.apply(change);
        } catch (IllegalArgumentException e) {
            proposals.add(new ProposalResult.Rejected(proposal));
            return;
        }
        var entry = new RaftLogEntry.Membership(lastIndex() + 1, term, next);
        unstable.append(entry);
        pendingMembershipIndex = entry.index();
        proposals.add(new ProposalResult.Accepted(proposal, entry.index(), term));
    }

    private void transferLeadership(String target) {
        if (!(role instanceof Leader leader)
            || target.equals(id)
            || !membership.isVoter(target)
            || target.equals(leader.transferTarget)) {
            return;
        }
        leader.transferTarget = target;
        electionElapsed = 0;
        var progress = leader.progress.get(target);
        if (progress != null && progress.match() == lastIndex()) {
            send(target, new TimeoutNow(term));
        }
    }

    private List<RaftLogEntry> takeCommitted() {
        if (appliedIndex >= commitIndex) {
            return List.of();
        }
        var committed = slice(appliedIndex + 1, commitIndex + 1, options.maxApplyBytes());
        for (RaftLogEntry entry : committed) {
            if (entry instanceof RaftLogEntry.Membership(_, _, var next)) {
                applyMembership(next);
            }
        }
        appliedIndex = committed.getLast().index();
        return committed;
    }

    private void applyMembership(RaftMembership next) {
        membership = next;
        if (!(role instanceof Leader leader)) {
            return;
        }
        long nextIndex = lastIndex() + 1;
        leader.progress.keySet().removeIf(member -> !member.equals(id) && !next.isMember(member));
        for (String member : next.members()) {
            leader.progress.computeIfAbsent(member, _ -> new Progress(nextIndex, options.maxInflightAppends()));
        }
        if (leader.transferTarget != null && !next.isVoter(leader.transferTarget)) {
            leader.transferTarget = null;
        }
        if (!next.isVoter(id)) {
            becomeFollower(term, null);
            return;
        }
        maybeCommit(leader);
    }

    private void flush(Leader leader) {
        boolean heartbeat = leader.heartbeatDue || leader.readRoundStarted;
        for (var entry : leader.progress.entrySet()) {
            String peer = entry.getKey();
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

    private boolean sendAppend(String peer, Progress progress, boolean force) {
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

    private void requestSnapshot(String peer, Progress progress) {
        if (!progress.recentActive()) {
            return;
        }
        progress.becomeSnapshot(firstIndex() - 1);
        snapshotRequests.add(peer);
    }

    private void readIndex(long read) {
        if (role instanceof Leader leader) {
            confirmRead(leader, null, read);
        } else if (leaderId != null) {
            forwardedReads.put(read, ticks);
            send(leaderId, new RaftMessage.ReadIndex(term, read));
        } else {
            reads.add(new ReadResult.Rejected(read));
        }
    }

    private void handleForwardedRead(String from, RaftMessage.ReadIndex read) {
        if (role instanceof Leader leader) {
            confirmRead(leader, from, read.id());
        } else {
            send(from, new ReadIndexResponse(term, read.id(), 0));
        }
    }

    private void handleReadIndexResponse(String from, ReadIndexResponse response) {
        if (!from.equals(leaderId) || forwardedReads.remove(response.id()) == null) {
            return;
        }
        reads.add(response.rejected()
            ? new ReadResult.Rejected(response.id())
            : new ReadResult.Confirmed(response.id(), response.readIndex()));
    }

    private void confirmRead(Leader leader, String requester, long read) {
        if (!committedInCurrentTerm()) {
            leader.deferredReads.add(new PendingRead(requester, read, 0, 0, Set.of()));
            return;
        }
        if (membership.quorum() == 1) {
            completeRead(requester, read, commitIndex);
            return;
        }
        if (!leader.readRoundStarted) {
            leader.readRound++;
            leader.readRoundStarted = true;
        }
        var acks = new HashSet<String>();
        acks.add(id);
        leader.pendingReads.add(new PendingRead(requester, read, commitIndex, leader.readRound, acks));
    }

    private void acknowledgeReads(Leader leader, String from, long round) {
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

    private void completeRead(String requester, long read, long index) {
        if (requester == null) {
            reads.add(index == 0 ? new ReadResult.Rejected(read) : new ReadResult.Confirmed(read, index));
        } else {
            send(requester, new ReadIndexResponse(term, read, index));
        }
    }

    private void rejectLeaderReads(Leader leader) {
        for (var pending : leader.pendingReads) {
            completeRead(pending.requester(), pending.id(), 0);
        }
        for (var deferred : leader.deferredReads) {
            completeRead(deferred.requester(), deferred.id(), 0);
        }
        leader.pendingReads.clear();
        leader.deferredReads.clear();
    }

    private void rejectForwardedReads() {
        for (long read : forwardedReads.keySet()) {
            reads.add(new ReadResult.Rejected(read));
        }
        forwardedReads.clear();
    }

    private void expireForwardedReads() {
        var iterator = forwardedReads.entrySet().iterator();
        while (iterator.hasNext()) {
            var read = iterator.next();
            if (ticks - read.getValue() >= 2L * options.electionTicks()) {
                reads.add(new ReadResult.Rejected(read.getKey()));
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

    private List<RaftLogEntry> slice(long fromIndex, long toIndex, long maxBytes) {
        if (fromIndex >= toIndex) {
            return List.of();
        }
        var result = new ArrayList<RaftLogEntry>();
        long stableEnd = Math.min(toIndex, unstable.offset());
        if (fromIndex < stableEnd) {
            var stable = log.entries(fromIndex, stableEnd, maxBytes);
            result.addAll(stable);
            if (result.size() < stableEnd - fromIndex) {
                return result;
            }
        }
        long bytes = result.stream().mapToLong(RaftLogEntry::payloadSize).sum();
        for (RaftLogEntry entry : unstable.slice(Math.max(fromIndex, unstable.offset()), toIndex)) {
            if (!result.isEmpty() && bytes + entry.payloadSize() > maxBytes) {
                break;
            }
            result.add(entry);
            bytes += entry.payloadSize();
        }
        return result;
    }

    private void send(String to, RaftMessage message) {
        messages.add(new Envelope(id, to, message));
    }
}
