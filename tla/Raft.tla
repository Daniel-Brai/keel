--------------------------------- MODULE Raft ----------------------------------
\* Elections and replication with a durable copy of each member's state.
\* A crash keeps only what was synced.
EXTENDS Naturals, Integers, FiniteSets, Sequences, TLC

CONSTANTS
    Server,
    Value,
    Nil,
    Noop,
    Campaigners,
    Silent,
    MaxTerm,
    MaxLogLen,
    MaxBatch,
    MaxMsgs,
    MaxCrashes,
    PreVote,
    Pipeline,
    \* Each flag below breaks one rule. A config that sets one must fail
    AckBeforeDurable,
    GrantBeforeDurable,
    LeaderCountsVolatile,
    CommitOldTermByCount,
    NoLogCheckOnVote,
    MatchIndexAssign

VARIABLES
    messages,
    currentTerm, state, votedFor,
    votesGranted, preVotes,
    nextIndex, matchIndex,
    log, commitIndex,
    durableTerm, durableVote, durableLog,
    heard, pending,
    elections, committed, crashes

serverVars == << currentTerm, state, votedFor >>
candidateVars == << votesGranted, preVotes >>
leaderVars == << nextIndex, matchIndex >>
logVars == << log, commitIndex >>
durableVars == << durableTerm, durableVote, durableLog >>
replyVars == << heard, pending >>
historyVars == << elections, committed, crashes >>

vars == << messages, serverVars, candidateVars, leaderVars, logVars,
    durableVars, replyVars, historyVars >>

Min(a, b) == IF a < b THEN a ELSE b
Max(a, b) == IF a > b THEN a ELSE b
MaxOf(S) == CHOOSE x \in S: \A y \in S: y <= x
MinOf(S) == CHOOSE x \in S: \A y \in S: y >= x

\* A silent member takes no step. It stands for a member that is cut off
Active == Server \ Silent

Quorum == {S \in SUBSET Server: Cardinality(S) * 2 > Cardinality(Server)}

LastTerm(xs) == IF Len(xs) = 0 THEN 0 ELSE xs[Len(xs)].term

TermAt(xs, n) == IF n = 0 THEN 0 ELSE xs[n].term

LogUpToDate(i, lastTerm, lastIndex) ==
    \/ NoLogCheckOnVote
    \/ lastTerm > LastTerm(log[i])
    \/ lastTerm = LastTerm(log[i]) /\ lastIndex >= Len(log[i])

Send(ms) == messages' = messages \union ms
Discard(m) == messages' = messages \ {m}
Reply(response, request) == messages' = (messages \ {request}) \union {response}

Follower == "Follower"
PreCandidate == "PreCandidate"
Candidate == "Candidate"
Leader == "Leader"

VoteRequestMsg(i, j, t, pre) ==
    [mtype |-> IF pre THEN "PreVoteRequest" ELSE "RequestVoteRequest",
        mterm |-> t,
        mlastLogTerm |-> LastTerm(log[i]),
        mlastLogIndex |-> Len(log[i]),
        msource |-> i,
        mdest |-> j]

VoteResponseMsg(i, j, t, granted, pre) ==
    [mtype |-> IF pre THEN "PreVoteResponse" ELSE "RequestVoteResponse",
        mterm |-> t,
        mvoteGranted |-> granted,
        msource |-> i,
        mdest |-> j]

AppendResponseMsg(i, j, success, matchIdx, logTerm) ==
    [mtype |-> "AppendEntriesResponse",
        mterm |-> currentTerm[i],
        msuccess |-> success,
        mmatchIndex |-> matchIdx,
        mlogTerm |-> logTerm,
        msource |-> i,
        mdest |-> j]

\* The durable state backs a vote for j in term t
CanGrant(i, r) ==
    \/ GrantBeforeDurable
    \/
        /\ durableTerm[i] >= r.mterm
        /\
            \/ durableTerm[i] > r.mterm
            \/ durableVote[i] = r.mdest
                
\* The durable log holds the entries an ack claims
CanAck(i, r) ==
    \/ AckBeforeDurable
    \/
        /\ durableTerm[i] >= r.mterm
        /\ Len(durableLog[i]) >= r.mmatchIndex
        /\ r.mmatchIndex <= Len(log[i])
        /\ SubSeq(durableLog[i], 1, r.mmatchIndex) = SubSeq(log[i], 1, r.mmatchIndex)
        /\
            \/ r.mmatchIndex = 0
            \/ r.mmatchIndex \in 1..Len(log[i]) /\ log[i][r.mmatchIndex].term = r.mlogTerm
                
\* How much of the log in memory is on disk
CommonLen(a, b) ==
    MaxOf({n \in 0..Min(Len(a), Len(b)): SubSeq(a, 1, n) = SubSeq(b, 1, n)})

DurableLen(i) == CommonLen(durableLog[i], log[i])

\* A member that heard a leader lately does not time out or grant a pre-vote
LeaderRecent(i) == PreVote /\ (heard[i] \/ state[i] = Leader)

Init ==
    /\ messages = {}
    /\ currentTerm = [i \in Server |-> 0]
    /\ state = [i \in Server |-> Follower]
    /\ votedFor = [i \in Server |-> Nil]
    /\ votesGranted = [i \in Server |-> {}]
    /\ preVotes = [i \in Server |-> {}]
    /\ nextIndex = [i \in Server |-> [j \in Server |-> 1]]
    /\ matchIndex = [i \in Server |-> [j \in Server |-> 0]]
    /\ log = [i \in Server |-> <<>>]
    /\ commitIndex = [i \in Server |-> 0]
    /\ durableTerm = [i \in Server |-> 0]
    /\ durableVote = [i \in Server |-> Nil]
    /\ durableLog = [i \in Server |-> <<>>]
    /\ heard = [i \in Server |-> FALSE]
    /\ pending = [i \in Server |-> {}]
    /\ elections = {}
    /\ committed = {}
    /\ crashes = 0

\* A sync completes. Everything written so far is durable
Persist(i) ==
    /\ durableTerm' = [durableTerm EXCEPT ![i] = currentTerm[i]]
    /\ durableVote' = [durableVote EXCEPT ![i] = votedFor[i]]
    /\ durableLog' = [durableLog EXCEPT ![i] = log[i]]
    /\ UNCHANGED << messages, serverVars, candidateVars, leaderVars, logVars,
        replyVars, historyVars >>

\* A crash and a restart. Only the durable copy survives
Restart(i) ==
    /\ crashes < MaxCrashes
    /\ crashes' = crashes + 1
    /\ state' = [state EXCEPT ![i] = Follower]
    /\ currentTerm' = [currentTerm EXCEPT ![i] = durableTerm[i]]
    /\ votedFor' = [votedFor EXCEPT ![i] = durableVote[i]]
    /\ log' = [log EXCEPT ![i] = durableLog[i]]
    /\ commitIndex' = [commitIndex EXCEPT ![i] = 0]
    /\ votesGranted' = [votesGranted EXCEPT ![i] = {}]
    /\ preVotes' = [preVotes EXCEPT ![i] = {}]
    /\ nextIndex' = [nextIndex EXCEPT ![i] = [j \in Server |-> 1]]
    /\ matchIndex' = [matchIndex EXCEPT ![i] = [j \in Server |-> 0]]
    /\ heard' = [heard EXCEPT ![i] = FALSE]
    /\ pending' = [pending EXCEPT ![i] = {}]
    /\ UNCHANGED << messages, durableVars, elections, committed >>

\* A reply that waits on a sync leaves once the sync covers its claim
SendVoteReply(i, r) ==
    /\ r \in pending[i]
    /\ r.mtype = "RequestVoteResponse"
    /\ CanGrant(i, r)
    /\ Send({r})
    /\ pending' = [pending EXCEPT ![i] = @ \ {r}]
    /\ UNCHANGED << serverVars, candidateVars, leaderVars, logVars,
        durableVars, heard, historyVars >>

SendAppendReply(i, r) ==
    /\ r \in pending[i]
    /\ r.mtype = "AppendEntriesResponse"
    /\ CanAck(i, r)
    /\ Send({r})
    /\ pending' = [pending EXCEPT ![i] = @ \ {r}]
    /\ UNCHANGED << serverVars, candidateVars, leaderVars, logVars,
        durableVars, heard, historyVars >>

Campaign(i) ==
    /\ currentTerm' = [currentTerm EXCEPT ![i] = @ + 1]
    /\ state' = [state EXCEPT ![i] = Candidate]
    /\ votedFor' = [votedFor EXCEPT ![i] = i]
    /\ votesGranted' = [votesGranted EXCEPT ![i] = {}]
    /\ preVotes' = [preVotes EXCEPT ![i] = {}]
    /\ Send({VoteRequestMsg(i, j, currentTerm[i] + 1, FALSE): j \in Active \ {i}})
    /\ UNCHANGED << leaderVars, logVars, durableVars, replyVars, historyVars >>

PreCampaign(i) ==
    /\ state' = [state EXCEPT ![i] = PreCandidate]
    /\ preVotes' = [preVotes EXCEPT ![i] = {i}]
    /\ votesGranted' = [votesGranted EXCEPT ![i] = {}]
    /\ Send({VoteRequestMsg(i, j, currentTerm[i] + 1, TRUE): j \in Active \ {i}})
    /\ UNCHANGED << currentTerm, votedFor, leaderVars, logVars, durableVars,
        replyVars, historyVars >>

Timeout(i) ==
    /\ i \in Campaigners
    /\ state[i] \in {Follower, PreCandidate, Candidate}
    /\ ~heard[i]
    /\ IF PreVote THEN PreCampaign(i) ELSE Campaign(i)

\* A pre-vote round that won a majority starts the real campaign
BecomeCandidate(i) ==
    /\ state[i] = PreCandidate
    /\ preVotes[i] \in Quorum
    /\ Campaign(i)

\* A candidate counts its own vote only once the vote is durable
SelfVote(i) ==
    IF durableTerm[i] = currentTerm[i] /\ durableVote[i] = i THEN {i} ELSE {}

BecomeLeader(i) ==
    /\ state[i] = Candidate
    /\ (votesGranted[i] \union SelfVote(i)) \in Quorum
    /\ LET noop == [term |-> currentTerm[i], value |-> Noop]
            newLog == Append(log[i], noop)
        IN
        /\ state' = [state EXCEPT ![i] = Leader]
        /\ log' = [log EXCEPT ![i] = newLog]
        /\ nextIndex' = [nextIndex EXCEPT ![i] = [j \in Server |-> Len(log[i]) + 1]]
        /\ matchIndex' = [matchIndex EXCEPT ![i] = [j \in Server |-> 0]]
        /\ elections' = elections \union
            {[eterm |-> currentTerm[i], eleader |-> i, elog |-> newLog]}
    /\ UNCHANGED << messages, currentTerm, votedFor, candidateVars, commitIndex,
            durableVars, replyVars, committed, crashes >>

\* A leader that hears from no majority steps down
StepDown(i) ==
    /\ PreVote
    /\ state[i] = Leader
    /\ state' = [state EXCEPT ![i] = Follower]
    /\ UNCHANGED << messages, currentTerm, votedFor, candidateVars, leaderVars,
        logVars, durableVars, replyVars, historyVars >>

\* The minimum election timeout passes with no word from a leader
ElectionTimeoutElapsed(i) ==
    /\ PreVote
    /\ heard[i]
    /\ heard' = [heard EXCEPT ![i] = FALSE]
    /\ UNCHANGED << messages, serverVars, candidateVars, leaderVars, logVars,
        durableVars, pending, historyVars >>

ClientRequest(i, v) ==
    /\ state[i] = Leader
    /\ log' = [log EXCEPT ![i] = Append(@, [term |-> currentTerm[i], value |-> v])]
    /\ UNCHANGED << messages, serverVars, candidateVars, leaderVars,
        commitIndex, durableVars, replyVars, historyVars >>

AppendEntries(i, j) ==
    /\ i /= j
    /\ j \in Active
    /\ state[i] = Leader
    /\ \E advance \in IF Pipeline THEN BOOLEAN ELSE {FALSE}:
        LET next == nextIndex[i][j]
            prev == next - 1
            last == Min(Len(log[i]), next + MaxBatch - 1)
        IN
            /\ Send({[mtype |-> "AppendEntriesRequest",
                mterm |-> currentTerm[i],
                mprevLogIndex |-> prev,
                mprevLogTerm |-> TermAt(log[i], prev),
                mentries |-> SubSeq(log[i], next, last),
                mcommitIndex |-> commitIndex[i],
                msource |-> i,
                mdest |-> j]})
            /\ nextIndex' = IF advance /\ last >= next
                THEN [nextIndex EXCEPT ![i][j] = last + 1]
                ELSE nextIndex
    /\ UNCHANGED << serverVars, candidateVars, matchIndex, logVars,
            durableVars, replyVars, historyVars >>

\* With Pipeline a leader also sends an empty append at the match index
\* The follower holds that entry on disk so it answers without a sync
Confirm(i, j) ==
    /\ Pipeline
    /\ i /= j
    /\ j \in Active
    /\ state[i] = Leader
    /\ LET prev == matchIndex[i][j]
        IN Send({[mtype |-> "AppendEntriesRequest",
            mterm |-> currentTerm[i],
            mprevLogIndex |-> prev,
            mprevLogTerm |-> TermAt(log[i], prev),
            mentries |-> <<>>,
            mcommitIndex |-> commitIndex[i],
            msource |-> i,
            mdest |-> j]})
    /\ UNCHANGED << serverVars, candidateVars, leaderVars, logVars,
            durableVars, replyVars, historyVars >>

\* The highest index a majority holds. The leader counts only what it has on disk
AdvanceCommitIndex(i) ==
    /\ state[i] = Leader
    /\ LET share(k) == IF k /= i THEN matchIndex[i][k]
        ELSE IF LeaderCountsVolatile THEN Len(log[i]) ELSE DurableLen(i)
            agreeing == {n \in 1..Len(log[i]): {k \in Server: share(k) >= n} \in Quorum}
            top == IF agreeing = {} THEN 0 ELSE MaxOf(agreeing)
            allowed == top > 0 /\ (CommitOldTermByCount \/ log[i][top].term = currentTerm[i])
            new == IF allowed THEN Max(commitIndex[i], top) ELSE commitIndex[i]
        IN
            /\ new > commitIndex[i]
            /\ commitIndex' = [commitIndex EXCEPT ![i] = new]
            /\ committed' = committed \union
                {[index |-> n, entry |-> log[i][n], cterm |-> currentTerm[i]]:
                    n \in (commitIndex[i] + 1)..new}
    /\ UNCHANGED << messages, serverVars, candidateVars, leaderVars, log,
            durableVars, replyVars, elections, crashes >>

\* A pre-vote never moves a term
PreVoteExempt(m) ==
    \/ m.mtype = "PreVoteRequest"
    \/ m.mtype = "PreVoteResponse" /\ m.mvoteGranted

\* A member that hears a live leader ignores a vote request from a higher term
Sticky(m) ==
    /\ m.mtype = "RequestVoteRequest"
    /\ LeaderRecent(m.mdest)

UpdateTerm(m) ==
    LET i == m.mdest IN
        /\ m.mterm > currentTerm[i]
        /\ ~PreVoteExempt(m)
        /\ ~Sticky(m)
        /\ currentTerm' = [currentTerm EXCEPT ![i] = m.mterm]
        /\ state' = [state EXCEPT ![i] = Follower]
        /\ votedFor' = [votedFor EXCEPT ![i] = Nil]
        /\ votesGranted' = [votesGranted EXCEPT ![i] = {}]
        /\ preVotes' = [preVotes EXCEPT ![i] = {}]
        /\ UNCHANGED << messages, leaderVars, logVars, durableVars, replyVars,
            historyVars >>

IgnoreSticky(m) ==
    /\ m.mterm > currentTerm[m.mdest]
    /\ Sticky(m)
    /\ Discard(m)
    /\ UNCHANGED << serverVars, candidateVars, leaderVars, logVars,
        durableVars, replyVars, historyVars >>

HandleRequestVoteRequest(m) ==
    LET i == m.mdest
        j == m.msource
        grant ==
            /\ m.mterm = currentTerm[i]
            /\ votedFor[i] \in {Nil, j}
            /\ LogUpToDate(i, m.mlastLogTerm, m.mlastLogIndex)
    IN
        /\ m.mtype = "RequestVoteRequest"
        /\ m.mterm <= currentTerm[i]
        /\ IF grant
            THEN
                /\ votedFor' = [votedFor EXCEPT ![i] = j]
                /\ pending' = [pending EXCEPT ![i] =
                    @ \union {VoteResponseMsg(i, j, currentTerm[i], TRUE, FALSE)}]
                /\ Discard(m)
            ELSE
                /\ Discard(m)
                /\ UNCHANGED << votedFor, pending >>
        /\ UNCHANGED << currentTerm, state, candidateVars, leaderVars, logVars,
                durableVars, heard, historyVars >>

HandleRequestVoteResponse(m) ==
    LET i == m.mdest IN
        /\ m.mtype = "RequestVoteResponse"
        /\ m.mterm <= currentTerm[i]
        /\ votesGranted' = [votesGranted EXCEPT ![i] =
            IF m.mterm = currentTerm[i] /\ state[i] = Candidate /\ m.mvoteGranted
            THEN @ \union {m.msource} ELSE @]
        /\ Discard(m)
        /\ UNCHANGED << serverVars, preVotes, leaderVars, logVars, durableVars,
            replyVars, historyVars >>

HandlePreVoteRequest(m) ==
    LET i == m.mdest
        j == m.msource
        grant ==
            /\ m.mterm >= currentTerm[i]
            /\ LogUpToDate(i, m.mlastLogTerm, m.mlastLogIndex)
            /\ ~LeaderRecent(i)
        term == IF grant THEN m.mterm ELSE currentTerm[i]
    IN
        /\ m.mtype = "PreVoteRequest"
        /\ Reply(VoteResponseMsg(i, j, term, grant, TRUE), m)
        /\ UNCHANGED << serverVars, candidateVars, leaderVars, logVars,
            durableVars, replyVars, historyVars >>

HandlePreVoteResponse(m) ==
    LET i == m.mdest IN
        /\ m.mtype = "PreVoteResponse"
        /\
            \/ m.mvoteGranted /\ m.mterm = currentTerm[i] + 1 /\ state[i] = PreCandidate
                /\ preVotes' = [preVotes EXCEPT ![i] = @ \union {m.msource}]
            \/ ~(m.mvoteGranted /\ m.mterm = currentTerm[i] + 1 /\ state[i] = PreCandidate)
                /\ (m.mvoteGranted \/ m.mterm <= currentTerm[i])
                /\ UNCHANGED preVotes
        /\ Discard(m)
        /\ UNCHANGED << serverVars, votesGranted, leaderVars, logVars, durableVars,
            replyVars, historyVars >>
    
\* One batch is merged in one step. It cuts the log at the first entry that
\* holds another term and appends what follows
HandleAppendEntriesRequest(m) ==
    LET i == m.mdest
        j == m.msource
        prev == m.mprevLogIndex
        entries == m.mentries
        n == Len(entries)
        ok == prev = 0 \/ (prev <= Len(log[i]) /\ log[i][prev].term = m.mprevLogTerm)
        conflicts == {k \in 1..n: prev + k > Len(log[i])
            \/ log[i][prev + k].term /= entries[k].term}
        c == IF conflicts = {} THEN n + 1 ELSE MinOf(conflicts)
        newLog == IF conflicts = {} THEN log[i]
        ELSE SubSeq(log[i], 1, prev + c - 1) \o SubSeq(entries, c, n)
        lastNew == prev + n
    IN
        /\ m.mtype = "AppendEntriesRequest"
        /\ m.mterm = currentTerm[i]
        /\ state[i] /= Leader
        /\ state' = [state EXCEPT ![i] = Follower]
        /\ heard' = [heard EXCEPT ![i] = PreVote]
        /\ IF ok
            THEN
                /\ log' = [log EXCEPT ![i] = newLog]
                /\ commitIndex' = [commitIndex EXCEPT ![i] =
                    Max(@, Min(m.mcommitIndex, lastNew))]
                /\ pending' = [pending EXCEPT ![i] =
                    @ \union {AppendResponseMsg(i, j, TRUE, lastNew,
                        TermAt(newLog, lastNew))}]
                /\ Discard(m)
            ELSE
                /\ Reply(AppendResponseMsg(i, j, FALSE, Len(log[i]), 0), m)
                /\ UNCHANGED << log, commitIndex, pending >>
        /\ UNCHANGED << currentTerm, votedFor, candidateVars, leaderVars,
                durableVars, historyVars >>
    
\* A request from an older term is refused. The refusal is not modelled
\* because it only tells the sender the term and no rule rests on it
RefuseStaleAppend(m) ==
    LET i == m.mdest IN
        /\ m.mtype = "AppendEntriesRequest"
        /\ m.mterm < currentTerm[i]
        /\ Discard(m)
        /\ UNCHANGED << serverVars, candidateVars, leaderVars, logVars,
            durableVars, replyVars, historyVars >>
    
\* A leader never receives an append of its own term
DropLeaderAppend(m) ==
    /\ m.mtype = "AppendEntriesRequest"
    /\ m.mterm = currentTerm[m.mdest]
    /\ state[m.mdest] = Leader
    /\ Discard(m)
    /\ UNCHANGED << serverVars, candidateVars, leaderVars, logVars,
        durableVars, replyVars, historyVars >>

HandleAppendEntriesResponse(m) ==
    LET i == m.mdest
        j == m.msource
        progress == matchIndex[i][j]
        reached == IF MatchIndexAssign THEN m.mmatchIndex ELSE Max(progress, m.mmatchIndex)
    IN
        /\ m.mtype = "AppendEntriesResponse"
        /\ m.mterm <= currentTerm[i]
        /\ IF m.mterm = currentTerm[i] /\ state[i] = Leader
            THEN IF m.msuccess
                THEN
                    /\ matchIndex' = [matchIndex EXCEPT ![i][j] = reached]
                    /\ nextIndex' = [nextIndex EXCEPT ![i][j] = Max(@, m.mmatchIndex + 1)]
                ELSE
                    /\ nextIndex' = [nextIndex EXCEPT ![i][j] =
                        Max(Max(Min(@ - 1, m.mmatchIndex + 1), progress + 1), 1)]
                    /\ UNCHANGED matchIndex
            ELSE UNCHANGED leaderVars
        /\ Discard(m)
        /\ UNCHANGED << serverVars, candidateVars, logVars, durableVars, replyVars,
            historyVars >>

Dispatch(m) ==
    \/ UpdateTerm(m)
    \/ IgnoreSticky(m)
    \/ HandleRequestVoteRequest(m)
    \/ HandleRequestVoteResponse(m)
    \/ HandlePreVoteRequest(m)
    \/ HandlePreVoteResponse(m)
    \/ HandleAppendEntriesRequest(m)
    \/ RefuseStaleAppend(m)
    \/ DropLeaderAppend(m)
    \/ HandleAppendEntriesResponse(m)

Receive == \E m \in messages: Dispatch(m)

Next ==
    \/ \E i \in Active: Restart(i)
    \/ \E i \in Active: Timeout(i)
    \/ \E i \in Active: BecomeCandidate(i)
    \/ \E i \in Active: BecomeLeader(i)
    \/ \E i \in Active: StepDown(i)
    \/ \E i \in Active: ElectionTimeoutElapsed(i)
    \/ \E i \in Active: Persist(i)
    \/ \E i \in Active: \E r \in pending[i]: SendVoteReply(i, r)
    \/ \E i \in Active: \E r \in pending[i]: SendAppendReply(i, r)
    \/ \E i \in Active, v \in Value: ClientRequest(i, v)
    \/ \E i \in Active, j \in Active: AppendEntries(i, j)
    \/ \E i \in Active, j \in Active: Confirm(i, j)
    \/ \E i \in Active: AdvanceCommitIndex(i)
    \/ Receive

Spec == Init /\ [][Next]_vars

\* Members that never campaign are alike. The campaigners stay where they are
Fixed == IF Campaigners = Server THEN Silent ELSE Campaigners \union Silent

Perms == {f \in Permutations(Server): \A c \in Fixed: f[c] = c}

Bounds ==
    /\ \A i \in Server: currentTerm[i] <= MaxTerm
    /\ \A i \in Server: Len(log[i]) <= MaxLogLen
    /\ Cardinality(messages) <= MaxMsgs

TypeOK ==
    /\ \A i \in Server: state[i] \in {Follower, PreCandidate, Candidate, Leader}
    /\ \A i \in Server: votedFor[i] \in Server \union {Nil}
    /\ \A i \in Server: commitIndex[i] <= Len(log[i])

ElectionSafety ==
    \A e1, e2 \in elections: e1.eterm = e2.eterm => e1.eleader = e2.eleader

LogMatching ==
    \A i, j \in Server:
        \A n \in 1..Min(Len(log[i]), Len(log[j])):
            log[i][n].term = log[j][n].term =>
                SubSeq(log[i], 1, n) = SubSeq(log[j], 1, n)

LeaderCompleteness ==
    \A e \in elections: \A c \in committed:
        c.cterm < e.eterm =>
            /\ c.index <= Len(e.elog)
            /\ e.elog[c.index] = c.entry

StateMachineSafety ==
    \A i, j \in Server:
        \A n \in 1..Min(commitIndex[i], commitIndex[j]): log[i][n] = log[j][n]

\* Every committed entry is on disk at a majority
CommittedDurable ==
    \A c \in committed:
        Cardinality({i \in Server:
            /\ c.index <= Len(durableLog[i])
            /\ durableLog[i][c.index] = c.entry}) * 2
        > Cardinality(Server)

Safety ==
    /\ TypeOK
    /\ ElectionSafety
    /\ LogMatching
    /\ LeaderCompleteness
    /\ StateMachineSafety
    /\ CommittedDurable

LeaderAppendOnly ==
[][\A i \in Server :
          state[i] = Leader /\ state'[i] = Leader /\ currentTerm[i] = currentTerm'[i]
            => /\ Len(log'[i]) >= Len(log[i])
               /\ SubSeq(log'[i], 1, Len(log[i])) = log[i]]_vars

MatchIndexMonotonic ==
[][\A i, j \in Server :
          state[i] = Leader /\ state'[i] = Leader /\ currentTerm'[i] = currentTerm[i]
            => matchIndex'[i][j] >= matchIndex[i][j]]_vars

CommitMonotonic ==
[][\A i \in Server : commitIndex'[i] < commitIndex[i] => Restart(i)]_vars

================================================================================