------------------------------ MODULE TraceCheck -------------------------------
\* Replays a simulator trace line by line against the model.
\* A trace that matches to the end leaves NotDone violated.
EXTENDS Raft, Json, TLC

CONSTANT TraceFile

VARIABLES ti, consumed

traceVars == << ti, consumed >>

Trace == Tail(ndJsonDeserialize(TraceFile))

Line == Trace[ti]

Done == ti > Len(Trace)

NotDone == ~Done

TraceInit ==
    /\ Init
    /\ ti = 1
    /\ consumed = FALSE

ConsumeMessage(m) ==
    \/ IgnoreSticky(m)
    \/ HandleRequestVoteRequest(m)
    \/ HandleRequestVoteResponse(m)
    \/ HandlePreVoteRequest(m)
    \/ HandlePreVoteResponse(m)
    \/ HandleAppendEntriesRequest(m)
    \/ RefuseStaleAppend(m)
    \/ DropLeaderAppend(m)
    \/ HandleAppendEntriesResponse(m)

\* The event of the line happens once
Consume(l) ==
    LET i == l.i IN
        \/ l.ev = "tick" /\ (Timeout(i) \/ StepDown(i))
        \/ l.ev = "message" /\ ConsumeMessage(l.msg)
        \/ l.ev = "persisted" /\ Persist(i)
        \/ l.ev = "propose" /\ \E v \in Value: ClientRequest(i, v)
        \/ l.ev = "restart" /\ Restart(i)
    
\* What the core does inside the same step. The order does not matter
Internal(l) ==
    LET i == l.i IN
        \/ ElectionTimeoutElapsed(i)
        \/ ~consumed /\ l.ev = "message" /\ UpdateTerm(l.msg)
        \/ consumed /\ BecomeCandidate(i)
        \/ consumed /\ BecomeLeader(i)
        \/ consumed /\ AdvanceCommitIndex(i)
        \/ consumed /\ \E r \in pending[i]: SendVoteReply(i, r)
        \/ consumed /\ \E r \in pending[i]: SendAppendReply(i, r)

AckMessages ==
{m \in messages:
    \/ m.mtype = "AppendEntriesResponse" /\ m.msuccess
    \/ m.mtype = "RequestVoteResponse" /\ m.mvoteGranted}

LoggedAcks(l) == {l.acks[k]: k \in 1..Len(l.acks)}

Matches(l) ==
    LET i == l.i IN
        /\ currentTerm[i] = l.term
        /\ state[i] = l.role
        /\ votedFor[i] = l.voted
        /\ log[i] = l.log
        /\ commitIndex[i] = l.commit
        /\ DurableLen(i) = l.durable
        /\ AckMessages = LoggedAcks(l)

ConsumeStep ==
    /\ ti <= Len(Trace)
    /\ ~consumed
    /\ Consume(Line)
    /\ consumed' = TRUE
    /\ UNCHANGED ti

InternalStep ==
    /\ ti <= Len(Trace)
    /\ Internal(Line)
    /\ UNCHANGED traceVars

CloseStep ==
    /\ ti <= Len(Trace)
    /\ consumed
    /\ Matches(Line)
    /\ messages' = {}
    /\ ti' = ti + 1
    /\ consumed' = FALSE
    /\ UNCHANGED << serverVars, candidateVars, leaderVars, logVars, durableVars,
        replyVars, historyVars >>

TraceNext == ConsumeStep \/ InternalStep \/ CloseStep

TraceSpec == TraceInit /\ [][TraceNext]_<< vars, traceVars >>

================================================================================