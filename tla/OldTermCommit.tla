----------------------------- MODULE OldTermCommit -----------------------------
\* Starts after two elections. S1 and S3 each hold an entry of their own term.
\* A leader that commits an old-term entry by counting can then lose it.
EXTENDS Raft

CONSTANTS S1, S2, S3

ASSUME Server = {S1, S2, S3}

Entry1 == [term |-> 1, value |-> Noop]
Entry2 == [term |-> 2, value |-> Noop]

Held == [i \in Server |-> IF i = S1 THEN << Entry1 >> ELSE IF i = S3 THEN << Entry2 >> ELSE <<>>]

InitMidway ==
    /\ messages = {}
    /\ currentTerm = [i \in Server |-> IF i = S1 THEN 1 ELSE 2]
    /\ state = [i \in Server |-> Follower]
    /\ votedFor = [i \in Server |-> IF i = S1 THEN S1 ELSE S3]
    /\ votesGranted = [i \in Server |-> {}]
    /\ preVotes = [i \in Server |-> {}]
    /\ nextIndex = [i \in Server |-> [j \in Server |-> 1]]
    /\ matchIndex = [i \in Server |-> [j \in Server |-> 0]]
    /\ log = Held
    /\ commitIndex = [i \in Server |-> 0]
    /\ durableTerm = currentTerm
    /\ durableVote = votedFor
    /\ durableLog = Held
    /\ heard = [i \in Server |-> FALSE]
    /\ pending = [i \in Server |-> {}]
    /\ elections = {[eterm |-> 1, eleader |-> S1, elog |-> Held[S1]],
        [eterm |-> 2, eleader |-> S3, elog |-> Held[S3]]}
    /\ committed = {}
    /\ crashes = 0

SpecMidway == InitMidway /\ [][Next]_vars

================================================================================