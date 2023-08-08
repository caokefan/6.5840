package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

type Log struct {
	Index int
	Term  int
	Cmd   interface{}
}

type Snapshot struct {
	hasPendingSnapshot bool
	Data               []byte
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role            Role
	currentTerm     int
	votedFor        int
	votedMe         int
	logs            []Log
	electionTimeout time.Duration
	lastElection    time.Time

	heartbeatTimeout time.Duration
	lastHeartbeat    time.Time
	applyCh          chan<- ApplyMsg
	commitIndex      int
	lastApplied      int

	nextIndex  []int
	matchIndex []int

	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          Snapshot

	claimToBeApplied  sync.Cond
}

func (rf *Raft) resetElectionTimer() {
	electionTimeout := 300 + (rand.Int63() % 300)
	rf.electionTimeout = time.Duration(electionTimeout) * time.Millisecond
	rf.lastElection = time.Now()
}

func (rf *Raft) resetHeartbeatTimer() {
	rf.lastHeartbeat = time.Now()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, !rf.killed() && rf.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if  e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.logs) != nil {
			panic("failed to Encode")
		}
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.currentTerm) != nil ||
	   d.Decode(&rf.votedFor) != nil || 
	   d.Decode(&rf.logs) != nil {
		panic("failed to decode")
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%v]: recv vote request from [%v]", rf.me, args.CandidateId)
	
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	m := Message{Type: Vote, From: args.CandidateId, Term: args.Term}
	ok, termChanged := rf.checkMessage(m)
	if termChanged {
		reply.Term = rf.currentTerm
		defer rf.persist()
	}
	if !ok {
		return
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.eligibleToVote(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateId
		rf.resetElectionTimer()
		reply.VoteGranted = true
	}
}

func (rf *Raft) eligibleToVote(candidateLastLogIndex, candidateLastLogTerm int) bool {
	lastLogIndex := rf.logs[len(rf.logs)-1].Index
	lastLogTerm, _ := rf.term(lastLogIndex)
	return candidateLastLogTerm > lastLogTerm || (candidateLastLogTerm == lastLogTerm && candidateLastLogIndex >= lastLogIndex)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := !rf.killed() && rf.role == Leader
	if !isLeader {
		return 0, 0, false
	}

	index := rf.logs[len(rf.logs)-1].Index + 1
	log := Log{Index: index, Term: rf.currentTerm, Cmd: command}
	rf.logs = append(rf.logs, log)
	rf.persist()

	rf.broadcastAppendEntries(true)

	return index, rf.currentTerm, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) becomeCandidate() {
	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.votedMe = 1
	rf.resetElectionTimer()
	rf.persist()
}

func (rf *Raft) becomeLeader() {
	rf.role = Leader
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.logs[len(rf.logs)-1].Index + 1
		rf.matchIndex[i] = 0
	}
	
}

func (rf *Raft) candidateSendRequestVote(server int, args *RequestVoteArgs) {
	reply := &RequestVoteReply{}
	if ok := rf.sendRequestVote(server, args, reply); ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		m := Message{Type: VoteReply, From: server, Term: reply.Term, ArgsTerm: args.Term}
		ok, termChanged := rf.checkMessage(m)
		if termChanged {
			defer rf.persist()
		}
		if !ok {
			return
		}

		if reply.VoteGranted {
			rf.votedMe++ 
			if 2*rf.votedMe > len(rf.peers) {
				rf.becomeLeader()
			}
		}
	}
}

func (rf *Raft) broadcastRequestVote() {
	for i := range rf.peers {
		if i != rf.me {
			lastLogIndex := rf.logs[len(rf.logs)-1].Index
			lastLogTerm, _ := rf.term(lastLogIndex)
			args := &RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm: lastLogTerm,
			}
			go rf.candidateSendRequestVote(i, args)
		}
	}
}


func (rf *Raft) becomeFollower(term int) bool {
	rf.role = Follower
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		return true
	}
	return false
}

type AppendEntriesArgs struct {
	Term			int
	LeaderId		int
	PrevLogIndex	int
	PrevLogTerm		int
	Entries			[]Log
	LeaderCommit	int	
}

type Err int

const (
	Rejected Err = iota
	Matched
	IndexNotMatched
	TermNotMatched
)

type AppendEntriesReply struct {
	Term			int
	Err				Err
	LastLogIndex	int
	ConflictIndex	int
	ConflictTerm	int
}

func (rf *Raft) clone(entries []Log) []Log {
	cloned := make([]Log, len(entries))
	copy(cloned, entries)
	return cloned
}

func (rf* Raft) slice(start, end int) []Log {
	if start == end {
		return nil
	}
	start = start - rf.logs[0].Index
	end = end - rf.logs[0].Index
	return rf.clone(rf.logs[start:end])
}

func (rf* Raft) term(index int) (int, error) {
	if index < rf.logs[0].Index || index > rf.logs[len(rf.logs)-1].Index {
		return 0, errors.New("index out of bound")
	}
	index = index - rf.logs[0].Index
	return rf.logs[index].Term, nil
}

func (rf *Raft) checkLogPrefixMatched(leaderPrevLogIndex, leaderPrevLogTerm int) Err {
	prevLogTerm, err := rf.term(leaderPrevLogIndex)
	if err != nil {
		return IndexNotMatched
	}
	if prevLogTerm != leaderPrevLogTerm {
		return TermNotMatched
	}
	return Matched
}

func (rf *Raft) findFirstConflict(index int) (int, int) {
	conflictTerm, _ := rf.term(index)
	firstConflictIndex := index
	for i := index - 1; i > rf.logs[0].Index; i-- {
		if term, _ := rf.term(i); term != conflictTerm {
			break
		}
		firstConflictIndex = i
	}
	return conflictTerm, firstConflictIndex
}

func (rf *Raft) truncateSuffix(index int) {
	if index <= rf.logs[0].Index || index > rf.logs[len(rf.logs)-1].Index {
		return
	}
	index = index - rf.logs[0].Index
	if len(rf.logs[index:]) > 0 {
		rf.logs = rf.logs[:index]
	}
}
 
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	DPrintf("[%v]: recv AppendEntries", rf.me)
	reply.Term = rf.currentTerm
	reply.Err = Rejected
	m := Message{Type: Append, From: args.LeaderId, Term: args.Term}
	ok, termChanged := rf.checkMessage(m)
	if termChanged {
		reply.Term = rf.currentTerm
		defer rf.persist()
	}
	if !ok {
		return
	}

	DPrintf("[%v]: valid", rf.me)
	reply.Err = rf.checkLogPrefixMatched(args.PrevLogIndex, args.PrevLogTerm)
	if reply.Err != Matched {
		if reply.Err == IndexNotMatched {
			reply.LastLogIndex = rf.logs[len(rf.logs)-1].Index
		} else {
			reply.ConflictTerm, reply.ConflictIndex = rf.findFirstConflict(args.PrevLogIndex)
		}
		return
	}

	for i, entry := range args.Entries {
		if term, err := rf.term(entry.Index); err != nil || term != entry.Term {
			rf.truncateSuffix(entry.Index)
			rf.logs = append(rf.logs, args.Entries[i:]...)
			rf.persist()
			break
		}
	}

	lastNewLogIndex := min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
	DPrintf("==================lastNewLogIndex is %v", lastNewLogIndex)
	rf.maybeCommittedTo(lastNewLogIndex)
}

func (rf *Raft) maybeCommittedTo(index int) {
	if index > rf.commitIndex {
		rf.commitIndex = index
		rf.claimToBeApplied.Signal()
	}
}

func (rf *Raft) getLogEntry(index int) Log {
	if index < 0 || index >= len(rf.logs) {
		return Log{}
	} else {
		return rf.logs[index]
	}
}

type MessageType string

const (
	Vote 		MessageType = "RequestVote"
	VoteReply	MessageType = "RequestVoteReply"
	Append		MessageType = "AppendEntries"
	AppendReply	MessageType = "AppendEntriesReply"
	Snap		MessageType = "InstallSnapshot"
	SnapReply	MessageType = "InstallSnapshotReply"
)

type Message struct {
	Type			MessageType
	From			int
	Term			int
	ArgsTerm		int
	PrevLogIndex	int
}

func (rf *Raft) quorumMatched(index int) bool {
	matched := 1
	// DPrintf("start quorumMatched")
	// DPrintf("%v", rf.matchIndex)
	for idx, i := range rf.matchIndex {
		DPrintf("[%v]: %v match %v", rf.me, idx, i)
		if i >= index {
			matched++
		}
	} 
	// DPrintf("matched is %v", matched)
	return 2*matched > len(rf.peers)
}

func (rf *Raft) committedTo(index int) {
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
}

func (rf *Raft) maybeCommitMatched(index int) bool {
	for i := index; i > rf.commitIndex; i-- {
		term, e := rf.term(i)
		if e!=nil {
			DPrintf(e.Error())
		}
		if term == rf.currentTerm && rf.quorumMatched(i) {
			DPrintf("[%v]: commit %v", rf.me, i)
			rf.committedTo(i)
			rf.claimToBeApplied.Signal()
			return true
		}
	}
	DPrintf("[%v]: can't commit %v", rf.me, index)
	return false
}

func (rf *Raft) leaderSendAppendEntries(server int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	DPrintf("[%v]: append to %v", rf.me, server)
	if ok := rf.peers[server].Call("Raft.AppendEntries", args, reply); ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		m := Message{
			Type: AppendReply,
			From: server,
			Term: reply.Term,
			ArgsTerm: args.Term,
			PrevLogIndex: args.PrevLogIndex,
		}
		ok, termChanged := rf.checkMessage(m)
		if termChanged {
			defer rf.persist()
		}
		if !ok {
			return
		}

		DPrintf("[%v]: handleAppendEntries, Err: %v", rf.me, reply.Err)
		switch reply.Err {
		case Rejected:

		case Matched:
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			DPrintf("[%v]: matchIndex[%v]=%v", rf.me, server, rf.matchIndex[server])
			DPrintf("%v", rf.matchIndex)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			if rf.maybeCommitMatched(rf.matchIndex[server]) {
				DPrintf("successfully commit %v", rf.matchIndex[server])
				rf.broadcastAppendEntries(true)
			}
		case IndexNotMatched:
			if reply.LastLogIndex < rf.logs[len(rf.logs)-1].Index {
				rf.nextIndex[server] = reply.LastLogIndex + 1
			} else {
				rf.nextIndex[server] = rf.logs[len(rf.logs)-1].Index + 1
			}
			rf.broadcastAppendEntries(true)
		case TermNotMatched:
			newNextIndex := reply.ConflictIndex
			for i := rf.logs[len(rf.logs)-1].Index; i > rf.logs[0].Index; i-- {
				if term, _ := rf.term(i); term == reply.ConflictTerm {
					newNextIndex = i
					break
				}
			}
			rf.nextIndex[server] = newNextIndex
			rf.broadcastAppendEntries(true)
		}
	
		
	}
}

func (rf *Raft) broadcastAppendEntries(forced bool) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		if forced || rf.logs[len(rf.logs)-1].Index >= rf.nextIndex[i] {
			nextIndex := rf.nextIndex[i]
			entries := rf.slice(nextIndex, rf.logs[len(rf.logs)-1].Index+1)
			prevLogIndex := nextIndex - 1
			prevLogTerm, _ := rf.term(prevLogIndex)
			args := &AppendEntriesArgs{
				Term: rf.currentTerm,
				LeaderId: rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm: prevLogTerm,
				Entries: entries,
				LeaderCommit: rf.commitIndex,
			}
			go rf.leaderSendAppendEntries(i, args)
		}
	}
}

func (rf *Raft) checkTerm(m Message) (bool, bool) {
	if m.Term < rf.currentTerm {
		return false, false
	}
	if m.Term > rf.currentTerm || (m.Type == Append || m.Type == Snap) {
		termChanged := rf.becomeFollower(m.Term)
		return true, termChanged
	}
	return true, false
}

func (rf *Raft) checkState(m Message) bool {
	eligible := false
	switch m.Type {
	case Vote:
		fallthrough
	case Append:
		eligible = rf.role == Follower
	case Snap:
		eligible = rf.role == Follower && !rf.snapshot.hasPendingSnapshot
	case VoteReply:
		eligible = rf.role == Candidate && rf.currentTerm == m.ArgsTerm
	case AppendReply:
		eligible = rf.role == Leader && rf.currentTerm == m.ArgsTerm && rf.nextIndex[m.From]-1 == m.PrevLogIndex
	case SnapReply:
		eligible = rf.role == Leader && rf.currentTerm == m.ArgsTerm && true //fixme
	default:

	}
	if rf.role == Follower && (m.Type == Append || m.Type == Snap) {
		rf.resetElectionTimer()
	}
	return eligible
}

func (rf *Raft) checkMessage(m Message) (bool, bool) {
	ok, termChanged := rf.checkTerm(m)
	if !ok || !rf.checkState(m) {
		return false, termChanged
	}
	return true, termChanged
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		switch rf.role {
		case Follower:
			fallthrough
		case Candidate:
			if time.Since(rf.lastElection) > rf.electionTimeout {
				rf.becomeCandidate()
				rf.broadcastRequestVote()
			}
		case Leader:
			// if rf.activePeers() * 2 < len(rf.peers) {
			// 	rf.becomeFollower(rf.currentTerm)
			// 	break
			// }
			forced := false
			if time.Since(rf.lastHeartbeat) > rf.heartbeatTimeout {
				forced = true
				rf.resetHeartbeatTimer()
			}
			rf.broadcastAppendEntries(forced)
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 100)
		time.Sleep(50 * time.Millisecond)
	}
}

func (rf *Raft) newCommittedEntries() []Log {
	start := rf.lastApplied + 1 - rf.logs[0].Index
	end := rf.commitIndex + 1 - rf.logs[0].Index
	if start >= end {
		return nil
	}
	return rf.clone(rf.logs[start:end])
}

func (rf *Raft) appliedTo(index int) {
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
}

func (rf *Raft) commiter() {
	cnt := 1
	rf.mu.Lock()
	for !rf.killed() {
		if rf.snapshot.hasPendingSnapshot {

		} else if newCommittedEntries := rf.newCommittedEntries(); len(newCommittedEntries) > 0 {
			rf.mu.Unlock()
			for _, entry := range newCommittedEntries {
				DPrintf("[%v]: apply Msg %v", rf.me, entry.Index)
				rf.applyCh <- ApplyMsg{CommandValid: true, Command: entry.Cmd, CommandIndex: entry.Index}
			}
			rf.mu.Lock()
			rf.appliedTo(newCommittedEntries[len(newCommittedEntries)-1].Index)
		} else {
			cnt++
			DPrintf("[%v]: %vth", rf.me, cnt)
			rf.claimToBeApplied.Wait()
		}
	}
	rf.mu.Unlock()
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = Follower
	rf.votedMe = 0

	rf.logs = []Log{{Index: 0, Term: 0}}
	if rf.persister.RaftStateSize() > 0 {
		rf.readPersist(rf.persister.ReadRaftState())
	} else {
		rf.currentTerm = 0
		rf.votedFor = -1
	}
	

	rf.resetElectionTimer()

	rf.heartbeatTimeout = 150 * time.Millisecond
	rf.applyCh = applyCh
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.snapshot = Snapshot{
		hasPendingSnapshot: false,
		Data:               nil,
	}

	rf.claimToBeApplied = *sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash
	// rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	DPrintf("start...\n")
	go rf.commiter()
	go rf.ticker()
	// go rf.commiter()

	return rf
}
