//
// raft.go
// =======
// Write your code in this file
// We will use the original version of all other
// files for testing
//

package raft

//
// API
// ===
// This is an outline of the API that your raft implementation should
// expose.
//
// rf = Make(...)
//   Create a new Raft peer.
//
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new log entry
//
// rf.GetState() (me, term, isLeader)
//   Ask a Raft peer for "me" (see line 58), its current term, and whether it thinks it
//   is a leader
//
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (e.g. tester) on the
//   same peer, via the applyCh channel passed to Make()
//

import (
	"math/rand"
	"sync"
	"time"

	"github.com/cmu440/rpc"
)

//
// ApplyMsg
// ========
//
// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same peer, via the applyCh passed to Make()
//
type ApplyMsg struct {
	Index   int
	Command interface{}
}

//
// Raft struct
// ===========
//
// A Go object implementing a single Raft peer
//
type Raft struct {
	mux             sync.Mutex       // Lock to protect shared access to this peer's state
	peers           []*rpc.ClientEnd // RPC end points of all peers
	me              int              // this peer's index into peers[]
	nodeType        int              // 1 followers, 2 candidates, 3 leader
	currentTerm     int
	votedFor        int
	heartBeat       chan *AppendEntriesArgs
	electionTimeout chan bool
	voteReplyChan   chan *RequestVoteReply
	hbReplyChan     chan *AppendEntriesReply
	timeoutSig      chan bool
	voteCount       int
	newLeader       chan bool
	// stopSendHeartbeat bool

	// Your data here (2A, 2B).
	// Look at the Raft paper's Figure 2 for a description of what
	// state a Raft peer should maintain

}

// setters
func (rf *Raft) setMe(n int) {
	rf.mux.Lock()
	rf.me = n
	rf.mux.Unlock()
}

func (rf *Raft) setNodeType(n int) {
	rf.mux.Lock()
	rf.nodeType = n
	rf.mux.Unlock()
}

func (rf *Raft) setCurrentTerm(term int) {
	rf.mux.Lock()
	rf.currentTerm = term
	rf.mux.Unlock()
}

func (rf *Raft) setVotedFor(n int) {
	rf.mux.Lock()
	rf.votedFor = n
	rf.mux.Unlock()
}

func (rf *Raft) setVoteCount(n int) {
	rf.mux.Lock()
	rf.voteCount = n
	rf.mux.Unlock()
}

// getters

func (rf *Raft) getMe() int {
	rf.mux.Lock()
	res := rf.me
	rf.mux.Unlock()
	return res
}

func (rf *Raft) getNodeType() int {
	rf.mux.Lock()
	res := rf.nodeType
	rf.mux.Unlock()
	return res
}

func (rf *Raft) getCurrentTerm() int {
	rf.mux.Lock()
	res := rf.currentTerm
	rf.mux.Unlock()
	return res
}

func (rf *Raft) getVotedFor() int {
	rf.mux.Lock()
	res := rf.votedFor
	rf.mux.Unlock()
	return res
}

func (rf *Raft) getVoteCount() int {
	rf.mux.Lock()
	res := rf.voteCount
	rf.mux.Unlock()
	return res
}

// var LOGF *log.Logger

//
// GetState()
// ==========
//
// Return "me", current term and whether this peer
// believes it is the leader
//
func (rf *Raft) GetState() (int, int, bool) {

	var me int
	var term int
	var isleader bool
	// Your code here (2A)
	if rf.getNodeType() == 3 {
		isleader = true
	}
	me = rf.getMe()
	term = rf.getCurrentTerm()
	return me, term, isleader
}

//
// RequestVoteArgs
// ===============
//
// Example RequestVote RPC arguments structure
//
// Please note
// ===========
// Field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B)
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// RequestVoteReply
// ================
//
// Example RequestVote RPC reply structure.
//
// Please note
// ===========
// Field names must start with capital letters!
//
//
type RequestVoteReply struct {
	// Your data here (2A)
	Term        int
	VoteGranted bool
}

//
// RequestVote
// ===========
//
// Example RequestVote RPC handler
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B)
	if args.Term < rf.getCurrentTerm() {
		reply.VoteGranted = false
	} else if args.Term == rf.getCurrentTerm() {
		if rf.getVotedFor() != -1 && rf.getVotedFor() != args.CandidateID {
			reply.VoteGranted = false
		} else {
			reply.VoteGranted = true
			rf.setVotedFor(args.CandidateID)
			rf.setNodeType(1)
			rf.timeoutSig <- true
		}
	} else {
		rf.setCurrentTerm(args.Term)
		reply.VoteGranted = true
		rf.setVotedFor(args.CandidateID)
		rf.setNodeType(1)
		rf.timeoutSig <- true
		if rf.getNodeType() == 2 {
			rf.electionTimeout <- true
		}
	}
	reply.Term = rf.getCurrentTerm()
}

//
// sendRequestVote
// ===============
//
// Example code to send a RequestVote RPC to a peer
//
// peer int -- index of the target peer in
// rf.peers[]
//
// args *RequestVoteArgs -- RPC arguments in args
//
// reply *RequestVoteReply -- RPC reply
//
// The types of args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers)
//
// The rpc package simulates a lossy network, in which peers
// may be unreachable, and in which requests and replies may be lost
//
// Call() sends a request and waits for a reply
//
// If a reply arrives within a timeout interval, Call() returns true;
// otherwise Call() returns false
//
// Thus Call() may not return for a while
//
// A false return can be caused by a dead peer, a live peer that
// can't be reached, a lost request, or a lost reply
//
// Call() is guaranteed to return (perhaps after a delay
// *except* if the handler function on the peer side does not return
//
// Thus there
// is no need to implement your own timeouts around Call()
//
// Please look at the comments and documentation in ../rpc/rpc.go
// for more details
//
// If you are having trouble getting RPC to work, check that you have
// capitalized all field names in the struct passed over RPC, and
// that the caller passes the address of the reply struct with "&",
// not the struct itself
//
func (rf *Raft) sendRequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[peer].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// Start
// =====
//
// The service using Raft (e.g. a k/v peer) wants to start
// agreement on the next command to be appended to Raft's log
//
// If this peer is not the leader, return false
//
// Otherwise start the agreement and return immediately
//
// There is no guarantee that this command will ever be committed to
// the Raft log, since the leader may fail or lose an election
//
// The first return value is the index that the command will appear at
// if it is ever committed
//
// The second return value is the current term
//
// The third return value is true if this peer believes it is
// the leader
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B)

	return index, term, isLeader
}

//
// Kill
// ====
//
// The tester calls Kill() when a Raft instance will not
// be needed again
//
// You are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance
//
func (rf *Raft) Kill() {
	// Your code here, if desired

}

//
// Make
// ====
//
// The service or tester wants to create a Raft peer
//
// The port numbers of all the Raft peers (including this one)
// are in peers[]
//
// This peer's port is peers[me]
//
// All the peers' peers[] arrays have the same order
//
// applyCh
// =======
//
// applyCh is a channel on which the tester or service expects
// Raft to send ApplyMsg messages
//
// Make() must return quickly, so it should start Goroutines
// for any long-running work
//
func Make(peers []*rpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	rf.nodeType = 1
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartBeat = make(chan *AppendEntriesArgs)
	rf.electionTimeout = make(chan bool)
	rf.voteReplyChan = make(chan *RequestVoteReply)
	rf.hbReplyChan = make(chan *AppendEntriesReply)
	rf.voteCount = 1
	rf.timeoutSig = make(chan bool)
	rf.newLeader = make(chan bool)
	go rf.mainRoutine()

	return rf
}

func getRand(start int, timeRange int) int {
	return rand.Intn(timeRange) + start
}

func (rf *Raft) mainRoutine() {
	timeRange := time.Duration(getRand(300, 400))
	timer := time.NewTicker(timeRange * time.Millisecond)
	for {
		if rf.getNodeType() == 3 {
			continue
		}
		select {
		case <-rf.timeoutSig:
			timer.Stop()
		case hb := <-rf.heartBeat:
			timer.Stop()
			if hb.Term >= rf.getCurrentTerm() {
				rf.setCurrentTerm(hb.Term)
				rf.setNodeType(1)
			}
		case <-timer.C:
			// rf.setNodeType(2)
			rf.setCurrentTerm(rf.getCurrentTerm() + 1)
			rf.setVotedFor(-1)
			if rf.getNodeType() == 2 {
				rf.electionTimeout <- true
			}
			rf.setNodeType(2)
			go rf.startElection()
		case <-rf.newLeader:
			timer.Stop()
			// fmt.Println("new leader!!!!!!!" + strconv.Itoa(rf.getMe()) + " in term " + strconv.Itoa(rf.getCurrentTerm()))
			rf.setNodeType(3)
			go rf.sendHeatbeat()
			go rf.receiveHeartbeat()
		}
		timer = time.NewTicker(timeRange * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.setVoteCount(1)
	voteReplyChan := make(chan *RequestVoteReply)
	for peer := range rf.peers {
		if peer != rf.me {
			voteRequest := &RequestVoteArgs{
				Term:        rf.getCurrentTerm(),
				CandidateID: rf.getMe(),
			}
			voteReply := &RequestVoteReply{
				VoteGranted: false,
			}
			go rf.vote(peer, voteRequest, voteReply, voteReplyChan)
		}
	}

	rf.receiveReply(voteReplyChan)
}

func (rf *Raft) vote(peer int, voteRequest *RequestVoteArgs, voteReply *RequestVoteReply, voteReplyChan chan *RequestVoteReply) {
	ok := rf.sendRequestVote(peer, voteRequest, voteReply)
	if ok {
		voteReplyChan <- voteReply
	}
}

func (rf *Raft) receiveReply(voteReply chan *RequestVoteReply) {
	for {
		select {
		case <-rf.electionTimeout:
			return
		case voteReply := <-voteReply:
			if voteReply.Term > rf.getCurrentTerm() {
				rf.setCurrentTerm(voteReply.Term)
				rf.setNodeType(1)
				rf.timeoutSig <- true
				return
			}
			if voteReply.VoteGranted {
				rf.setVoteCount(rf.getVoteCount() + 1)
				if rf.getVoteCount() > len(rf.peers)/2 && rf.getNodeType() == 2 {
					rf.setVotedFor(rf.getMe())
					rf.newLeader <- true
					return
				}
			}
		}
	}
}

func (rf *Raft) sendHeatbeat() {
	for {
		for peer := range rf.peers {
			if peer != rf.getMe() {
				if rf.getNodeType() != 3 {
					return
				}
				hb := &AppendEntriesArgs{
					Term:     rf.getCurrentTerm(),
					LeaderID: rf.getMe(),
					Entries:  []string{},
				}
				hbReply := &AppendEntriesReply{}
				go rf.heartbeatRound(peer, hb, hbReply)
			}
		}
		time.Sleep(110 * time.Millisecond)
	}
}

func (rf *Raft) heartbeatRound(peer int, hb *AppendEntriesArgs, hbReply *AppendEntriesReply) {

	ok := rf.sendAppendEntries(peer, hb, hbReply)
	// fmt.Println("*************hb reply after send: " + strconv.FormatBool(ok))
	if ok {
		rf.hbReplyChan <- hbReply
	}
}

func (rf *Raft) receiveHeartbeat() {
	// fmt.Println("start receive hb reply...... ")
	for {
		if rf.getNodeType() != 3 {
			return
		}
		select {
		case hbReply := <-rf.hbReplyChan:
			// fmt.Println("got hb reply: %%%%%%%%%%%%%%%%%%%" + strconv.Itoa(rf.me))
			if rf.getCurrentTerm() < hbReply.Term {
				rf.setNodeType(1)
				rf.setVotedFor(-1)
				rf.setCurrentTerm(hbReply.Term)
				rf.setVoteCount(1)
			}
		}
	}
}

// AppendEntrues RPC
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []string
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// fmt.Println("node " + strconv.Itoa(rf.getMe()) + " receive hb from " + strconv.Itoa(args.LeaderID))
	// fmt.Println("curterm " + strconv.Itoa(rf.getCurrentTerm()) + " from term " + strconv.Itoa(args.Term))
	if args.Term < rf.getCurrentTerm() {
		reply.Term = rf.getCurrentTerm()
		reply.Success = false
	} else {
		if args.Term > rf.getCurrentTerm() {
			rf.setCurrentTerm(args.Term)
			rf.setNodeType(1)
			rf.timeoutSig <- true
		}
		if len(args.Entries) == 0 {
			// fmt.Println("handle as hb!")
			rf.heartBeat <- args
			if args.Term > rf.getCurrentTerm() {
				rf.setCurrentTerm(args.Term)
				rf.setNodeType(1)
				rf.timeoutSig <- true
			}
			reply.Term = rf.getCurrentTerm()
			reply.Success = true
		}
	}

}

func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
	return ok
}
