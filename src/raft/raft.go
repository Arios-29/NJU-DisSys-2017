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
	"log"
	"sync"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

type Log struct {
	Term     int
	LogIndex int
	Command  interface{}
}

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//三种状态
const (
	leader    = 0
	candidate = 1
	follower  = 2
)

// Raft : A Go object implementing a single Raft peer.
type Raft struct {
	mu                     sync.Mutex
	peers                  []*labrpc.ClientEnd
	persister              *Persister
	me                     int // index into peers[]
	heartBeatMissedCounter int // election timeout

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//所有状态都需要持久化的属性
	state    int   // 节点当前状态
	term     int   //节点current term
	votedFor int   //本节点当前term投票给了哪个节点
	log      []Log //日志

	//volatile state on all servers
	commitIndex int //对本节点而言已知的最新一条committed的日志的索引
	lastApplied int //本节点最新一条已被applied的日志的索引

	//volatile state on a leader
	nextIndex  []int // nextIndex[i]表示要让follower_i同步的下一条日志的索引
	matchIndex []int // matchIndex[i]表示与follower_i匹配的最后一条日志的索引

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//====================================================================================================================

// AppendEntriesArgs : example AppendEntries RPC arguments structure
type AppendEntriesArgs struct {
	Term        int   // leader's term
	LeaderId    int   //leader的id
	PreLogIndex int   //要同步的日志的前一条日志的索引
	PreLogTerm  int   //要同步的的日志的前一条日志的term
	CommitIndex int   //leader已经committed的最近一条日志的索引
	Entries     []Log //需要同步的日志
}

// AppendEntriesReply : example AppendEntries RPC reply structure
type AppendEntriesReply struct {
	Term        int  // follower的current term
	Success     bool //AppendEntries是否成功
	CommitIndex int  //用于告知leader本节点已知的最近一条committed的日志的索引
}

func Min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
func Max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

// AppendEntries : example AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// leader的term大于或等于本节点的term
	if args.Term >= rf.term {
		rf.term = args.Term       //更新本节点的term
		if rf.state != follower { //term更小的leader或candidate要转变为本leader的follower
			rf.TurnFollower(rf.term)
			rf.votedFor = args.LeaderId
		}
		rf.persist()

		//要同步的日志为空,说明此时为心跳信号
		if len(args.Entries) == 0 {
			log.Printf("Leader %d in term %d sends Peer %d in term %d HeartBeat", args.LeaderId, args.Term, rf.me, rf.term)
			// 更新本节点committed的log索引
			if rf.commitIndex < args.CommitIndex {
				if args.PreLogIndex < len(rf.log) && rf.log[args.PreLogIndex].Term == args.PreLogTerm {
					rf.commitIndex = Min(args.PreLogIndex, args.CommitIndex)
				}
			}
			rf.heartBeatMissedCounter = 0 //收到valid心跳信号,重新计时
			reply.Success = true
			reply.CommitIndex = rf.commitIndex
			reply.Term = args.Term
			return
		} else { //要同步的日志不为空
			log.Printf("Leader %d in term %d ask Peer %d in term %d to append entries(index starts from %d)", args.LeaderId, args.Term, rf.me, rf.term, args.PreLogIndex+1)
			// 本节点已经committed过了leader发来的日志记录
			if rf.commitIndex >= args.PreLogIndex+len(args.Entries) {
				log.Printf("Peer % d in term %d has already committed the entries Leader %d in term %d sends", rf.me, rf.term, args.LeaderId, args.Term)
				reply.Success = true
				reply.Term = rf.term
				reply.CommitIndex = rf.commitIndex
				return
			}
			// 没有匹配到追加点
			if args.PreLogIndex >= len(rf.log) || rf.log[args.PreLogIndex].Term != args.PreLogTerm {
				log.Printf("Leader %d in term %d ask Peer %d in term %d to append entries(index starts from %d), not match", args.LeaderId, args.Term, rf.me, rf.term, args.PreLogIndex+1)
				reply.Success = false
				reply.Term = args.Term
				reply.CommitIndex = rf.commitIndex
				return
			} else { //匹配到追加点
				log.Printf("Leader %d in term %d ask Peer %d in term %d to append entries(index starts from %d), match", args.LeaderId, args.Term, rf.me, rf.term, args.PreLogIndex+1)
				//同步leader发来的日志
				rf.log = rf.log[0 : args.PreLogIndex+1]
				for _, entry := range args.Entries {
					rf.log = append(rf.log, entry)
				}
				reply.Term = rf.term
				reply.Success = true
				//更新本节点committed的日志的索引
				if rf.commitIndex < args.CommitIndex {
					rf.commitIndex = Min(args.CommitIndex, Max(args.PreLogIndex, rf.commitIndex))
				}
				reply.CommitIndex = rf.commitIndex
				rf.persist()
				return
			}
		}
	} else {
		log.Printf("Leader %d in term %d find a Peer %d whose term %d is bigger", args.LeaderId, args.Term, rf.me, rf.term)
		reply.Term = rf.term  //让leader更新term
		reply.Success = false //拒绝承认该leader
		reply.CommitIndex = rf.commitIndex
		return
	}
}

//=====================================================================================================================

// RequestVoteArgs : example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	Term         int // candidate's term
	Candidate    int //请求投票的candidate的id
	LastLogIndex int // candidate的日志中最后一条entry的索引
	LastLogTerm  int // candidate的日志中最后一条entry的term
}

// RequestVoteReply : example RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term        int  //投票者的current term
	VoteGranted bool //投票与否
}

// RequestVote : example RequestVote RPC handler.
// DONE
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// candidate的term小于或等于voter的term
	if args.Term <= rf.term {
		log.Printf("refuse to vote: Candidate %d in term %d requests vote, but its term is less than Voter %d in term %d", args.Candidate, args.Term, rf.me, rf.term)
		reply.Term = args.Term    //让该Candidate更新term，转为follower
		reply.VoteGranted = false //拒绝投票
		return
	} else {
		// 判断candidate的log是不是更up-to-date
		// Rule 1: candidate的log的最后一条日志的term更大,candidate的log更up-to-date
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term {
			log.Printf("agree to vote: Candidate %d in term %d has a more up-to-date log than Voter %d in term %d with Rule 1", args.Candidate, args.Term, rf.me, rf.term)
			rf.TurnFollower(args.Term) //voter成为该candidate的follower
			rf.votedFor = args.Candidate
			rf.persist()
			reply.Term = rf.term
			reply.VoteGranted = true //同意投票
			return
		}
		// Rule 2: candidate的log的最后一条日志的term与voter相同,则判断log长度
		if args.LastLogTerm == rf.log[len(rf.log)-1].Term {
			// candidate的log更长或相同长, candidate的log更up-to-date
			if args.LastLogIndex >= len(rf.log) {
				log.Printf("agree to vote: Candidate %d in term %d has a more up-to-date log than Voter %d in term %d with Rule2", args.Candidate, args.Term, rf.me, rf.term)
				rf.TurnFollower(args.Term)
				rf.votedFor = args.Candidate
				rf.persist()
				reply.Term = rf.term
				reply.VoteGranted = true //同意投票
				return
			} else {
				log.Printf("refuse to vote: Candidate %d in term %d has a shorter log than Voter %d in term %d", args.Candidate, args.Term, rf.me, rf.term)
				if rf.state == leader {
					//candidate的term大于本voter的term,如果本voter是leader则必须变成follower
					rf.TurnFollower(args.Term)
				}
				//follower和candidate只需要更新term,不用重新计时election timeout
				rf.term = args.Term
				rf.persist()
				reply.Term = rf.term
				reply.VoteGranted = false //拒绝投票
				return
			}
		}
		// candidate的log的最后一条日志的term小于voter, voter的log更up-to-date
		if args.LastLogTerm < rf.log[len(rf.log)-1].Term {
			log.Printf("refuse to vote: Candidate %d in term %d has a less up-to-date log than Voter %d in term %d", args.Candidate, args.Term, rf.me, rf.term)
			if rf.state == leader {
				//candidate的term大于本voter的term,如果本voter是leader则必须变成follower
				rf.TurnFollower(args.Term)
			}
			//follower和candidate只需要更新term,不用重新计时election timeout
			rf.term = args.Term
			rf.persist()
			reply.Term = rf.term
			reply.VoteGranted = false //拒绝投票
			return
		}
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//====================================================================================================================

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// TurnFollower : 变为follower状态并重新计时election timeout
func (rf *Raft) TurnFollower(term int) {
	log.Printf("Peer %d in term %d turns a follower", rf.me, rf.term)
	rf.heartBeatMissedCounter = 0 //election timeout设置为0
	rf.term = term
	rf.state = follower
	rf.votedFor = -1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
