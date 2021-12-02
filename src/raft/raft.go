package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isLeader)
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
	"encoding/gob"
	"log"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)
import "labrpc"

type Log struct {
	Term        int
	LogIndex    int
	Command     interface{}
	IsEmpty     bool
	IsHeartBeat bool
}

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	Valid       bool
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
	me                     int   // index into peers[]
	heartBeatMissedCounter int   // election timeout
	dead                   int32 //如果dead==1，则说明该peer被移除

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

	applyCh  *chan ApplyMsg
	logCount int
}

//=====================================================================================================================
// persist
//save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	if e.Encode(rf.term) != nil || e.Encode(rf.votedFor) != nil || e.Encode(rf.log) != nil {
		log.Printf("Peer %d Persist Failed", rf.me)
		return
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	log.Printf("Peer %d Persist Successed", rf.me)
}

// readPersist
// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var term int
	var votedFor int
	var logs []Log
	if d.Decode(&term) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		log.Printf("Peer %d readRersist Failed", rf.me)
		return
	} else {
		rf.mu.Lock()
		rf.term = term
		rf.votedFor = votedFor
		rf.log = logs
		log.Printf("Peer %d readPersist Successed", rf.me)
		rf.mu.Unlock()
	}
}

//====================================================================================================================

// AppendEntriesArgs : example AppendEntries RPC arguments structure
type AppendEntriesArgs struct {
	Term        int   // leader's term
	LeaderId    int   //leader的id
	PreLogIndex int   //要同步的日志的前一条日志的索引
	PreLogTerm  int   //要同步的的日志的前一条日志的term
	CommitIndex int   //leader已经committed的最近一条日志的索引,告知follower让其知道哪些日志可以apply
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

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// SendHeartBeat : leader发送心跳信号
func (rf *Raft) SendHeartBeat() {
	for {
		time.Sleep(time.Millisecond * 100)
		rf.mu.Lock()
		if rf.state == leader { //只有leader向外发送心跳信号
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me { //不给自己发心跳信号
					continue
				}
				emptyLog := make([]Log, 0)
				args := &AppendEntriesArgs{rf.term, rf.me, rf.nextIndex[i] - 1, rf.log[rf.nextIndex[i]-1].Term, rf.commitIndex, emptyLog}
				reply := &AppendEntriesReply{0, false, 0} //初始化的reply,无意义
				serverID := i
				go func() {
					rf.SendAppendEntries(serverID, args, reply)
				}() //多线程发送心跳信号
			}
		}
		rf.mu.Unlock()
		if rf.killed() {
			return
		}
	}
}

// SendAppendEntriesToPeers : leader发送同步命令
func (rf *Raft) SendAppendEntriesToPeers(peerID int) {
	for {
		time.Sleep(time.Millisecond * 50) //每隔50ms检查同步是否完成
		if rf.killed() {
			return
		}
		for {
			rf.mu.Lock()
			if rf.state != leader { //只有leader才会sendAppendEntries
				rf.mu.Unlock()
				return
			}
			if rf.nextIndex[peerID] >= len(rf.log) { //leader已经把要同步的日志和peerID的peer同步完成了
				rf.mu.Unlock()
				break
			}
			log.Printf("Leader %d in term %d sends logs(%d ~ %d) to Peer %d", rf.me, rf.term, rf.nextIndex[peerID], len(rf.log), peerID)
			entries := make([]Log, len(rf.log[rf.nextIndex[peerID]:len(rf.log)]))
			copy(entries, rf.log[rf.nextIndex[peerID]:len(rf.log)])
			args := &AppendEntriesArgs{rf.term, rf.me, rf.nextIndex[peerID] - 1, rf.log[rf.nextIndex[peerID]-1].Term, rf.commitIndex, entries}
			reply := &AppendEntriesReply{0, false, 0} //初始化的reply,无意义
			rf.mu.Unlock()
			wait := sync.WaitGroup{}
			wait.Add(1)
			ok := false

			go func() {
				ok = rf.SendAppendEntries(peerID, args, reply)
				wait.Done()
			}()

			ch := make(chan bool)
			go func() {
				wait.Wait()
				ch <- false
			}()

			select {
			case <-time.After(time.Millisecond * 100): //超时
				rf.mu.Lock()
				log.Printf("Timeout: Leader %d in term %d sends logs(%d ~ %d) to Peer %d", rf.me, rf.term, rf.nextIndex[peerID], len(rf.log), peerID)
				rf.mu.Unlock()
			case <-ch: //完成
				rf.mu.Lock()
				log.Printf("Done: Leader %d in term %d sends logs(%d ~ %d) to Peer %d", rf.me, rf.term, rf.nextIndex[peerID], len(rf.log), peerID)
				rf.mu.Unlock()
			}

			if ok {
				if reply.Success { //同步成功
					rf.mu.Lock()
					rf.matchIndex[peerID] = Max(rf.matchIndex[peerID], rf.nextIndex[peerID]+len(entries)-1) //更新对该peer的match点
					rf.nextIndex[peerID] += len(entries)                                                    //更新对该peer的nextIndex
					log.Printf("Success: Leader %d in term %d sends logs(%d ~ %d) to Peer %d", rf.me, rf.term, rf.nextIndex[peerID], len(rf.log), peerID)
					rf.mu.Unlock()
				} else { //同步失败
					rf.mu.Lock()
					if reply.Term <= rf.term { //因为没有match所有同步失败
						rf.nextIndex[peerID] = reply.CommitIndex + 1
						log.Printf("Failed (Unmatch): Leader %d in term %d sends logs(%d ~ %d) to Peer %d", rf.me, rf.term, rf.nextIndex[peerID], len(rf.log), peerID)
						rf.mu.Unlock()
					} else { //发现term更大的peer
						rf.mu.Unlock()
						return
					}
				}
			} else {
				break
			}

		}
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
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//====================================================================================================================

func (rf *Raft) GetEmptyLogCount() int {
	count := 0
	for _, entry := range rf.log {
		if entry.IsEmpty {
			count++
		}
	}
	return count
}

// Start
//the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise, start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != leader {
		isLeader = false
		log.Printf("Peer %d in term %d does not start as a Leader", rf.me, rf.term)
	} else {
		index = len(rf.log) - rf.GetEmptyLogCount() + 1
		entry := Log{rf.term, index, command, false, false}
		rf.log = append(rf.log, entry)
		term = rf.term
		rf.persist()
		log.Printf("Peer %d in term %d starts as a Leader", rf.me, rf.term)
	}
	return index, term, isLeader
}

// Kill
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	dead := atomic.LoadInt32(&rf.dead)
	return dead == 1
}

//===================================================================================================================

// TurnFollower : 变为follower状态并重新计时election timeout
func (rf *Raft) TurnFollower(term int) {
	log.Printf("Peer %d in term %d turns a follower", rf.me, rf.term)
	rf.heartBeatMissedCounter = 0 //election timeout设置为0
	rf.term = term
	rf.state = follower
	rf.votedFor = -1
}

// TurnLeader :成为leader状态
func (rf *Raft) TurnLeader() {
	log.Printf("Peer %d in term %d turns a leader", rf.me, rf.term)
	rf.votedFor = -1
	rf.state = leader
	rf.heartBeatMissedCounter = 0
	for i := 0; i < len(rf.matchIndex); i++ {
		rf.nextIndex[i] = len(rf.log) //第一次默认同步下一条日志
		rf.matchIndex[i] = 0          //默认匹配点为0
	}
	// 同步一条空日志通知其他节点
	entry := Log{rf.term, 0, -1, true, false}
	rf.log = append(rf.log, entry)
	rf.persist()
	for peerID := 0; peerID < len(rf.peers); peerID++ {
		if peerID == rf.me {
			continue
		}
		go rf.SendAppendEntriesToPeers(peerID)
	}
}

// TurnCandidate :成为candidate状态并发起投票
func (rf *Raft) TurnCandidate() {
	for {
		electionTimeout := rand.Intn(100) //election timeout是0~100ms中的一个随机值
		time.Sleep(time.Millisecond * time.Duration(electionTimeout))
		if rf.killed() {
			return
		}

		rf.mu.Lock()
		if rf.state == follower || rf.state == leader { //其他peer先成为candidate
			rf.mu.Unlock()
			return
		}
		//成为candidate
		rf.term += 1        //增加term
		rf.votedFor = rf.me //给自己投票
		rf.persist()
		log.Printf("Peer %d in term %d turns a candidate and starts an election", rf.me, rf.term)
		rf.mu.Unlock()
		//发起投票
		wait := sync.WaitGroup{}
		var myVotes int32 = 1                               //统计票数
		for peerID := 0; peerID < len(rf.peers); peerID++ { //要求其他节点投票
			if peerID == rf.me { //自己已经给自己投过票了
				continue
			}
			wait.Add(1)
			args := &RequestVoteArgs{rf.term, rf.me, len(rf.log), rf.log[len(rf.log)-1].Term}
			reply := &RequestVoteReply{0, false} //初始化的reply没有意义
			voter := peerID
			go func() {
				ok := rf.sendRequestVote(voter, args, reply)
				wait.Done()
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.VoteGranted {
						atomic.AddInt32(&myVotes, 1) //获得一个投票
					} else { //未获得投票
						if reply.Term > rf.term { //发现一个term更大的peer,转为follower
							rf.TurnFollower(reply.Term)
							return
						}
						// term相同却未投票给本candidate,可能是投给其他candidate了或者超时了
						// term比本candidate小却未投票给本candidate,可能是超时了
					}
				}

			}()

		}

		ch := make(chan bool)
		go func() {
			wait.Wait()
			ch <- false
		}()
		select {
		case <-time.After(time.Millisecond * 100):
			rf.mu.Lock()
			log.Printf("RPC timeout: Candidate %d in term %d requets vote", rf.me, rf.term)
			rf.mu.Unlock()
		case <-ch:
			rf.mu.Lock()
			log.Printf("RPC done: Candidate %d in term %d requets vote", rf.me, rf.term)
			rf.mu.Unlock()
		}

		rf.mu.Lock()
		if rf.state != candidate { //在请求投票过程中发现term更高的peer或者leader,本节点已经变成follower了
			rf.mu.Unlock()
			return
		}
		//本节点还是candidate
		sumVotes := atomic.LoadInt32(&myVotes) //总票数
		log.Printf("Candidate %d in term %d gets %d votes", rf.me, rf.term, sumVotes)
		//如果票数过半则变为leader
		if int32(len(rf.peers)) < sumVotes*2 {
			rf.TurnLeader()
		} else { //票数不过半变回follower
			rf.TurnFollower(rf.term)
		}
		rf.mu.Unlock()
		return
	}

}

//=====================================================================================================================

// GetState : return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	rf.mu.Lock()
	term = rf.term
	if rf.state == leader {
		isLeader = true
	} else {
		isLeader = false
	}
	rf.mu.Unlock()
	return term, isLeader
}

// Working : 节点的服务线程,主要是leader提交日志、同步日志
func (rf *Raft) Working() {
	for {
		time.Sleep(time.Millisecond * 50)
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		matchIndex := make([]int, len(rf.matchIndex))
		copy(matchIndex, rf.matchIndex)
		matchIndex[rf.me] = len(rf.log) - 1
		sort.Ints(matchIndex)

		if rf.state == leader {
			mid := len(matchIndex) / 2
			if rf.log[matchIndex[mid]].Term == rf.term { //提交日志, leader只提交自己term内的日志
				rf.commitIndex = Max(matchIndex[mid], rf.commitIndex)
			}
		}
		rf.persist()

		for ; rf.lastApplied <= rf.commitIndex; rf.lastApplied++ { //apply已committed的日志
			if rf.log[rf.lastApplied].IsEmpty { //命令为空的日志不用apply
				continue
			} else {
				message := ApplyMsg{rf.log[rf.lastApplied].LogIndex, rf.log[rf.lastApplied].Command, true, false, nil}
				*rf.applyCh <- message
			}
		}
		rf.mu.Unlock()

	}
}

// Timing : 计时,follower在election timeout后变成candidate
func (rf *Raft) Timing() {
	for {
		time.Sleep(time.Millisecond * 100)
		if rf.killed() {
			return
		}
		rf.mu.Lock()

		if rf.state != follower { //只有follower在election timeout后变成candidate
			rf.mu.Unlock()
			continue
		} else {
			rf.heartBeatMissedCounter += 1     //没有收到心跳信号次数+1
			if rf.heartBeatMissedCounter > 2 { //连续两次没有收到心跳信号则变为candidate
				rf.heartBeatMissedCounter = 0
				rf.state = candidate
				go rf.TurnCandidate()
			}
			rf.mu.Unlock()
		}
	}
}

// LogRecording : 打印日志,模拟执行命令
func (rf *Raft) LogRecording() {
	for {
		time.Sleep(time.Millisecond * 100)
		if rf.killed() {
			return
		}

		rf.mu.Lock()
		str := ""
		str += strconv.Itoa(rf.commitIndex)
		str += "["
		for _, entry := range rf.log {
			str += strconv.Itoa(entry.Term)
			str += "("
			switch entry.Command.(type) {
			case int:
				str += strconv.Itoa(entry.Command.(int))
			case string:
				str += entry.Command.(string)
			default:
				log.Printf("Cannot parse entry %d of Peer %d", entry.LogIndex, rf.me)
				log.Fatal()
			}
			str += ")"
			str += ","
		}
		str += "]"
		log.Printf("Peer %d :"+str, rf.me)
		rf.mu.Unlock()
	}
}

//=====================================================================================================================

// Make :
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
	rf.heartBeatMissedCounter = 0
	rf.state = follower
	rf.term = 1
	rf.votedFor = -1
	rf.log = make([]Log, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for peerID := 0; peerID < len(rf.nextIndex); peerID++ {
		rf.nextIndex[peerID] = 0
		rf.matchIndex[peerID] = 0

	}
	rf.applyCh = &applyCh
	rf.logCount = 0
	rand.Seed(time.Now().UnixNano())
	log.Printf("Peer %d initialized", rf.me)
	firstEntry := Log{0, 0, -1, true, false}
	rf.log = append(rf.log, firstEntry)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// running
	go rf.Timing()
	go rf.SendHeartBeat()
	go rf.Working()
	//	go rf.LogRecording()

	return rf
}
