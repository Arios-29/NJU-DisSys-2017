package raft

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Entry : 条目定义
type Entry struct {
	Index   int         //条目的索引
	Command interface{} //条目代表的命令
	Term    int         //条目的term
}

// ApplyMsg : 将Entry封装成ApplyMsg进行apply
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//状态定义
const (
	leader    = 0
	candidate = 1
	follower  = 2
)

// Raft :
type Raft struct {
	mu        sync.Mutex          //互斥锁,涉及到对Raft属性的读取和修改就必须获得锁
	peers     []*labrpc.ClientEnd //集群信息
	persister *Persister          //持久化相关
	me        int                 // 本节点的id
	dead      int32               //如果dead==1，则说明该节点被移除

	//所有状态都需要持久化的属性
	currentTerm int     //节点current term
	votedFor    int     //本节点当前term投票给了哪个节点
	log         []Entry //日志

	//volatile state on all servers
	commitIndex int //对本节点而言已知的最新一条committed的条目的索引
	lastApplied int //本节点最新一条已被applied的条目的索引

	//volatile state on a leader
	nextIndex  []int // nextIndex[i]表示要让follower_i同步的下一条条目的索引
	matchIndex []int // matchIndex[i]表示follower_i最近同步的条目的索引

	//其他属性
	state          int           // 节点当前状态
	leaderId       int           // leader的id
	lastActiveTime time.Time     // 上一次收到有效信号的时间,用于election
	applyCh        chan ApplyMsg // 将封装成ApplyMsg的Entry放入此通道,模拟apply
}

// 测试用
func (rf *Raft) printState() {
	if rf.state == leader {
		log.Printf("server %d is leader with term %d\n", rf.me, rf.currentTerm)
	} else if rf.state == candidate {
		log.Printf("server %d is candidate with term %d\n", rf.me, rf.currentTerm)
	} else {
		log.Printf("server %d is follower with term %d\n", rf.me, rf.currentTerm)
	}
}

//Leader election========================================================================================

// RequestVoteArgs : 请求投票信号
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int //请求投票的candidate的id
	LastLogIndex int // candidate的日志中最后一条entry的索引
	LastLogTerm  int // candidate的日志中最后一条entry的term
}

// RequestVoteReply : 对请求投票信号的回复信号
type RequestVoteReply struct {
	Term        int  //投票者的current term
	VoteGranted bool //投票与否
}

// sendRequestVote : leader通过RPC请求server投票
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// MoreUpToDate : 如果投票请求者的log更up-to-date则返回true
func (rf *Raft) MoreUpToDate(args *RequestVoteArgs) bool {
	candidateLastEntryTerm := args.LastLogTerm
	myLastEntryTerm := rf.log[len(rf.log)-1].Term
	if candidateLastEntryTerm > myLastEntryTerm {
		return true
	} else if candidateLastEntryTerm < myLastEntryTerm {
		return false
	} else {
		candidateLogLen := args.LastLogIndex + 1
		myLogLen := len(rf.log)
		if candidateLogLen >= myLogLen {
			return true
		} else {
			return false
		}
	}
}

// RequestVote : server处理来自candidate的投票请求
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	candidateTerm := args.Term
	myTerm := rf.currentTerm
	//log.Printf("server %d is asked to vote for candidate %d", rf.me, args.CandidateId)
	// candidate的term更小,拒绝投票
	if candidateTerm < myTerm {
		//log.Printf("sever %d with term %d refused to vote for candidate %d with a smaller term %d\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		reply.VoteGranted = false
		reply.Term = myTerm
		return
	}

	// candidate的term更大, 无论什么状态都应该转化为该term的follower
	if candidateTerm > myTerm {
		//log.Printf("candidate %d has a higher term %d than voter %d with term %d\n", args.CandidateId, args.Term, rf.me, rf.currentTerm)
		rf.TurnFollower(candidateTerm)
		myTerm = rf.currentTerm
	}

	if candidateTerm == myTerm {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId { //本节点还未投票
			if rf.MoreUpToDate(args) { //candidate的log更up-to-date
				//log.Printf("server %d votes for candidate %d\n", rf.me, args.CandidateId)
				reply.VoteGranted = true
				reply.Term = myTerm
				rf.votedFor = args.CandidateId
				rf.lastActiveTime = time.Now() //对于投票的follower重新计时
				rf.persist()
				return
			} else {
				//log.Printf("server %d refuses to vote for candidate %d because of up-to-date rule\n", rf.me, args.CandidateId)
				reply.VoteGranted = false
				reply.Term = myTerm
				//rf.persist()
				return
			}
		} else {
			//log.Printf("server %d refuses to vote for candidate %d it has voted\n", rf.me, args.CandidateId)
			reply.VoteGranted = false
			reply.Term = myTerm
			rf.persist()
			return
		}
	}
}

// Timing : 一段时间没有收到心跳信号或投票请求则发起投票, 一轮选举后没产生leader则再计时
func (rf *Raft) Timing() {
	for {
		if rf.killed() {
			return
		}
		time.Sleep(50 * time.Millisecond)

		rf.mu.Lock()
		//log.Printf("server %d with term %d starts test election timeout\n", rf.me, rf.currentTerm)
		rand.Seed(time.Now().UnixNano() + int64(100*rf.me))
		electionTimeOut := time.Duration(200+rand.Intn(150)) * time.Millisecond //election timeout是200~350ms中的随机值
		//log.Printf("server %d election timeout = %d\n", rf.me, electionTimeOut)
		elapse := time.Now().Sub(rf.lastActiveTime) //持续没有收到心跳信号或投票请求的时间
		if rf.state == leader {
			//log.Printf("server %d is a leader with term %d so election timeout is not used\n", rf.me, rf.currentTerm)
			rf.mu.Unlock()
			continue
		}
		if elapse > electionTimeOut {
			//log.Printf("follower %d election timeout with term %d", rf.me, rf.currentTerm)
			rf.TurnCandidate() //转变为candidate并且已经给自己投了一票
			//发起投票
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.log) - 1,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
			}
			rf.mu.Unlock()
			//log.Printf("candidate %d with term %d start an election\n", rf.me, rf.currentTerm)
			rf.StartAnLeaderElection(args)
		} else {
			rf.mu.Unlock()
			//log.Printf("server %d with term not election timeout\n", rf.me)
		}
	}
}

// Vote : 定义一个投票结果
type Vote struct {
	reply *RequestVoteReply
	voter int
}

// StartAnLeaderElection : 发起一轮选举,广播一个请求投票信号
func (rf *Raft) StartAnLeaderElection(args *RequestVoteArgs) {
	sumVotes := 1 //总票数
	received := 1 //已统计的票数
	voteChan := make(chan *Vote, len(rf.peers))
	//w := sync.WaitGroup{}
	for id := 0; id < len(rf.peers); id++ {
		//w.Add(1)
		go func(id int) {
			//defer w.Done()
			if id == rf.me {
				return
			}
			reply := &RequestVoteReply{}
			//log.Printf("candidate %d with term %d ask server %d to vote\n", rf.me, rf.currentTerm, id)
			ok := rf.sendRequestVote(id, args, reply)
			if ok {
				voteChan <- &Vote{reply, id}
			} else {
				voteChan <- &Vote{nil, id}
			}
		}(id)
	}
	maxTerm := 0 //发现的最大term
	//统计结果
	//w.Wait()
	//log.Printf("candidate %d starts to statistic votes\n", rf.me)
	undone := true
	for undone {
		select {
		case vote := <-voteChan:
			received += 1
			//log.Printf("candidate %d gets a reply\n", rf.me)
			if vote.reply != nil { //成功收到一个回复
				if vote.reply.VoteGranted == true {
					sumVotes += 1
				}
				if vote.reply.Term > maxTerm {
					maxTerm = vote.reply.Term
				}
			}
			if 2*sumVotes > len(rf.peers) || received == len(rf.peers) { //退出统计过程
				undone = false
			}
		}
	}
	//根据结果做出相应动作
	//log.Printf("candidate %d finished statistic\n", rf.me)
	rf.mu.Lock()
	if rf.state != candidate { // 在发送投票信号或统计投票期间被一个合法leader变成follower了
		//log.Printf("candidate %d turns follower during election\n", rf.me)
		rf.mu.Unlock()
		return
	}
	if maxTerm > rf.currentTerm { // 发现一个更高的term
		//log.Printf("candidate %d finds a server with higher term\n", rf.me)
		rf.TurnFollower(maxTerm)
		rf.persist()
		rf.mu.Unlock()
		return
	}
	if 2*sumVotes > len(rf.peers) { //赢得选举
		//log.Printf("candidate %d won \n", rf.me)
		rf.TurnLeader()
		rf.mu.Unlock()
		return
	} else {
		//log.Printf("candidate %d does not have enough votes\n", rf.me)
		//rf.TurnFollower(rf.currentTerm+1) //选举没有产生结果,变回follower重新计时等待再次选举
		rf.mu.Unlock()
		return
	}
}

// Log replication======================================================================================

// AppendEntriesArgs : 同步信号或心跳信号
type AppendEntriesArgs struct {
	Term        int     // leader's term
	LeaderId    int     //leader的id
	PreLogIndex int     //要同步的日志的前一条日志的索引
	PreLogTerm  int     //要同步的的日志的前一条日志的term
	CommitIndex int     //leader已经committed的最近一条日志的索引,告知follower让其知道哪些日志可以apply
	Entries     []Entry //需要同步的日志
	IsHeartBeat bool    //是否为心跳信号
}

// AppendEntriesReply : 对同步信号或心跳信号的回复信号
type AppendEntriesReply struct {
	Term    int  // follower的current term
	Success bool //AppendEntries是否成功
}

// SendAppendEntries : 通过RPC使server同步日志或向其发送一个心跳信号
func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// AppendEntries : server对同步信号或心跳信号做出反应
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm

	if args.Term > rf.currentTerm {
		rf.TurnFollower(args.Term)
		rf.lastActiveTime = time.Now()
		return
	}
	if args.Term < rf.currentTerm {
		return
	}
	//同一个term不会有两个leader
	if rf.state == candidate { //本term的另一个candidate先成为leader,本candidate收到其信号应该变成follower
		rf.TurnFollower(args.Term)
	}
	rf.lastActiveTime = time.Now()

	if args.PreLogIndex > len(rf.log)-1 || args.PreLogTerm != rf.log[args.PreLogIndex].Term {
		if args.PreLogIndex <= len(rf.log)-1 {
			rf.log = rf.log[0:args.PreLogIndex]
		}
		return
	}

	rf.log = append(rf.log[0:args.PreLogIndex+1], args.Entries...)
	reply.Success = true
	rf.persist()

	if args.CommitIndex > rf.commitIndex {
		rf.commitIndex = Min(args.CommitIndex, len(rf.log)-1)
	}
}

// BroadcastMsg : leader广播心跳信号或同步信号
func (rf *Raft) BroadcastMsg() {
	for {
		if rf.killed() {
			return
		}
		time.Sleep(100 * time.Millisecond)
		rf.mu.Lock()
		if rf.state != leader {
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()
		go rf.Broadcasting(true)
	}
}

// Broadcasting : leader广播一轮信号,要同步的日志为空则是心跳信号,同步信号和心跳信号没有本质区别
func (rf *Raft) Broadcasting(heartbeat bool) {
	rf.mu.Lock()
	if rf.killed() || rf.state != leader {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	finished := false
	for id := 0; id < len(rf.peers); id++ {
		if id == rf.me {
			continue
		}
		if len(rf.log)-1 > rf.nextIndex[id] || heartbeat {
			rf.mu.Lock()
			nextIndex := rf.nextIndex[id]
			if nextIndex <= 0 {
				nextIndex = 1
			}
			if nextIndex > len(rf.log) {
				nextIndex = len(rf.log) - 1
			}
			preLog := rf.log[nextIndex-1]
			args := &AppendEntriesArgs{
				Term:        rf.currentTerm,
				LeaderId:    rf.me,
				CommitIndex: rf.commitIndex,
				Entries:     make([]Entry, 0),
				PreLogIndex: preLog.Index,
				PreLogTerm:  preLog.Term,
			}
			for index := nextIndex; index < len(rf.log); index++ {
				args.Entries = append(args.Entries, rf.log[index])
			}
			rf.mu.Unlock()
			go func(id int, args *AppendEntriesArgs) {
				reply := &AppendEntriesReply{}
				ok := rf.SendAppendEntries(id, args, reply)
				if !ok {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if finished || args.Term != rf.currentTerm || rf.state != leader {
					return
				}
				if reply.Term > rf.currentTerm {
					rf.TurnFollower(reply.Term)
					finished = true
					return
				}
				if reply.Success {
					match := args.PreLogIndex + len(args.Entries)
					rf.matchIndex[id] = Max(match, rf.matchIndex[id])
					rf.nextIndex[id] = Max(match+1, rf.nextIndex[id])
				} else {
					preIndex := args.PreLogIndex
					for preIndex > 0 && rf.log[preIndex].Term == args.PreLogTerm {
						preIndex--
					}
					rf.nextIndex[id] = preIndex + 1
				}

			}(id, args)

		}
	}
}

func Max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func Min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

//周期性commit & apply===========================================================================================

func (rf *Raft) Commit() {
	for index := rf.commitIndex + 1; index <= len(rf.log)-1; index++ {
		if rf.log[index].Term != rf.currentTerm {
			continue
		}
		count := 1
		for id := 0; id < len(rf.peers); id++ {
			if id == rf.me || rf.matchIndex[id] < index {
				continue
			}
			count++
			if count > len(rf.peers)/2 {
				rf.commitIndex = Max(rf.commitIndex, index)
				break
			}
		}
	}
}

func (rf *Raft) Apply() {
	for rf.lastApplied < rf.commitIndex && rf.lastApplied < len(rf.log)-1 {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			Index:       rf.lastApplied,
			Command:     rf.log[rf.lastApplied].Command,
			UseSnapshot: false,
			Snapshot:    nil,
		}
		rf.applyCh <- applyMsg
	}
}

func (rf *Raft) CommitAndApply() {
	for {
		if rf.killed() {
			return
		}
		time.Sleep(20 * time.Millisecond)
		rf.mu.Lock()
		if rf.state == leader {
			rf.Commit()
		}
		rf.Apply()
		rf.mu.Unlock()
	}

}

//状态转换================================================================================================

func (rf *Raft) TurnFollower(term int) {
	//log.Printf("server %d turns follower with new term %d\n", rf.me, term)
	rf.currentTerm = term
	rf.state = follower
	rf.votedFor = -1
	rf.leaderId = -1
	//rf.lastActiveTime = time.Now()
}

func (rf *Raft) TurnCandidate() {
	rf.currentTerm += 1
	rf.state = candidate
	//log.Printf("follower %d turns candidate with term %d\n", rf.me, rf.currentTerm)
	rf.votedFor = rf.me
	rf.lastActiveTime = time.Now()
	rf.persist()
}

func (rf *Raft) TurnLeader() {
	rf.state = leader
	rf.leaderId = rf.me
	//log.Printf("server %d turns leader \n", rf.me)
	for id := 0; id < len(rf.peers); id++ {
		rf.nextIndex[id] = len(rf.log)
		rf.matchIndex[id] = 0
	} // necessary?
	rf.matchIndex[rf.me] = len(rf.log) - 1
	go rf.Broadcasting(true)
	//马上发广播心跳信号
	//rf.persist()
}

//持久化==================================================================================================
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil || e.Encode(rf.votedFor) != nil || e.Encode(rf.log) != nil {
		return
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

}
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var term int
	var votedFor int
	var logs []Entry
	if d.Decode(&term) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		return
	} else {
		rf.mu.Lock()
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.log = logs
		rf.mu.Unlock()
	}
}

//模拟节点失效====================================================================================================

func (rf *Raft) Kill() {
	//log.Printf("server %d killed\n", rf.me)
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	dead := atomic.LoadInt32(&rf.dead)
	return dead == 1
}

//Test相关===============================================================================================

// GetState : 返回本节点的term以及其是否为leader
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == leader {
		isLeader = true
		//log.Printf("server %d is leader\n", rf.me)
	} else {
		isLeader = false
	}
	rf.mu.Unlock()
	return term, isLeader
}

// Start : 模拟客户端向本节点发送命令,如果本节点是leader则该命令被写入日志并返回相应的index、term
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == leader {
		entry := Entry{
			Term:    rf.currentTerm,
			Index:   len(rf.log),
			Command: command,
		}
		rf.log = append(rf.log, entry)
		rf.persist()
		return entry.Index, entry.Term, true
	} else {
		return -1, -1, false
	}
}

// Make : 返回一个Raft状态机
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	raft := &Raft{
		mu:             sync.Mutex{},
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		currentTerm:    0,
		votedFor:       -1,
		log:            make([]Entry, 0),
		commitIndex:    0,
		lastApplied:    0,
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		state:          follower,
		leaderId:       -1,
		lastActiveTime: time.Now(),
		applyCh:        applyCh,
	}
	//初始化一条空日志
	entry := Entry{
		Index:   0,
		Command: -1,
		Term:    0,
	}
	raft.log = append(raft.log, entry)
	raft.readPersist(persister.raftstate)
	//running
	go raft.Timing()
	go raft.BroadcastMsg()
	go raft.CommitAndApply()
	return raft
}
