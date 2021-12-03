package raft

import (
	"bytes"
	"encoding/gob"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)
import "labrpc"

type Entry struct {
	Term        int
	LogIndex    int
	Command     interface{}
	IsEmpty     bool
	IsHeartBeat bool
}

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
	state       int     // 节点当前状态
	currentTerm int     //节点current currentTerm
	votedFor    int     //本节点当前term投票给了哪个节点
	log         []Entry //日志

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
	if e.Encode(rf.currentTerm) != nil || e.Encode(rf.votedFor) != nil || e.Encode(rf.log) != nil {
		//log.Printf("Peer %d Persist Failed", rf.me)
		return
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	//log.Printf("Peer %d Persist Successes", rf.me)
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
	var logs []Entry
	if d.Decode(&term) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		//log.Printf("Peer %d readPersist Failed", rf.me)
		return
	} else {
		rf.mu.Lock()
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.log = logs
		//log.Printf("Peer %d readPersist Successes", rf.me)
		rf.mu.Unlock()
	}
}

//====================================================================================================================

// AppendEntriesArgs : example AppendEntries RPC arguments structure
type AppendEntriesArgs struct {
	Term        int     // leader's currentTerm
	LeaderId    int     //leader的id
	PreLogIndex int     //要同步的日志的前一条日志的索引
	PreLogTerm  int     //要同步的的日志的前一条日志的term
	CommitIndex int     //leader已经committed的最近一条日志的索引,告知follower让其知道哪些日志可以apply
	Entries     []Entry //需要同步的日志
}

// AppendEntriesReply : example AppendEntries RPC reply structure
type AppendEntriesReply struct {
	Term        int  // follower的current currentTerm
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

	if args.Term < rf.currentTerm {
		//log.Printf("Leader %d in currentTerm %d find a Peer %d whose currentTerm %d is bigger", args.LeaderId, args.Term, rf.me, rf.currentTerm)
		reply.Term = rf.currentTerm //让leader更新term
		reply.Success = false       //拒绝承认该leader
		reply.CommitIndex = rf.commitIndex
		return
	}

	if args.Term > rf.currentTerm {
		rf.TurnFollower(args.Term)
	}

	// leader的term等于本节点的term
	if args.Term == rf.currentTerm {
		rf.heartBeatMissedCounter = 0
		rf.persist()

		//要同步的日志为空,说明此时为心跳信号
		if len(args.Entries) == 0 {
			//log.Printf("Leader %d in currentTerm %d sends Peer %d in currentTerm %d HeartBeat", args.LeaderId, args.Term, rf.me, rf.currentTerm)
			// 更新本节点committed的log索引
			if rf.commitIndex < args.CommitIndex {
				if args.PreLogIndex < len(rf.log) && rf.log[args.PreLogIndex].Term == args.PreLogTerm {
					rf.commitIndex = Min(args.PreLogIndex, args.CommitIndex)
				}
			}
			//rf.heartBeatMissedCounter = 0 //收到valid心跳信号,重新计时
			reply.Success = true
			reply.CommitIndex = rf.commitIndex
			reply.Term = rf.currentTerm
			return
		} else { //要同步的日志不为空
			//log.Printf("Leader %d in currentTerm %d ask Peer %d in currentTerm %d to append entries(index starts from %d)", args.LeaderId, args.Term, rf.me, rf.currentTerm, args.PreLogIndex+1)
			// 本节点已经committed过了leader发来的日志记录
			if rf.commitIndex >= args.PreLogIndex+len(args.Entries) {
				//log.Printf("Peer % d in currentTerm %d has already committed the entries Leader %d in currentTerm %d sends", rf.me, rf.currentTerm, args.LeaderId, args.Term)
				reply.Success = true
				reply.Term = rf.currentTerm
				reply.CommitIndex = rf.commitIndex
				return
			}
			// 没有匹配到追加点
			if args.PreLogIndex >= len(rf.log) || rf.log[args.PreLogIndex].Term != args.PreLogTerm {
				//log.Printf("Leader %d in currentTerm %d ask Peer %d in currentTerm %d to append entries(index starts from %d), not match", args.LeaderId, args.Term, rf.me, rf.currentTerm, args.PreLogIndex+1)
				reply.Success = false
				reply.Term = args.Term
				reply.CommitIndex = rf.commitIndex
				return
			} else { //匹配到追加点
				//log.Printf("Leader %d in currentTerm %d ask Peer %d in currentTerm %d to append entries(index starts from %d), match", args.LeaderId, args.Term, rf.me, rf.currentTerm, args.PreLogIndex+1)
				//同步leader发来的日志
				rf.log = rf.log[0 : args.PreLogIndex+1]
				for _, entry := range args.Entries {
					rf.log = append(rf.log, entry)
				}
				reply.Term = rf.currentTerm
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
	}
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) SendHeartBeat() {
	for {
		time.Sleep(time.Millisecond * 100)
		rf.mu.Lock()
		if rf.state == leader { //只有leader向外发送心跳信号
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me { //不给自己发心跳信号
					continue
				}
				emptyLog := make([]Entry, 0)
				args := &AppendEntriesArgs{rf.currentTerm, rf.me, rf.nextIndex[i] - 1, rf.log[rf.nextIndex[i]-1].Term, rf.commitIndex, emptyLog}
				reply := &AppendEntriesReply{} //初始化的reply,无意义
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
			//log.Printf("Leader %d in term %d sends logs(%d ~ %d) to Peer %d", rf.me, rf.currentTerm, rf.nextIndex[peerID], len(rf.log), peerID)
			entries := make([]Entry, len(rf.log[rf.nextIndex[peerID]:len(rf.log)]))
			copy(entries, rf.log[rf.nextIndex[peerID]:len(rf.log)])
			args := &AppendEntriesArgs{rf.currentTerm, rf.me, rf.nextIndex[peerID] - 1, rf.log[rf.nextIndex[peerID]-1].Term, rf.commitIndex, entries}
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
				//log.Printf("Timeout: Leader %d in term %d sends logs(%d ~ %d) to Peer %d", rf.me, rf.currentTerm, rf.nextIndex[peerID], len(rf.log), peerID)
				rf.mu.Unlock()
			case <-ch: //完成
				rf.mu.Lock()
				//log.Printf("Done: Leader %d in term %d sends logs(%d ~ %d) to Peer %d", rf.me, rf.currentTerm, rf.nextIndex[peerID], len(rf.log), peerID)
				rf.mu.Unlock()
			}

			if ok {
				if reply.Success { //同步成功
					rf.mu.Lock()
					rf.matchIndex[peerID] = Max(rf.matchIndex[peerID], rf.nextIndex[peerID]+len(entries)-1) //更新对该peer的match点
					rf.nextIndex[peerID] += len(entries)                                                    //更新对该peer的nextIndex
					log.Printf("Success: Leader %d in term %d sends logs(%d ~ %d) to Peer %d", rf.me, rf.currentTerm, rf.nextIndex[peerID], len(rf.log), peerID)
					rf.mu.Unlock()
				} else { //同步失败
					rf.mu.Lock()
					if reply.Term <= rf.currentTerm { //因为没有match所有同步失败
						rf.nextIndex[peerID] = reply.CommitIndex + 1
						log.Printf("Failed (Unmatch): Leader %d in term %d sends logs(%d ~ %d) to Peer %d", rf.me, rf.currentTerm, rf.nextIndex[peerID], len(rf.log), peerID)
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

// RequestVoteArgs : 请求投票信号
type RequestVoteArgs struct {
	Term         int // candidate的currentTerm
	Candidate    int // candidate的id
	LastLogIndex int // candidate的日志中最后一条entry的索引
	LastLogTerm  int // candidate的日志中最后一条entry的term
}

// RequestVoteReply : 对请求投票信号的回复信号
type RequestVoteReply struct {
	Term        int  //投票者的currentTerm
	VoteGranted bool //投票与否
}

// MoreUpToDate : 如果投票信号中请求者的log更up-to-date则返回true
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

// RequestVote : 对投票请求进行处理
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// candidate的term小于或等于voter的term
	if args.Term <= rf.currentTerm {
		//log.Printf("refuse to vote: Candidate %d in currentTerm %d requests vote, but its currentTerm is less than Voter %d in currentTerm %d", args.Candidate, args.Term, rf.me, rf.currentTerm)
		reply.Term = args.Term    //让该Candidate更新term，转为follower
		reply.VoteGranted = false //拒绝投票
		return
	}
	if args.Term > rf.currentTerm {
		rf.TurnFollower(args.Term)
		rf.persist()
	}

	if rf.votedFor == -1 || rf.votedFor == args.Candidate {
		if rf.MoreUpToDate(args) {
			rf.TurnFollower(args.Term)    //voter成为该candidate的follower
			rf.heartBeatMissedCounter = 0 //重新计时
			rf.votedFor = args.Candidate
			rf.persist()
			reply.Term = rf.currentTerm
			reply.VoteGranted = true //同意投票
			return
		}
	}
	// 其他情况拒绝投票
	reply.Term = rf.currentTerm
	reply.VoteGranted = false //拒绝投票
	return
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// StartAnElection :成为candidate状态并发起投票
func (rf *Raft) StartAnElection() {
	for {
		electionTimeout := rand.Intn(100) //election timeout是0~100ms中的一个随机值
		time.Sleep(time.Millisecond * time.Duration(electionTimeout))
		if rf.killed() {
			return
		}

		rf.mu.Lock()
		if rf.state != candidate { //只有candidate可以发起选举
			rf.mu.Unlock()
			return
		}

		rf.currentTerm += 1 //增加term
		rf.votedFor = rf.me //给自己投票
		rf.persist()
		//log.Printf("Peer %d in currentTerm %d turns a candidate and starts an election", rf.me, rf.currentTerm)
		rf.mu.Unlock()
		//发起投票
		wait := sync.WaitGroup{}
		var myVotes int32 = 1                               //统计票数
		for peerID := 0; peerID < len(rf.peers); peerID++ { //要求其他节点投票
			if peerID == rf.me { //自己已经给自己投过票了
				continue
			}
			wait.Add(1)
			args := &RequestVoteArgs{rf.currentTerm, rf.me, len(rf.log) - 1, rf.log[len(rf.log)-1].Term}
			reply := &RequestVoteReply{}
			voter := peerID
			go func() {
				ok := rf.sendRequestVote(voter, args, reply)
				defer wait.Done()
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.VoteGranted {
						atomic.AddInt32(&myVotes, 1) //获得一个投票
					} else { //未获得投票
						if reply.Term > rf.currentTerm { //发现一个term更大的peer,转为follower
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
		go func() { //该线程用于判断是否统计完所有票
			wait.Wait()
			ch <- true //统计完所有票数
		}()
		select { //100ms内完成统计则可以根据统计结果做出相应动作, 100ms后不论所有结果是否统计完成都开始进行相应动作
		case <-time.After(time.Millisecond * 100):

		case <-ch:

		}

		rf.mu.Lock()
		if rf.state != candidate { //在请求投票过程中发现更高的term,本节点已经变成follower了
			rf.mu.Unlock()
			return
		}
		//本节点还是candidate
		sumVotes := atomic.LoadInt32(&myVotes) //总票数
		//log.Printf("Candidate %d in currentTerm %d gets %d votes", rf.me, rf.currentTerm, sumVotes)
		//如果票数过半则变为leader
		if int32(len(rf.peers)) < sumVotes*2 {
			rf.TurnLeader()
		}
		//没得到足够多的选票则保持candidate状态等待再次发起选举
		rf.mu.Unlock()
		return
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

		if rf.state == leader { //只有follower在election timeout后变成candidate
			rf.mu.Unlock()
			continue
		} else {
			rf.heartBeatMissedCounter += 1     //没有收到心跳信号次数+1
			if rf.heartBeatMissedCounter > 2 { //连续两次没有收到心跳信号则变为candidate并发起选举
				rf.heartBeatMissedCounter = 0
				rf.state = candidate
				go rf.StartAnElection()
			}
			rf.mu.Unlock()
		}
	}
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

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != leader {
		isLeader = false
	} else {
		index = len(rf.log) - rf.GetEmptyLogCount() + 1
		entry := Entry{rf.currentTerm, index, command, false, false}
		rf.log = append(rf.log, entry)
		term = rf.currentTerm
		rf.persist()
	}
	return index, term, isLeader
}

//===================================================================================================================

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
	//log.Printf("Peer %d in currentTerm %d turns a follower", rf.me, rf.currentTerm)
	//rf.heartBeatMissedCounter = 0 //election timeout设置为0
	rf.currentTerm = term
	rf.state = follower
	rf.votedFor = -1
}

// TurnLeader :成为leader状态
func (rf *Raft) TurnLeader() {
	//log.Printf("Peer %d in currentTerm %d turns a leader", rf.me, rf.currentTerm)
	rf.votedFor = -1
	rf.state = leader
	rf.heartBeatMissedCounter = 0
	for i := 0; i < len(rf.matchIndex); i++ {
		rf.nextIndex[i] = len(rf.log) //第一次默认同步下一条日志
		rf.matchIndex[i] = 0          //默认匹配点为0
	}
	entry := Entry{rf.currentTerm, 0, -1, true, false}
	rf.log = append(rf.log, entry)
	rf.persist()
	for peerID := 0; peerID < len(rf.peers); peerID++ {
		if peerID == rf.me {
			continue
		}
		go rf.SendAppendEntriesToPeers(peerID)
	}
}

//=====================================================================================================================

// GetState : return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == leader {
		isLeader = true
	} else {
		isLeader = false
	}
	rf.mu.Unlock()
	return term, isLeader
}

func (rf *Raft) Commit() {
	matchIndex := make([]int, len(rf.matchIndex))
	copy(matchIndex, rf.matchIndex)
	matchIndex[rf.me] = len(rf.log) - 1
	sort.Ints(matchIndex)
	mid := len(matchIndex) / 2
	if rf.log[matchIndex[mid]].Term == rf.currentTerm { //提交日志, leader只提交自己term内的日志
		rf.commitIndex = Max(matchIndex[mid], rf.commitIndex)
	}
}

func (rf *Raft) Apply() {
	for ; rf.lastApplied <= rf.commitIndex; rf.lastApplied++ { //apply已committed的日志
		if rf.log[rf.lastApplied].IsEmpty { //命令为空的日志不用apply
			continue
		} else {
			message := ApplyMsg{rf.log[rf.lastApplied].LogIndex, rf.log[rf.lastApplied].Command, true, false, nil}
			*rf.applyCh <- message
		}
	}
}

// CommittingAndApplying : 节点的服务线程,主要是leader提交日志、同步日志
func (rf *Raft) CommittingAndApplying() {
	for {
		time.Sleep(time.Millisecond * 50)
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		if rf.state == leader {
			rf.Commit()
		}
		rf.persist()
		rf.Apply()
		rf.mu.Unlock()
	}
}

//=====================================================================================================================

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.heartBeatMissedCounter = 0
	rf.state = follower
	rf.currentTerm = 1
	rf.votedFor = -1
	rf.log = make([]Entry, 0)
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
	//log.Printf("Peer %d initialized", rf.me)
	firstEntry := Entry{0, 0, -1, true, false}
	rf.log = append(rf.log, firstEntry)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// running
	go rf.Timing()
	go rf.SendHeartBeat()
	go rf.CommittingAndApplying()

	return rf
}
