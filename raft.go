package main

import (
	"sync"
	"time"
	"math/rand"
	"encoding/json"
	"bytes"
	"fmt"
	"net/http"
)

type Role string

const (
	Follower Role = "Follower"
	Candidate Role = "Candidate"
	Leader Role = "Leader"
)

//1. Define Raft node state
// Node ID (id)
// Current term (currentTerm)
// Who it voted for in this term (votedFor)
// Role (Follower, Candidate, Leader)
// Election timer (time.Timer)
// Peers (list of peer URLs)

type RaftNode struct {
	mu sync.Mutex
	id string
	peers []string
	currentTerm int
	votedFor string
	role Role
	electionTimer *time.Timer
	voteCount int
}

// Define RPC messages
// RequestVoteArgs:
// term, candidateId, lastLogIndex, lastLogTerm

type RequestVoteArgs struct {
	Term int
	CandidateId string
	// lastLogIndex int
	// lastLogTerm int
}

// RequestVoteReply
// term, voteGranted

type RequestVoteReply struct {
	Term int
	VoteGranted bool
}

// AppendEntriesArgs

type AppendEntriesArgs  struct {
	Term int
	LeaderId string
}

// AppnedEntiresReply

type AppendEntriesReply struct {
	Term int
	Success bool
}

//Constructor for new raft node
func NewRaftNode(id string, peers []string) *RaftNode{
	node := &RaftNode{
		id: id,
		peers: peers,
		currentTerm: 0,
		votedFor: "",
		role: Follower,
	}
	return node
}

//start election on timeout
//become candidate
//update currentTerm
//vote self
//reset voteCount
//send vote request to peers
func (rn *RaftNode) startElectionTimer() {
	timeout := time.Duration(150+rand.Intn(150))*time.Millisecond
	rn.electionTimer = time.NewTimer(timeout)
	rn.role = Candidate
	rn.currentTerm = rn.currentTerm + 1
	rn.votedFor = rn.id
	rn.voteCount = 1
	for _, peer := range rn.peers {
		go rn.requestVoteRPC(peer)
	}	
}

//reset election timer
func (rn *RaftNode) resetElectionTimer() {
	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
	}

	timeout := time.Duration(150+rand.Intn(150))*time.Millisecond
	rn.electionTimer = time.NewTimer(timeout)

	go func() {
		<-rn.electionTimer.C
		rn.startElectionTimer()
	}()
}

func (rn *RaftNode) startHeartBeat(){
	ticker := time.NewTicker(100 * time.Millisecond)
	go func() {
		for rn.role == Leader {
			for _, peer := range rn.peers {
				go rn.sendHeartBeat(peer)
			}
			<-ticker.C
		}
		ticker.Stop()
	}()
}

//Implement handleRequestVote
// Parse incoming RequestVoteArgs (from r.Body)
// Compare incoming term to local currentTerm:
// If term > currentTerm → update term, step down to Follower
// If not voted in current term → grant vote
// If already voted → reject
// Send RequestVoteReply back as JSON

func (rn *RaftNode) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	//define RequestVoteArgs
	var args RequestVoteArgs

	//Decode incoming request body
	err := json.NewDecoder(r.Body).Decode(&args)
	if err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	rn.mu.Lock()
	defer rn.mu.Unlock()

	//if my term is stale, update term, become follower and reset votedFor
	if args.Term > rn.currentTerm {
		rn.currentTerm = args.Term
		rn.role = Follower
		rn.votedFor = ""
	}

	voteGranted := false

	//send vote to candidate and set voteGranted to true else reject vote
	if rn.votedFor == "" || rn.votedFor == args.CandidateId {
		rn.votedFor = args.CandidateId
		voteGranted = true
	} else{
		voteGranted = false
	}

	// build request to vote reply
	reply := RequestVoteReply{
		Term: rn.currentTerm,
		VoteGranted: voteGranted,
	}

	//encode json to reply
	json.NewEncoder(w).Encode(reply)
}

//append entries handler
func (rn *RaftNode) handleAppendEntries(w http.ResponseWriter, r *http.Request){
	
	var args AppendEntriesArgs

	err := json.NewDecoder(r.Body).Decode(&args)
	if err != nil {
		http.Error(w, "Invalid json", http.StatusBadRequest)
	}

	rn.mu.Lock()
	defer rn.mu.Unlock()

	
	if args.Term > rn.currentTerm {
		rn.currentTerm = args.Term
		rn.role = Follower
		rn.resetElectionTimer()
	}

	reply := AppendEntriesReply {
		Term: rn.currentTerm,
		Success: true,
	}

	json.NewEncoder(w).Encode(reply)
}


//Request Vote RPC

func (rn *RaftNode) requestVoteRPC(peer string){
	
	//prpeare args for sending
	args := RequestVoteArgs {
		Term: rn.currentTerm,
		CandidateId: rn.id,
	}
	
	//encode in json to be sent via http post
	body, err := json.Marshal(args)
	if err != nil {
		fmt.Println("Failed to encode RequestVoteArgs:", err)
		return
	}

	//send post request to peers
	resp, err = http.Post(peer+"/request-vote", "application/json", bytes.NewBuffer(body))
	if err != nil {
		fmt.Println("Failed to send RequestVote to", peer, ":", err)
		return
	}


	defer resp.Body.Close()

	//decode reply from peers response
	var reply RequestVoteReply 
	err = json.NewDecoder(resp.Body).Decode(&reply)
	if err != nil {
		fmt.Println("Faile to decode RequestVoteReply from", peer, ":", err)
		return
	}

	// Lock the node
	rn.mu.Lock()
	defer rn.mu.Unlock()

	//become leader
	if reply.Term > rn.currentTerm {
		rn.currentTerm = reply.Term
		rn.role = Follower
		rn.votedFor = ""
		return
	}

	if reply.VoteGranted {
		rn.voteCount++
		if rn.voteCount > (len(rn.peers) + 1) / 2 {
			fmt.Println(rn.id, "won the electino and became Leader")
			rn.role = Leader
			if(rn.electionTimer != nil){
				rn.electionTimer.Stop()
			}

			rn.startHeartBeat()
		}
	}

	
}

func (rn *RaftNode) sendHeartBeat(peer string){
	args := AppendEntriesArgs {
		Term: rn.currentTerm,
		LeaderId: rn.id,
	}

	body, err := json.Marshal(args)
	if (err != nil) {
		fmt.Println("Failed to encode heartbeat", err)
		return
	}

	resp, err := http.Post(peer+"/append-entries", "application/json", bytes.NewBuffer(body))
	if(err != nil){
		fmt.Println("Failed to send heartbeat to", peer, ":", err)
		return
	}

	defer resp.Body.Close()

	var reply AppendEntriesReply

	err = json.NewDecoder(resp.Body).Decode(&reply)
	if err != nil {
		fmt.Println("Failed to decode heartbeat reply from", peer, ":", err)
		return
	}

	rn.mu.Lock()
	defer rn.mu.Unlock()

	if reply.Term > rn.currentTerm{
		rn.currentTerm = reply.Term
		rn.role = Follower
		rn.votedFor = ""
		rn.resetElectionTimer()
	}
}

func main() {
	node := NewRaftNode(...)
	http.HandleFunc("/request-vote", func(w , r) {
		node.handleRequestVote(w, r)
	})

	http.HandleFunc("/append-entries", func(w, r) {
		node.handleAppendEntries(w,r)
	})

	fmt.Println("Starting Raft node at :8080")
	http.ListenAndServe(":8080", nil)

}



