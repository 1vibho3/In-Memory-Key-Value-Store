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

// 2. Define RPC messages
// RequestVoteArgs:
// term, candidateId, lastLogIndex, lastLogTerm

type RequestVoteArgs struct {
	Term int
	CandidateId string
	// lastLogIndex int
	// lastLogTerm int
}

// RequestVoteReply:
// term, voteGranted

typeRequestVoteReply struct {
	Term int
	VoteGranted bool
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
	if args.term > rn.currentTerm {
		rn.currentTerm = args.term
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

func (rn *RaftNode) RequestVoteRPC(peer string){
	
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

	rn.mu.Lock()
	defer rn.mu.Unlock()

	if reply.Term > rn.currentTerm {
		rn.currentTerm = reply.Term
		rn.role = Follower
		rn.votedFor = ""
		return
	}

	if reply.voteGranted {
		rn.voteCount++
		if rn.voteCount > len(rn.peers) + 1 / 2 {
			fmt.Println(rn.id, "won the electino and became Leader")
			rn.role = Leader
			if(rn.electionTimer != nil){
				rn.electionTimer.Stop()
			}
		}
	}

	
}

func main() {
	node := NewRaftNode(...)
	http.HandleFunc("/request-vote", func(w , r) {
		node.handleRequestVote(w, r)
	})
}



