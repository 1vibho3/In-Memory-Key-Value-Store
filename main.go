// main.go — a tiny Raft‑backed, in‑memory key‑value store.
//
// Spin up three nodes in separate shells, e.g.:
//
//   go run main.go --port=8080 --id=node1 --peers=http://localhost:8081,http://localhost:8082
//   go run main.go --port=8081 --id=node2 --peers=http://localhost:8080,http://localhost:8082
//   go run main.go --port=8082 --id=node3 --peers=http://localhost:8080,http://localhost:8081
//
// The first node to collect a majority of votes becomes leader and
// sends 100 ms heartbeats to keep the cluster stable.  Use:
//
//   curl -d '{"key":"foo","value":"bar"}' -H "Content-Type: application/json" \
//        http://localhost:8080/set
//
// from the leader, then:
//
//   curl 'http://localhost:8081/get?key=foo'
//
// on a follower to see replication in action.

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"
)

/* ───────────────────────────
   Raft roles & shared types
   ─────────────────────────── */

type Role string

const (
	Follower  Role = "Follower"
	Candidate Role = "Candidate"
	Leader    Role = "Leader"
)

/* ───────────────────────────
   Key‑value store
   ─────────────────────────── */

type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Store struct {
	mu    sync.RWMutex
	data  map[string]string
	peers []string
}

func NewStore(peers []string) *Store {
	return &Store{data: make(map[string]string), peers: peers}
}

func (st *Store) setStore(key, value string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.data[key] = value
}

func (st *Store) getStore(key string) string {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return st.data[key]
}

// set locally **then** fire‑and‑forget replication RPCs to peers.
func (st *Store) setAndReplicate(key, value string) {
	st.setStore(key, value)
	for _, peer := range st.peers {
		go func(p string) {
			body, _ := json.Marshal(KeyValue{Key: key, Value: value})
			http.Post(p+"/replicate", "application/json", bytes.NewBuffer(body))
		}(peer)
	}
}

/* ───────────────────────────
   Raft node definition
   ─────────────────────────── */

type RaftNode struct {
	mu            sync.Mutex
	id            string
	peers         []string
	currentTerm   int
	votedFor      string
	role          Role
	electionTimer *time.Timer
	voteCount     int
	store         *Store
}

type RequestVoteArgs struct {
	Term        int    `json:"Term"`
	CandidateId string `json:"CandidateId"`
}
type RequestVoteReply struct {
	Term        int  `json:"Term"`
	VoteGranted bool `json:"VoteGranted"`
}

type AppendEntriesArgs struct {
	Term     int    `json:"Term"`
	LeaderId string `json:"LeaderId"`
}
type AppendEntriesReply struct {
	Term    int  `json:"Term"`
	Success bool `json:"Success"`
}

func NewRaftNode(id string, peers []string, store *Store) *RaftNode {
	return &RaftNode{
		id:    id,
		peers: peers,
		role:  Follower,
		store: store,
	}
}

/* ───────────────────────────
   Election‑timer helpers
   ─────────────────────────── */

// resetElectionTimer sets a new random timeout (150‑300 ms).
// The goroutine that waits on it starts a new election **only if we
// are still a follower**; a leader ignores the timeout.
func (rn *RaftNode) resetElectionTimer() {
	if rn.electionTimer != nil {
		if !rn.electionTimer.Stop() {
			select { // drain if it had already fired
			case <-rn.electionTimer.C:
			default:
			}
		}
	}

	timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	rn.electionTimer = time.NewTimer(timeout)

	go func() {
		<-rn.electionTimer.C
		rn.mu.Lock()
		if rn.role != Leader { // followers only
			fmt.Println(rn.id, "election timeout fired; starting election")
			rn.mu.Unlock()
			rn.startElection()
		} else {
			rn.mu.Unlock()
		}
	}()
}

// helper: save vote & restart timer atomically
func (rn *RaftNode) resetAndPersistVote(candidate string) {
	rn.votedFor = candidate
	rn.resetElectionTimer()
}

/* ───────────────────────────
   Election logic
   ─────────────────────────── */

func (rn *RaftNode) startElection() {
	rn.mu.Lock()
	rn.role = Candidate
	rn.currentTerm++
	rn.votedFor = rn.id
	rn.voteCount = 1
	term := rn.currentTerm
	fmt.Printf("%s became Candidate for term %d\n", rn.id, term)
	rn.mu.Unlock()

	for _, p := range rn.peers {
		go rn.requestVoteRPC(p, term)
	}

	// New timer in case election fails
	rn.resetElectionTimer()
}

func (rn *RaftNode) requestVoteRPC(peer string, term int) {
	args := RequestVoteArgs{Term: term, CandidateId: rn.id}
	body, _ := json.Marshal(args)

	for attempt := 1; attempt <= 3; attempt++ {
		resp, err := http.Post(peer+"/request-vote", "application/json", bytes.NewBuffer(body))
		if err != nil {
			time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
			continue
		}
		defer resp.Body.Close()

		var reply RequestVoteReply
		if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
			return
		}

		rn.mu.Lock()
		defer rn.mu.Unlock()

		// step down if peer’s term is newer
		if reply.Term > rn.currentTerm {
			rn.currentTerm = reply.Term
			rn.role = Follower
			rn.votedFor = ""
			rn.resetElectionTimer()
			return
		}

		if reply.VoteGranted && rn.role == Candidate && rn.currentTerm == term {
			rn.voteCount++
			if rn.voteCount > (len(rn.peers)+1)/2 {
				fmt.Println(rn.id, "won election and became Leader")
				rn.role = Leader
				rn.startHeartbeat()
			}
		}
		return
	}
}

/* ───────────────────────────
   Heartbeats
   ─────────────────────────── */

func (rn *RaftNode) startHeartbeat() {
	ticker := time.NewTicker(100 * time.Millisecond)
	go func() {
		for range ticker.C {
			rn.mu.Lock()
			if rn.role != Leader {
				rn.mu.Unlock()
				ticker.Stop()
				return
			}
			rn.mu.Unlock()

			for _, p := range rn.peers {
				go rn.sendHeartbeat(p)
			}
		}
	}()
}

func (rn *RaftNode) sendHeartbeat(peer string) {
	rn.mu.Lock()
	args := AppendEntriesArgs{Term: rn.currentTerm, LeaderId: rn.id}
	rn.mu.Unlock()

	body, _ := json.Marshal(args)
	resp, err := http.Post(peer+"/append-entries", "application/json", bytes.NewBuffer(body))
	if err != nil {
		return
	}
	defer resp.Body.Close()

	var reply AppendEntriesReply
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return
	}

	// step down if newer term seen
	if reply.Term > rn.currentTerm {
		rn.mu.Lock()
		rn.currentTerm = reply.Term
		rn.role = Follower
		rn.votedFor = ""
		rn.mu.Unlock()
		rn.resetElectionTimer()
	}
}

/* ───────────────────────────
   RPC handlers
   ─────────────────────────── */

func (rn *RaftNode) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	var args RequestVoteArgs
	raw, err := io.ReadAll(r.Body)
	if err != nil || json.Unmarshal(raw, &args) != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	rn.mu.Lock()
	defer rn.mu.Unlock()

	// update term if necessary
	if args.Term > rn.currentTerm {
		rn.currentTerm = args.Term
		rn.role = Follower
		rn.votedFor = ""
		rn.resetElectionTimer()
	}

	reply := RequestVoteReply{Term: rn.currentTerm, VoteGranted: false}

	if args.Term == rn.currentTerm &&
		(rn.votedFor == "" || rn.votedFor == args.CandidateId) {

		reply.VoteGranted = true
		rn.resetAndPersistVote(args.CandidateId)
	}

	json.NewEncoder(w).Encode(reply)
}

func (rn *RaftNode) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	var args AppendEntriesArgs
	if json.NewDecoder(r.Body).Decode(&args) != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	rn.mu.Lock()
	defer rn.mu.Unlock()

	if args.Term >= rn.currentTerm {
		rn.currentTerm = args.Term
		rn.role = Follower
		rn.resetElectionTimer()
	}

	json.NewEncoder(w).Encode(AppendEntriesReply{Term: rn.currentTerm, Success: true})
}

/* ───────────────────────────
   KV HTTP endpoints
   ─────────────────────────── */

func handleSet(w http.ResponseWriter, r *http.Request, st *Store, raft *RaftNode) {
	if raft.role != Leader {
		http.Error(w, "Not the leader", http.StatusForbidden)
		return
	}
	var kv KeyValue
	if json.NewDecoder(r.Body).Decode(&kv) != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	st.setAndReplicate(kv.Key, kv.Value)
	fmt.Fprintln(w, "OK")
}

func handleGet(w http.ResponseWriter, r *http.Request, st *Store) {
	key := r.URL.Query().Get("key")
	val := st.getStore(key)
	json.NewEncoder(w).Encode(map[string]string{"value": val})
}

func handleReplicate(w http.ResponseWriter, r *http.Request, st *Store) {
	var kv KeyValue
	if json.NewDecoder(r.Body).Decode(&kv) != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	st.setStore(kv.Key, kv.Value)
	fmt.Fprintln(w, "OK")
}

/* ───────────────────────────
   main
   ─────────────────────────── */

func main() {
	rand.Seed(time.Now().UnixNano())

	port := flag.String("port", "8080", "HTTP port")
	id := flag.String("id", "", "Node ID")
	peersArg := flag.String("peers", "", "comma‑separated peer URLs")
	flag.Parse()

	if *id == "" {
		panic("must supply --id")
	}

	var peers []string
	if *peersArg != "" {
		peers = strings.Split(*peersArg, ",")
	}

	store := NewStore(peers)
	raft := NewRaftNode(*id, peers, store)

	// HTTP routes
	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		handleSet(w, r, store, raft)
	})
	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		handleGet(w, r, store)
	})
	http.HandleFunc("/replicate", func(w http.ResponseWriter, r *http.Request) {
		handleReplicate(w, r, store)
	})
	http.HandleFunc("/request-vote", raft.handleRequestVote)
	http.HandleFunc("/append-entries", raft.handleAppendEntries)

	go func() {
		addr := ":" + *port
		fmt.Println("Raft KV node running on", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			panic(err)
		}
	}()

	// give server a moment, then kick off the first timer
	time.Sleep(500 * time.Millisecond)
	raft.resetElectionTimer()

	select {} // block forever
}
