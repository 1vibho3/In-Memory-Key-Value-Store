package main

import (
	"fmt"
	"sync"
	"net/http"
	"encoding/json"
	"flag"
	"strings"
	"bytes"
)

// struct for KeyValue
type KeyValue struct {
	Key string `json:"key"`
	Value string `json:"value"`
}

type Store struct {
	store map[string]string
	mu sync.RWMutex
	peers []string
}

func NewStore(peers []string) *Store{
	return &Store{
		store: make(map[string]string),
		peers: peers,
	}
}

func (st *Store) setStore(key string, value string){
	st.mu.Lock()
	defer st.mu.Unlock()
	st.store[key] = value	
}

func (st *Store) getStore(key string) string{
	st.mu.RLock()
	defer st.mu.RUnlock()
	return st.store[key]
}

func (st *Store) setAndReplicate(key string, value string) {
	st.setStore(key, value)

	for _, peer := range st.peers {
		peerURL := peer

		go func(peer string) {

			kv := KeyValue{Key: key, Value: value}
			body, err := json.Marshal(kv)
			if err != nil {
				fmt.Println("Failed to encode JSON", err)
				return
			}

			resp, err := http.Post(peerURL+"/replicate", "application/json", bytes.NewBuffer(body))
			if err != nil {
				fmt.Println("Replicateion to", peerURL, "failed", err)
				return 
			}
			resp.Body.Close()
		}(peerURL)
	}	
}

func handleReplicate(w http.ResponseWriter, r *http.Request, st *Store){
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	var kv KeyValue
	err := json.NewDecoder(r.Body).Decode(&kv)
	if err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	st.setStore(kv.Key, kv.Value)

	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "Value set successfully")

}


func handleSet(w http.ResponseWriter, r *http.Request, st *Store) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	var kv KeyValue
	err := json.NewDecoder(r.Body).Decode(&kv)
	if err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	st.setAndReplicate(kv.Key, kv.Value)

	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "Value set successfully")
}

func handleGet(w http.ResponseWriter, r *http.Request, st *Store) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key parameter", http.StatusBadRequest)
		return
	}

	val := st.getStore(key)
	json.NewEncoder(w).Encode(map[string]string{"value": val})
}

func main() {
	// Define CLI flags
	port := flag.String("port", "8080", "Port to run the server on")
	peersArg := flag.String("peers", "", "Comma-separated list of peer URLs")

	flag.Parse()

	// Parse the peers from comma-separated list
	var peers []string
	if *peersArg != "" {
		peers = strings.Split(*peersArg, ",")
	}

	// Create store with peer list
	store := NewStore(peers)

	// Set up HTTP handlers
	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		handleSet(w, r, store)
	})
	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		handleGet(w, r, store)
	})
	http.HandleFunc("/replicate", func(w http.ResponseWriter, r *http.Request) {
		handleReplicate(w, r, store)
	})

	addr := fmt.Sprintf(":%s", *port)
	fmt.Println("Node running on", addr)
	http.ListenAndServe(addr, nil)
}
