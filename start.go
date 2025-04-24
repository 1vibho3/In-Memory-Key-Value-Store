package main

import (
	"fmt"
	"sync"
	"net/http"
	"encoding/json"
)

var instance = NewStore()

type KeyValue struct {
	Key string `json:"key"`
	Value string `json:"value"`
}

type Store struct {
	store map[string]string
	mu sync.RWMutex
}

func NewStore() *Store{
	return &Store{
		store: make(map[string]string),
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

func handleSet(w http.ResponseWriter, r *http.Request) {
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

	instance.setStore(kv.Key, kv.Value)

	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "Value set successfully")
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key parameter", http.StatusBadRequest)
		return
	}

	val := instance.getStore(key)
	json.NewEncoder(w).Encode(map[string]string{"value": val})
}



func main() {

	http.HandleFunc("/set", handleSet)
	http.HandleFunc("/get", handleGet)
	http.ListenAndServe(":8080", nil)
}