package main

import (
	"fmt"
	"sync"
	"time"
)

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

func main() {
	var wg sync.WaitGroup
	wg.Add(6)

	instance := NewStore()

	go func() {
		defer wg.Done()
		time.Sleep(100 * time.Millisecond)
		instance.setStore("apple", "red")
		fmt.Println("Wrote: red")
	}()

	go func() {
		defer wg.Done()
		time.Sleep(200 * time.Millisecond)
		fmt.Println(instance.getStore("apple"))
	}()

	go func() {
		defer wg.Done()
		time.Sleep(300 * time.Millisecond)
		instance.setStore("apple", "dark red")
		fmt.Println("Wrote: dark red")
	}()

	go func() {
		defer wg.Done()
		time.Sleep(400 * time.Millisecond)
		instance.setStore("mango", "yellow")
		fmt.Println("Wrote: Yellow")
	}()

	go func() {
		defer wg.Done()
		time.Sleep(500 * time.Millisecond)
		fmt.Println(instance.getStore("mango"))
	}()

	go func() {
		defer wg.Done()
		time.Sleep(600 * time.Millisecond)
		fmt.Println(instance.getStore("apple"))
	}()
	
	wg.Wait()

	// color1 :=  instance.getStore("apple")
	// color2 :=  instance.getStore("mango")
	
	
	// fmt.Println(color1)
	// fmt.Println(color2)

	
}