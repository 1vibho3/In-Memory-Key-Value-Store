package main

import "fmt"

type Store struct {
	store map[string]string
}

func NewStore() *Store{
	return &Store{
		store: make(map[string]string),
	}
}

func (st *Store) setStore(key string, value string){
	st.store[key] = value
}

func (st *Store) getStore(key string) string{
	return st.store[key]
}

func main() {
	instance := NewStore()
	instance.setStore("apple", "red")
	instance.setStore("mango", "yellow")
	color1 := instance.getStore("apple")
	color2 := instance.getStore("mango")

	fmt.Println(color1)
	fmt.Println(color2)
}