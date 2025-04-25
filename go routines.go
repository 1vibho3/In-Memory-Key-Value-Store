// var wg sync.WaitGroup
	// wg.Add(6)

	// instance := NewStore()

	// go func() {
	// 	defer wg.Done()
	// 	time.Sleep(100 * time.Millisecond)
	// 	instance.setStore("apple", "red")
	// 	fmt.Println("Wrote: red")
	// }()

	// go func() {
	// 	defer wg.Done()
	// 	time.Sleep(200 * time.Millisecond)
	// 	fmt.Println(instance.getStore("apple"))
	// }()

	// go func() {
	// 	defer wg.Done()
	// 	time.Sleep(300 * time.Millisecond)
	// 	instance.setStore("apple", "dark red")
	// 	fmt.Println("Wrote: dark red")
	// }()

	// go func() {
	// 	defer wg.Done()
	// 	time.Sleep(400 * time.Millisecond)
	// 	instance.setStore("mango", "yellow")
	// 	fmt.Println("Wrote: Yellow")
	// }()

	// go func() {
	// 	defer wg.Done()
	// 	time.Sleep(500 * time.Millisecond)
	// 	fmt.Println(instance.getStore("mango"))
	// }()

	// go func() {
	// 	defer wg.Done()
	// 	time.Sleep(600 * time.Millisecond)
	// 	fmt.Println(instance.getStore("apple"))
	// }()
	
	// wg.Wait()