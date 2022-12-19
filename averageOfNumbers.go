package main

import (
	"fmt"
	"math/rand"
	"time"
)

func main() {
	fmt.Println("Five Random Numbers")
	var randomNumbers [5]int
	var sum int
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < 5; i++ {
		randomNumbers[i] = rand.Intn(39) + 10
		sum += randomNumbers[i]
	}
	average := sum / 5
	fmt.Println(randomNumbers)
	fmt.Println(average)
}
