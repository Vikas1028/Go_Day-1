package main

import (
	"fmt"
	"strconv"
)

func main() {
	var number string
	fmt.Print("Enter a number: ")
	fmt.Scanln(&number)

	result, _ := strconv.ParseInt(number, 10, 64)
	result = result * 10
	fmt.Println(result)
	number = strconv.Itoa(int(result))
	fmt.Println("10 times the number:", number)
}
