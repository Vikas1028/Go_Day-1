package main

import (
	"fmt"
)

func main() {
	var firstName, lastName string
	fmt.Print("Enter your friend FirstName: ")
	fmt.Scanln(&firstName)
	fmt.Print("Enter your friend FirstName: ")
	fmt.Scanln(&lastName)

	fmt.Print(firstName, " ", lastName)

}
