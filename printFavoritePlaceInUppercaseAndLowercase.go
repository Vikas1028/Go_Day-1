package main

import (
	"fmt"
	"strings"
)

func main() {
	var place string
	fmt.Print("Enter a place you would like to visit most: ")
	fmt.Scanln(&place)

	// Display the place name in uppercase
	fmt.Println("Place name in uppercase:", strings.ToUpper(place))

	// Display the place name in lowercase
	fmt.Println("Place name in lowercase:", strings.ToLower(place))
}
