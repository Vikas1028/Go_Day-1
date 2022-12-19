package main

import (
	"fmt"
)

func main() {
	var tempC float64
	fmt.Print("Enter temperature in degree Celsius: ")
	fmt.Scanln(&tempC)
	tempF := (tempC * 9 / 5) + 32
	fmt.Printf("Temperature in Fahrenheit: ", tempF)
}
