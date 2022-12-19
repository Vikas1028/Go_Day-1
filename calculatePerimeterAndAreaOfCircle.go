package main

import (
	"fmt"
)

func main() {
	var diameter float32
	fmt.Print("Enter diameter of the circle: ")
	fmt.Scanln(&diameter)

	radius := diameter / 2

	perimeter := 2 * 3.14 * radius

	area := 3.14 * (radius * radius)

	fmt.Println("Perimeter (circumference) of the circle: ", perimeter)
	fmt.Println("Area of the circle: ", area)
}
