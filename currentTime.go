package main

import (
	"fmt"
	"time"
)

func main() {

	currDateTime := time.Now()
	fmt.Println("Current date and time:", currDateTime)

	year, month, day := currDateTime.Date()
	fmt.Println("Year:", year)
	fmt.Println("Month:", month)
	fmt.Println("Day:", day)

	dateString := currDateTime.Format("02-Jan-2006")
	fmt.Println("DD-MMM-YYYY : ", dateString)
}
