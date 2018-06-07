package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Println(os.ExpandEnv("$USER has home ${HOME} ip $ip"))
	return
}
