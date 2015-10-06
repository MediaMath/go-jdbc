package main

import (
	gojdbc ".."
	"flag"
	"fmt"
	"os"
)

var (
	name = flag.String("conn", "", "The connection string to the groovy server")
)

func init() {
	flag.Parse()
}

func main() {
	if *name == "" {
		flag.Usage()
		os.Exit(1)
	}

	errExit := func(e error) {
		if e != nil {
			fmt.Println(e)
			os.Exit(1)
		}
	}

	s, e := gojdbc.ServerStatus(*name)
	errExit(e)
	fmt.Println(s)

}
