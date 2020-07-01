package main

import (
	"os"

	"github.com/oliverkra/gqm/cmd"
)

func main() {
	if err := cmd.Run(os.Args); err != nil {
		panic(err)
	}
}