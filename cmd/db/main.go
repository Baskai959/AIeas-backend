package main

import (
	"context"
	"fmt"
	"os"
)

func main() {
	if err := newCLI(os.Stdout, os.Stderr).run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
