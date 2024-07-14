package main

import (
	"github.com/co-defi/api-server/cmd"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	cmd.Execute()
}
