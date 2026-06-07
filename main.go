package main

import (
	"flag"

	"aieas_backend/internal/app"
	"aieas_backend/internal/config"
)

func main() {
	configPath := flag.String("config", config.DefaultPath, "config file path")
	flag.Parse()

	h := app.NewServerFromConfigPath(*configPath)
	h.Spin()
}
