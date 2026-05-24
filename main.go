package main

import "aieas_backend/internal/app"

func main() {
	h := app.NewServer()
	h.Spin()
}
