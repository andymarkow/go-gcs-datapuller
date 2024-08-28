package main

import (
	"log"

	"github.com/andymarkow/go-gcs-datapuller/internal/app"
)

func main() {
	a, err := app.NewApp()
	if err != nil {
		log.Fatalf("app.NewApp: %v", err)
	}

	if err := a.Start(); err != nil {
		if err := a.Shutdown(); err != nil {
			log.Fatalf("app.Shutdown: %v", err)
		}

		log.Fatalf("app.Start: %v", err)
	}

	if err := a.Shutdown(); err != nil {
		log.Fatalf("app.Shutdown: %v", err)
	}
}
