package main

import (
	"errors"
	"log"

	"github.com/andymarkow/go-gcs-datapuller/internal/app"
)

func main() {
	a, err := app.NewApp()
	if err != nil {
		log.Fatalf("init: %v", errors.Unwrap(err))
	}

	if err := a.Start(); err != nil {
		if err := a.Shutdown(); err != nil {
			log.Fatalf("shutdown: %v", errors.Unwrap(err))
		}

		log.Fatalf("startup: %v", err)
	}

	if err := a.Shutdown(); err != nil {
		log.Fatalf("shutdown: %v", err)
	}
}
