# Usage:
# make        		# run default command

# To check entire script:
# cat -e -t -v Makefile

.EXPORT_ALL_VARIABLES:

LOG_LEVEL=debug
GCS_BUCKET_NAME=
READ_INTERVAL=15s
READ_TIMEOUT=15s

.PHONY: all

all: fmt tidy lint

fmt:
	go fmt ./...

tidy:
	go mod tidy

run:
	go run ./cmd/app

vet:
	go vet ./...

lint:
	docker run --rm --name golangci-lint -v `pwd`:/workspace -w /workspace \
		golangci/golangci-lint:latest-alpine golangci-lint run --issues-exit-code 1

test:
	go clean -testcache
	go test -race -v ./...

coverage:
	go clean -testcache
	go test -v -cover -coverprofile=.coverage.cov ./...
	go tool cover -func=.coverage.cov
	go tool cover -html=.coverage.cov
	rm .coverage.cov

docker-build:
	docker build -f Dockerfile -t datapuller:latest .

docker-run:
	docker run --rm --name="datapuller" \
		-e GCS_BUCKET_NAME=$(GCS_BUCKET_NAME) \
		datapuller:latest
