export GOBIN=$(PWD)/bin
OUTPUT_DIR=bin

.PHONY: clean

all: build

build:
	@mkdir -p bin/
	go build -o $(OUTPUT_DIR)/qcow2util cmd/main.go

fmt:
	gofmt -w ./lib ./cmd 

vet:
	go vet ./lib/... ./cmd/...

# Run tests
uinttest: fmt vet
	go test ./lib/... ./cmd/... -covermode=atomic -coverprofile=coverage.txt

.PHONY: clean
clean:
	@rm -rf bin/
	@echo "ok"

test:
	./test.sh
