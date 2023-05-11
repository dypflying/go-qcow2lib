export GOBIN=$(PWD)/bin
OUTPUT_DIR=bin

.PHONY: clean

all: build

build:
	@mkdir -p bin/
	go build -o $(OUTPUT_DIR)/qcow2util cmd/main.go

fmt:
	gofmt -w ./qcow2 ./cmd 

vet:
	go vet ./qcow2/... ./cmd/...

# Run tests
unit: vet
	go test ./qcow2/... -covermode=atomic -coverprofile=coverage.txt

.PHONY: clean
clean:
	@rm -rf bin/
	@echo "ok"

test:
	./test.sh
