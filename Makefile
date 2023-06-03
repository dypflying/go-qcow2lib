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

.PHONY: examples
examples:
	mkdir -p $(OUTPUT_DIR)/examples
	go build -o $(OUTPUT_DIR)/examples/simple examples/simple/qcow2_simple.go
	go build -o $(OUTPUT_DIR)/examples/backing examples/backing/qcow2_backing.go
	go build -o $(OUTPUT_DIR)/examples/datafile examples/datafile/qcow2_datafile.go
	go build -o $(OUTPUT_DIR)/examples/zerowrite examples/zerowrite/qcow2_zerowrite.go