BIN := yhmatrix
PROBE := yhprobe

.PHONY: all build test vet fmt run mock proto clean

all: build

build:
	go build -trimpath -o bin/$(BIN) ./cmd/yhmatrix
	go build -trimpath -o bin/$(PROBE) ./cmd/yhprobe

test:
	go test ./...

vet:
	go vet ./...

fmt:
	gofmt -w ./cmd ./internal

run:
	go run ./cmd/yhmatrix -config config.yaml

mock:
	go run ./cmd/yhmatrix -mock -bind 127.0.0.1:8008 -data ./tmpdata

proto:
	protoc -I proto --go_out=. --go_opt=module=github.com/yunhu/yhmatrix proto/yh.proto

clean:
	rm -rf bin tmpdata
