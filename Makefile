all: clean test build

clean:
	rm -fr ./k6

build:
	go mod tidy
	go install go.k6.io/xk6/cmd/xk6@latest
	CGO_ENABLED=1 $(HOME)/go/bin/xk6 build --with $(shell go list -m)=.

test: unit system

unit: build
	go test -cover -race ./...

system: build
	./k6 run --vus 10 --duration 30s tests/cdc.js
