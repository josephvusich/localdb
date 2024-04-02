.PHONY: install test

test:
	go test ./...

install: test
	go install ./...
