.PHONY: build
build:
	go build -mod=vendor -o build/bin/kvdroid-server cmd/kvdroid-server/kvdroid-server.go
	go build -mod=vendor -o build/bin/kvdroid-stop cmd/kvdroid-stop/kvdroid-stop.go

init: tidy
tidy: 
	go mod tidy
	go mod vendor

test:
	go vet -mod=vendor ./...
	# count=1 deactivate test results caching
	go test -mod=vendor -failfast -race -count=1 -timeout 10s ./...  

clean:
	rm -rf build/
