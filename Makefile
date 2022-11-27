build:
	mkdir -p bin
	go build -o bin/tiproxytest main.go

run:
	./bin/tiproxytest
