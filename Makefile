build:
	mkdir -p bin
	go build -o bin/tiproxytest main.go

run:
	./bin/tiproxytest

run-tiproxy:
	./bin/tiproxytest -pd-host=182.83.1.86 -pd-port=2379 --message="Verify that TiDB node restart is invisible to client"

run-nlb:
	./bin/tiproxytest -pd-host=182.83.1.86 -pd-port=2379 -db-host=mgtest-acaaf60595c86139.elb.us-east-1.amazonaws.com -db-port=4000 --message="Verify DB retry process druing TiDB nodes restart"

run-no-restart:
	./bin/tiproxytest -pd-host=182.83.1.86 -pd-port=2379 -db-host=mgtest-acaaf60595c86139.elb.us-east-1.amazonaws.com -db-port=4000 --interval=0 --message="No TiDB node restart to measure the execution time"
