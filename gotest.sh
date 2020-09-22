# run this script in project root directory

# Vet examines Go source code and reports suspicious constructs
go vet ./...

# Run all unittests
source env.sh
go clean -testcache &&\
    go test -v ./... | grep FAIL -B 1 -A 1
