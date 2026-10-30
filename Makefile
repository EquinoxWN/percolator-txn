.PHONY: setup lint test test-norace bench demo contend ci audit

setup:
	go mod download

lint:
	@files="$$(gofmt -l $$(go list -f '{{$$d := .Dir}}{{range .GoFiles}}{{$$d}}/{{.}} {{end}}{{range .TestGoFiles}}{{$$d}}/{{.}} {{end}}{{range .XTestGoFiles}}{{$$d}}/{{.}} {{end}}' ./...))"; test -z "$$files" || (echo "$$files"; echo "run gofmt -w"; exit 1)
	go vet ./...

test:
	go test -race -count=1 ./...

# Same tests without the race detector (it needs cgo, which not every machine has).
test-norace:
	go test -count=1 ./...

bench:
	go test -run '^$$' -bench . -benchmem -benchtime 2s .

demo:
	go run ./cmd/percolator-txn demo

contend:
	go run ./cmd/percolator-txn contend -workers 8 -txns 20000 -accounts 1000 -seed 1

# Known vulnerabilities in the code paths actually called (needs Go 1.26+, as in CI).
audit:
	go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./...

ci: setup lint test
