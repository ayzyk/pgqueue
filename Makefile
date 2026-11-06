DATABASE_URL ?= postgres://pgqueue:pgqueue@localhost:5434/pgqueue?sslmode=disable

.PHONY: db demo test

db:
	docker compose up -d --wait

demo:
	DATABASE_URL="$(DATABASE_URL)" go run ./cmd/demo -jobs 200

test:
	TEST_DATABASE_URL="$(DATABASE_URL)" go test -race -count=1 ./...
