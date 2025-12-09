#!/usr/bin/env just --justfile

update:
  go get -u
  go mod tidy -v

up:
  docker compose up --build --detach

down:
    docker compose down -v

downstream:
    go run ./test/downstream

upstream:
    go run ./test/upstream