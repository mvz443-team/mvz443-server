.PHONY: all build proto clean

all: build

build: proto
	@mkdir -p dist
	@go build -o dist/mvzserver .

proto:
	@sh scripts/gen-proto.sh

clean:
	@rm -rf dist
