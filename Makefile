.PHONY: all build proto certs clean

all: build

build: proto certs
	@mkdir -p dist
	@go build -o dist/mvzserver .

proto:
	@sh scripts/gen-proto.sh

certs:
	@sh scripts/gen-certs.sh

clean:
	@rm -rf dist
