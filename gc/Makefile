BINARY   := cvmfs-gc
MIRROR   := cvmfs-mirror
CMD      := ./cmd/cvmfs-gc
MIRROR_CMD := ./cmd/cvmfs-mirror
GOFLAGS  ?=

# Ensure go.mod/go.sum are up to date.
.PHONY: tidy
tidy:
	go mod tidy

# Default: build both binaries for the host platform.
.PHONY: build
build: tidy
	CGO_ENABLED=0 go build $(GOFLAGS) -o $(BINARY) $(CMD)
	CGO_ENABLED=0 go build $(GOFLAGS) -o $(MIRROR) $(MIRROR_CMD)

# Cross-compile static linux/amd64 binaries.
# No cgo or extra toolchain needed — SQLite is pure Go.
.PHONY: linux
linux: tidy
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
		go build $(GOFLAGS) -o $(BINARY)-linux-amd64 $(CMD)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
		go build $(GOFLAGS) -o $(MIRROR)-linux-amd64 $(MIRROR_CMD)

.PHONY: test
test:
	go test ./... -v -count=1

.PHONY: clean
clean:
	rm -f $(BINARY) $(BINARY)-linux-amd64
	rm -f $(MIRROR) $(MIRROR)-linux-amd64

.PHONY: help
help:
	@echo "Targets:"
	@echo "  build   Build cvmfs-gc and cvmfs-mirror for the host platform (default)"
	@echo "  linux   Cross-compile static linux/amd64 binaries"
	@echo "  tidy    Run go mod tidy"
	@echo "  test    Run all tests"
	@echo "  clean   Remove built binaries"
