.SHELLFLAGS = -ecuo pipefail
SHELL = /bin/bash

# Arguments
TMPDIR ?= ./tmp
COVERPROFILE ?= coverage.out

# Commands
GO := go
GOTEST := $(GO) test
GOBUILD := $(GO) build
GOCLEAN := $(GO) clean

# Flags
GOTEST_FLAGS := -v -coverprofile=$(COVERPROFILE)
GOBUILD_FLAGS :=

.PHONY: test
test:
	$(GOTEST) $(GOTEST_FLAGS) ./...


.PHONY: build
build:
	$(GOBUILD) $(GOBUILD_FLAGS) -o $(TMPDIR)/main .

.PHONY: clean
clean:
	$(GOCLEAN)
	rm -rf $(TMPDIR)

.PHONY: lint
lint:
	golangci-lint run --fix --verbose