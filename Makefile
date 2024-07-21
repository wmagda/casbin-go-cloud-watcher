.SHELLFLAGS = -ecuo pipefail
SHELL = /bin/bash

# Arguments
TMPDIR ?= ./tmp
COVERPROFILE ?= coverage.out
RELEASE_DRYRUN ?=

# Commands
GO := go
GOTEST := $(GO) test
GOBUILD := $(GO) build
GOCLEAN := $(GO) clean

# Flags
GOTEST_FLAGS := -v -coverprofile=$(COVERPROFILE)
GOBUILD_FLAGS :=
RELEASEFLAGS :=
ifneq ($(RELEASE_DRYRUN),)
	RELEASEFLAGS += --dry-run
endif

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

.PHONE: update
update:
	$(GO) mod tidy
	$(GO) get -u

.PHONY: release
release:
	yarn install
	yarn run semantic-release $(RELEASEFLAGS)