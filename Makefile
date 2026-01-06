# CortexDB Build System
# ====================

# Default target
.PHONY: all
all: build

# Build targets
.PHONY: build
build:
	cargo build

.PHONY: build-release
build-release:
	cargo build --release

.PHONY: clean
clean:
	cargo clean
	rm -rf target/
	rm -rf python/build/
	rm -rf python/dist/
	rm -rf python/cortexdb.egg-info/

# Test targets
.PHONY: test
test:
	cargo test

.PHONY: test-release
test-release:
	cargo test --release

.PHONY: test-integration
test-integration:
	cargo test --test integration_test

# Format and lint
.PHONY: format
format:
	cargo fmt

.PHONY: lint
lint:
	cargo clippy

.PHONY: check
check:
	cargo check

# Documentation
.PHONY: doc
doc:
	cargo doc --no-deps --open

# Python targets
.PHONY: python-build
python-build:
	cd python && python -m build

.PHONY: python-install
python-install:
	cd python && pip install -e .

.PHONY: python-test
python-test:
	cd python && python -m pytest tests/ -v

# Docker targets
.PHONY: docker-build
docker-build:
	docker build -t cortexdb:latest .

.PHONY: docker-run
docker-run:
	docker run -p 8080:8080 cortexdb:latest

# Benchmark targets
.PHONY: bench
bench:
	cargo bench

# Utility targets
.PHONY: update-deps
update-deps:
	cargo update

.PHONY: help
help:
	@echo "CortexDB Build System"
	@echo "===================="
	@echo ""
	@echo "Build targets:"
	@echo "  build           - Build the project in debug mode"
	@echo "  build-release   - Build the project in release mode"
	@echo "  clean           - Clean the build artifacts"
	@echo ""
	@echo "Test targets:"
	@echo "  test            - Run all tests"
	@echo "  test-release    - Run tests in release mode"
	@echo "  test-integration - Run integration tests"
	@echo ""
	@echo "Format and lint:"
	@echo "  format          - Format the code"
	@echo "  lint            - Run clippy linter"
	@echo "  check           - Run cargo check"
	@echo ""
	@echo "Documentation:"
	@echo "  doc             - Generate and open documentation"
	@echo ""
	@echo "Python targets:"
	@echo "  python-build    - Build Python package"
	@echo "  python-install  - Install Python package in development mode"
	@echo "  python-test     - Run Python tests"
	@echo ""
	@echo "Docker targets:"
	@echo "  docker-build    - Build Docker image"
	@echo "  docker-run      - Run Docker container"
	@echo ""
	@echo "Benchmark targets:"
	@echo "  bench           - Run benchmarks"
	@echo ""
	@echo "Utility targets:"
	@echo "  update-deps     - Update dependencies"
	@echo "  help            - Show this help message"
