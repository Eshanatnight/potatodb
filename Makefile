.PHONY: all help build check clippy-full clippy-base clippy clippy-fix fmt fmt-check clean test \
       rust-test bench bench-html release run run-repl run-release doc ffi ffi-example ffi-test \
       python-test package perftest perf-save perf-compare perf-mimalloc perf-jemalloc perf-alloc-compare

all: fmt clippy test build

PYTHON ?= $(shell command -v python3 || command -v python)
PACKAGE_DIR ?= dist
PACKAGE_NAME ?= potatodb
PACKAGE_VERSION ?=
PACKAGE_FILE ?=

help:
	@echo "PotatoDB Make Targets"
	@echo ""
	@echo "  all             Format, lint, test, and build"
	@echo "  build           Build all workspace crates"
	@echo "  release         Build all workspace crates in release mode"
	@echo "  check           Run cargo check"
	@echo "  clippy          Run all clippy checks (base + full)"
	@echo "  clippy-base     Run standard clippy checks (warnings denied)"
	@echo "  clippy-full     Run full clippy checks (warnings allowed)"
	@echo "  clippy-fix      Auto-fix clippy suggestions where possible"
	@echo "  fmt             Format Rust code"
	@echo "  fmt-check       Check Rust formatting"
	@echo "  test            Run all test suites (Rust + FFI/C++)"
	@echo "  rust-test       Run Rust workspace tests"
	@echo "  test            Run all test suites (Rust + FFI/C++ + Python)"
	@echo "  bench           Run workspace benchmarks (cargo bench)"
	@echo "  bench-html      Run benches and print HTML report path"
	@echo "  perftest        Run performance test suite (release mode)"
	@echo "  perf-save       Run perf test and save baseline to PERF_BASELINE"
	@echo "  perf-compare    Run perf test and compare against PERF_BASELINE"
	@echo "  run             Run potatodb binary (TUI, default)"
	@echo "  run-repl        Run potatodb with line-mode REPL"
	@echo "  run-release     Run potatodb in release mode (TUI)"
	@echo "  doc             Build and open docs"
	@echo "  ffi             Build FFI crate (release)"
	@echo "  ffi-example     Build C++ FFI example binary"
	@echo "  ffi-test        Build and run C++ FFI unit tests (doctest)"
	@echo "  python-test     Build and run Python binding tests (uv + pytest)"
	@echo "  perftest        Run performance test suite (release mode)"
	@echo "  perf-mimalloc   Run perftest with mimalloc, save to perf_mimalloc.json"
	@echo "  perf-jemalloc   Run perftest with jemalloc, save to perf_jemalloc.json"
	@echo "  perf-alloc-compare  Run mimalloc then jemalloc vs mimalloc baseline (Unix)"
	@echo "  perf-save       Run perf test and save baseline to PERF_BASELINE"
	@echo "  perf-compare    Run perf test and compare against PERF_BASELINE"
	@echo "                  Tune with PERF_SCALE=N PERF_ITERS=N PERF_BASELINE=file.json"
	@echo "  package         Create a .tar.gz package in dist/"
	@echo "                  Override with PACKAGE_DIR/PACKAGE_NAME/PACKAGE_VERSION/PACKAGE_FILE"
	@echo "                  and set PYTHON='py -3' on Windows if needed"
	@echo "  clean           Remove build artifacts"

# ── Build ────────────────────────────────────────────────────

build:
	cargo build --workspace

release:
	cargo build --workspace --release
	@echo "Building FFI crate and examples..."
	@$(MAKE) ffi || true
	@$(MAKE) ffi-example || true
	# Try to build ffi CMake tests if present (non-fatal)
	@if [ -f crates/ffi/CMakeLists.txt ]; then \
	  cmake -B build -S crates/ffi 2>/dev/null || true; \
	  cmake --build build --config Release 2>/dev/null || true; \
	fi
	@echo "Release build complete. Artifacts in target/release/"

check:
	cargo check --workspace

# ── Quality ──────────────────────────────────────────────────

clippy-base:
	cargo clippy --workspace --all-targets -- -D warnings

clippy-full:
	cargo clippy --workspace -- -W clippy::pedantic -W clippy::nursery \
		-W clippy::correctness -W clippy::complexity \
		-W clippy::perf -W clippy::style -W clippy::all -D warnings

clippy: clippy-base clippy-full

clippy-fix:
	cargo clippy --workspace --fix --allow-dirty -- -W clippy::all -W clippy::correctness \
		-W clippy::complexity -W clippy::perf -W clippy::style -D warnings

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

rust-test:
	cargo test --workspace

test: rust-test ffi-test python-test

bench:
	cargo bench --workspace

bench-html: bench
	@echo "Benchmark HTML report:"
	@echo "  target/criterion/report/index.html"

PERF_BASELINE ?= perf_baseline.json
PERF_SCALE ?= 1
PERF_ITERS ?= 5

perftest:
	cargo run --release --example perftest -- --scale $(PERF_SCALE) --iterations $(PERF_ITERS)

perf-save:
	cargo run --release --example perftest -- --scale $(PERF_SCALE) --iterations $(PERF_ITERS) \
		--save $(PERF_BASELINE)

perf-compare:
	cargo run --release --example perftest -- --scale $(PERF_SCALE) --iterations $(PERF_ITERS) \
		--baseline $(PERF_BASELINE)

perf-mimalloc:
	cargo run --release --example perftest --features use-mimalloc --no-default-features \
		-- --scale $(PERF_SCALE) --iterations $(PERF_ITERS) --save perf_mimalloc.json

perf-jemalloc:
	cargo run --release --example perftest --features use-jemalloc --no-default-features \
		-- --scale $(PERF_SCALE) --iterations $(PERF_ITERS) --save perf_jemalloc.json

perf-alloc-compare: perf-mimalloc
	cargo run --release --example perftest --features use-jemalloc --no-default-features \
		-- --baseline perf_mimalloc.json --save perf_jemalloc.json \
		--scale $(PERF_SCALE) --iterations $(PERF_ITERS)

# ── Run ──────────────────────────────────────────────────────

run:
	cargo run -p potatodb

run-repl:
	cargo run -p potatodb -- --repl

run-release:
	cargo run -p potatodb --release

# ── Documentation ────────────────────────────────────────────

doc:
	cargo doc --workspace --open

# ── FFI ──────────────────────────────────────────────────────

ffi:
	cargo build --release -p potatodb-ffi

ffi-example: ffi
	g++ -std=c++17 -fno-exceptions -O2 \
		-Icrates/ffi/include \
		crates/ffi/examples/main.cpp \
		-Ltarget/release -lpotatodb_ffi \
		-lpthread -ldl -lm \
		-o target/release/potatodb_cpp_example

ffi-test: ffi
	cmake -B build -S crates/ffi
	cmake --build build --target potatodb_tests --config Release
	ctest --test-dir build --output-on-failure -C Release

# ── Python bindings ──────────────────────────────────────────

python-test:
	cd crates/python && uv sync && uv run pytest tests/ -v

# ── Packaging ────────────────────────────────────────────────

package:
	$(PYTHON) -c "import pathlib,re,tarfile; root=pathlib.Path('.').resolve(); pkg_dir=pathlib.Path('$(PACKAGE_DIR)'); pkg_name='$(PACKAGE_NAME)'; ver='$(PACKAGE_VERSION)'.strip(); out='$(PACKAGE_FILE)'.strip(); txt=pathlib.Path('Cargo.toml').read_text(encoding='utf-8'); m=re.search(r'(?ms)^\\[workspace\\.package\\]\\s+.*?^version\\s*=\\s*\"([^\"]+)\"', txt); ver=ver or (m.group(1) if m else '0.0.0'); pkg_dir.mkdir(parents=True, exist_ok=True); out=out or str(pkg_dir / ('%s-%s.tar.gz' % (pkg_name, ver))); excluded={'target','.git','.cursor','.idea','.vscode', pkg_dir.as_posix()}; tar=tarfile.open(out,'w:gz'); [tar.add(path, arcname=str(path.relative_to(root))) for path in root.rglob('*') if not any(part in excluded for part in path.relative_to(root).parts)]; tar.close(); print('Created %s' % out)"

# ── Cleanup ──────────────────────────────────────────────────

clean:
	cargo clean
	rm -f target/release/potatodb_cpp_example
