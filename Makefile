.PHONY: test proto fmt clippy

# Application protos: sibling checkout, else BSR, else GitHub (join/heartbeat are a separate module).
PROTO_DIR ?= ../clusdr/proto/api
BSR_MODULE ?= buf.build/clusdr/api
PROTO_REF ?= main
GIT_PROTO ?= https://github.com/clusdr/clusdr.git#branch=$(PROTO_REF),subdir=proto/api

proto:
	@command -v buf >/dev/null || { echo "buf is required: https://buf.build/docs/cli/installation"; exit 1; }
	@if [ -d "$(PROTO_DIR)/clusdr/v1alpha1" ]; then \
		buf export "$(PROTO_DIR)" -o proto; \
	elif buf build "$(BSR_MODULE):$(PROTO_REF)" >/dev/null 2>&1; then \
		buf export "$(BSR_MODULE):$(PROTO_REF)" -o proto; \
	else \
		buf export "$(GIT_PROTO)" -o proto; \
	fi

test:
	cargo test

fmt:
	cargo fmt --check

clippy:
	cargo clippy --all-targets -- -D warnings
