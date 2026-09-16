.PHONY: test proto fmt clippy

PROTO_DIR ?= ../clusdr/proto

proto:
	cp $(PROTO_DIR)/clusdr/v1alpha1/health.proto proto/clusdr/v1alpha1/
	cp $(PROTO_DIR)/clusdr/v1alpha1/membership.proto proto/clusdr/v1alpha1/
	cp $(PROTO_DIR)/clusdr/v1alpha1/watch.proto proto/clusdr/v1alpha1/
	cp $(PROTO_DIR)/clusdr/v1alpha1/events.proto proto/clusdr/v1alpha1/
	cp $(PROTO_DIR)/clusdr/v1alpha1/locks.proto proto/clusdr/v1alpha1/
	cp $(PROTO_DIR)/clusdr/v1alpha1/leases.proto proto/clusdr/v1alpha1/

test:
	cargo test

fmt:
	cargo fmt --check

clippy:
	cargo clippy --all-targets -- -D warnings
