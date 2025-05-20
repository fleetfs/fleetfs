VERSION = $(shell git describe --tags --always --dirty)
INTERACTIVE ?= i

build: pre
	cargo build

release: pre
	cargo build --release

pre:
	cargo deny check licenses
	cargo fmt --all -- --check
	cargo clippy --all

profile:
	RUSTFLAGS='-Cforce-frame-pointers' cargo build --release

build_integration_tests: pre
	docker build -t fleetfs:tests -f Dockerfile.integration_tests .

xfstests: build_integration_tests
	# Additional permissions are needed to be able to mount FUSE
	docker run --rm -$(INTERACTIVE)t --cap-add SYS_ADMIN --cap-add CAP_SYSLOG --device /dev/fuse --security-opt apparmor:unconfined \
	 --memory=2g --kernel-memory=200m \
	 -v "$(shell pwd)/logs:/code/logs" fleetfs:tests bash -c "cd /code/fleetfs && ./xfstests.sh"

pjdfs_tests: build_integration_tests
	# Additional permissions are needed to be able to mount FUSE
	docker run --rm -$(INTERACTIVE)t --cap-add SYS_ADMIN --device /dev/fuse --security-opt apparmor:unconfined \
	 -v "$(shell pwd)/logs:/code/logs" fleetfs:tests bash -c "cd /code/fleetfs && ./pjdfs.sh"

test: pre pjdfs_tests xfstests
	./test.sh
	# Additional tests with no redundancy
	./test.sh 0
	cargo test

publish:
	docker build -t fleetfs/fleetfs:${VERSION} .
	docker push fleetfs/fleetfs:${VERSION}

audit:
	cargo audit
