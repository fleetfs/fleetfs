build: pre
	cargo build

release: pre
	cargo build --release

pre:
	cargo fmt --all -- --check
	cargo clippy --all

profile:
	RUSTFLAGS='-Cforce-frame-pointers' cargo build --release

test:
	bash -m ./test.sh
	docker build -t fleetfs:tests -f Dockerfile.integration_tests .
	# Additional permissions are needed to be able to mount FUSE
	docker run --rm -it --cap-add SYS_ADMIN --device /dev/fuse --security-opt apparmor:unconfined -v "$(shell pwd)/logs:/code/logs" fleetfs:tests
