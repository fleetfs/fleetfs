build: pre
	cargo build

release: pre
	cargo build --release

pre:
	cargo fmt --all -- --check
	cargo clippy --all

profile:
	RUSTFLAGS='-Cforce-frame-pointers' cargo build --release
