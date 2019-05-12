build:
	cargo build
	cargo fmt --all -- --check

release:
	cargo build --release
	cargo fmt --all -- --check

profile:
	RUSTFLAGS='-Cforce-frame-pointers' cargo build --release
