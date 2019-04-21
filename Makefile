build:
	cargo build

release:
	cargo build --release

profile:
	RUSTFLAGS='-Cforce-frame-pointers' cargo build --release
