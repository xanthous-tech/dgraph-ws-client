
watch:
	cargo watch --shell "clear && RUST_LOG='dgraph_ws_client=debug' cargo check"

watch-test:
	cargo watch --shell "clear && cargo test"
