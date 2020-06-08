

watch: export RUST_LOG = info
watch:
	cargo watch --shell "clear && cargo check"