alias a := all
alias c := check
alias f := format
alias t := test

# Formats and checks the code
all: format check

# Run the clippy and formatter
check: c-clippy c-fmt

# Run the clippy check
c-clippy:
	cargo clippy --all-targets --all-features -- -D warnings

# Run the fmt check
c-fmt: update-nightly-fmt
	cargo +nightly-2023-04-16 fmt --all -- --check

# Format the code
format: update-nightly-fmt
	cargo +nightly-2023-04-16 fmt --all

# Run all tests
test:
	cargo test --lib --bins --tests
	cargo test --doc -- --test-threads 1

# Installs/updates the nightly rustfmt installation
update-nightly-fmt:
	rustup toolchain install --profile minimal nightly-2023-04-16 --no-self-update
	rustup component add rustfmt --toolchain nightly-2023-04-16
