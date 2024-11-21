FROM rust

WORKDIR /usr/src/app

ADD Cargo.toml .
ADD Cargo.lock .

RUN cargo fetch

ADD src/ src/

RUN cargo build --release

ENTRYPOINT [ "./target/release/ds-project" ]
