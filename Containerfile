FROM rust:slim-trixie as build

RUN mkdir /tmp/tokad

WORKDIR /tmp/tokad

COPY --exclude=./target . ./

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y protobuf-compiler
RUN cargo build --release

FROM debian:trixie-slim

COPY --from=build /tmp/tokad/target/release/tokad /bin/

CMD ["/bin/tokad"]
