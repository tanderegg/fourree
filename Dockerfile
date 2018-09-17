FROM alpine:3.8

RUN apk add --no-cache gcc musl-dev rust cargo
ADD . /app
RUN cd /app && cargo build --release

ENTRYPOINT ["/app/target/release/fourree"]
