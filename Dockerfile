FROM alpine:3.8 AS build

RUN apk add --no-cache openssl-dev gcc musl-dev rust cargo
ADD Cargo.toml /app
RUN cd /app && cargo install
ADD . /app
RUN cd /app && cargo build --release

FROM alpine:3.8
RUN apk add --no-cache openssl gcc
COPY --from=build /app/target/release/fourree /bin/fourree

ENTRYPOINT ["/bin/fourree"]
CMD ["-h"]
