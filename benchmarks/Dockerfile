FROM golang:1.24-bookworm AS build
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 go install github.com/bojand/ghz/cmd/ghz@v0.120.0
FROM debian:bookworm-slim
COPY --from=build /go/bin/ghz /usr/local/bin/ghz
WORKDIR /work
ENTRYPOINT ["ghz"]
