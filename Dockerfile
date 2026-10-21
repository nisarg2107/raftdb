FROM golang:1.24-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 go build -trimpath -o /out/raftdb ./cmd/raftdb && CGO_ENABLED=0 go build -trimpath -o /out/raftctl ./cmd/raftctl && CGO_ENABLED=0 go build -trimpath -o /out/raftprobe ./cmd/raftprobe

FROM build AS test
RUN --mount=type=cache,target=/root/.cache/go-build GOMAXPROCS=4 CGO_ENABLED=1 go test -race -p 1 -count=1 -timeout 180s ./... && go vet ./...

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates iproute2 iptables && rm -rf /var/lib/apt/lists/* && useradd --uid 10001 --create-home raftdb && mkdir /data && chown raftdb:raftdb /data
COPY --from=build /out/ /usr/local/bin/
USER 10001:10001
EXPOSE 50051 7000 9091
ENTRYPOINT ["raftdb"]
