# Builder runs on $BUILDPLATFORM and cross-compiles for $TARGETPLATFORM
# so multi-arch builds don't go through QEMU emulation.
FROM --platform=$BUILDPLATFORM golang:1.26.4-alpine AS builder
ARG TARGETOS
ARG TARGETARCH

RUN apk --update add ca-certificates

WORKDIR /src
COPY . .

ENV CGO_ENABLED=0

WORKDIR /src/cmd/otel-collector
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -tags timetzdata -o /out/otel-collector

# Final stage
FROM scratch

ARG USER_UID=10001

USER ${USER_UID}

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /out/otel-collector /

EXPOSE 4317 4318
ENTRYPOINT ["/otel-collector"]
CMD ["--config", "/etc/otel/config.yaml"]
