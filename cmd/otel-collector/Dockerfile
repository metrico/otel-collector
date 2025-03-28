# Builder stage
FROM golang:1.24.1-alpine as builder
RUN apk --update add ca-certificates

WORKDIR /src
COPY . .

ENV OS111MODULE=on
ENV CGO_ENABLED=0

RUN cd cmd/otel-collector && go build -tags timetzdata -o /out/otel-collector

# Final stage
FROM scratch

ARG USER_UID=10001

USER ${USER_UID}

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /out/otel-collector /

EXPOSE 4317 4318
ENTRYPOINT ["/otel-collector"]
CMD ["--config", "/etc/otel/config.yaml"]
