FROM golang:1.10 as build

ADD . ${GOPATH}/src/github.com/visheyra/nats-es-consumer

RUN go install github.com/visheyra/nats-es-consumer

CMD ["/go/bin/nats-es-consumer"]

FROM gcr.io/distroless/base

COPY --from=build /go/bin/nats-es-consumer /

CMD ["/nats-es-consumer"]