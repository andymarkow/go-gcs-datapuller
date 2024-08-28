FROM --platform=$BUILDPLATFORM golang:1.23-alpine as builder

ARG TARGETOS
ARG TARGETARCH

ENV GOOS=$TARGETOS \
    GARCH=$TARGETARCH \
    CGO_ENABLED=0 \
    GO111MODULE=on

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download &&\
    go mod verify

COPY . .
RUN apk update &&\
    apk add --update --no-cache git ca-certificates &&\
    go build -v -o /build/datapuller ./cmd/datapuller


FROM alpine:3.20

COPY --from=builder /build/datapuller /usr/local/bin/datapuller

RUN apk --no-cache add ca-certificates &&\
    rm -rf /var/cache/apk/* &&\
    update-ca-certificates

ENTRYPOINT ["/usr/local/bin/datapuller"]
