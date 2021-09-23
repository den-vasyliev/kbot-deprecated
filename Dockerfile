# syntax=docker/dockerfile:experimental
FROM --platform=${BUILDPLATFORM} golang:1.13 as builder
ARG APP_BUILD_INFO=$APP_BUILD_INFO
WORKDIR /go/src/app
COPY src/ .
RUN export GOPATH=/go
RUN go get -d -v .

FROM builder AS build
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=arm go build -o app -a -installsuffix cgo -ldflags "-X main.Version=$APP_BUILD_INFO" -v ./...

FROM golangci/golangci-lint:v1.27-alpine AS lint-base

FROM builder AS unit-test
RUN go test -v 

FROM builder AS lint
COPY --from=lint-base /usr/bin/golangci-lint /usr/bin/golangci-lint
RUN ls -l 
RUN GO111MODULE=on golangci-lint run --disable-all -E typecheck main.go

FROM scratch AS bin
WORKDIR /
COPY --from=build /go/src/app/app .
COPY --from=alpine:latest /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
ENTRYPOINT ["/app"]