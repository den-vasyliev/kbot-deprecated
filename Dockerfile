FROM golang:1.13 as builder
WORKDIR /go/src/app
COPY src .
RUN go get -d -v .
RUN export GOPATH=/go
RUN CGO_ENABLED=0 GOOS=linux  GOARCH=arm GOARM=7 go build -o app -a -installsuffix cgo

FROM scratch
WORKDIR /
COPY --from=builder /go/src/app/app .
COPY --from=alpine:latest /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
ENTRYPOINT ["/app"]