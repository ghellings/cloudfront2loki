FROM golang:alpine AS builder
RUN apk update && apk add --no-cache git
ENV USER=appuser
ENV UID=10001 
RUN adduser \    
    --disabled-password \    
    --gecos "" \    
    --home "/nonexistent" \    
    --shell "/sbin/nologin" \    
    --no-create-home \    
    --uid "${UID}" \    
    "${USER}"
WORKDIR $GOPATH/src/cloudfront2loki
COPY . .
RUN go get -d -v
RUN go mod download
RUN go mod verify
RUN GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o /go/bin/cloudfront2loki

# STEP 2 build a small image
############################
FROM alpine
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group
COPY --from=builder /go/bin/cloudfront2loki /go/bin/cloudfront2loki
COPY cloudfront2loki.conf /go/src/cloudfront2loki/
USER appuser:appuser
WORKDIR /go/src/cloudfront2loki
ENTRYPOINT ["/go/bin/cloudfront2loki"]