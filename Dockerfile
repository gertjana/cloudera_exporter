# Builder image
FROM golang:1.9.0 AS builder
WORKDIR /go
COPY . .
RUN go get \
	github.com/prometheus/client_golang/prometheus \
	github.com/prometheus/common/log \
	github.com/prometheus/common/version \
	gopkg.in/alecthomas/kingpin.v2
RUN CGO_ENABLED=0 GOOS=linux go build -a -tags netgo -ldflags '-w'

# Final image.
FROM scratch
LABEL maintainer "Gertjan Assies <g.assies@newmotion.com"
COPY --from=builder /go/ClouderaExporter .
EXPOSE 9107
ENTRYPOINT "/ClouderaExporter --cloudera.uri=$CLOUDERA_URI --cloudera.username=$CLOUDERA_USERNAME --cloudera.password=$CLOUDERA_PASSWORD"