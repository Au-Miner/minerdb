FROM golang:1.22-alpine as builder



WORKDIR /app

COPY ./go.mod .

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o myapp main.go

FROM alpine:latest
WORKDIR /root/

COPY --from=builder /app/myapp .

CMD ["./myapp"]
