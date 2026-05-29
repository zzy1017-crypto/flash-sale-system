FROM golang:1.25.4

WORKDIR /app

COPY . .

RUN go mod tidy

RUN go build -o main ./cmd

EXPOSE 8080

CMD ["./main"]