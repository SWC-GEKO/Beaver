FROM golang:latest

WORKDIR /app

COPY ../../go.mod ./
RUN go mod download

COPY . .

EXPOSE 8000
EXPOSE 9001

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

RUN go build -o ./processor ./pkg/processor

CMD ["./processor"]