FROM golang:latest
WORKDIR /app

COPY ../../go.mod ./
RUN go mod download

COPY . .

EXPOSE 8000
EXPOSE 9000

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

RUN go build -o ./merger ./pkg/merger

CMD ["./merger"]