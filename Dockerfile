FROM golang:1.23 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/billing-worker ./cmd/billing-worker

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/billing-worker /usr/local/bin/billing-worker
ENTRYPOINT ["billing-worker"]
