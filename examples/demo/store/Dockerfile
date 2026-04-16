FROM golang:1.25 AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /store .

FROM gcr.io/distroless/static-debian12
COPY --from=build /store /store
ENTRYPOINT ["/store"]
