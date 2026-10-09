FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /server ./cmd/server && CGO_ENABLED=0 go build -o /loadgen ./cmd/loadgen
FROM alpine:3.22
RUN adduser -D app
USER app
COPY --from=build /server /server
COPY --from=build /loadgen /loadgen
ENTRYPOINT ["/server"]
