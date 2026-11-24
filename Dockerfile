FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/api ./cmd/api \
 && CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/worker ./cmd/worker

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/api /out/worker /
EXPOSE 8080
ENTRYPOINT []
CMD ["/api"]
