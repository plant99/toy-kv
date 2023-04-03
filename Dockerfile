# syntax=docker/dockerfile:1

FROM golang:1.19-alpine
RUN apk add build-base
WORKDIR /app
COPY go.mod ./
COPY go.sum ./
RUN go mod download
COPY . ./
RUN go build -o /tkv
EXPOSE 8080
CMD [ "/tkv" ]