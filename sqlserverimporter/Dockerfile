FROM golang:latest
MAINTAINER datapunt.ois@amsterdam.nl


COPY . src/db2csv

WORKDIR /go/src/db2csv

RUN go get
RUN go build
