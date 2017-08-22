FROM amsterdam/docker_python:latest

FROM golang:latest as gobuilder

COPY . src/db2csv
WORKDIR /go/src/db2csv

RUN go get
RUN go build


FROM amsterdam/docker_python:latest
MAINTAINER datapunt.ois@amsterdam.nl

ENV PYTHONUNBUFFERED 1

EXPOSE 8000

RUN apt-get update \
	&& apt-get install -y \
		netcat \
	&& apt-get clean \
	&& rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
	&& adduser --system datapunt

WORKDIR /app

COPY requirements.txt /app
RUN pip install --no-cache-dir -r requirements.txt

COPY app /app
# ADAPT: COPY import.sh flake.cfg tests Makefile docker-compose.yml /app/
COPY Makefile flake.cfg /app/


# Include the Go binary from the earlier step:
COPY --from=gobuilder /go/src/db2csv /bin/db2csv

USER datapunt
