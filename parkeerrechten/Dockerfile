# We sadly need this line due to a bug in the Jenkins Docker Build Step plugin.
FROM amsterdam/docker_python:latest

# Build the one Go binary we need for database import:
FROM golang:latest as gobuilder
WORKDIR /go/src/db2csv
COPY ./sqlserverimporter .
RUN go get
RUN go build

# Build the Python app and copy the Go binary to it:
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
COPY run_import.sh /app
COPY app /app/
COPY Makefile flake.cfg /app/
COPY --from=gobuilder /go/src/db2csv/db2csv /bin/db2csv

# Do the .jenkins directory dance to enable data imports:
COPY .jenkins/import /.jenkins-import/
COPY .jenkins /app/.jenkins


#USER datapunt
