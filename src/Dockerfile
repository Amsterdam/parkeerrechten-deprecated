


FROM amsterdam/docker_python:latest

MAINTAINER datapunt.ois@amsterdam.nl

ENV PYTHONUNBUFFERED 1
EXPOSE 8000

# We need PostgreSQL 9.5 client library, which is not packaged for Debian Jessie
# which is the basis for our Python image. PostgreSQL offers this library.
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ jessie-pgdg main" > /etc/apt/sources.list
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -



RUN apt-get update \
	&& apt-get install -y \
		netcat \
		freetds-dev \
		postgresql-client-9.5 \
	&& apt-get clean \
	&& rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
	&& adduser --system datapunt


WORKDIR /app
COPY requirements.txt /app
COPY docker-wait.sh /app
RUN pip install --no-cache-dir -r requirements.txt
COPY parkeerrechten /app/parkeerrechten/

#
# # Do the .jenkins directory dance to enable data imports:
# COPY .jenkins/import /.jenkins-import/
# COPY .jenkins /app/.jenkins
