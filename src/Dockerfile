


FROM amsterdam/docker_python:latest

MAINTAINER datapunt.ois@amsterdam.nl

ENV PYTHONUNBUFFERED 1
EXPOSE 8000


RUN apt-get update \
	&& apt-get install -y \
		netcat \
		freetds-dev \
	&& apt-get clean \
	&& rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
	&& adduser --system datapunt


WORKDIR /app
COPY requirements.txt /app
COPY docker-wait.sh /app
RUN pip install --no-cache-dir -r requirements.txt
COPY parkeerrechten /app/parkeerrechten/

# Do the .jenkins directory dance to enable data imports:
COPY .jenkins/import /.jenkins-import/
# COPY .jenkins /app/.jenkins
