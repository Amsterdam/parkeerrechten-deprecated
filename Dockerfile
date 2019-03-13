FROM amsterdam/python:latest

MAINTAINER datapunt.ois@amsterdam.nl

ENV PYTHONUNBUFFERED 1
EXPOSE 8000


RUN apt-get update \
	&& apt-get install -y \
		freetds-bin \
		freetds-common \
		freetds-dev \
		netcat \
	&& apt-get clean \
	&& rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
	&& adduser --system datapunt


WORKDIR /app
COPY . /app
#COPY ./src/docker-wait.sh /app
#COPY ./src/parkeerrechten /app/parkeerrechten/
# RUN pip install --no-cache-dir -e .[test]
RUN pip install --no-cache-dir .[test]


# Do the .jenkins directory dance to enable data imports:
COPY ./src/.jenkins/import /.jenkins-import/
# COPY .jenkins /app/.jenkins

