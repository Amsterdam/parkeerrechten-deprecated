####################################
# App
####################################
FROM amsterdam/python:3.8-buster as app

WORKDIR /app_install
ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt

WORKDIR /src
ADD src .

# Prepare the mountpoint to be accessible by the datapunt user
RUN mkdir /data
RUN chown datapunt /data

USER datapunt

CMD ["python", "main.py"]


####################################
# devserver
####################################
FROM app as dev

USER root
WORKDIR /app_install
ADD requirements_dev.txt requirements_dev.txt
RUN pip install -r requirements_dev.txt
RUN chmod -R a+r /app_install

WORKDIR /src
USER datapunt

# Any process that requires to write in the home dir
# we write to /tmp since we have no home dir
ENV HOME /tmp
