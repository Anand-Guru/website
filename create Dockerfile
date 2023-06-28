FROM ubuntu
RUN apt update
RUN apt install apache2 -y
ENDPOINT apachectl -D FOREGROUND
COPY . /var/www/html
