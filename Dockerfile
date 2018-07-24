FROM scireum/sirius-runtime:2

RUN mkdir /var/s3 &&\
    mkdir /var/s3/data &&\
    mkdir /var/s3/multipart

ADD target/release-dir /root/

VOLUME /var/s3/data
VOLUME /root/logs
EXPOSE 80
