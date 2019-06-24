FROM scireum/sirius-runtime:9

RUN mkdir /var/s3 &&\
    mkdir /var/s3/data &&\
    mkdir /var/s3/multipart

ADD target/release-dir /root/

VOLUME /var/s3/data
VOLUME /root/logs
EXPOSE 80
