FROM 7bcf64501d2c

USER root
RUN mkdir /var/s3 &&\
    mkdir /var/s3/data &&\
    mkdir /var/s3/multipart &&\
    mkdir /home/sirius/logs

ADD target/release-dir /home/sirius/
RUN chown sirius:sirius -R /var/s3 /home/sirius

USER sirius

VOLUME /var/s3/data
VOLUME /home/sirius/logs
EXPOSE 9000
