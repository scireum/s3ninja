FROM scireum/sirius-runtime

RUN mkdir /var/s3 &&\
    mkdir /var/s3/data &&\
    mkdir /var/s3/multipart

ADD target/release-dir /root/
RUN chmod +x /root/run.sh

WORKDIR /root

VOLUME /var/s3/data
VOLUME /root/logs
EXPOSE 80

ENV ide=true
ENV JAVA_XMX=1024m

CMD /root/run.sh
