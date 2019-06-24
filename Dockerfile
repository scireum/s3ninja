FROM scireum/sirius-runtime:9

RUN mkdir /home/sirius/data && \
    mkdir /home/sirius/multipart && \
    mkdir /home/sirius/logs

ADD target/release-dir /home/sirius/

USER root
RUN chown sirius:sirius -R /home/sirius
USER sirius

VOLUME /home/sirius/data
VOLUME /home/sirius/logs
EXPOSE 9000
