FROM scireum/sirius-runtime-jdk18:58

RUN mkdir /home/sirius/data && \
    mkdir /home/sirius/multipart && \
    mkdir /home/sirius/logs

USER root
ADD --chown=sirius:sirius target/release-dir /home/sirius/

USER sirius

VOLUME /home/sirius/data
VOLUME /home/sirius/logs
EXPOSE 9000
