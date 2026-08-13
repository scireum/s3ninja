FROM scireum/sirius-runtime-jre26:81.1

RUN mkdir /home/sirius/data && \
    mkdir /home/sirius/multipart && \
    mkdir /home/sirius/logs

USER root
ADD --chown=sirius:sirius target/release-dir /home/sirius/

USER sirius

VOLUME /home/sirius/data
VOLUME /home/sirius/logs
EXPOSE 9000
