FROM jelastic/maven:3.9.5-openjdk-21 AS builder

WORKDIR /app

COPY pom.xml .
COPY src src/

ENV MAVEN_OPTS="-Dmaven.resolver.transport=wagon -Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true -Dmaven.wagon.http.ssl.ignore.validity.dates=true"

RUN mvn -q -DskipTests clean package && ls -lah target/release-dir || (echo "Build failed" && exit 1)

FROM scireum/sirius-runtime-jre24:78

RUN mkdir /home/sirius/data && \
    mkdir /home/sirius/multipart && \
    mkdir /home/sirius/logs

USER root

ADD --chown=sirius:sirius target/release-dir /home/sirius/

USER sirius

VOLUME /home/sirius/data
VOLUME /home/sirius/logs

EXPOSE 9000

