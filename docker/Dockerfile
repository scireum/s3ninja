FROM openjdk:8u131-jre-alpine

LABEL "org.label-schema.schema-version" = "1.0"
LABEL "org.label-schema.vendor"="Universal Music"
LABEL "org.label-schema.build-date"=${BUILDDATE}
LABEL "org.label-schema.name"="s3ninja"
LABEL "org.label-schema.description"="S3 ninja image for EST"
LABEL "org.label-schema.url"="http://s3ninja.net"
LABEL "org.label-schema.vcs-url"="https://github.com/scireum/s3ninja"
LABEL "org.label-schema.docker.cmd"="docker run -d -e S3NINJA_PORT=80 -p 9444:80 s3ninja"

ENV S3NINJA_HOME=/opt/s3ninja

COPY s3ninja.zip ${S3NINJA_HOME}/
COPY entrypoint.sh /

WORKDIR ${S3NINJA_HOME}

RUN apk --no-cache --update upgrade &&\
    apk --no-cache --update add bash openssl &&\
    chmod u+x  /entrypoint.sh &&\
    mkdir -p ${S3NINJA_HOME}/data/s3 &&\
    mkdir -p ${S3NINJA_HOME}/data/s3 ${S3NINJA_HOME}/data/resources &&\
    unzip -q s3ninja.zip &&\
    rm s3ninja.zip

CMD sh /entrypoint.sh
