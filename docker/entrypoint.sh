#!/bin/bash -ex

if [ ! -z "${S3NINJA_PORT}" ]; then
  tee ${S3NINJA_HOME}/instance.conf <<EOF
http {
  port = ${S3NINJA_PORT}
}
EOF
fi

${S3NINJA_HOME}/sirius.sh start
tail -n+0 -F ${S3NINJA_HOME}/logs/application.log ${S3NINJA_HOME}/logs/stdout.txt
${S3NINJA_HOME}/sirius.sh stop
