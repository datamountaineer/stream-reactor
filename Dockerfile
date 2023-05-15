# Sample docker file
FROM hseeberger/scala-sbt:8u222_1.3.5_2.13.1 as BUILD_S3_CONNECTOR_PLUGIN
WORKDIR /tmp
RUN apt-get update && apt-get install -y git
RUN git clone https://github.com/lensesio/stream-reactor.git
RUN cd /tmp/stream-reactor && sbt "project aws-s3-kafka-3-1" compile && sbt "project aws-s3-kafka-3-1" assembly