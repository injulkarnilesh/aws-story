FROM openjdk:8-jdk-alpine

RUN mkdir -p /usr/src

COPY target/aws-story-0.0.1-SNAPSHOT.jar /usr/src/aswsstory.jar

WORKDIR /usr/src

ENV NODE docker
ENV SPRING_PROFILE docker
ENV AWS_ACCESS_KEY ""
ENV AWS_SECRET_KEY ""

ENTRYPOINT exec java -jar -Dnode=$NODE -Dspring.profiles.active=$SPRING_PROFILE -Daws.accessKeyId=$AWS_ACCESS_KEY -Daws.secretKey=$AWS_SECRET_KEY aswsstory.jar