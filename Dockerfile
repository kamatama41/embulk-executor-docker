FROM openjdk:8 as builder
WORKDIR /usr/src/app
COPY build.gradle settings.gradle gradlew gradle.properties ./
COPY ./gradle ./gradle
COPY ./src ./src
COPY ./.git ./.git
RUN ./gradlew --no-daemon executableEmbulkServer

FROM openjdk:8-jre
WORKDIR /root/
COPY ./docker .
COPY --from=builder /usr/src/app/build/libs/embulk-server-*.jar ./embulk-server.jar
CMD ["/root/run_embulk_server.sh"]
EXPOSE 30001
