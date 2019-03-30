FROM openjdk:8
WORKDIR /usr/src/app
COPY build.gradle settings.gradle gradlew gradle.properties ./
COPY ./gradle ./gradle
COPY ./src ./src
RUN ./gradlew executableEmbulkServer

FROM openjdk:8-jre
WORKDIR /root/
COPY ./docker .
COPY --from=0 /usr/src/app/build/libs/embulk-server-*.jar ./embulk-server.jar
ENTRYPOINT ["/root/run_embulk_server.sh"]
EXPOSE 30001
