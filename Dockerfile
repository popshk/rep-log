FROM gradle:8.12.1-jdk21 AS build
WORKDIR /app
COPY settings.gradle build.gradle gradle.properties ./
COPY src ./src
RUN gradle --no-daemon installDist

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/build/install/replicated-log ./
EXPOSE 8080
ENTRYPOINT ["./bin/replicated-log"]
