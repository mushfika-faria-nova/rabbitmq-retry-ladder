# The demo app in a container, so trying it needs only Docker: no local Java or Gradle.

# Build the demo's jar with the Gradle wrapper.
FROM eclipse-temurin:25.0.5_8-jdk-noble AS build
WORKDIR /src
# Build files first, so Docker can reuse the downloaded Gradle and dependencies when only code changes.
COPY gradlew settings.gradle.kts build.gradle.kts ./
COPY gradle gradle
COPY retry-ladder-spring-boot-starter/build.gradle.kts retry-ladder-spring-boot-starter/
COPY demo/build.gradle.kts demo/
RUN ./gradlew --no-daemon -q :demo:dependencies > /dev/null
COPY retry-ladder-spring-boot-starter/src retry-ladder-spring-boot-starter/src
COPY demo/src demo/src
RUN ./gradlew --no-daemon -q :demo:bootJar

# Run only the jar, on the smaller Java runtime image, as a non-root user.
FROM eclipse-temurin:25.0.5_8-jre-noble
WORKDIR /app
COPY --from=build /src/demo/build/libs/demo-*.jar app.jar
USER 1000
ENTRYPOINT ["java", "-jar", "app.jar"]
