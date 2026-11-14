# ---------- Stage 1: build the jar and run the unit tests ----------
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build

# Download dependencies first, so code changes don't re-download them
COPY pom.xml .
RUN mvn -q -B dependency:go-offline

COPY src ./src
RUN mvn -q -B package

# ---------- Stage 2: small runtime image ----------
FROM eclipse-temurin:21-jre

# Run as a non-root user that owns the data folder
RUN useradd --system --uid 1001 --home-dir /app raft \
 && mkdir -p /app/data \
 && chown -R raft /app
WORKDIR /app

COPY --from=build /build/target/raft-config-service-0.1.0-SNAPSHOT.jar app.jar
USER raft

ENV SERVER_PORT=8080 \
    JAVA_OPTS="-Xmx256m"
EXPOSE 8080

# exec: java becomes PID 1 and receives SIGTERM directly, so Spring shuts down cleanly
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]