# Stage 1: Build application using Maven and Java 21
FROM maven:3.9-eclipse-temurin-21 AS builder

WORKDIR /workspace

COPY pom.xml .
COPY src ./src

RUN mvn -B -DskipTests package dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory=/app/dependency

# Stage 2: Lightweight Java 21 runtime
FROM eclipse-temurin:21-jre

WORKDIR /app

RUN mkdir -p /app/dependency \
    && chown -R 10001:10001 /app

COPY --from=builder --chown=10001:10001 /workspace/target/lyapunov-0.1.0-SNAPSHOT.jar /app/app.jar
COPY --from=builder --chown=10001:10001 /app/dependency/ /app/dependency/

USER 10001:10001

ENV JAVA_MAIN_CLASS=""

ENTRYPOINT ["sh", "-c", "if [ -z \"$JAVA_MAIN_CLASS\" ]; then echo 'ERROR: Set JAVA_MAIN_CLASS to a valid LYAPUNOV CLI class'; exit 2; fi; exec java ${JAVA_OPTS:-} -cp '/app/app.jar:/app/dependency/*' \"$JAVA_MAIN_CLASS\" \"$@\"", "--"]
