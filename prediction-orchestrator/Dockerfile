#
# Build stage
#
FROM docker.io/library/maven:3.9.2-eclipse-temurin-17 AS build
COPY src /home/app/src
COPY pom.xml /home/app
RUN mvn -f /home/app/pom.xml clean package

FROM docker.io/library/eclipse-temurin:17-jre

#
# Package stage
#
COPY --from=build home/app/target/predictionorchestrator-0.0.1-SNAPSHOT.jar /usr/local/lib/predictionorchestrator.jar

EXPOSE 3333

# Command to run the application
CMD ["java", "-jar", "/usr/local/lib/predictionorchestrator.jar"]