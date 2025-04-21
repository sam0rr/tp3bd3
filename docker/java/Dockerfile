FROM openjdk:21-jdk-slim
WORKDIR /app

COPY pom.xml .
COPY app/src/ ./src/

RUN apt-get update && apt-get install -y maven
RUN mvn clean package

CMD ["java", "-cp", "target/classes", "Main"]
