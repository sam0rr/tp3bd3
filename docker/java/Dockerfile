FROM openjdk:21-jdk-slim
WORKDIR /app

COPY pom.xml .
COPY src ./src
COPY data ./data

RUN apt-get update \
 && apt-get install -y maven \
 && mvn clean package -DskipTests

CMD ["java", "--enable-preview", "-jar", "target/tp3bd3-1.0-SNAPSHOT.jar"]