# -------- Base Image (Java Runtime Only = smaller image) --------
FROM eclipse-temurin:17-jre-jammy

# -------- App directory inside container --------
WORKDIR /app

# -------- Copy jar from host to container --------
COPY target/*.jar app.jar

# -------- Expose Spring Boot port --------
EXPOSE 8090

# -------- Run Spring Boot --------
ENTRYPOINT ["java","-jar","app.jar"]
