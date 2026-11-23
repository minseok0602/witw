FROM eclipse-temurin:21-jre
WORKDIR /app
ARG VARIANT=after
COPY .generated/backend-${VARIANT}.jar /app/app.jar
EXPOSE 8080 9091 5005
ENTRYPOINT ["java", "-Xms256m", "-Xmx768m", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005", "-jar", "/app/app.jar"]
