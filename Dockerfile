FROM navikt/java:17
COPY init.sh /init-scripts/init.sh
COPY build/libs/tms-doknotstatus-converter-all.jar /app/app.jar
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75 \
               -XX:+HeapDumpOnOutOfMemoryError \
               -XX:HeapDumpPath=/oom-dump.hprof"
ENV PORT=8080
EXPOSE $PORT

USER root
RUN apt-get install -y curl
USER apprunner