FROM docker.io/confluentinc/cp-schema-registry:latest-ubi9 AS schema-registry

USER root
RUN microdnf install -y unzip zip tar gzip
USER appuser
RUN curl -s "https://get.sdkman.io" | bash

RUN source "/home/appuser/.sdkman/bin/sdkman-init.sh" \
    && sdk install java 25.0.3-graal

ENV JAVA_HOME=/home/appuser/.sdkman/candidates/java/25.0.3-graal
