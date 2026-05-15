FROM docker.io/confluentinc/cp-schema-registry:latest-ubi9 AS schema-registry
FROM  docker.io/redhat/ubi9-minimal:latest AS native-schema-registry
#micro doesn't have the tools ootb to add users etc.

RUN useradd -u 1000 -g 0 appuser
COPY --from=schema-registry /usr/bin/ub /usr/bin/ub
COPY ./java-stub /usr/bin/java
COPY --from=schema-registry /etc/confluent /etc/confluent
COPY --from=schema-registry --chown=appuser:0 /etc/schema-registry /etc/schema-registry
RUN chown -R 1000:0 /etc/schema-registry


USER appuser
COPY --chown=appuser ./kafka-schema-registry-package /home/appuser/kafka-schema-registry-package
COPY ./schema-registry-start /usr/bin/schema-registry-start

ENTRYPOINT [ "/etc/confluent/docker/run" ]
ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    container=oci \
    LANG=C.UTF-8 \
    USE_LOG4J_2=True \
    COMPONENT=schema-registry
