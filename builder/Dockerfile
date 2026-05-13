FROM docker.io/confluentinc/cp-schema-registry:latest-ubi9 AS schema-registry

FROM container-registry.oracle.com/graalvm/native-image:25 AS native-builder
COPY --from=schema-registry /usr/share/java /usr/share/java
COPY --from=schema-registry /etc/schema-registry/ /etc/schema-registry/
COPY --from=schema-registry /etc/confluent/ /etc/confluent/


FROM native-builder
COPY compile .
ADD graalvm-reachability-metadata*.tar.gz /community-reachability-metadata/META-INF/native-image
COPY instrumented-reachability-metadata/* /instrumented-reachability-metadata/META-INF/native-image/

RUN ./compile
