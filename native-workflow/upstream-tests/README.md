<!-- Explain the upstream suite selection and its image adapter here so future changes preserve real server coverage. -->
# Upstream image integration tests

The runner uses tests from [confluent-kafka-python v2.12.2](https://github.com/confluentinc/confluent-kafka-python/tree/2acaa79509569d7031aa3c16e24500611c5f18a3/tests/integration/schema_registry/_sync). Upstream documents [bring-your-own cluster support](https://github.com/confluentinc/confluent-kafka-python/blob/2acaa79509569d7031aa3c16e24500611c5f18a3/tests/README.md). The local `conftest.py` supplies a real external client with the test CA because that release's BYO fixture does not forward TLS configuration. Containers share the registry's network namespace so `https://localhost:8082` matches its test certificate and the Kafka broker remains reachable.

`suite.json` pins the source and expected selection. `requirements.lock` pins the matching client wheel and dependencies; Trivup comes from the hashed upstream archive and is imported by the original harness but never used to start services. The 7.0.1-only parameter is excluded explicitly. An AST-checked adaptation changes the incompatibility registry-code assertion from `409` to `40901`, as defined by [v8.2.0](https://github.com/confluentinc/schema-registry/blob/v8.2.0/core/src/main/java/io/confluent/kafka/schemaregistry/rest/exceptions/Errors.java) and [v8.3.2](https://github.com/confluentinc/schema-registry/blob/v8.3.2/core/src/main/java/io/confluent/kafka/schemaregistry/rest/exceptions/Errors.java). Its HTTP-status assertion remains `409`; no other test semantics are changed. Everything else must pass without skips. `test_contract.py` strengthens actual round-trip equality and reference backlinks where some upstream assertions only test nonempty lists or compare the return values of `sort()`.

The suite runs before agent shutdown in refresh mode, after compilation on native, and on both environment-configured deployment images. Metadata is retained only after validation succeeds. Default build mode reads the saved registrations and runs the same gates without collecting new compilation inputs.

Other public candidates inspected:

| Repository | Useful coverage | Integration cost |
| --- | --- | --- |
| [schema-registry REST suites](https://github.com/confluentinc/schema-registry/tree/v8.3.2/core/src/test/java/io/confluent/kafka/schemaregistry/rest) | Contexts, modes, transitive compatibility, tags, metadata, cluster behavior | Adapt the embedded harness and reset Kafka/registry state; some tests access server internals. |
| [confluent-kafka-dotnet integration tests](https://github.com/confluentinc/confluent-kafka-dotnet/tree/master/test/Confluent.SchemaRegistry.IntegrationTests) | External-server API calls, references, normalization, basic auth, TLS | Configure the JSON server parameters and provision auth variants for the corresponding tests. |
| [confluent-kafka-go client tests](https://github.com/confluentinc/confluent-kafka-go/blob/master/schemaregistry/schemaregistry_client_test.go) | Client APIs and serdes | Explicit real-server configuration is essential; missing configuration falls back to mocks, and some cases always use mocks. |
| [kcp Schema Registry scan tests](https://github.com/confluentinc/kcp/tree/main/integration-tests/schema-registry) | Schema scanning and authenticated deployments | Additional Go scanner setup and image overrides; narrower server API coverage. |
| [cp-docker-images tests](https://github.com/confluentinc/cp-docker-images/blob/master/tests/test_schema_registry.py) | Legacy image configuration and basic health checks | Docker Machine, ZooKeeper, and old Python idioms make this a poor fit for the current pipeline. |

Client-side rules or encryption executed inside the external Python process do not collect reflection metadata for Java server classes. Cluster/failover and server extensions need separate workloads and deployment configurations.
