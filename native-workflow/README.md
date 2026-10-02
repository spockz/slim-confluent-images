<!-- Keep source and image build instructions here; the repository root Compose files are legacy experiments. -->
# Schema Registry native workflow

`run.py` builds an isolated Schema Registry image from a Git archive, starts a Kafka broker, compiles the standalone JAR using saved reachability metadata, and checks behavior across the JVM and native runtimes. Instrumentation is a separate, explicitly selected refresh mode.

The runner keeps its work under a new directory for each invocation. It records the source commit, JAR and native binary hashes, selected platform, image digests, native build arguments, logs, and reachability files there. It archives the requested Git ref without checking it out or changing the source repository. Release normalization happens only in that private archive. The legacy 8.2.0 snapshot also receives the service-descriptor merge described below; `provenance.json` records this adjustment.

Metadata refresh is additive. The runner retains the committed `reachability-metadata.json` files, including the existing test and instrumentation directories, and preserves metadata embedded in the standalone JAR. It copies each committed file to `committed-metadata/`, verifies and copies the saved snapshot to `saved-metadata/`, retains fresh agent output separately under `metadata/` when refreshing, then uses the pinned GraalVM `native-image-configure generate` tool to merge committed inputs, the saved snapshot, and any fresh agent output. Native compilation reads that union from `native/reachability/` in addition to the JAR's embedded metadata. `provenance.json` records the metadata policy and hashes of the committed, fresh, and merged inputs. Neither the source metadata nor a supplied JAR is rewritten.

The functional checks keep a plaintext HTTP readiness and build-metadata control, then run the schema workloads over a directly exposed HTTPS listener. They register and evolve Avro, JSON Schema, and Protobuf subjects, check both compatible and incompatible Avro changes, read each canonical schema body by subject and schema ID, replay JVM-written schemas in the native process, write additional schemas natively, and read them after restarting the native process against the same Kafka data directory. The JVM and native listeners are each probed with verified TLS 1.2 and TLS 1.3 handshakes and explicit HTTP/1.1 ALPN negotiation. The runner also confirms that an untrusted CA and a wrong hostname fail with certificate-verification errors.

## Run

From this repository, use the source checkout next to it:

```sh
python3 native-workflow/run.py \
  --schema-repo ../schema-registry \
  --schema-ref 8.2.0-native \
  --release-version 8.2.0 \
  --platform linux/arm64
```

Use the 8.3.2 source ref and version in a second invocation:

```sh
python3 native-workflow/run.py \
  --schema-repo ../schema-registry \
  --schema-ref native-8.3.2 \
  --release-version 8.3.2 \
  --platform linux/arm64
```

The default reference is `8.2.0-native`, the default release is `8.2.0`, and the default target follows the container engine's architecture. The requested target must match the engine because GraalVM native-image compiles for the platform on which it runs. Set `CONTAINER_ENGINE=podman` to use Podman; otherwise Docker Compose is selected. The engine and its Compose provider must be installed and running.

When the standalone JAR was already built from the selected source archive, pass it with `--jar /path/to/kafka-schema-registry-package-VERSION-standalone.jar`. The runner still archives the selected ref to record its commit and read any legacy native-image arguments from the package POM. Ensure the supplied JAR was built from that ref and release version and contains the merged JDK ALPN service provider. An old JAR assembled without that merge is rejected; omit `--jar` to rebuild the legacy source with the correction.

The runner requires Python 3.12 or later. The Maven build uses `-Pstandalone -pl package-schema-registry -am -DskipTests -Dcyclonedx.skip=true clean package`. Static analysis remains enabled; the CycloneDX release SBOM is skipped to avoid unrelated remote dependency resolution during this local native workflow. For the 8.2.0 source, the runner selects SpotBugs 4.9.8.0 so analysis can parse Java 25 class files; 8.3.2 already pins that compatible version. All container images are pulled by version tag, resolved to registry digests, and then used by digest: GraalVM Native Image 25.0.4, Kafka 4.3.0, and UBI 9.6. Each startup must report the archived source commit and requested release version at `/v1/metadata/version`. Each run gets a unique Compose project; shutdown only removes that project's containers and network. Run artifacts and the local Kafka data directory remain under the output directory, which defaults to `artifacts/native-workflow/`.

If a stage fails, the runner keeps the run directory and writes available container logs and state there. It exits nonzero and reports the failing command or REST request.

Run the metadata regression tests with `python3 -m unittest discover -s native-workflow/tests`. They use the pinned GraalVM container to verify preservation of existing members, access flags, conditional registrations, ordered method signatures and proxies, and resource patterns, plus rejection of invalid or modified inputs. The release workflow runs these tests before building deployment images.

## Save and reuse metadata

The default `--metadata-mode build` reuses `native-workflow/metadata/VERSION/`. Each snapshot contains `configuration/` and a manifest with configuration hashes, the Schema Registry commit, GraalVM version, collection platform, and instrumented JAR hash. The snapshot is copied into each private run and merged with source metadata. A missing, modified, or stale snapshot causes an explicit error; builds never silently start instrumentation. The JAR hash records collection provenance rather than requiring byte-identical Maven output on subsequent builds.

Refresh both versions explicitly:

```sh
python3 native-workflow/build_images.py --metadata-mode refresh
```

Or refresh one release:

```sh
python3 native-workflow/run.py \
  --schema-ref 8.2.0-native \
  --release-version 8.2.0 \
  --metadata-mode refresh
```

Refresh includes existing saved metadata even when moving to a newer source commit. It saves the merged snapshot only after native compilation, schema operations, TLS, and restart tests pass. A failed refresh leaves the previous snapshot intact. Review and commit the resulting `native-workflow/metadata/` changes; the scripts do not commit or push them automatically. Subsequent default builds use that committed configuration without recollecting compilation inputs.

`run.py --metadata-dir PATH` selects a snapshot directory; `build_images.py --metadata-root PATH` selects a root with one directory per version. Snapshots contain a shared registration set for Linux AMD64 and ARM64; the manifest identifies where collection was performed, while every release build tests its own target architecture. Add observations from both architectures through refresh when needed. The initial snapshots come from the successful additive Linux ARM64 runs on 2026-10-01. The 8.2.0 snapshot includes all four unchanged historical source files.

The packaged instrumented image is still tested with its agent enabled to verify that image's contract. Its smoke-test output is retained as a diagnostic artifact and never changes the metadata used to compile the native image.

## Validation

On 2026-09-30, both `8.2.0-native` and `native-8.3.2` passed the runner's Maven source build, JVM HTTPS instrumentation, native image build, verified TLS 1.2 and TLS 1.3 connections, certificate rejection, HTTPS schema operations, and restart checks on Linux ARM64 with GraalVM 25.0.4. Additional probes confirmed explicit HTTP/1.1 ALPN negotiation on both native binaries. The Maven `native:compile-no-fork` entry point was also verified in Linux, preserving its input JAR. The dual-image builder also passed separate deployment-image boot, TLS, stored-subject, and instrumentation-output checks for both releases.

Each run creates an RSA 2048 self-signed certificate with a `localhost` SAN and PKCS12 keystore using `keytool` from the pinned GraalVM image. Generation runs inside a temporary container; the container CLI copies the files back with host ownership, avoiding root-owned bind-mount outputs on Linux runners. The JVM and native contexts receive the keystore and truststore; the native runtime copies them with read-only permissions for UID 10001. The REST listener accepts both HTTP and HTTPS, with HTTPS restricted to TLS 1.2 and TLS 1.3. The broker remains plaintext. Its per-run data directory is writable by the broker UID, which can differ from the host runner UID. JSSE handshake diagnostics are included in the retained agent and native container logs, and `result.json` records negotiated protocols, ciphers, peer certificates, and certificate-rejection checks.

A rootful Linux ARM64 regression also reproduced the original host permission failures for TLS output and Kafka data, then passed corrected ownership, JVM and native HTTPS workloads, all 12 TLS checks, and native restart persistence using the previously verified 8.3.2 executable. TLS generation also passed with the macOS rootless engine.

On 2026-10-01, the native and instrumented deployment images for both releases were rebuilt from the previously verified, hash-checked artifacts and passed the environment-only startup checks on Linux ARM64. No properties file was mounted. The checks covered HTTP version reporting, TLS 1.2 and TLS 1.3, certificate rejection, Avro/JSON/Protobuf evolution, agent metadata output, native writes, and persistence after native restart. Native compilation was not repeated for this deployment-only change.

The subsequent additive-metadata validation rebuilt both releases from source, collected fresh metadata, compiled new native binaries, and passed the full workflow and environment-only deployment checks on Linux ARM64. For 8.2.0, all four committed metadata files remained byte-identical to the archived source; their 1,532 distinct reflection types and the fresh agent types were retained in the merged input's 1,549 reflection entries. The 8.3.2 branch has no committed project JSON; its embedded dependency metadata remains included alongside the new agent output. The merge regression tests also passed against the pinned GraalVM tool.

On 2026-10-02, both releases passed complete Maven builds, native compilation from the saved repository snapshots, and the workflow and deployment checks on Linux ARM64. The JVM controls ran without instrumentation and produced no fresh agent files. The compiler configurations were byte-identical to the saved snapshots; all four historical 8.2.0 source files were preserved. Each release passed 24 TLS checks, schema evolution, and restart persistence, with no properties file mounted in the deployment images. The seven metadata regression tests also passed. AMD64 has not been validated locally for this change.

These checks use a single plaintext Kafka broker. Other deployment configurations need their own representative instrumentation workloads.

## HTTPS regression

The old standalone assembly unpacked conflicting service-provider files without merging them. The resulting `META-INF/services/org.eclipse.jetty.io.ssl.ALPNProcessor$Server` retained only Confluent's Bouncy Castle provider and dropped Jetty's `JDK9ServerALPNProcessor`. With the default SunJSSE engine, Jetty rejected the accepted socket with `No ALPN Processor for sun.security.ssl.SSLEngineImpl` before consuming ClientHello. This occurs on the matching JVM as well as native and explains why JSSE handshake logs contain no handshake exchange.

The 8.3.2 Schema Registry branch fixes the assembly with Maven's `metaInf-services` descriptor handler. Both `8.2.0-native` and `native-8.3.2` now include this correction. For older 8.2.0 refs, the runner still applies it in the private source snapshot. Refresh mode collects metadata from actual HTTPS requests before native compilation; both modes test TLS 1.2 and TLS 1.3 independently, including certificate verification failures and schema operations after native restart.

An independent assembly collision overwrites Log4j's binary `Log4j2Plugins.dat` cache with a smaller dependency cache. It prevents normal Log4j configuration and can hide Jetty diagnostics. Service-descriptor merging does not fix that cache; its merge remains a separate packaging issue. JSSE diagnostics in this workflow are written directly to standard error.

## Build both release images

From this repository:

```sh
python3 native-workflow/build_images.py
```

This builds `8.2.0-native` as 8.2.0 and `native-8.3.2` as 8.3.2 from the neighboring Schema Registry checkout. Each version runs the source build, native compilation from saved metadata, HTTPS workload, and restart checks before packaging deployment images. The JVM control runs without the GraalVM agent in build mode. The deployment images are booted separately using `SCHEMA_REGISTRY_*` environment variables, with no properties file mounted. These checks register and evolve Avro, JSON, and Protobuf schemas, verify HTTP and HTTPS, collect agent metadata, and check native writes after restart. No test certificates or broker settings are included in the release images.

The default local tags are:

| Image | Version tags | Architecture tags |
| --- | --- | --- |
| `schema-registry-native` | `8.2.0`, `8.3.2` | `8.2.0-arm64`, `8.3.2-arm64` on ARM64; `-amd64` on AMD64 |
| `kafka-schema-registry-graalvm-instrumented` | `8.2.0`, `8.3.2` | The same architecture suffixes |

Use `--version 8.3.2` to build one release. `--schema-repo`, `--platform`, `--engine`, and `--maven` select the source checkout and build tools. `--image-name` and `--instrumented-image-name` select image repositories. Native compilation requires a container engine running the requested architecture. The default command builds the current architecture; GitHub Actions builds both AMD64 and ARM64 on their matching runners.

Build records and logs are retained under `artifacts/native-images/`. `images.json` records the source commits, input hashes, image identities, and tags after all requested builds succeed. Local builds do not push images or assign a `latest` tag.

Deployment images use UID 10001 and generate configuration from the existing `SCHEMA_REGISTRY_*` environment variables:

```sh
docker run --rm -p 8081:8081 \
  -e SCHEMA_REGISTRY_HOST_NAME=schema-registry \
  -e SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081 \
  -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://broker:29092 \
  schema-registry-native:8.3.2
```

The broker address must be reachable from the container. Both deployment images retain Confluent's `ub` renderer and configuration templates, copied from the matching release image pinned by digest. The shared entrypoint writes `/etc/schema-registry/schema-registry.properties` and launches the selected runtime. Schema Registry itself waits for Kafka; the legacy Java readiness stub is not used. `images.json` records the configuration image's identity alongside the artifact hashes.

For HTTPS, mount your keystore and configure it through `SCHEMA_REGISTRY_SSL_KEYSTORE_LOCATION`, `SCHEMA_REGISTRY_SSL_KEYSTORE_TYPE`, `SCHEMA_REGISTRY_SSL_KEYSTORE_PASSWORD`, and `SCHEMA_REGISTRY_SSL_KEY_PASSWORD`. Mounted TLS files must be readable by UID 10001. An explicitly supplied properties-file path still bypasses environment generation; native runtime arguments can precede that path after the image name. `SCHEMA_REGISTRY_OPTS` supplies whitespace-separated runtime options.

The instrumented image uses the same environment-based startup and writes agent metadata to `/opt/reachability`. Mount a directory writable by UID 10001 there to keep metadata. Stop the container gracefully so the agent flushes it.

`.github/workflows/release-images.yaml` reuses committed snapshots by default, builds and verifies both versions on AMD64 and ARM64, publishes native and instrumented architecture images, then creates version manifests. Its manual `metadata_mode=refresh` option produces candidate snapshots as downloadable artifacts and skips all image publishing. Each architecture produces a separate artifact; changes must be reviewed and committed before regular release builds use them. Only 8.3.2 receives `latest`. The workflow requires the updated `8.2.0-native` and renamed `native-8.3.2` branches to be pushed to the Schema Registry fork first.
