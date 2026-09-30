<!-- Keep source and image build instructions here; the repository root Compose files are legacy experiments. -->
# Schema Registry native workflow

`run.py` builds an isolated Schema Registry image from a Git archive, starts a Kafka broker, collects fresh GraalVM reachability metadata from a JVM run, compiles that same standalone JAR into a Linux native executable, and checks behavior across the JVM and native runtimes.

The runner keeps its work under a new directory for each invocation. It records the source commit, JAR and native binary hashes, selected platform, image digests, native build arguments, logs, and fresh reachability files there. It archives the requested Git ref without checking it out or changing the source repository. Release normalization and removal of committed native-image JSON defaults happen only in the private archive used by the runner. The legacy 8.2.0 snapshot also receives the service-descriptor merge described below; `provenance.json` records this adjustment.

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

## Validation

On 2026-09-30, both `8.2.0-native` and `native-8.3.2` passed the runner's Maven source build, JVM HTTPS instrumentation, native image build, verified TLS 1.2 and TLS 1.3 connections, certificate rejection, HTTPS schema operations, and restart checks on Linux ARM64 with GraalVM 25.0.4. Additional probes confirmed explicit HTTP/1.1 ALPN negotiation on both native binaries. The Maven `native:compile-no-fork` entry point was also verified in Linux, preserving its input JAR. The dual-image builder also passed separate deployment-image boot, TLS, stored-subject, and instrumentation-output checks for both releases.

Each run creates an RSA 2048 self-signed certificate with a `localhost` SAN and PKCS12 keystore using `keytool` from the pinned GraalVM image. Generation runs inside a temporary container; the container CLI copies the files back with host ownership, avoiding root-owned bind-mount outputs on Linux runners. The JVM and native contexts receive the keystore and truststore; the native runtime copies them with read-only permissions for UID 10001. The REST listener accepts both HTTP and HTTPS, with HTTPS restricted to TLS 1.2 and TLS 1.3. The broker remains plaintext. Its per-run data directory is writable by the broker UID, which can differ from the host runner UID. JSSE handshake diagnostics are included in the retained agent and native container logs, and `result.json` records negotiated protocols, ciphers, peer certificates, and certificate-rejection checks.

A rootful Linux ARM64 regression also reproduced the original host permission failures for TLS output and Kafka data, then passed corrected ownership, JVM and native HTTPS workloads, all 12 TLS checks, and native restart persistence using the previously verified 8.3.2 executable. TLS generation also passed with the macOS rootless engine.

These checks use a single plaintext Kafka broker. Other deployment configurations need their own representative instrumentation workloads.

## HTTPS regression

The old standalone assembly unpacked conflicting service-provider files without merging them. The resulting `META-INF/services/org.eclipse.jetty.io.ssl.ALPNProcessor$Server` retained only Confluent's Bouncy Castle provider and dropped Jetty's `JDK9ServerALPNProcessor`. With the default SunJSSE engine, Jetty rejected the accepted socket with `No ALPN Processor for sun.security.ssl.SSLEngineImpl` before consuming ClientHello. This occurs on the matching JVM as well as native and explains why JSSE handshake logs contain no handshake exchange.

The 8.3.2 Schema Registry branch fixes the assembly with Maven's `metaInf-services` descriptor handler. Both `8.2.0-native` and `native-8.3.2` now include this correction. For older 8.2.0 refs, the runner still applies it in the private source snapshot. It collects metadata from actual HTTPS requests before native compilation and tests TLS 1.2 and TLS 1.3 independently, including certificate verification failures and schema operations after native restart.

An independent assembly collision overwrites Log4j's binary `Log4j2Plugins.dat` cache with a smaller dependency cache. It prevents normal Log4j configuration and can hide Jetty diagnostics. Service-descriptor merging does not fix that cache; its merge remains a separate packaging issue. JSSE diagnostics in this workflow are written directly to standard error.

## Build both release images

From this repository:

```sh
python3 native-workflow/build_images.py
```

This builds `8.2.0-native` as 8.2.0 and `native-8.3.2` as 8.3.2 from the neighboring Schema Registry checkout. Each version runs the complete source build, instrumentation, native compilation, HTTPS workload, and restart checks before packaging deployment images. The deployment images are booted and checked separately with mounted configuration. No test certificates or broker settings are included in them.

The default local tags are:

| Image | Version tags | Architecture tags |
| --- | --- | --- |
| `schema-registry-native` | `8.2.0`, `8.3.2` | `8.2.0-arm64`, `8.3.2-arm64` on ARM64; `-amd64` on AMD64 |
| `kafka-schema-registry-graalvm-instrumented` | `8.2.0`, `8.3.2` | The same architecture suffixes |

Use `--version 8.3.2` to build one release. `--schema-repo`, `--platform`, `--engine`, and `--maven` select the source checkout and build tools. `--image-name` and `--instrumented-image-name` select image repositories. Native compilation requires a container engine running the requested architecture. The default command builds the current architecture; GitHub Actions builds both AMD64 and ARM64 on their matching runners.

Build records and logs are retained under `artifacts/native-images/`. `images.json` records the source commits, input hashes, image identities, and tags after all requested builds succeed. Local builds do not push images or assign a `latest` tag.

Deployment images use UID 10001 and accept a mounted Schema Registry properties file:

```sh
docker run --rm -p 8081:8081 \
  -v "$PWD/schema-registry.properties:/etc/schema-registry.properties:ro" \
  schema-registry-native:8.3.2
```

The properties file must specify reachable Kafka bootstrap servers and listeners. Mount your own keystore and truststore for HTTPS and reference their container paths in that file. Mounted properties and TLS files must be readable by UID 10001. These images use the properties-file interface instead of the old Confluent environment-variable wrapper. Additional native runtime arguments can precede the properties-file path after the image name.

The instrumented image uses the same properties-file interface and writes agent metadata to `/opt/reachability`. Mount a directory writable by UID 10001 there to keep metadata. Stop the container gracefully so the agent flushes it.

`.github/workflows/release-images.yaml` builds and verifies both versions on AMD64 and ARM64, publishes native and instrumented architecture images, then creates version manifests. Only 8.3.2 receives `latest`. The workflow requires the updated `8.2.0-native` and renamed `native-8.3.2` branches to be pushed to the Schema Registry fork first.
