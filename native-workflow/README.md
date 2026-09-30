<!-- Keep this guide scoped to the opt-in workflow; the repository root Compose files are legacy experiments. -->
# Schema Registry native workflow

`run.py` builds an isolated Schema Registry image from a Git archive, starts a Kafka broker, collects fresh GraalVM reachability metadata from a JVM run, compiles that same standalone JAR into a Linux native executable, and checks behavior across the JVM and native runtimes.

The runner keeps its work under a new directory for each invocation. It records the source commit, JAR and native binary hashes, selected platform, image digests, native build arguments, logs, and fresh reachability files there. It archives the requested Git ref without checking it out or changing the source repository. Release normalization and removal of committed native-image JSON defaults happen only in the private archive used by the runner.

The functional checks register and evolve Avro, JSON Schema, and Protobuf subjects, check both compatible and incompatible Avro changes, read each canonical schema body by subject and schema ID, replay the JVM-written schemas in the native process, write additional schemas natively, and read them after restarting the native process against the same Kafka data directory.

## Run

From this repository, use the source checkout next to it:

```sh
python3 native-workflow/run.py \
  --schema-repo ../schema-registry \
  --schema-ref origin/8.2.0-native \
  --release-version 8.2.0 \
  --platform linux/arm64
```

Use the 8.3.2 source ref and version in a second invocation:

```sh
python3 native-workflow/run.py \
  --schema-repo ../schema-registry \
  --schema-ref codex/native-8.3.2 \
  --release-version 8.3.2 \
  --platform linux/arm64
```

The default reference is `origin/8.2.0-native`, the default release is `8.2.0`, and the default target follows the container engine's architecture. The requested target must match the engine because GraalVM native-image compiles for the platform on which it runs. Set `CONTAINER_ENGINE=podman` to use Podman; otherwise Docker Compose is selected. The engine and its Compose provider must be installed and running.

When the standalone JAR was already built from the selected source archive, pass it with `--jar /path/to/kafka-schema-registry-package-VERSION-standalone.jar`. The runner still archives the selected ref to record its commit and read any legacy native-image arguments from the package POM. Ensure the supplied JAR was built from that ref and release version.

The runner requires Python 3.12 or later. The Maven build uses `-Pstandalone -pl package-schema-registry -am -DskipTests -Dcyclonedx.skip=true clean package`. Static analysis remains enabled; the CycloneDX release SBOM is skipped to avoid unrelated remote dependency resolution during this local native workflow. For the 8.2.0 source, the runner selects SpotBugs 4.9.8.0 so analysis can parse Java 25 class files; 8.3.2 already pins that compatible version. All container images are pulled by version tag, resolved to registry digests, and then used by digest: GraalVM Native Image 25.0.4, Kafka 4.3.0, and UBI 9.6. Each startup must report the archived source commit and requested release version at `/v1/metadata/version`. Each run gets a unique Compose project; shutdown only removes that project's containers and network. Run artifacts and the local Kafka data directory remain under the output directory, which defaults to `artifacts/native-workflow/`.

If a stage fails, the runner keeps the run directory and writes available container logs and state there. It exits nonzero and reports the failing command or REST request.

## Validation

On 2026-09-30, both `origin/8.2.0-native` and the `v8.3.2` port passed the JVM instrumentation, native image build, schema operations, and restart checks on Linux ARM64 with GraalVM 25.0.4. The baseline used a source-built JAR through `--jar`; the 8.3.2 run also exercised the runner's Maven source build. The 8.3.2 Maven `native:compile-no-fork` entry point was separately compiled in Linux with the same JAR and agent metadata, with its JAR hash unchanged.

These checks use a single plaintext Kafka broker. Other deployment configurations need their own representative instrumentation workloads.
