<!-- Keep workflow entry points here and detailed commands in their workflow guides. -->
# Slim confluent images

The repeatable source build, GraalVM instrumentation, native compilation, and functional test sequence is documented in [native-workflow/README.md](native-workflow/README.md). It supports the original `8.2.0-native` patch and the `v8.3.2` port.

# Schema Registry

Reduces the docker image by 3.68x from ±788MiB to 214MiB and startup time to 3½ seconds.

## Problem

Loading the Schema Registry docker file is about ~~2GiB~~ 788MiB after the latest updates.
This is still larger than needs be.

* ±100MiB for base image
* ±280MiB for the JRE
* ±380MiB for the sleuth of Jars for confluent products

## Solution

Create a new image based on the same ubi9 image, but minimal, only copying over the essential software (`ub`, `/etc/schema-registry`, and `/etc/schema-registry`) together with a native compiled image. (Todo: Go down to `-micro` and create the user directory and entries ourselves.)

Result: a ±214MiB image consisting of a ±100MiB base layer and a ±100MiB app. No more JRE, no more python, no more cruft.

The new image contains:
  
* The Schema Registry app compiled to a native binary (amd64, aarch64) from the Schema Registry source as forked in [my schema-registry fork](https://github.com/spockz/schema-registry).
* A modified version of the `schema-registry-start` script in `/usr/bin/schema-registry-start` which performs the same setup as the original and calls the native binary directly instead of the JRE.
* A script `java-stub` installed as `/usr/bin/java` to explicitly support the `kafka-ready` check performed by `ub`. The check is not implemented in `ub` directly, instead it branches out to some java code. As the image is lacking a JRE we have to stub the call.
* In addition to the custom work above, the image copies over from the published schema registry docker image:
  * `ub`: The new golang based tool for all kinds of verifications
  * The `/etc/confluent/docker` scripts: (these take care of all the env var handling etc)
  * The `/etc/schema-registry` directory.

## Aproach

Generating native binaries out of Java applications is done using (GraalVM)[https://www.graalvm.org/latest/getting-started/]. The GraalVM team has been working tirelessly on making this process easier (on the mind) and faster (for the CPU) since at least 2016 when I tried it first. 
Although the tooling and compiler have improved spectacularly since then, it still cannot perform magic. 
Sometimes it needs information from the runtime to properly determine which symbols should be available for reflection.

We achieve this information by instrumenting the schema-registry at runtime using the GraalVM JRE and Agent in the following setup:

1. Run the registry together with kafka and hit it with some calls to prime the registry, causing all/most paths to be hit.
   * Have the registry instrumented with the GraalVM agent for the reflection code
   * Persist the generated reachability information into the directory for use in the next step, it can also act as cache.
   * Orchestrated through a shell script and docker compose.
2. Use a docker build to copy the pertinent files from the schema registry into the native build image from Oracle together with the community reachability database and the instrumented reachability.
3. Profit

### Tested environments

Machines:

* AMD Ryzen 5900X, 32GiB DDR4-3800.

Deployments:

* Docker Compose environment with a single Kafka broker (Kraft, no zookeeper).


### Shenanigans

The Confluent schema registry image contains a few key files for executing "properly"
* ~~The two tools based on python to properly set all kinds of environment variables and configs~~
  * These have been replaced by a single go-lang based `ub` rewrite since I previously looked at this.
* The whole sleuth of dependencies and jars of Confluent all together in a single image

The setup kind of works although it is hampered by a more complicated classpath than is necessary.
Moreover, maintaining this work, especially the build and run time initialisation configuration, would be better suited to keep in the schema-registry project itself.
Therefore this approach is abandoned in favour of my fork + extension of the schema registry project: https://github.com/spockz/schema-registry. 
This also increases the chances that the native-image build is upstreamed.

### Considered other options to reduce the image size

* ~~**Deduplicate all the jars in the image**~~:
  Already done in the latest version using symlinks
* ~~**Only retain relevant jars**~~:
  * Read all jar files on startup during a (docker) `build` step.
    * Use `-verbose:class` to make the JRE emit all loaded class files including their jar location.
    * **Filter the jar locations and make them unique**: already done in the latest version using symlinks
    * Copy only these jar files to the new image
  * This was a hassle. It is easier to stay with the source.
