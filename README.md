# Slim confluent images

## Problem

Loading the Schema Registry docker file is about ~~2GiB~~ 788MiB after the latest updates.
This is still larger than needs be.

* ±100MiB for base image
* ±280MiB for the JRE
* ±380MiB for the sleuth of Jars for confluent products


## Aproach

1. Run the registry together with kafka and hit it with some calls to prime the registry, causing all/most paths to be hit.
   * Have the registry instrumented with the GraalVM agent for the reflection code
   * Persist the generated reachability information into the directory for use in the next step, it can also act as cache.
   * Orchestrated through a shell script and docker compose.
2. Use a docker build to copy the pertinent files from the schema registry into the native build image from Oracle together with the community reachability database and the instrumented reachability.
3. Profit


### Shenanigans

* The Confluent schema registry image contains a few key files for executing "properly"
  * The two tools based on python to properly set all kinds of environment variables and configs
  * The whole sleuth of dependencies and jars of Confluent all together in a single image


### Options

* Read all jar files on startup during a (docker) `build` step.
  * Use `-verbose:class` to make the JRE emit all loaded class files including their jar location.
  * Filter the jar locations and make them unique
  * Copy only these jar files to the new image
  *
