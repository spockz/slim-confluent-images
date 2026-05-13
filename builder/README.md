This directory contains the initial work for compiling the Confluent Schema Registry to a native based on the published docker images.

The setup kind of works although it is hampered by a more complicated classpath than is necessary.
Moreover, maintaining this work, especially the build and run time initialisation configuration, would be better suited to keep in the schema-registry project itself.
Therefore this approach is abandoned in favour of my fork + extension of the schema registry project: https://github.com/spockz/schema-registry
