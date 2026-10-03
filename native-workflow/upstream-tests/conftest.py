# Bind the upstream tests to a real external registry with certificate verification, never a mock or spawned JVM.
import os

import pytest

from confluent_kafka.schema_registry import SchemaRegistryClient
from tests.integration.cluster_fixture import ByoFixture


class ImageFixture(ByoFixture):
    def schema_registry(self, conf=None):
        options = {"url": self._sr_url, "ssl.ca.location": os.environ["SR_CA"]}
        if conf:
            options.update(conf)
        return SchemaRegistryClient(options)


@pytest.fixture(scope="session")
def kafka_cluster():
    cluster = ImageFixture({"bootstrap.servers": os.environ["BROKERS"], "schema.registry.url": os.environ["SR_URL"]})
    yield cluster
    cluster.stop()
