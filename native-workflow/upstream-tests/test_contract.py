# Supplement upstream tests whose list-truthiness assertions cannot prove equality or reference correctness.
import json

import pytest

from confluent_kafka import TopicPartition
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer, JSONSerializer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer, ProtobufSerializer
from tests.integration.schema_registry.data.proto.TestProto_pb2 import TestMessage as ContractMessage


@pytest.mark.parametrize("schema_type", ["AVRO", "JSON", "PROTOBUF"])
def test_actual_round_trip(kafka_cluster, schema_type):
    client = kafka_cluster.schema_registry()
    topic = kafka_cluster.create_topic_and_wait_propogation("native-contract-" + schema_type.lower())
    if schema_type == "AVRO":
        schema = json.dumps({"type": "record", "name": "Contract", "fields": [{"name": "value", "type": "string"}]})
        serializer = AvroSerializer(client, schema)
        deserializer = AvroDeserializer(client)
        expected = {"value": "registry-contract"}
    elif schema_type == "JSON":
        schema = json.dumps({"title": "Contract", "type": "object", "properties": {"value": {"type": "string"}},
                             "required": ["value"]})
        serializer = JSONSerializer(schema, client)
        deserializer = JSONDeserializer(schema, schema_registry_client=client)
        expected = {"value": "registry-contract"}
    else:
        serializer = ProtobufSerializer(ContractMessage, client, {"use.deprecated.format": False})
        deserializer = ProtobufDeserializer(ContractMessage, {"use.deprecated.format": False})
        expected = ContractMessage(test_string="registry-contract", test_bool=True, test_bytes=b"contract")
    producer = kafka_cluster.producer(value_serializer=serializer)
    consumer = kafka_cluster.consumer(value_deserializer=deserializer)
    try:
        consumer.assign([TopicPartition(topic, 0)])
        producer.produce(topic, value=expected, partition=0)
        assert producer.flush(30) == 0
        message = consumer.poll(30)
        assert message is not None and message.error() is None
        assert message.value() == expected
        registered = client.get_latest_version(topic + "-value")
        assert registered.schema.schema_type == schema_type
        assert client.get_schema(registered.schema_id).schema_str == registered.schema.schema_str
    finally:
        consumer.close()


def test_reference_graph_matches_registered_versions(kafka_cluster):
    client = kafka_cluster.schema_registry()
    seen_types = set()
    for subject in client.get_subjects():
        registered = client.get_latest_version(subject)
        for reference in registered.schema.references:
            dependency = client.get_version(reference.subject, reference.version)
            assert dependency.version == reference.version
            assert dependency.schema.schema_type == registered.schema.schema_type
            assert registered.schema_id in client.get_referenced_by(reference.subject, reference.version)
            seen_types.add(registered.schema.schema_type)
    assert seen_types == {"AVRO", "JSON", "PROTOBUF"}
