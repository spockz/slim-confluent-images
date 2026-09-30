#!/usr/bin/env python3
# Keep this runner beside its private build contexts so root Compose cannot reuse stale mounts.
"""Build and exercise a native Schema Registry from an archived source ref."""

from __future__ import annotations

import argparse
from collections import deque
import hashlib
import http.client
import json
import os
import re
import shlex
import socket
import shutil
import ssl
import subprocess
import sys
import tarfile
import time
import urllib.error
import urllib.request
import uuid
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path


GRAAL_IMAGE = "container-registry.oracle.com/graalvm/native-image:25.0.4-ol9"
KAFKA_IMAGE = "apache/kafka:4.3.0"
RUNTIME_IMAGE = "registry.access.redhat.com/ubi9/ubi-minimal:9.6"
NATIVE_PROPERTIES = "META-INF/native-image/io.confluent/kafka-schema-registry-package/native-image.properties"
NS = "{http://maven.apache.org/POM/4.0.0}"


class WorkflowError(RuntimeError):
    pass


def run(command: list[str], *, cwd: Path | None = None, log: Path | None = None,
        capture_all: bool = False) -> str:
    print("+ " + shlex.join(command), flush=True)
    process = subprocess.Popen(
        command,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    tail: deque[str] = deque(maxlen=240)
    complete: list[str] | None = [] if capture_all else None
    if log:
        log.parent.mkdir(parents=True, exist_ok=True)
        output_stream = log.open("w", encoding="utf-8")
    else:
        output_stream = None
    try:
        assert process.stdout is not None
        for line in process.stdout:
            print(line, end="", flush=True)
            tail.append(line)
            if complete is not None:
                complete.append(line)
            if output_stream:
                output_stream.write(line)
                output_stream.flush()
        return_code = process.wait()
    finally:
        if output_stream:
            output_stream.close()
    output = "".join(complete if complete is not None else tail)
    if return_code:
        raise WorkflowError(
            f"Command failed with status {return_code}: {shlex.join(command)}\n{output}"
        )
    return output


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def inspect_image(engine: str, image: str) -> dict:
    raw = run([engine, "image", "inspect", image], capture_all=True)
    inspected = json.loads(raw)
    record = inspected[0] if isinstance(inspected, list) else inspected
    return {
        "id": record.get("Id", record.get("ID")),
        "digest": record.get("Digest"),
        "repoDigests": record.get("RepoDigests", []),
    }


def pin_image(engine: str, image: str, platform: str) -> tuple[str, dict]:
    run([engine, "pull", "--platform", platform, image])
    record = inspect_image(engine, image)
    digests = record.get("repoDigests") or []
    target_name = image.rsplit("/", 1)[-1].split(":", 1)[0]
    matching = [entry for entry in digests if entry.split("@", 1)[0].rsplit("/", 1)[-1] == target_name]
    if not matching:
        raise WorkflowError(f"Image engine returned no registry digest for {image}: {record}")
    pinned = sorted(matching)[0]
    record["pinnedReference"] = pinned
    return pinned, record


def copy_native_binary(engine: str, image: str, image_id: str | None, run_dir: Path,
                       project: str) -> Path:
    name = f"{project}-artifact"
    container = run([engine, "create", "--name", name, image]).strip()
    destination = run_dir / "schema-registry.native"
    try:
        inspected = json.loads(run([engine, "inspect", container], capture_all=True))
        container_image = inspected[0].get("Image") if isinstance(inspected, list) else inspected.get("Image")
        if image_id and container_image != image_id:
            raise WorkflowError(f"Artifact container uses image {container_image}, expected {image_id}")
        run([engine, "cp", f"{container}:/usr/local/bin/schema-registry", str(destination)])
    finally:
        run([engine, "rm", container])
    return destination


def check_runtime_version(base: str, label: str, release_version: str, commit: str,
                         ssl_context: ssl.SSLContext | None = None) -> None:
    metadata = http_json("GET", base, "/v1/metadata/version", ssl_context=ssl_context)
    if metadata.get("version") != release_version or metadata.get("commitId") != commit:
        raise WorkflowError(
            f"{label} reports build metadata {metadata}; expected version={release_version}, commitId={commit}"
        )


def check_container_image(engine: str, compose_prefix: list[str], image_id: str,
                          service: str = "native") -> None:
    container = run([*compose_prefix, "ps", "-q", service]).strip()
    if not container:
        raise WorkflowError(f"Compose did not report a running {service} container")
    inspected = json.loads(run([engine, "inspect", container], capture_all=True))
    record = inspected[0] if isinstance(inspected, list) else inspected
    if record.get("Image") != image_id:
        raise WorkflowError(
            f"Running {service} container uses image {record.get('Image')}, expected {image_id}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema-repo", type=Path, default=Path("../schema-registry"))
    parser.add_argument("--schema-ref", default="8.2.0-native")
    parser.add_argument("--release-version", default="8.2.0")
    parser.add_argument("--platform", choices=("linux/amd64", "linux/arm64"))
    parser.add_argument("--output-dir", type=Path, default=Path("artifacts/native-workflow"))
    parser.add_argument("--jar", type=Path, help="Use a previously built standalone JAR")
    parser.add_argument("--maven", default=os.environ.get("MAVEN", "mvn"))
    parser.add_argument("--engine", choices=("docker", "podman"), default=os.environ.get("CONTAINER_ENGINE", "docker"))
    parser.add_argument("--ready-timeout", type=int, default=240)
    return parser.parse_args()


def compose_command(engine: str) -> list[str]:
    if engine == "docker":
        if shutil.which("docker-compose"):
            return ["docker-compose"]
        return ["docker", "compose"]
    return ["podman", "compose"]


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def engine_architecture(engine: str) -> str:
    template = "{{.Architecture}}" if engine == "docker" else "{{.Host.Arch}}"
    architecture = run([engine, "info", "--format", template]).strip().lower()
    if architecture in {"aarch64", "arm64"}:
        return "linux/arm64"
    if architecture in {"x86_64", "amd64"}:
        return "linux/amd64"
    raise WorkflowError(f"Unsupported container engine architecture: {architecture}")


def source_snapshot(repo: Path, ref: str, destination: Path) -> str:
    repo = repo.resolve()
    commit = run(["git", "-C", str(repo), "rev-parse", "--verify", f"{ref}^{{commit}}"]).strip()
    archive = destination.parent / "schema-source.tar"
    with archive.open("wb") as output:
        subprocess.run(
            ["git", "-C", str(repo), "archive", "--format=tar", commit],
            stdout=output,
            check=True,
        )
    destination.mkdir(parents=True, exist_ok=False)
    with tarfile.open(archive) as source:
        source.extractall(destination, filter="data")
    archive.unlink()
    return commit


def remove_stale_metadata(source: Path) -> None:
    for path in source.rglob("META-INF/native-image"):
        if not path.is_dir():
            continue
        for metadata in (path / "reachability-metadata.json",):
            if metadata.exists():
                metadata.unlink()
        for metadata_dir in (path / "test", path / "instrumented-reachability-metadata"):
            if metadata_dir.exists():
                shutil.rmtree(metadata_dir)


def scrub_jar_metadata(jar: Path) -> None:
    temporary = jar.with_suffix(".clean.jar")
    with zipfile.ZipFile(jar) as incoming, zipfile.ZipFile(temporary, "w") as outgoing:
        for item in incoming.infolist():
            parts = Path(item.filename).parts
            try:
                metadata_root = parts.index("native-image")
            except ValueError:
                metadata_root = -1
            metadata_path = parts[metadata_root + 1:] if metadata_root >= 0 else ()
            if (metadata_path == ("reachability-metadata.json",)
                    or metadata_path[:1] in {("test",), ("instrumented-reachability-metadata",)}):
                continue
            outgoing.writestr(item, incoming.read(item.filename))
    temporary.replace(jar)


def normalize_source_versions(source: Path, release_version: str) -> None:
    for pom in source.rglob("pom.xml"):
        tree = ET.parse(pom)
        changed = False
        for element in tree.getroot().iter():
            value = (element.text or "").strip()
            if value == f"{release_version}-0":
                element.text = release_version
                changed = True
            elif value.startswith(f"[{release_version}-0,"):
                element.text = release_version
                changed = True
        if changed:
            # Maven's model reader requires unprefixed element names.
            ET.register_namespace("", NS[1:-1])
            tree.write(pom, encoding="UTF-8", xml_declaration=True)


def native_args_from_pom(pom: Path) -> list[str]:
    root = ET.parse(pom).getroot()
    result: list[str] = []
    for plugin in root.iter(f"{NS}plugin"):
        artifact = plugin.findtext(f"{NS}artifactId", default="")
        if artifact not in {"native-maven-plugin", "native-image-maven-plugin"}:
            continue
        config = plugin.find(f"{NS}configuration")
        if config is None:
            continue
        for build_args in config.iter():
            if build_args.tag.rsplit("}", 1)[-1] not in {"buildArgs", "build-args"}:
                continue
            result.extend((child.text or "").strip() for child in build_args if (child.text or "").strip())
    return result


def merge_legacy_services(source: Path, release_version: str) -> bool:
    if release_version != "8.2.0":
        return False
    path = source / "package-schema-registry" / "src" / "assembly" / "standalone.xml"
    tree = ET.parse(path)
    root = tree.getroot()
    namespace = root.tag.split("}", 1)[0] + "}"
    handlers = root.find(f"{namespace}containerDescriptorHandlers")
    if handlers is None:
        handlers = ET.Element(f"{namespace}containerDescriptorHandlers")
        file_sets = root.find(f"{namespace}fileSets")
        if file_sets is None:
            raise WorkflowError(f"Legacy standalone assembly has no fileSets: {path}")
        root.insert(list(root).index(file_sets), handlers)
    if any(handler.findtext(f"{namespace}handlerName") == "metaInf-services" for handler in handlers):
        return False
    handler = ET.SubElement(handlers, f"{namespace}containerDescriptorHandler")
    ET.SubElement(handler, f"{namespace}handlerName").text = "metaInf-services"
    ET.register_namespace("", namespace[1:-1])
    tree.write(path, encoding="UTF-8", xml_declaration=True)
    return True


def compose_file(run_dir: Path, project: str, platform: str, kafka_image: str,
                 agent_port: int, native_port: int, agent_tls_port: int,
                 native_tls_port: int) -> Path:
    document = {
        "services": {
            "broker": {
                "image": kafka_image,
                "platform": platform,
                "hostname": "broker",
                "environment": {
                    "KAFKA_NODE_ID": "1",
                    "KAFKA_PROCESS_ROLES": "broker,controller",
                    "KAFKA_LISTENERS": "PLAINTEXT://:29092,CONTROLLER://:29093",
                    "KAFKA_ADVERTISED_LISTENERS": "PLAINTEXT://broker:29092",
                    "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
                    "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@broker:29093",
                    "KAFKA_INTER_BROKER_LISTENER_NAME": "PLAINTEXT",
                    "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
                    "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
                    "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
                    "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
                    "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
                    "KAFKA_LOG_DIRS": "/var/lib/kafka/data",
                    "CLUSTER_ID": "MkU3OEVBNTcwNTJENDM2Qk",
                },
                "volumes": ["./kafka-data:/var/lib/kafka/data"],
            },
            "agent": {
                "build": {"context": "./agent", "dockerfile": "Dockerfile"},
                "platform": platform,
                "depends_on": ["broker"],
                "ports": [f"127.0.0.1:{agent_port}:8081", f"127.0.0.1:{agent_tls_port}:8082"],
                "volumes": ["./metadata:/opt/reachability"],
                "stop_grace_period": "45s",
            },
            "native": {
                "image": f"{project}-native",
                "build": {"context": "./native", "dockerfile": "Dockerfile"},
                "platform": platform,
                "depends_on": ["broker"],
                "ports": [f"127.0.0.1:{native_port}:8081", f"127.0.0.1:{native_tls_port}:8082"],
                "stop_grace_period": "45s",
            },
        }
    }
    path = run_dir / "compose.yaml"
    path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    return path


def generate_tls_material(engine: str, graal_image: str, run_dir: Path) -> None:
    tls_dir = run_dir / "tls"
    tls_dir.mkdir()
    keystore = tls_dir / "server.p12"
    certificate = tls_dir / "server.pem"
    truststore = tls_dir / "truststore.p12"
    password = "native-workflow-test"
    mount = f"{tls_dir}:/tls"
    run([
        engine, "run", "--rm", "-v", mount, "--entrypoint", "keytool", graal_image,
        "-genkeypair", "-alias", "schema-registry", "-keyalg", "RSA", "-keysize", "2048",
        "-validity", "2", "-dname", "CN=localhost", "-ext", "SAN=dns:localhost",
        "-storetype", "PKCS12", "-keystore", "/tls/server.p12", "-storepass", password,
        "-keypass", password, "-noprompt",
    ])
    run([
        engine, "run", "--rm", "-v", mount, "--entrypoint", "keytool", graal_image,
        "-exportcert", "-rfc", "-alias", "schema-registry", "-keystore", "/tls/server.p12",
        "-storepass", password, "-file", "/tls/server.pem",
    ])
    run([
        engine, "run", "--rm", "-v", mount, "--entrypoint", "keytool", graal_image,
        "-importcert", "-noprompt", "-alias", "schema-registry", "-file", "/tls/server.pem",
        "-storetype", "PKCS12", "-keystore", "/tls/truststore.p12", "-storepass", password,
    ])
    for path in (keystore, certificate, truststore):
        if not path.is_file() or path.stat().st_size == 0:
            raise WorkflowError(f"keytool did not create TLS material: {path}")
        path.chmod(0o644)


def write_context(run_dir: Path, jar: Path, native_args: list[str], graal_image: str,
                  runtime_image: str) -> None:
    agent = run_dir / "agent"
    native = run_dir / "native"
    for path in (agent, native, run_dir / "metadata", run_dir / "kafka-data"):
        path.mkdir(parents=True, exist_ok=True)
    for context in (agent, native):
        shutil.copy2(jar, context / "schema-registry.jar")
        shutil.copy2(run_dir / "tls" / "server.p12", context / "server.p12")
        shutil.copy2(run_dir / "tls" / "truststore.p12", context / "truststore.p12")
        registry_host = "schema-registry-agent" if context == agent else "schema-registry-native"
        (context / "schema-registry.properties").write_text(
            "listeners=http://0.0.0.0:8081,https://0.0.0.0:8082\n"
            f"host.name={registry_host}\n"
            "ssl.keystore.location=/opt/app/server.p12\n"
            "ssl.keystore.type=PKCS12\n"
            "ssl.keystore.password=native-workflow-test\n"
            "ssl.key.password=native-workflow-test\n"
            "ssl.protocol=TLS\n"
            "ssl.enabled.protocols=TLSv1.2,TLSv1.3\n"
            "kafkastore.bootstrap.servers=PLAINTEXT://broker:29092\n"
            "kafkastore.topic=_schemas\n"
            "kafkastore.topic.replication.factor=1\n"
            "kafkastore.init.timeout.ms=60000\n"
            "kafkastore.timeout.ms=10000\n",
            encoding="utf-8",
        )
    (agent / "Dockerfile").write_text(
        f"FROM {graal_image}\n"
        "WORKDIR /opt/app\n"
        "COPY schema-registry.jar /opt/app/schema-registry.jar\n"
        "COPY schema-registry.properties /opt/app/schema-registry.properties\n"
        "COPY server.p12 truststore.p12 /opt/app/\n"
        'ENTRYPOINT ["java", "-Djavax.net.debug=ssl,handshake", "-Djavax.net.ssl.trustStore=/opt/app/truststore.p12", "-Djavax.net.ssl.trustStoreType=PKCS12", "-Djavax.net.ssl.trustStorePassword=native-workflow-test", "-agentlib:native-image-agent=config-output-dir=/opt/reachability", "-jar", "/opt/app/schema-registry.jar", "/opt/app/schema-registry.properties"]\n',
        encoding="utf-8",
    )
    options = ["--no-fallback", "-H:ConfigurationFileDirectories=/opt/reachability", *native_args]
    command = " ".join(shlex.quote(option) for option in options)
    (native / "Dockerfile").write_text(
        f"FROM {graal_image} AS compiler\n"
        "WORKDIR /opt/app\n"
        "COPY schema-registry.jar /opt/app/schema-registry.jar\n"
        "COPY schema-registry.properties /opt/app/schema-registry.properties\n"
        "COPY server.p12 truststore.p12 /opt/app/\n"
        "COPY reachability /opt/reachability\n"
        f"RUN native-image {command} -jar /opt/app/schema-registry.jar /opt/app/schema-registry\n"
        f"FROM {runtime_image}\n"
        "COPY --from=compiler /opt/app/schema-registry /usr/local/bin/schema-registry\n"
        "COPY schema-registry.properties /etc/schema-registry.properties\n"
        "COPY --from=compiler /opt/app/server.p12 /opt/app/server.p12\n"
        "COPY --from=compiler /opt/app/truststore.p12 /opt/app/truststore.p12\n"
        "USER 10001\n"
        'ENTRYPOINT ["/usr/local/bin/schema-registry", "-Djavax.net.debug=ssl,handshake", "-Djavax.net.ssl.trustStore=/opt/app/truststore.p12", "-Djavax.net.ssl.trustStoreType=PKCS12", "-Djavax.net.ssl.trustStorePassword=native-workflow-test", "/etc/schema-registry.properties"]\n',
        encoding="utf-8",
    )


def http_json(method: str, base: str, path: str, payload: dict | None = None,
              ssl_context: ssl.SSLContext | None = None) -> dict | list:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = urllib.request.Request(
        base + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=12, context=ssl_context) as response:
            return json.loads(response.read())
    except (urllib.error.HTTPError, urllib.error.URLError, http.client.HTTPException, OSError) as error:
        body = error.read().decode("utf-8", errors="replace") if isinstance(error, urllib.error.HTTPError) else str(error)
        raise WorkflowError(f"Schema Registry request {method} {path} failed: {body}") from error


def verified_tls_context(certificate: Path, version: ssl.TLSVersion | None = None) -> ssl.SSLContext:
    context = ssl.create_default_context(cafile=str(certificate))
    context.set_alpn_protocols(["http/1.1"])
    if version is not None:
        context.minimum_version = version
        context.maximum_version = version
    return context


def tls_protocol_probe(port: int, version: ssl.TLSVersion, label: str,
                       release_version: str, commit: str, certificate: Path) -> dict:
    context = verified_tls_context(certificate, version)
    connection = http.client.HTTPSConnection("localhost", port, context=context, timeout=12)
    try:
        connection.connect()
        sock = connection.sock
        if sock is None:
            raise WorkflowError(f"{label} did not retain its TLS socket after connecting")
        negotiated = sock.version()
        alpn_protocol = sock.selected_alpn_protocol()
        cipher = sock.cipher()
        peer = sock.getpeercert()
        certificate_sha256 = hashlib.sha256(sock.getpeercert(binary_form=True)).hexdigest()
        if negotiated != version.name.replace("TLSv1_", "TLSv1."):
            raise WorkflowError(f"{label} negotiated {negotiated}, expected {version.name}")
        if not cipher:
            raise WorkflowError(f"{label} did not report a negotiated cipher")
        if alpn_protocol != "http/1.1":
            raise WorkflowError(f"{label} negotiated ALPN {alpn_protocol!r}, expected http/1.1")
        connection.request("GET", "/v1/metadata/version")
        response = connection.getresponse()
        body = json.loads(response.read())
        if response.status != 200:
            raise WorkflowError(f"{label} returned HTTP {response.status} during TLS probe: {body}")
        if body.get("version") != release_version or body.get("commitId") != commit:
            raise WorkflowError(f"{label} returned unexpected metadata during TLS probe: {body}")
        return {
            "label": label,
            "requestedProtocol": version.name,
            "negotiatedProtocol": negotiated,
            "alpnProtocol": alpn_protocol,
            "cipher": cipher[0],
            "certificateSha256": certificate_sha256,
            "peerCertificate": peer,
            "metadata": body,
            "result": "passed",
        }
    except (OSError, ssl.SSLError, http.client.HTTPException, json.JSONDecodeError) as error:
        raise WorkflowError(f"{label} TLS {version.name} probe failed: {error}") from error
    finally:
        connection.close()


def certificate_verification_error(error: BaseException) -> ssl.SSLCertVerificationError | None:
    pending: list[BaseException] = [error]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, ssl.SSLCertVerificationError):
            return current
        for nested in (getattr(current, "reason", None), current.__cause__, current.__context__):
            if isinstance(nested, BaseException):
                pending.append(nested)
    return None


def expect_certificate_rejection(host: str, port: int, context: ssl.SSLContext,
                                 label: str) -> dict:
    connection = http.client.HTTPSConnection(host, port, context=context, timeout=12)
    try:
        connection.request("GET", "/v1/metadata/version")
        response = connection.getresponse()
        response.read()
    except (OSError, ssl.SSLError, http.client.HTTPException, urllib.error.URLError) as error:
        verification_error = certificate_verification_error(error)
        if verification_error is None:
            raise WorkflowError(f"{label} failed without a certificate verification error: {error}") from error
        return {
            "check": label,
            "result": "passed",
            "verificationCode": verification_error.verify_code,
            "verificationMessage": verification_error.verify_message,
        }
    finally:
        connection.close()
    raise WorkflowError(f"{label} unexpectedly accepted the server certificate")


def check_tls(port: int, label: str, release_version: str, commit: str,
              certificate: Path) -> list[dict]:
    results = [
        tls_protocol_probe(port, version, label, release_version, commit, certificate)
        for version in (ssl.TLSVersion.TLSv1_2, ssl.TLSVersion.TLSv1_3)
    ]
    results.append(expect_certificate_rejection(
        "localhost", port, ssl.create_default_context(), f"{label} rejects untrusted CA",
    ))
    results.append(expect_certificate_rejection(
        "127.0.0.1", port, verified_tls_context(certificate), f"{label} rejects wrong hostname",
    ))
    return results


SCHEMAS = {
    "avro": (
        "AVRO",
        '{"type":"record","name":"WorkflowRecord","fields":[{"name":"name","type":"string"}]}',
        '{"type":"record","name":"WorkflowRecord","fields":[{"name":"name","type":"string"},{"name":"note","type":["null","string"],"default":null}]}',
    ),
    "json": (
        "JSON",
        '{"$schema":"http://json-schema.org/draft-07/schema#","title":"WorkflowRecord","type":"object","properties":{"name":{"type":"string"}},"required":["name"],"additionalProperties":false}',
        '{"$schema":"http://json-schema.org/draft-07/schema#","title":"WorkflowRecord","type":"object","properties":{"name":{"type":"string"},"note":{"type":"string"}},"required":["name"],"additionalProperties":false}',
    ),
    "protobuf": (
        "PROTOBUF",
        'syntax = "proto3";\nmessage WorkflowRecord { string name = 1; }',
        'syntax = "proto3";\nmessage WorkflowRecord { string name = 1; string note = 2; }',
    ),
}


def read_version(base: str, subject: str, schema_type: str, version: int,
                 expected_schema: str | None = None,
                 ssl_context: ssl.SSLContext | None = None) -> dict:
    subject_record = http_json("GET", base, f"/subjects/{subject}/versions/{version}", ssl_context=ssl_context)
    if (subject_record.get("version") != version or subject_record.get("subject") != subject
            or subject_record.get("schemaType", "AVRO") != schema_type):
        raise WorkflowError(f"Unexpected subject version record: {subject_record}")
    schema_id = subject_record.get("id")
    if not isinstance(schema_id, int):
        raise WorkflowError(f"Subject version has no integer schema ID: {subject_record}")
    by_id = http_json("GET", base, f"/schemas/ids/{schema_id}", ssl_context=ssl_context)
    if by_id.get("schemaType", "AVRO") != schema_type:
        raise WorkflowError(f"Schema type mismatch for {subject}: {by_id}")
    if by_id.get("schema") != subject_record.get("schema"):
        raise WorkflowError(f"Subject and ID lookup returned different canonical schema bodies for {subject}")
    if schema_type == "PROTOBUF":
        if "WorkflowRecord" not in by_id.get("schema", ""):
            raise WorkflowError(f"Protobuf body missing expected message: {by_id}")
        if expected_schema is not None and re.sub(r"\s+", "", by_id["schema"]) != re.sub(r"\s+", "", expected_schema):
            raise WorkflowError(f"Unexpected Protobuf schema body for {subject}: {by_id}")
    else:
        try:
            parsed = json.loads(by_id["schema"])
            expected = json.loads(expected_schema) if expected_schema is not None else None
        except (KeyError, json.JSONDecodeError) as error:
            raise WorkflowError(f"Invalid {schema_type} schema body for {subject}: {by_id}") from error
        if parsed.get("title", parsed.get("name")) != "WorkflowRecord":
            raise WorkflowError(f"Unexpected {schema_type} canonical schema body: {by_id}")
        if expected is not None and parsed != expected:
            raise WorkflowError(f"Unexpected {schema_type} schema for {subject}: {by_id}")
    return {
        "id": schema_id,
        "schemaType": schema_type,
        "schema": by_id["schema"],
    }


def check_versions(base: str, label: str, subjects: dict[str, dict],
                   ssl_context: ssl.SSLContext | None = None) -> None:
    actual_types = http_json("GET", base, "/schemas/types", ssl_context=ssl_context)
    if not set(item.upper() for item in actual_types) >= {"AVRO", "JSON", "PROTOBUF"}:
        raise WorkflowError(f"Registry does not advertise all schema types: {actual_types}")
    for subject, details in subjects.items():
        schema_type = details["schemaType"]
        expected_versions = details["versions"]
        for version, expected in expected_versions.items():
            actual = read_version(base, subject, schema_type, version, ssl_context=ssl_context)
            if actual != expected:
                raise WorkflowError(f"{label} changed subject {subject} version {version}: {actual} != {expected}")
        latest = http_json("GET", base, f"/subjects/{subject}/versions/latest", ssl_context=ssl_context)
        if latest.get("version") != max(expected_versions):
            raise WorkflowError(f"Latest version mismatch for {subject}: {latest}")
    check_incompatible_avro(base, label, subjects, ssl_context)
    print(f"Functional schema checks passed against {label}", flush=True)


def check_incompatible_avro(base: str, label: str, subjects: dict[str, dict],
                            ssl_context: ssl.SSLContext | None = None) -> None:
    subject = "native-workflow-avro-value"
    before = http_json("GET", base, f"/subjects/{subject}/versions/latest", ssl_context=ssl_context)
    incompatible = {
        "schemaType": "AVRO",
        "schema": '{"type":"record","name":"WorkflowRecord","fields":[{"name":"name","type":"int"}]}',
    }
    result = http_json(
        "POST", base, f"/compatibility/subjects/{subject}/versions/latest", incompatible,
        ssl_context=ssl_context,
    )
    if result.get("is_compatible") is not False:
        raise WorkflowError(f"{label} accepted an incompatible Avro change: {result}")
    after = http_json("GET", base, f"/subjects/{subject}/versions/latest", ssl_context=ssl_context)
    if after != before or before.get("version") != max(subjects[subject]["versions"]):
        raise WorkflowError(f"Compatibility check changed the stored Avro subject: {before} -> {after}")


def register_jvm_schemas(base: str, ssl_context: ssl.SSLContext | None = None) -> dict[str, dict]:
    subjects: dict[str, dict] = {}
    for kind, (schema_type, v1, v2) in SCHEMAS.items():
        subject = f"native-workflow-{kind}-value"
        first = http_json("POST", base, f"/subjects/{subject}/versions", {"schemaType": schema_type, "schema": v1}, ssl_context)
        if not isinstance(first.get("id"), int):
            raise WorkflowError(f"Registration did not return an ID for {subject}: {first}")
        compatible = http_json(
            "POST", base, f"/compatibility/subjects/{subject}/versions/latest",
            {"schemaType": schema_type, "schema": v2},
            ssl_context,
        )
        if compatible.get("is_compatible") is not True:
            raise WorkflowError(f"Compatible {schema_type} evolution was rejected for {subject}: {compatible}")
        second = http_json("POST", base, f"/subjects/{subject}/versions", {"schemaType": schema_type, "schema": v2}, ssl_context)
        if not isinstance(second.get("id"), int):
            raise WorkflowError(f"Second registration did not return an ID for {subject}: {second}")
        v1_record = read_version(base, subject, schema_type, 1, v1, ssl_context)
        v2_record = read_version(base, subject, schema_type, 2, v2, ssl_context)
        if first["id"] != v1_record["id"] or second["id"] != v2_record["id"]:
            raise WorkflowError(f"Registration IDs do not match subject reads for {subject}")
        subjects[subject] = {
            "schemaType": schema_type,
            "versions": {1: v1_record, 2: v2_record},
        }
    check_versions(base, "instrumented JVM", subjects, ssl_context)
    return subjects


def register_native_schemas(base: str, subjects: dict[str, dict],
                            ssl_context: ssl.SSLContext | None = None) -> None:
    for kind, (schema_type, _v1, v2) in SCHEMAS.items():
        subject = f"native-workflow-{kind}-value"
        if kind == "avro":
            native_schema = '{"type":"record","name":"WorkflowRecord","fields":[{"name":"name","type":"string"},{"name":"note","type":["null","string"],"default":null},{"name":"native_only","type":"string","default":""}]}'
        elif kind == "json":
            doc = json.loads(v2)
            doc["properties"]["native_only"] = {"type": "string"}
            native_schema = json.dumps(doc, separators=(",", ":"))
        else:
            native_schema = 'syntax = "proto3";\nmessage WorkflowRecord { string name = 1; string note = 2; string native_only = 3; }'
        compatible = http_json(
            "POST", base, f"/compatibility/subjects/{subject}/versions/latest",
            {"schemaType": schema_type, "schema": native_schema},
            ssl_context,
        )
        if compatible.get("is_compatible") is not True:
            raise WorkflowError(f"Native {schema_type} evolution was rejected for {subject}: {compatible}")
        result = http_json(
            "POST", base, f"/subjects/{subject}/versions",
            {"schemaType": schema_type, "schema": native_schema},
            ssl_context,
        )
        if not isinstance(result.get("id"), int):
            raise WorkflowError(f"Native write did not return an ID for {subject}: {result}")
        v3_record = read_version(base, subject, schema_type, 3, native_schema, ssl_context)
        if result["id"] != v3_record["id"]:
            raise WorkflowError(f"Native registration ID does not match subject read for {subject}")
        prior_ids = {version["id"] for version in subjects[subject]["versions"].values()}
        if v3_record["id"] in prior_ids:
            raise WorkflowError(f"Native registration reused a previous schema ID for {subject}")
        subjects[subject]["versions"][3] = v3_record


def wait_http(base: str, timeout: int, ssl_context: ssl.SSLContext | None = None) -> None:
    deadline = time.monotonic() + timeout
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            http_json("GET", base, "/subjects", ssl_context=ssl_context)
            return
        except WorkflowError as error:
            last = error
            time.sleep(2)
    raise WorkflowError(f"Registry at {base} was not ready after {timeout}s: {last}")


def main() -> int:
    args = parse_args()
    engine_platform = engine_architecture(args.engine)
    args.platform = args.platform or engine_platform
    if args.platform != engine_platform:
        raise WorkflowError(
            f"Requested {args.platform}, but {args.engine} runs {engine_platform}; native-image needs a runnable target platform"
        )
    repo = args.schema_repo.resolve()
    if not (repo / ".git").exists():
        raise WorkflowError(f"Schema repository is not a Git checkout: {repo}")
    args.output_dir = args.output_dir.resolve()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    run_id = f"{args.release_version}-{time.strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}"
    run_dir = args.output_dir / run_id
    run_dir.mkdir()
    source = run_dir / "source"
    commit = source_snapshot(repo, args.schema_ref, source)
    remove_stale_metadata(source)
    normalize_source_versions(source, args.release_version)
    legacy_service_merge = merge_legacy_services(source, args.release_version)
    pom = source / "package-schema-registry" / "pom.xml"
    if not pom.exists():
        raise WorkflowError(f"Package POM missing from {args.schema_ref}: {pom}")
    native_args = native_args_from_pom(pom)
    if args.jar:
        jar = args.jar.resolve()
        if not jar.is_file():
            raise WorkflowError(f"Standalone JAR does not exist: {jar}")
        shutil.copy2(jar, run_dir / "schema-registry.jar")
        jar = run_dir / "schema-registry.jar"
        if args.release_version == "8.2.0":
            scrub_jar_metadata(jar)
        jar_origin = str(args.jar.resolve())
    else:
        build_log = run_dir / "maven-build.log"
        maven_options = [
            args.maven, "--batch-mode", "-Pstandalone", "-pl", "package-schema-registry", "-am",
            "-DskipTests", "-Dcyclonedx.skip=true", "-Dmaven.buildNumber.skip=true", f"-DgitCommitID={commit}",
            f"-Dio.confluent.schema-registry.version={args.release_version}",
        ]
        if args.release_version == "8.2.0":
            maven_options.append("-Dspotbugs.maven.plugin.version=4.9.8.0")
        run(
            [*maven_options, "clean", "package"],
            cwd=source,
            log=build_log,
        )
        jars = list((source / "package-schema-registry" / "target").glob("*-standalone.jar"))
        if len(jars) != 1:
            raise WorkflowError(f"Expected one standalone JAR, found: {jars}")
        jar = run_dir / "schema-registry.jar"
        shutil.copy2(jars[0], jar)
        jar_origin = str(jars[0].relative_to(source))
    with zipfile.ZipFile(jar) as archive:
        if args.release_version != "8.2.0" and NATIVE_PROPERTIES not in archive.namelist():
            raise WorkflowError(f"Standalone JAR is missing its native-image options: {NATIVE_PROPERTIES}")
        alpn_service = "META-INF/services/org.eclipse.jetty.io.ssl.ALPNProcessor$Server"
        alpn_providers = archive.read(alpn_service).decode("utf-8").splitlines() if alpn_service in archive.namelist() else []
        if "org.eclipse.jetty.alpn.java.server.JDK9ServerALPNProcessor" not in alpn_providers:
            raise WorkflowError(
                "Standalone JAR is missing the JDK ALPN service provider; rebuild it with "
                "the metaInf-services assembly handler (omit --jar for the legacy 8.2.0 build)"
            )
    provenance = {
        "releaseVersion": args.release_version,
        "schemaRef": args.schema_ref,
        "schemaCommit": commit,
        "jarSource": jar_origin,
        "jarSha256": sha256(jar),
        "platform": args.platform,
        "imageTags": {"graalvm": GRAAL_IMAGE, "kafka": KAFKA_IMAGE, "runtime": RUNTIME_IMAGE},
        "nativeArgsFromPackagePom": native_args,
        "legacyServiceDescriptorMerge": legacy_service_merge and args.jar is None,
        "tlsChecks": [],
    }
    graal_ref, graal_inspection = pin_image(args.engine, GRAAL_IMAGE, args.platform)
    kafka_ref, kafka_inspection = pin_image(args.engine, KAFKA_IMAGE, args.platform)
    runtime_ref, runtime_inspection = pin_image(args.engine, RUNTIME_IMAGE, args.platform)
    provenance["images"] = {
        "graalvm": {**graal_inspection, "reference": graal_ref},
        "kafka": {**kafka_inspection, "reference": kafka_ref},
        "runtime": {**runtime_inspection, "reference": runtime_ref},
    }
    (run_dir / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n", encoding="utf-8")
    project = "srnative" + re.sub(r"[^a-z0-9]", "", run_id.lower())[-18:]
    agent_port = free_port()
    allocated_ports = {agent_port}
    ports = []
    while len(ports) < 3:
        candidate = free_port()
        if candidate not in allocated_ports:
            allocated_ports.add(candidate)
            ports.append(candidate)
    native_port, agent_tls_port, native_tls_port = ports
    generate_tls_material(args.engine, graal_ref, run_dir)
    provenance["tlsCertificateSha256"] = sha256(run_dir / "tls" / "server.pem")
    (run_dir / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n", encoding="utf-8")
    compose = compose_file(
        run_dir, project, args.platform, kafka_ref, agent_port, native_port,
        agent_tls_port, native_tls_port,
    )
    write_context(run_dir, jar, native_args, graal_ref, runtime_ref)
    base = f"http://127.0.0.1:{agent_port}"
    tls_base = f"https://localhost:{agent_tls_port}"
    tls_certificate = run_dir / "tls" / "server.pem"
    tls_context = verified_tls_context(tls_certificate)
    engine = compose_command(args.engine)
    prefix = [*engine, "-p", project, "-f", str(compose)]
    (run_dir / "metadata").mkdir(exist_ok=True)
    failure: Exception | None = None
    try:
        run([*prefix, "up", "-d", "broker", "agent"], log=run_dir / "agent-build.log")
        wait_http(base, args.ready_timeout)
        check_runtime_version(base, "instrumented JVM", args.release_version, commit)
        provenance["tlsChecks"].extend(check_tls(
            agent_tls_port, "instrumented JVM", args.release_version, commit, tls_certificate,
        ))
        (run_dir / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n", encoding="utf-8")
        subjects = register_jvm_schemas(tls_base, tls_context)
        run([*prefix, "stop", "-t", "45", "agent"])
        run([*prefix, "logs", "--no-color", "agent"], log=run_dir / "agent.log")
        metadata_files = sorted((run_dir / "metadata").glob("*.json"))
        if not metadata_files:
            raise WorkflowError("GraalVM agent produced no reachability JSON files")
        for metadata in metadata_files:
            with metadata.open(encoding="utf-8") as stream:
                json.load(stream)
        provenance["agentMetadata"] = [
            {"path": str(path.relative_to(run_dir)), "sha256": sha256(path)}
            for path in metadata_files
        ]
        (run_dir / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n", encoding="utf-8")
        shutil.copytree(run_dir / "metadata", run_dir / "native" / "reachability", dirs_exist_ok=True)
        run([*prefix, "build", "native"], log=run_dir / "native-build.log")
        native_image = f"{project}-native"
        provenance["nativeImage"] = inspect_image(args.engine, native_image)
        native_binary = copy_native_binary(
            args.engine, native_image, provenance["nativeImage"]["id"], run_dir, project
        )
        provenance["nativeBinarySha256"] = sha256(native_binary)
        (run_dir / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n", encoding="utf-8")
        run([*prefix, "up", "-d", "native"])
        native_base = f"http://127.0.0.1:{native_port}"
        native_tls_base = f"https://localhost:{native_tls_port}"
        wait_http(native_base, args.ready_timeout)
        check_container_image(args.engine, prefix, provenance["nativeImage"]["id"])
        check_runtime_version(native_base, "native binary", args.release_version, commit)
        provenance["tlsChecks"].extend(check_tls(
            native_tls_port, "native binary", args.release_version, commit, tls_certificate,
        ))
        (run_dir / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n", encoding="utf-8")
        native_tls_context = verified_tls_context(tls_certificate)
        check_versions(native_tls_base, "native replay of JVM registrations", subjects, native_tls_context)
        register_native_schemas(native_tls_base, subjects, native_tls_context)
        run([*prefix, "stop", "-t", "45", "native"])
        run([*prefix, "up", "-d", "native"])
        wait_http(native_base, args.ready_timeout)
        check_container_image(args.engine, prefix, provenance["nativeImage"]["id"])
        check_runtime_version(native_base, "restarted native binary", args.release_version, commit)
        provenance["tlsChecks"].extend(check_tls(
            native_tls_port, "restarted native binary", args.release_version, commit, tls_certificate,
        ))
        (run_dir / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n", encoding="utf-8")
        check_versions(
            native_tls_base, "restarted native binary", subjects,
            verified_tls_context(tls_certificate),
        )
        run([*prefix, "logs", "--no-color", "broker", "native"], log=run_dir / "native.log")
        provenance["agentMetadata"] = [
            {"path": str(path.relative_to(run_dir)), "sha256": sha256(path)}
            for path in sorted((run_dir / "metadata").rglob("*")) if path.is_file()
        ]
        (run_dir / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n", encoding="utf-8")
    except Exception as error:
        failure = error
        try:
            run([*prefix, "logs", "--no-color"], log=run_dir / "failure.log")
            run([*prefix, "ps", "-a"], log=run_dir / "containers.log")
        except Exception as diagnostics_error:
            print(f"Could not collect all container diagnostics: {diagnostics_error}", file=sys.stderr)
    finally:
        try:
            run([*prefix, "down", "--remove-orphans"])
        except Exception as cleanup_error:
            print(f"Container cleanup failed for project {project}: {cleanup_error}", file=sys.stderr)
            if failure is None:
                failure = cleanup_error
    if failure:
        (run_dir / "result.json").write_text(
            json.dumps({
                "status": "failed", "error": str(failure),
                "tlsChecks": provenance["tlsChecks"],
            }, indent=2) + "\n",
            encoding="utf-8",
        )
        raise failure
    (run_dir / "result.json").write_text(
        json.dumps({"status": "passed", "project": project, "tlsChecks": provenance["tlsChecks"]}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"Workflow passed; artifacts: {run_dir}", flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except WorkflowError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1)
