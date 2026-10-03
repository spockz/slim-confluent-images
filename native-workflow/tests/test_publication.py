# Exercise registry HTTP responses and release retries so immutable publication cannot hide failures or mix revisions.
from copy import deepcopy
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import sys
from threading import Thread
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import publish_images as publication
import run as workflow


def encode(value):
    return json.dumps(value, sort_keys=True).encode()


def digest(body):
    return "sha256:" + hashlib.sha256(body).hexdigest()


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.documents = {}
        self.errors = {}
        self.commands = []
        self.local_images = {}
        self.redirects = {}
        self.storage_authorization = {}
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                path = urlsplit(self.path).path
                if path in fixture.errors:
                    self.send_error(fixture.errors[path])
                    return
                if path in fixture.redirects:
                    self.send_response(302)
                    self.send_header("Location", fixture.redirects[path])
                    self.end_headers()
                    return
                if path.startswith("/storage/"):
                    fixture.storage_authorization[path] = self.headers.get("Authorization")
                    body = fixture.documents[path]
                elif path == "/token":
                    if not self.headers.get("Authorization", "").startswith("Basic "):
                        self.send_error(401)
                        return
                    body = encode({"token": "test-token"})
                else:
                    if self.headers.get("Authorization") != "Bearer test-token":
                        self.send_error(401)
                        return
                    body = fixture.documents.get(path)
                    if body is None:
                        self.send_error(404)
                        return
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Docker-Content-Digest", digest(body))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(thread.join)
        self.addCleanup(server.shutdown)
        self.registry = publication.Registry("fixture", "secret", f"http://127.0.0.1:{server.server_port}")
        self.storage_origin = f"http://localhost:{server.server_port}"
        self.runner = patch.object(workflow, "run", side_effect=self.command)
        self.runner.start()
        self.addCleanup(self.runner.stop)

    def build(self, architecture, version="8.2.0"):
        schema, pipeline = "a" * 40, "b" * 40
        tag = publication.immutable_release_tag(version, schema, pipeline)
        images = {}
        for kind in ("native", "instrumented"):
            image = "ghcr.io/spockz/v2/schema-registry-" + kind
            reference = f"{image}:{tag}-{architecture}"
            configuration = encode({"architecture": architecture, "os": "linux", "config": {"Labels": {
                "org.opencontainers.image.revision": schema, "io.spockz.pipeline.revision": pipeline,
                "org.opencontainers.image.version": version,
            }}})
            repository = image.removeprefix("ghcr.io/")
            self.documents[f"/v2/{repository}/blobs/{digest(configuration)}"] = configuration
            self.local_images[reference] = encode({"schemaVersion": 2, "config": {"digest": digest(configuration)},
                                                   "layers": []})
            images[kind] = [{"tag": f"{image}:{version}-{architecture}"}, {"tag": reference}]
        return {"status": "passed", "metadataMode": "build", "pipelineDirty": False, "version": version,
                "schemaCommit": schema, "pipelineCommit": pipeline, "immutableTag": tag,
                "platform": "linux/" + architecture, "images": images}

    def store(self, image, reference, body):
        repository = image.removeprefix("ghcr.io/")
        self.documents[f"/v2/{repository}/manifests/{reference}"] = body
        self.documents[f"/v2/{repository}/manifests/{digest(body)}"] = body

    def command(self, command):
        self.commands.append(command)
        if command[:2] == ["docker", "push"]:
            image, tag = command[2].rsplit(":", 1)
            path = f"/v2/{image.removeprefix('ghcr.io/')}/manifests/{tag}"
            self.assertNotIn(path, self.documents, "Publisher attempted to overwrite an existing architecture tag")
            self.store(image, tag, self.local_images[command[2]])
            return ""
        self.assertEqual(["docker", "buildx", "imagetools", "create"], command[:4])
        tags = []
        sources = []
        index = 4
        while index < len(command):
            argument = command[index]
            if argument == "--tag":
                tags.append(command[index + 1])
                index += 2
            elif argument == "--prefer-index=false":
                index += 1
            else:
                sources.append(argument)
                index += 1
        if len(sources) == 1:
            image, source_digest = sources[0].split("@")
            body = self.documents[f"/v2/{image.removeprefix('ghcr.io/')}/manifests/{source_digest}"]
        else:
            descriptors = []
            for source in sources:
                image, source_digest = source.split("@")
                repository = image.removeprefix("ghcr.io/")
                manifest = json.loads(self.documents[f"/v2/{repository}/manifests/{source_digest}"])
                configuration = json.loads(self.documents[f"/v2/{repository}/blobs/{manifest['config']['digest']}"])
                descriptors.append({"digest": source_digest, "platform": {
                    "os": configuration["os"], "architecture": configuration["architecture"],
                }})
            body = encode({"schemaVersion": 2, "manifests": descriptors})
        for reference in tags:
            image, tag = reference.rsplit(":", 1)
            self.store(image, tag, body)
        return ""

    def release(self, version="8.2.0"):
        return [publication.publish_architecture(self.build(architecture, version), self.registry)
                for architecture in ("amd64", "arm64")]

    def test_publication_and_retry_keep_commit_digests_and_aliases_together(self):
        for version in ("8.2.0", "8.3.2"):
            with self.subTest(version=version):
                records = self.release(version)
                result = publication.publish_manifests(records, self.registry)
                original = dict(self.documents)
                self.commands.clear()
                for architecture in ("amd64", "arm64"):
                    build = self.build(architecture, version)
                    for entry in build["images"].values():
                        self.local_images[entry[1]["tag"]] = b"different bytes on retry"
                    publication.publish_architecture(build, self.registry)
                retried = publication.publish_manifests(records, self.registry)
                self.assertEqual(result, retried)
                self.assertEqual(original, self.documents)
                self.assertFalse(any(command[:2] == ["docker", "push"] for command in self.commands))
                self.assertFalse(any(f":{result['immutableTag']}" == part[-len(result['immutableTag'])-1:]
                                     for command in self.commands for part in command if "@" not in part))
                for kind, image in result["images"].items():
                    repository = records[0]["images"][kind]["repository"].removeprefix("ghcr.io/")
                    alias = self.registry.manifest(repository, version)
                    self.assertEqual(image["digest"], alias.digest)
                    latest = self.registry.manifest(repository, "latest")
                    if version == "8.3.2":
                        self.assertEqual(image["digest"], latest.digest)
                    else:
                        self.assertIsNone(latest)

    def test_only_manifest_404_means_a_missing_tag(self):
        for status in (401, 403, 429, 500, 503):
            with self.subTest(status=status):
                self.errors["/v2/spockz/v2/image/manifests/absent"] = status
                with self.assertRaises(workflow.WorkflowError):
                    self.registry.manifest("spockz/v2/image", "absent")
        self.errors.clear()
        self.assertIsNone(self.registry.manifest("spockz/v2/image", "absent"))
        self.errors["/token"] = 403
        with self.assertRaisesRegex(workflow.WorkflowError, "authentication"):
            self.registry.manifest("another/image", "absent")
        self.assertEqual([], self.commands)

    def test_conflicting_full_commit_is_not_overwritten(self):
        build = self.build("amd64")
        publication.publish_architecture(build, self.registry)
        build["schemaCommit"] = "a" * 12 + "c" * 28
        self.commands.clear()
        original = dict(self.documents)
        with self.assertRaisesRegex(workflow.WorkflowError, "conflicting identity"):
            publication.publish_architecture(build, self.registry)
        self.assertEqual([], self.commands)
        self.assertEqual(original, self.documents)

    def test_blob_storage_redirect_does_not_receive_registry_credentials(self):
        body = encode({"architecture": "arm64"})
        self.documents["/storage/configuration"] = body
        self.redirects["/v2/spockz/v2/image/blobs/" + digest(body)] = self.storage_origin + "/storage/configuration"
        configuration = self.registry.configuration("spockz/v2/image", publication.Manifest(
            "unused", {"config": {"digest": digest(body)}},
        ))
        self.assertEqual({"architecture": "arm64"}, configuration)
        self.assertIsNone(self.storage_authorization["/storage/configuration"])

    def test_mixed_revisions_and_missing_architectures_cannot_publish(self):
        records = self.release()
        self.commands.clear()
        for candidate in ([records[0]], [records[0], records[0]],
                          [records[0], {**records[1], "pipelineCommit": "c" * 40}],
                          [records[0], {**records[1], "schemaCommit": "c" * 40}]):
            with self.subTest(candidate=candidate):
                with self.assertRaises(workflow.WorkflowError):
                    publication.publish_manifests(candidate, self.registry)
        self.assertEqual([], self.commands)

    def test_conflicting_manifest_prevents_all_alias_updates(self):
        records = self.release()
        publication.publish_manifests(records, self.registry)
        image = records[0]["images"]["instrumented"]["repository"]
        tag = records[0]["immutableTag"]
        self.store(image, tag, encode({"schemaVersion": 2, "manifests": []}))
        self.commands.clear()
        original = dict(self.documents)
        with self.assertRaisesRegex(workflow.WorkflowError, "conflicting architecture digests"):
            publication.publish_manifests(records, self.registry)
        self.assertEqual([], self.commands)
        self.assertEqual(original, self.documents)

    def test_unvalidated_dirty_and_refresh_builds_cannot_publish(self):
        build = self.build("amd64")
        for field, value in (("status", "failed"), ("pipelineDirty", True), ("metadataMode", "refresh")):
            candidate = deepcopy(build)
            candidate[field] = value
            with self.subTest(field=field):
                with self.assertRaises(workflow.WorkflowError):
                    publication.publish_architecture(candidate, self.registry)
        self.assertEqual([], self.commands)
