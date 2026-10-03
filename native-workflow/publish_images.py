#!/usr/bin/env python3
# Preserve commit tags on retries and assemble releases from recorded digests so architecture builds cannot drift.
from __future__ import annotations

import argparse
import base64
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import urllib.error
import urllib.parse
import urllib.request

import run as workflow


@dataclass
class Manifest:
    digest: str
    document: dict


class RegistryRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, new_url):
        redirected = super().redirect_request(request, response, code, message, headers, new_url)
        if redirected is not None and urllib.parse.urlsplit(request.full_url).netloc != urllib.parse.urlsplit(new_url).netloc:
            # Blob storage redirects must not receive registry credentials.
            redirected.remove_header("Authorization")
        return redirected


def immutable_release_tag(version: str, schema_commit: str, pipeline_commit: str) -> str:
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version):
        raise workflow.WorkflowError("Release tags require a supported numeric release version")
    if any(not re.fullmatch(r"[0-9a-f]{40}", commit) for commit in (schema_commit, pipeline_commit)):
        raise workflow.WorkflowError("Release tags require full hexadecimal schema and pipeline commits")
    return f"{version}-{schema_commit[:12]}-{pipeline_commit[:12]}"


class Registry:
    def __init__(self, username: str, password: str, origin: str = "https://ghcr.io"):
        self.origin = origin
        self.authorization = "Basic " + base64.b64encode(f"{username}:{password}".encode()).decode()
        self.tokens: dict[str, str] = {}
        self.opener = urllib.request.build_opener(RegistryRedirectHandler())

    def token(self, repository: str) -> str:
        if repository not in self.tokens:
            query = urllib.parse.urlencode({"service": "ghcr.io", "scope": f"repository:{repository}:pull,push"})
            request = urllib.request.Request(f"{self.origin}/token?{query}",
                                             headers={"Authorization": self.authorization})
            try:
                with self.opener.open(request, timeout=60) as response:
                    document = json.load(response)
            except (OSError, ValueError) as error:
                if isinstance(error, urllib.error.HTTPError):
                    error.close()
                raise workflow.WorkflowError(f"Registry authentication failed: {error}") from error
            if not isinstance(document, dict):
                raise workflow.WorkflowError("Registry authentication returned a non-object response")
            token = document.get("token") or document.get("access_token")
            if not isinstance(token, str) or not token:
                raise workflow.WorkflowError("Registry authentication returned no token")
            self.tokens[repository] = token
        return self.tokens[repository]

    def get(self, repository: str, path: str, *, missing_ok: bool = False) -> tuple[bytes, str | None] | None:
        request = urllib.request.Request(f"{self.origin}/v2/{repository}/{path}", headers={
            "Authorization": "Bearer " + self.token(repository),
            "Accept": ", ".join(("application/vnd.oci.image.index.v1+json",
                                  "application/vnd.oci.image.manifest.v1+json",
                                  "application/vnd.docker.distribution.manifest.list.v2+json",
                                  "application/vnd.docker.distribution.manifest.v2+json")),
        })
        try:
            with self.opener.open(request, timeout=60) as response:
                return response.read(), response.headers.get("Docker-Content-Digest")
        except urllib.error.HTTPError as error:
            error.close()
            if missing_ok and error.code == 404:
                return None
            raise workflow.WorkflowError(f"Registry request failed for {repository}/{path}: HTTP {error.code}") from error
        except OSError as error:
            raise workflow.WorkflowError(f"Registry request failed for {repository}/{path}: {error}") from error

    def manifest(self, repository: str, reference: str) -> Manifest | None:
        response = self.get(repository, f"manifests/{reference}", missing_ok=True)
        if response is None:
            return None
        body, digest = response
        if digest != "sha256:" + hashlib.sha256(body).hexdigest():
            raise workflow.WorkflowError(f"Registry returned an invalid manifest digest for {repository}:{reference}")
        try:
            document = json.loads(body)
        except ValueError as error:
            raise workflow.WorkflowError("Registry returned invalid manifest JSON") from error
        if not isinstance(document, dict):
            raise workflow.WorkflowError("Registry returned a non-object manifest")
        return Manifest(digest, document)

    def configuration(self, repository: str, manifest: Manifest) -> dict:
        digest = manifest.document.get("config", {}).get("digest")
        if not isinstance(digest, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
            raise workflow.WorkflowError("Architecture tag must identify a single-platform image with a config digest")
        body, _ = self.get(repository, f"blobs/{digest}")
        if digest != "sha256:" + hashlib.sha256(body).hexdigest():
            raise workflow.WorkflowError("Registry image configuration failed its digest check")
        try:
            document = json.loads(body)
        except ValueError as error:
            raise workflow.WorkflowError("Registry returned invalid image configuration JSON") from error
        if not isinstance(document, dict):
            raise workflow.WorkflowError("Registry returned a non-object image configuration")
        return document


def publish_architecture(build: dict, registry: Registry) -> dict:
    if build.get("status") != "passed" or build.get("metadataMode") != "build" or build.get("pipelineDirty") is not False:
        raise workflow.WorkflowError("Only validated builds from committed pipeline inputs can be published")
    tag = immutable_release_tag(build["version"], build["schemaCommit"], build["pipelineCommit"])
    if build.get("immutableTag") != tag or build["platform"] not in ("linux/amd64", "linux/arm64"):
        raise workflow.WorkflowError("Build release identity or platform is invalid")
    architecture = build["platform"].split("/")[1]
    images = {}
    for kind in ("native", "instrumented"):
        candidates = [entry["tag"] for entry in build["images"][kind] if entry["tag"].endswith(f":{tag}-{architecture}")]
        if len(candidates) != 1 or not candidates[0].startswith("ghcr.io/"):
            raise workflow.WorkflowError(f"Missing unique GHCR commit tag for {kind}")
        reference = candidates[0]
        repository = reference.removeprefix("ghcr.io/").rsplit(":", 1)[0]
        manifest = registry.manifest(repository, f"{tag}-{architecture}")
        if manifest is None:
            workflow.run(["docker", "push", reference])
            manifest = registry.manifest(repository, f"{tag}-{architecture}")
        if manifest is None:
            raise workflow.WorkflowError(f"Published architecture image is missing: {reference}")
        configuration = registry.configuration(repository, manifest)
        labels = configuration.get("config", {}).get("Labels", {})
        expected = {"org.opencontainers.image.revision": build["schemaCommit"],
                    "io.spockz.pipeline.revision": build["pipelineCommit"],
                    "org.opencontainers.image.version": build["version"]}
        if (any(labels.get(key) != value for key, value in expected.items())
                or configuration.get("architecture") != architecture or configuration.get("os") != "linux"):
            raise workflow.WorkflowError(f"Existing commit tag has a conflicting identity: {reference}")
        images[kind] = {"repository": "ghcr.io/" + repository, "digest": manifest.digest, "tag": reference}
    return {key: build[key] for key in ("version", "schemaCommit", "pipelineCommit", "platform", "immutableTag")} | {
        "images": images,
    }


def publish_manifests(records: list[dict], registry: Registry) -> dict:
    if len(records) != 2 or {record["platform"] for record in records} != {"linux/amd64", "linux/arm64"}:
        raise workflow.WorkflowError("Publication requires exactly one AMD64 and one ARM64 record")
    identity = {key: records[0][key] for key in ("version", "schemaCommit", "pipelineCommit", "immutableTag")}
    if any(any(record[key] != value for key, value in identity.items()) for record in records):
        raise workflow.WorkflowError("Architecture builds used different schema or pipeline commits")
    tag = immutable_release_tag(identity["version"], identity["schemaCommit"], identity["pipelineCommit"])
    if identity["immutableTag"] != tag:
        raise workflow.WorkflowError("Publication records contain an invalid commit tag")
    images = {}
    for kind in ("native", "instrumented"):
        repositories = {record["images"][kind]["repository"] for record in records}
        if len(repositories) != 1 or not next(iter(repositories)).startswith("ghcr.io/"):
            raise workflow.WorkflowError("Architecture images use different GHCR repositories")
        image = next(iter(repositories))
        repository = image.removeprefix("ghcr.io/")
        expected = {(record["platform"], record["images"][kind]["digest"]) for record in records}
        manifest = registry.manifest(repository, tag)
        if manifest is None:
            sources = [f"{image}@{digest}" for _, digest in sorted(expected)]
            workflow.run(["docker", "buildx", "imagetools", "create", "--tag", f"{image}:{tag}", *sources])
            manifest = registry.manifest(repository, tag)
        if manifest is None:
            raise workflow.WorkflowError(f"Published multi-architecture image is missing: {image}:{tag}")
        descriptors = manifest.document.get("manifests", [])
        actual = {(f"{entry.get('platform', {}).get('os')}/{entry.get('platform', {}).get('architecture')}",
                   entry.get("digest")) for entry in descriptors}
        if len(descriptors) != 2 or actual != expected:
            raise workflow.WorkflowError(f"Existing commit manifest has conflicting architecture digests: {image}:{tag}")
        images[kind] = {"tag": f"{image}:{tag}", "digest": manifest.digest}
    for kind, published in images.items():
        image = records[0]["images"][kind]["repository"]
        aliases = [identity["version"]]
        if identity["version"] == "8.3.2":
            aliases.append("latest")
        command = ["docker", "buildx", "imagetools", "create"]
        for alias in aliases:
            command.extend(("--tag", f"{image}:{alias}"))
        workflow.run([*command, f"{image}@{published['digest']}"])
        for record in records:
            architecture = record["platform"].split("/")[1]
            workflow.run(["docker", "buildx", "imagetools", "create", "--prefer-index=false",
                          "--tag", f"{image}:{identity['version']}-{architecture}",
                          f"{image}@{record['images'][kind]['digest']}"])
    return {**identity, "images": images}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=("architecture", "manifests"))
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    username, password = os.environ.get("GHCR_USERNAME"), os.environ.get("GHCR_TOKEN")
    if not username or not password:
        raise workflow.WorkflowError("GHCR_USERNAME and GHCR_TOKEN are required for publication")
    registry = Registry(username, password)
    if args.stage == "architecture":
        builds = workflow.read_json_object(args.input)["builds"]
        selected = [build for build in builds if build["version"] == args.version]
        if len(selected) != 1:
            raise workflow.WorkflowError("Expected one validated build for the requested version")
        result = publish_architecture(selected[0], registry)
    else:
        records = [workflow.read_json_object(path) for path in sorted(args.input.glob("*/publication.json"))]
        selected = [record for record in records if record["version"] == args.version]
        result = publish_manifests(selected, registry)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    try:
        main()
    except workflow.WorkflowError as error:
        raise SystemExit(f"ERROR: {error}") from error
