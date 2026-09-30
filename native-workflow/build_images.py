#!/usr/bin/env python3
# This entry point turns only runner-verified artifacts into deployable images, keeping the TLS test setup out of release contexts.
"""Build deployable native and instrumented images for selected Schema Registry versions."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import uuid

import run as workflow


VERSIONS = {
    "8.2.0": "8.2.0-native",
    "8.3.2": "native-8.3.2",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", action="append", choices=tuple(VERSIONS), dest="versions",
                        help="Version to build; repeat to select multiple (default: both supported versions)")
    parser.add_argument("--schema-repo", type=Path, default=Path("../schema-registry"))
    parser.add_argument("--output-dir", type=Path, default=Path("artifacts/native-images"))
    parser.add_argument("--engine", choices=("docker", "podman"),
                        default=os.environ.get("CONTAINER_ENGINE", "docker"))
    parser.add_argument("--platform", choices=("linux/amd64", "linux/arm64"))
    parser.add_argument("--maven", default=os.environ.get("MAVEN", "mvn"))
    parser.add_argument("--image-name", default="schema-registry-native")
    parser.add_argument("--instrumented-image-name", default="kafka-schema-registry-graalvm-instrumented")
    return parser.parse_args()


def json_file(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise workflow.WorkflowError(f"Could not read valid JSON from {path}: {error}") from error
    if not isinstance(value, dict):
        raise workflow.WorkflowError(f"Expected a JSON object in {path}")
    return value


def validate_run(run_dir: Path, version: str, schema_ref: str, platform: str) -> dict:
    provenance = json_file(run_dir / "provenance.json")
    result = json_file(run_dir / "result.json")
    expected = {
        "releaseVersion": version,
        "schemaRef": schema_ref,
        "platform": platform,
    }
    for field, value in expected.items():
        if provenance.get(field) != value:
            raise workflow.WorkflowError(
                f"Run {run_dir} has {field}={provenance.get(field)!r}; expected {value!r}"
            )
    if result.get("status") != "passed":
        raise workflow.WorkflowError(f"Native workflow did not pass for {version}: {result}")
    commit = provenance.get("schemaCommit")
    if not isinstance(commit, str) or len(commit) != 40:
        raise workflow.WorkflowError(f"Run {run_dir} has no full schema source commit: {commit!r}")
    artifact_paths = {
        "nativeBinarySha256": run_dir / "schema-registry.native",
        "jarSha256": run_dir / "schema-registry.jar",
    }
    for key, artifact in artifact_paths.items():
        expected_hash = provenance.get(key)
        if not artifact.is_file():
            raise workflow.WorkflowError(f"Run artifact is missing: {artifact}")
        if not isinstance(expected_hash, str) or workflow.sha256(artifact) != expected_hash:
            raise workflow.WorkflowError(f"SHA-256 mismatch for {artifact} against provenance field {key}")
    images = provenance.get("images")
    if not isinstance(images, dict):
        raise workflow.WorkflowError(f"Run {run_dir} does not record pinned base images")
    pinned = {}
    for key in ("runtime", "graalvm"):
        record = images.get(key)
        reference = record.get("reference") if isinstance(record, dict) else None
        if not isinstance(reference, str) or "@sha256:" not in reference:
            raise workflow.WorkflowError(f"Run {run_dir} has no pinned {key} image reference")
        pinned[key] = reference
    return {"provenance": provenance, "binary": artifact_paths["nativeBinarySha256"],
            "jar": artifact_paths["jarSha256"], "pinnedImages": pinned}


def run_version(args: argparse.Namespace, version: str, platform: str) -> dict:
    version_output = args.output_dir / "workflow" / version
    version_output.mkdir(parents=True, exist_ok=True)
    before = {path.resolve() for path in version_output.iterdir() if path.is_dir()}
    schema_ref = VERSIONS[version]
    command = [
        sys.executable, str(Path(__file__).with_name("run.py")),
        "--schema-repo", str(args.schema_repo.resolve()),
        "--schema-ref", schema_ref,
        "--release-version", version,
        "--platform", platform,
        "--output-dir", str(version_output),
        "--engine", args.engine,
        "--maven", args.maven,
    ]
    workflow.run(command)
    after = {path.resolve() for path in version_output.iterdir() if path.is_dir()}
    created = sorted(after - before)
    if len(created) != 1:
        raise workflow.WorkflowError(
            f"Expected exactly one new workflow run directory for {version}; found {created}"
        )
    return {"runDirectory": created[0],
            "artifacts": validate_run(created[0], version, schema_ref, platform)}


def build_image(args: argparse.Namespace, *, dockerfile: str, image_name: str,
                version: str, platform: str, run_dir: Path, artifacts: dict) -> list[dict]:
    architecture = platform.split("/", 1)[1]
    tags = [f"{image_name}:{version}-{architecture}", f"{image_name}:{version}"]
    provenance = artifacts["provenance"]
    with tempfile.TemporaryDirectory(prefix=f"schema-registry-{version}-") as temporary:
        context = Path(temporary)
        if dockerfile == "Dockerfile.native":
            source = artifacts["binary"]
            destination = context / "schema-registry"
            expected_hash = provenance["nativeBinarySha256"]
            mode = 0o555
            base_argument = f"RUNTIME_IMAGE={artifacts['pinnedImages']['runtime']}"
        elif dockerfile == "Dockerfile.instrumented":
            source = artifacts["jar"]
            destination = context / "schema-registry.jar"
            expected_hash = provenance["jarSha256"]
            mode = 0o444
            base_argument = f"GRAAL_IMAGE={artifacts['pinnedImages']['graalvm']}"
        else:
            raise workflow.WorkflowError(f"Unknown deployment Dockerfile: {dockerfile}")
        shutil.copy2(source, destination)
        # Changing artifact permissions in a later image layer duplicates its bytes.
        destination.chmod(mode)
        if workflow.sha256(destination) != expected_hash:
            raise workflow.WorkflowError(f"Copied artifact changed while preparing {run_dir}: {source}")
        command = [args.engine, "build", "--platform", platform, "-f",
                   str(Path(__file__).with_name(dockerfile))]
        for tag in tags:
            command.extend(("-t", tag))
        command.extend((
            "--build-arg", base_argument,
            "--build-arg", f"RELEASE_VERSION={version}",
            "--build-arg", f"SCHEMA_REVISION={provenance['schemaCommit']}",
            str(context),
        ))
        workflow.run(command)
    identities = []
    for tag in tags:
        inspected = workflow.inspect_image(args.engine, tag)
        if not inspected.get("id"):
            raise workflow.WorkflowError(f"Engine returned no image ID after building {tag}: {inspected}")
        identities.append({"tag": tag, **inspected})
    return identities


def smoke_test(args: argparse.Namespace, run_dir: Path, version: str, platform: str,
               native: list[dict], instrumented: list[dict]) -> dict:
    smoke_dir = run_dir / "deployment-smoke"
    metadata_dir = smoke_dir / "metadata"
    smoke_dir.mkdir(exist_ok=True)
    metadata_dir.mkdir(exist_ok=True)
    metadata_dir.chmod(0o777)
    original = json_file(run_dir / "compose.yaml")
    services = original.get("services", {})
    if not {"broker", "agent", "native"} <= services.keys():
        raise workflow.WorkflowError(f"Runner Compose file has unexpected services: {services.keys()}")
    broker = services["broker"]
    agent = dict(services["agent"])
    native_service = dict(services["native"])
    agent.pop("build", None)
    native_service.pop("build", None)
    agent["image"] = instrumented[0]["id"]
    native_service["image"] = native[0]["id"]
    agent["volumes"] = [
        f"{run_dir / 'agent' / 'schema-registry.properties'}:/etc/schema-registry.properties:ro",
        f"{run_dir / 'tls' / 'server.p12'}:/opt/app/server.p12:ro",
        f"{run_dir / 'tls' / 'truststore.p12'}:/opt/app/truststore.p12:ro",
        f"{metadata_dir}:/opt/reachability",
    ]
    native_service["volumes"] = [
        f"{run_dir / 'native' / 'schema-registry.properties'}:/etc/schema-registry.properties:ro",
        f"{run_dir / 'tls' / 'server.p12'}:/opt/app/server.p12:ro",
        f"{run_dir / 'tls' / 'truststore.p12'}:/opt/app/truststore.p12:ro",
    ]
    broker["volumes"] = [f"{run_dir / 'kafka-data'}:/var/lib/kafka/data"]
    document = {"services": {"broker": broker, "agent": agent, "native": native_service}}
    compose = smoke_dir / "compose.yaml"
    compose.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    project = "srdeploy" + uuid.uuid4().hex[:12]
    prefix = [*workflow.compose_command(args.engine), "-p", project, "-f", str(compose)]
    provenance = json_file(run_dir / "provenance.json")
    commit = provenance["schemaCommit"]
    tls_certificate = run_dir / "tls" / "server.pem"
    checks = {}

    failure = None
    try:
        workflow.run([*prefix, "up", "-d", "broker", "agent"], log=smoke_dir / "compose-agent.log")
        agent_base = f"http://127.0.0.1:{agent['ports'][0].split(':')[1]}"
        agent_tls = int(agent["ports"][1].split(":")[1])
        workflow.wait_http(agent_base, 240)
        workflow.check_container_image(args.engine, prefix, instrumented[0]["id"], "agent")
        workflow.check_runtime_version(agent_base, "packaged instrumented JVM", version, commit)
        checks["instrumentedTls"] = workflow.check_tls(agent_tls, "packaged instrumented JVM", version,
                                                        commit, tls_certificate)
        subjects = workflow.http_json("GET", f"https://localhost:{agent_tls}", "/subjects",
                                      ssl_context=workflow.verified_tls_context(tls_certificate))
        if not isinstance(subjects, list) or not any(
                isinstance(subject, str) and subject.startswith("native-workflow-") for subject in subjects):
            raise workflow.WorkflowError(f"Packaged JVM did not expose the runner's stored workload: {subjects}")
        checks["recordedSubjects"] = subjects
        workflow.run([*prefix, "stop", "-t", "45", "agent"])
        workflow.run([*prefix, "logs", "--no-color", "agent"], log=smoke_dir / "agent.log")
        metadata_files = sorted(metadata_dir.glob("*.json"))
        if not metadata_files:
            raise workflow.WorkflowError("Packaged instrumented image produced no reachability JSON files")
        has_metadata = False
        for metadata in metadata_files:
            try:
                value = json.loads(metadata.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as error:
                raise workflow.WorkflowError(f"Invalid instrumented reachability metadata {metadata}: {error}") from error
            if not isinstance(value, (dict, list)):
                raise workflow.WorkflowError(f"Invalid reachability metadata document: {metadata}")
            has_metadata = has_metadata or bool(value)
        if not has_metadata:
            raise workflow.WorkflowError("Packaged instrumented image produced only empty reachability metadata")
        checks["reachabilityMetadata"] = [
            {"path": str(path.relative_to(run_dir)), "sha256": workflow.sha256(path)}
            for path in metadata_files
        ]
        workflow.run([*prefix, "up", "-d", "native"], log=smoke_dir / "compose-native.log")
        native_base = f"http://127.0.0.1:{native_service['ports'][0].split(':')[1]}"
        native_tls = int(native_service["ports"][1].split(":")[1])
        workflow.wait_http(native_base, 240)
        workflow.check_container_image(args.engine, prefix, native[0]["id"])
        workflow.check_runtime_version(native_base, "packaged native image", version, commit)
        checks["nativeTls"] = workflow.check_tls(native_tls, "packaged native image", version,
                                                  commit, tls_certificate)
        native_subjects = workflow.http_json("GET", f"https://localhost:{native_tls}", "/subjects",
                                             ssl_context=workflow.verified_tls_context(tls_certificate))
        if native_subjects != subjects:
            raise workflow.WorkflowError(
                f"Packaged native image does not read the workload exposed by the packaged JVM: "
                f"{native_subjects} != {subjects}"
            )
        checks["nativeSubjects"] = native_subjects
        workflow.run([*prefix, "logs", "--no-color", "native"], log=smoke_dir / "native.log")
        workflow.run([*prefix, "logs", "--no-color", "broker"], log=smoke_dir / "broker.log")
    except Exception as error:
        failure = error
        for service in ("agent", "native", "broker"):
            try:
                workflow.run([*prefix, "logs", "--no-color", service], log=smoke_dir / f"{service}.log")
            except Exception as log_error:
                print(f"Could not collect {service} smoke logs: {log_error}", file=sys.stderr)
    finally:
        try:
            workflow.run([*prefix, "down", "--remove-orphans"])
        except Exception as cleanup_error:
            print(f"Deployment smoke cleanup failed for {project}: {cleanup_error}", file=sys.stderr)
            if failure is None:
                failure = cleanup_error
    report = {"status": "failed" if failure else "passed", "version": version,
              "platform": platform, "project": project, "checks": checks,
              "error": str(failure) if failure else None}
    (smoke_dir / "smoke.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    if failure:
        raise workflow.WorkflowError(f"Packaged deployment image smoke test failed: {failure}") from failure
    return report


def main() -> int:
    args = parse_args()
    args.versions = list(dict.fromkeys(args.versions or VERSIONS.keys()))
    args.schema_repo = args.schema_repo.resolve()
    if not (args.schema_repo / ".git").exists():
        raise workflow.WorkflowError(f"Schema repository is not a Git checkout: {args.schema_repo}")
    platform = args.platform or workflow.engine_architecture(args.engine)
    args.output_dir = args.output_dir.resolve()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    manifest = args.output_dir / "images.json"
    manifest.unlink(missing_ok=True)
    for version in args.versions:
        ref = VERSIONS[version]
        workflow.run(["git", "-C", str(args.schema_repo), "rev-parse", "--verify", f"{ref}^{{commit}}"])

    builds = []
    for version in args.versions:
        run_result = run_version(args, version, platform)
        run_dir = run_result["runDirectory"]
        artifacts = run_result["artifacts"]
        native = build_image(args, dockerfile="Dockerfile.native", image_name=args.image_name,
                             version=version, platform=platform, run_dir=run_dir, artifacts=artifacts)
        instrumented = build_image(
            args, dockerfile="Dockerfile.instrumented", image_name=args.instrumented_image_name,
            version=version, platform=platform, run_dir=run_dir, artifacts=artifacts,
        )
        smoke_test(args, run_dir, version, platform, native, instrumented)
        builds.append({
            "version": version,
            "schemaRef": artifacts["provenance"]["schemaRef"],
            "schemaCommit": artifacts["provenance"]["schemaCommit"],
            "nativeBinarySha256": artifacts["provenance"]["nativeBinarySha256"],
            "jarSha256": artifacts["provenance"]["jarSha256"],
            "platform": platform,
            "runDirectory": str(run_dir),
            "images": {"native": native, "instrumented": instrumented},
        })
    temporary = manifest.with_name(manifest.name + ".tmp")
    temporary.write_text(json.dumps({"builds": builds}, indent=2) + "\n", encoding="utf-8")
    temporary.replace(manifest)
    print(f"Built deployable images; manifest: {manifest}", flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except workflow.WorkflowError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1)
