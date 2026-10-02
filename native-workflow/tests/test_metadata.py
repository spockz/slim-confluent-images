# Exercise the real GraalVM merger so preserving existing metadata is checked against its actual configuration semantics.
from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import run as workflow


class AdditiveMetadataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.engine = os.environ.get("CONTAINER_ENGINE", "docker")
        cls.graal_image, _ = workflow.pin_image(
            cls.engine, workflow.GRAAL_IMAGE, workflow.engine_architecture(cls.engine),
        )

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="metadata merge ")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "source"
        self.run_dir = self.root / "run"
        self.agent = self.run_dir / "metadata" / "reachability-metadata.json"
        self.agent.parent.mkdir(parents=True)
        self.baseline = (self.source / "package/src/main/resources/META-INF/native-image"
                         / "reachability-metadata.json")
        self.baseline.parent.mkdir(parents=True)

    def write_metadata(self, baseline, fresh):
        self.baseline.write_text(json.dumps(baseline) + "\n", encoding="utf-8")
        self.agent.write_text(json.dumps(fresh) + "\n", encoding="utf-8")

    def test_merge_retains_members_flags_conditions_proxies_and_resources(self):
        self.write_metadata({
            "reflection": [
                {"type": "fixture.Flags", "allDeclaredMethods": True, "jniAccessible": True},
                {"type": "fixture.Members", "fields": [{"name": "before"}],
                 "methods": [{"name": "call", "parameterTypes": ["java.lang.String", "int"]}]},
                {"type": "fixture.Guarded", "condition": {"typeReached": "fixture.Before"}},
                {"type": {"proxy": ["fixture.First", "fixture.Second"]}},
                {"type": "fixture.OldOnly"},
            ],
            "resources": [{"glob": "before/*.txt"}],
        }, {
            "reflection": [
                {"type": "fixture.Flags", "allDeclaredMethods": False, "jniAccessible": False},
                {"type": "fixture.Members", "fields": [{"name": "after"}],
                 "methods": [{"name": "call", "parameterTypes": ["int", "java.lang.String"]}]},
                {"type": "fixture.Guarded", "condition": {"typeReached": "fixture.After"}},
                {"type": {"proxy": ["fixture.Second", "fixture.First"]}},
                {"type": "fixture.NewOnly"},
            ],
            "resources": [{"glob": "after/*.txt"}],
        })
        test_metadata = self.baseline.parent / "test" / "reachability-metadata.json"
        test_metadata.parent.mkdir()
        test_metadata.write_text(json.dumps({"reflection": [{"type": "fixture.TestOnly"}]}))
        original = self.baseline.read_bytes()
        committed = workflow.collect_committed_metadata(self.source, self.run_dir)
        self.assertEqual(2, len(committed))
        records = workflow.merge_reachability_metadata(self.engine, self.graal_image, self.run_dir, committed)
        self.assertTrue(records)
        merged = json.loads((self.run_dir / "native/reachability/reachability-metadata.json").read_text())
        reflection = merged["reflection"]
        by_type = {entry["type"]: entry for entry in reflection if isinstance(entry["type"], str)}
        self.assertTrue(by_type["fixture.Flags"]["allDeclaredMethods"])
        self.assertTrue(by_type["fixture.Flags"]["jniAccessible"])
        self.assertEqual({"before", "after"}, {field["name"] for field in by_type["fixture.Members"]["fields"]})
        self.assertEqual({("java.lang.String", "int"), ("int", "java.lang.String")},
                         {tuple(method["parameterTypes"]) for method in by_type["fixture.Members"]["methods"]})
        self.assertEqual({"fixture.Before", "fixture.After"},
                         {entry["condition"]["typeReached"] for entry in reflection if entry["type"] == "fixture.Guarded"})
        self.assertEqual({("fixture.First", "fixture.Second"), ("fixture.Second", "fixture.First")},
                         {tuple(entry["type"]["proxy"]) for entry in reflection if isinstance(entry["type"], dict)})
        self.assertTrue({"fixture.OldOnly", "fixture.NewOnly", "fixture.TestOnly"} <= by_type.keys())
        self.assertEqual({"before/*.txt", "after/*.txt"}, {entry["glob"] for entry in merged["resources"]})
        self.assertEqual(original, self.baseline.read_bytes())
        for record in committed:
            self.assertEqual(record["sha256"], workflow.sha256(self.run_dir / record["path"]))

    def test_changed_baseline_is_rejected(self):
        self.write_metadata({"reflection": [{"type": "fixture.Before"}]}, {})
        committed = workflow.collect_committed_metadata(self.source, self.run_dir)
        (self.run_dir / committed[0]["path"]).write_text("{}")
        with self.assertRaisesRegex(workflow.WorkflowError, "metadata changed"):
            workflow.merge_reachability_metadata(self.engine, self.graal_image, self.run_dir, committed)

    def test_invalid_agent_metadata_is_rejected(self):
        self.write_metadata({"reflection": [{"type": "fixture.Before"}]}, {})
        self.agent.write_text("{not valid JSON}")
        committed = workflow.collect_committed_metadata(self.source, self.run_dir)
        with self.assertRaises(workflow.WorkflowError):
            workflow.merge_reachability_metadata(self.engine, self.graal_image, self.run_dir, committed)

    def save_verified_fixture(self, directory):
        self.write_metadata({"reflection": [{"type": "fixture.OldOnly"}]},
                            {"reflection": [{"type": "fixture.NewOnly"}]})
        committed = workflow.collect_committed_metadata(self.source, self.run_dir)
        merged = workflow.merge_reachability_metadata(self.engine, self.graal_image, self.run_dir, committed)
        provenance = {"releaseVersion": "8.2.0", "schemaCommit": "a" * 40,
                      "platform": "linux/arm64", "jarSha256": "b" * 64,
                      "imageTags": {"graalvm": workflow.GRAAL_IMAGE}, "mergedMetadata": merged}
        (self.run_dir / "provenance.json").write_text(json.dumps(provenance))
        (self.run_dir / "result.json").write_text(json.dumps({"status": "passed"}))
        workflow.save_metadata(self.run_dir, directory)

    def test_saved_metadata_rebuild_without_agent_and_additive_refresh(self):
        snapshot = self.root / "saved metadata"
        self.save_verified_fixture(snapshot)
        original = (snapshot / "configuration/reachability-metadata.json").read_bytes()
        rebuild = self.root / "rebuild"
        (rebuild / "metadata").mkdir(parents=True)
        records, manifest = workflow.collect_saved_metadata(snapshot, rebuild, "8.2.0", "a" * 40, refresh=False)
        workflow.merge_reachability_metadata(self.engine, self.graal_image, rebuild, records)
        merged = json.loads((rebuild / "native/reachability/reachability-metadata.json").read_text())
        self.assertEqual({"fixture.OldOnly", "fixture.NewOnly"}, {entry["type"] for entry in merged["reflection"]})
        self.assertEqual("a" * 40, manifest["schemaCommit"])
        self.assertEqual(original, (snapshot / "configuration/reachability-metadata.json").read_bytes())
        refresh = self.root / "refresh"
        (refresh / "metadata").mkdir(parents=True)
        (refresh / "metadata/reachability-metadata.json").write_text(json.dumps({"reflection": [{"type": "fixture.Third"}]}))
        records, _ = workflow.collect_saved_metadata(snapshot, refresh, "8.2.0", "c" * 40, refresh=True)
        workflow.merge_reachability_metadata(self.engine, self.graal_image, refresh, records)
        merged = json.loads((refresh / "native/reachability/reachability-metadata.json").read_text())
        self.assertEqual({"fixture.OldOnly", "fixture.NewOnly", "fixture.Third"},
                         {entry["type"] for entry in merged["reflection"]})

    def test_snapshot_rejects_stale_missing_modified_and_unrecorded_inputs(self):
        missing = self.root / "missing"
        with self.assertRaisesRegex(workflow.WorkflowError, "missing"):
            workflow.collect_saved_metadata(missing, self.root / "copy", "8.2.0", "a" * 40, refresh=False)
        self.assertEqual(([], None), workflow.collect_saved_metadata(
            missing, self.root / "copy", "8.2.0", "a" * 40, refresh=True))
        snapshot = self.root / "snapshot"
        self.save_verified_fixture(snapshot)
        with self.assertRaisesRegex(workflow.WorkflowError, "source commit"):
            workflow.collect_saved_metadata(snapshot, self.root / "copy", "8.2.0", "c" * 40, refresh=False)
        with self.assertRaisesRegex(workflow.WorkflowError, "release"):
            workflow.collect_saved_metadata(snapshot, self.root / "copy", "8.3.2", "a" * 40, refresh=True)
        extra = snapshot / "configuration/unrecorded.json"
        extra.write_text("{}")
        with self.assertRaisesRegex(workflow.WorkflowError, "not recorded"):
            workflow.collect_saved_metadata(snapshot, self.root / "copy", "8.2.0", "a" * 40, refresh=False)
        extra.unlink()
        (snapshot / "configuration/reachability-metadata.json").write_text("{}")
        with self.assertRaisesRegex(workflow.WorkflowError, "hash mismatch"):
            workflow.collect_saved_metadata(snapshot, self.root / "copy", "8.2.0", "a" * 40, refresh=False)

    def test_failed_refresh_leaves_snapshot_unchanged(self):
        snapshot = self.root / "snapshot"
        self.save_verified_fixture(snapshot)
        original = {path.relative_to(snapshot): path.read_bytes() for path in snapshot.rglob("*") if path.is_file()}
        (self.run_dir / "result.json").write_text(json.dumps({"status": "failed"}))
        with self.assertRaisesRegex(workflow.WorkflowError, "failed or unverified"):
            workflow.save_metadata(self.run_dir, snapshot)
        self.assertEqual(original, {path.relative_to(snapshot): path.read_bytes()
                                    for path in snapshot.rglob("*") if path.is_file()})
        (self.run_dir / "result.json").write_text(json.dumps({"status": "passed"}))
        (self.run_dir / "native/reachability/reachability-metadata.json").write_text("{}")
        with self.assertRaisesRegex(workflow.WorkflowError, "metadata changed"):
            workflow.save_metadata(self.run_dir, snapshot)
        self.assertEqual(original, {path.relative_to(snapshot): path.read_bytes()
                                    for path in snapshot.rglob("*") if path.is_file()})

    def test_build_control_has_no_instrumentation_agent(self):
        jar = self.root / "application.jar"
        jar.write_bytes(b"fixture")
        tls = self.run_dir / "tls"
        tls.mkdir()
        for filename in ("server.p12", "truststore.p12"):
            (tls / filename).write_bytes(b"fixture")
        workflow.write_context(self.run_dir, jar, [], "graal", "runtime", instrument=False)
        content = (self.run_dir / "agent/Dockerfile").read_text()
        entrypoint = json.loads(content.split("ENTRYPOINT ")[1])
        self.assertIn("java", entrypoint)
        self.assertFalse(any("native-image-agent" in option for option in entrypoint))
        workflow.write_context(self.run_dir, jar, [], "graal", "runtime", instrument=True)
        content = (self.run_dir / "agent/Dockerfile").read_text()
        entrypoint = json.loads(content.split("ENTRYPOINT ")[1])
        self.assertTrue(any("native-image-agent" in option for option in entrypoint))


if __name__ == "__main__":
    unittest.main()
