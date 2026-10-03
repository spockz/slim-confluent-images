# Reject incomplete or skipped external test runs so a nominal container exit cannot approve a native build.
from pathlib import Path
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import run as workflow


class UpstreamReportTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.report = Path(temporary.name) / "results.xml"

    def write_report(self, names, status=None):
        root = ET.Element("testsuites")
        suite = ET.SubElement(root, "testsuite")
        for name in names:
            case = ET.SubElement(suite, "testcase", classname="external", name=name)
            if status:
                ET.SubElement(case, status)
        ET.ElementTree(root).write(self.report)

    def test_complete_run_retains_case_identity(self):
        self.write_report(["second", "first"])
        self.assertEqual(["external::first", "external::second"], workflow.validate_upstream_report(self.report, 2))

    def test_incomplete_duplicate_and_skipped_runs_are_rejected(self):
        for names, status in [([], None), (["one"], None), (["one", "one"], None),
                              (["one", "two"], "skipped"), (["one", "two"], "failure"),
                              (["one", "two"], "error")]:
            with self.subTest(names=names, status=status):
                self.write_report(names, status)
                with self.assertRaises(workflow.WorkflowError):
                    workflow.validate_upstream_report(self.report, 2)

    def test_missing_and_malformed_reports_are_rejected(self):
        with self.assertRaises(workflow.WorkflowError):
            workflow.validate_upstream_report(self.report, 2)
        self.report.write_text("not XML")
        with self.assertRaises(workflow.WorkflowError):
            workflow.validate_upstream_report(self.report, 2)

    def test_compatibility_adaptation_changes_only_the_verified_error_code(self):
        source = ("# Preserve attribution and comments.\n"
                  "def test_api_register_schema_incompatible():\n"
                  "    assert e.value.http_status_code == 409\n"
                  "    assert e.value.error_code == 409\n"
                  "def unrelated():\n"
                  "    assert value == 409\n")
        self.assertEqual(source.replace("e.value.error_code == 409", "e.value.error_code == 40901"),
                         workflow.adapt_upstream_assertion(source, 40901))
        for altered in (source.replace("error_code == 409", "error_code == 400"),
                        source.replace("    assert e.value.error_code == 409\n", ""),
                        source.replace("    assert e.value.error_code == 409\n",
                                       "    assert e.value.error_code == 409\n" * 2)):
            with self.subTest(source=altered):
                with self.assertRaises(workflow.WorkflowError):
                    workflow.adapt_upstream_assertion(altered, 40901)
