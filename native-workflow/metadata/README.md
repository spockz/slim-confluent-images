<!-- Keep reviewed reachability snapshots beside the build runner so release builds can reuse them without instrumentation. -->
# Saved reachability metadata

Each release directory contains the additive configuration and its provenance manifest. Build mode checks file hashes, the source commit, and the GraalVM version before use. Refresh mode merges existing registrations with source metadata and fresh agent observations, then replaces the snapshot only after the functional workflow passes.

These registrations are shared by Linux AMD64 and ARM64 builds. `collectedPlatform` records the platform of the latest refresh; it does not claim validation on other platforms. Each image build runs target-specific functional tests. The initial snapshots were exported from successful Linux ARM64 additive builds on 2026-10-01, retaining all historical 8.2.0 project metadata.

Use `python3 native-workflow/build_images.py --metadata-mode refresh` to update candidates. Review and commit the changed configuration and manifest together. CI refresh runs upload candidates without publishing images or committing repository changes.
