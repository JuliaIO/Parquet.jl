#!/usr/bin/env python3
import datetime
import decimal
import hashlib
import os
import pathlib
import platform
import stat
import subprocess
import sys
import tempfile
import textwrap
import types
import unittest
import zipfile

SCRIPT_DIRECTORY = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIRECTORY))
import common


class CommonHarnessTests(unittest.TestCase):
    def assert_harness_error(self, function, *arguments):
        with self.assertRaises(common.HarnessError):
            function(*arguments)

    def test_safe_relative_paths(self):
        self.assertTrue(common.safe_relative("data/a-b_1.parquet"))
        for value in ("", "/tmp/file", "../file", "data//file",
                "data/./file", "data/../file", "data\\file", "data/é"):
            self.assertFalse(common.safe_relative(value), value)

    def test_toml_snapshot_binds_values_and_digest_to_one_read(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "control.toml"
            original = b'version = "one"\n'
            path.write_bytes(original)
            value, source, digest = common.load_toml_snapshot(path)
            path.write_bytes(b'version = "two"\n')
            self.assertEqual(value, {"version": "one"})
            self.assertEqual(source, original)
            self.assertEqual(digest, hashlib.sha256(original).hexdigest())

    def test_decimal_values_are_exact(self):
        self.assertEqual(common.decimal_unscaled(decimal.Decimal("1.230"), 2),
            "123")
        self.assertEqual(common.decimal_unscaled(decimal.Decimal("-1.23"), 2),
            "-123")
        value = decimal.Decimal("12345678901234567890123456789012345678")
        self.assertEqual(common.decimal_unscaled(value, 0), str(value))
        self.assert_harness_error(common.decimal_unscaled,
            decimal.Decimal("0.001"), 2)
        self.assert_harness_error(common.decimal_unscaled,
            decimal.Decimal("Infinity"), 2)

    def test_timestamps_use_utc_instants(self):
        offset = datetime.timezone(datetime.timedelta(hours=1))
        value = datetime.datetime(1970, 1, 1, 1, tzinfo=offset)
        self.assertEqual(common.epoch_nanoseconds(value), 0)
        value = datetime.datetime(1970, 1, 1, 0, 0, 0, 1)
        self.assertEqual(common.epoch_nanoseconds(value), 1000)

    def test_checked_fixture_rejects_links_and_identity_changes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            fixture = root / "fixture.parquet"
            fixture.write_bytes(b"PAR1testPAR1")
            digest = hashlib.sha256(fixture.read_bytes()).hexdigest()
            snapshots = root / "snapshots"
            snapshots.mkdir()
            snapshot = common.checked_file(root, "fixture.parquet", digest,
                fixture.stat().st_size, snapshots)
            self.assertEqual(snapshot.read_bytes(), fixture.read_bytes())
            self.assertEqual(stat.S_IMODE(snapshot.stat().st_mode), 0o600)
            fixture.write_bytes(b"PAR1new!PAR1")
            self.assertNotEqual(snapshot.read_bytes(), fixture.read_bytes())
            fixture.write_bytes(b"PAR1testPAR1")
            self.assert_harness_error(common.checked_file, root,
                "fixture.parquet", "0" * 64, fixture.stat().st_size,
                snapshots)
            link = root / "linked.parquet"
            link.symlink_to(fixture)
            self.assert_harness_error(common.checked_file, root,
                "linked.parquet", digest, fixture.stat().st_size, snapshots)
            root_link = root.parent / f"{root.name}-link"
            root_link.symlink_to(root, target_is_directory=True)
            try:
                self.assert_harness_error(common.checked_file, root_link,
                    "fixture.parquet", digest, fixture.stat().st_size,
                    snapshots)
            finally:
                root_link.unlink()

    def test_raw_evidence_requires_canonical_bounded_jsonl(self):
        run = {
            "producer": "n6-raw-java",
            "record": "run",
            "schema_version": 2,
        }
        file_record = {"case_id": "case", "record": "file"}
        column = {
            "case_id": "case",
            "leaf": 0,
            "record": "column_statistics",
            "row_group": 0,
        }
        result = {"case_id": "case", "record": "case_result"}
        records = [run, file_record, column, result]
        value = common.evidence_bytes(records, 4096, 4096, 8)
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "raw.jsonl"
            path.write_bytes(value)
            loaded_run, files, columns, digest = common.load_raw_facts(path,
                4096, 4096, 8)
            self.assertEqual(loaded_run, run)
            self.assertEqual(files["case"], file_record)
            self.assertEqual(columns[("case", 0, 0)], column)
            self.assertEqual(digest, hashlib.sha256(value).hexdigest())
            path.write_bytes(value.replace(b'"producer":', b'"producer" :', 1))
            self.assert_harness_error(common.load_raw_facts, path, 4096, 4096,
                8)
            path.write_bytes(b"[]\n")
            self.assert_harness_error(common.load_raw_facts, path, 4096, 4096,
                8)
            path.write_bytes(b'{"record":"unknown"}\n')
            self.assert_harness_error(common.load_raw_facts, path, 4096, 4096,
                8)
            path.write_bytes(b'{"record":"run","record":"run"}\n')
            self.assert_harness_error(common.load_raw_facts, path, 4096, 4096,
                8)
            path.write_bytes(common.canonical_json(run).encode("utf-8"))
            self.assert_harness_error(common.load_raw_facts, path, 4096, 4096,
                8)
            path.write_bytes(value)
            self.assert_harness_error(common.load_raw_facts, path,
                len(value) - 1, 4096, 8)
            self.assert_harness_error(common.load_raw_facts, path, 4096,
                len(value.splitlines()[0]), 8)
            self.assert_harness_error(common.load_raw_facts, path, 4096, 4096,
                3)

    def test_atomic_output_is_bounded_and_link_safe(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            output = root / "nested" / "evidence.jsonl"
            value = b'{"record":"run"}\n'
            common.atomic_output(output, value, False)
            self.assertEqual(output.read_bytes(), value)
            self.assertEqual(stat.S_IMODE(output.stat().st_mode), 0o644)
            common.atomic_output(output, value, True)
            output.write_bytes(value + b"stale")
            self.assert_harness_error(common.atomic_output, output, value, True)
            missing = root / "missing" / "evidence.jsonl"
            self.assert_harness_error(common.atomic_output, missing, value, True)
            self.assertFalse(missing.parent.exists())
            target = root / "target"
            target.mkdir()
            link = root / "parent-link"
            link.symlink_to(target, target_is_directory=True)
            linked_output = link / "evidence.jsonl"
            common.atomic_output(linked_output, value, False)
            self.assertEqual((target / "evidence.jsonl").read_bytes(), value)
            destination_link = root / "destination-link"
            destination_link.symlink_to(output)
            self.assert_harness_error(common.atomic_output, destination_link,
                value, False)

    def test_evidence_bindings_require_exact_upstream_and_digest(self):
        digest = "1" * 64
        raw = {
            "authority": "n6-raw-java",
            "format": "normalized-jsonl",
            "id": "normalized-raw-java-apache-corpus",
            "sha256": digest,
            "status": "verified",
        }
        target = {
            "authority": "pyarrow",
            "format": "normalized-jsonl",
            "id": "normalized-pyarrow",
            "status": "verified",
            "upstream_evidence": ["normalized-raw-java-apache-corpus"],
        }
        manifest = {"frozen_evidence": [raw, target]}
        self.assertEqual(common._verify_evidence_bindings(manifest,
            "pyarrow", "normalized-pyarrow", raw, digest, False), target)
        raw["sha256"] = "2" * 64
        self.assert_harness_error(common._verify_evidence_bindings, manifest,
            "pyarrow", "normalized-pyarrow", raw, digest, False)
        raw["sha256"] = digest
        target["upstream_evidence"] = []
        self.assert_harness_error(common._verify_evidence_bindings, manifest,
            "pyarrow", "normalized-pyarrow", raw, digest, False)
        target["upstream_evidence"] = [
            "normalized-raw-java-apache-corpus"]
        raw.pop("sha256")
        raw["status"] = "planned"
        target["status"] = "planned"
        manifest = {"planned_evidence": [raw, target]}
        common._verify_evidence_bindings(manifest, "pyarrow",
            "normalized-pyarrow", raw, digest, True)
        self.assert_harness_error(common._verify_evidence_bindings, manifest,
            "pyarrow", "normalized-pyarrow", raw, digest, False)

    def test_repository_inputs_reject_links_and_escapes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            source = root / "input.toml"
            source.write_text("version = 1\n")
            self.assertEqual(common.repository_input(root, "input.toml"),
                source.resolve())
            link = root / "link.toml"
            link.symlink_to(source)
            self.assert_harness_error(common.repository_input, root,
                "link.toml")
            self.assert_harness_error(common.repository_input, root,
                "../input.toml")

    def test_descriptor_sources_are_exact(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            harness = root / "harness.py"
            support = root / "common.py"
            test = root / "test_common.py"
            for path, value in ((harness, b"harness\n"),
                    (support, b"support\n"), (test, b"test\n")):
                path.write_bytes(value)
            descriptor = {
                "harness_file": harness.name,
                "harness_sha256": common.sha256_file(harness),
                "support_files": [{
                    "file": support.name,
                    "sha256": common.sha256_file(support),
                }],
                "test_file": test.name,
                "test_sha256": common.sha256_file(test),
            }
            paths = common.descriptor_repository_inputs(root, descriptor)
            self.assertEqual(set(paths), {path.resolve()
                for path in (harness, support, test)})
            descriptor["support_files"][0]["sha256"] = "0" * 64
            self.assert_harness_error(common.descriptor_repository_inputs,
                root, descriptor)

    def test_descriptor_source_override_authenticates_executed_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            harness = root / "harness.py"
            support = root / "common.py"
            test = root / "test_common.py"
            snapshot = root / "snapshot.py"
            for path, value in ((harness, b"old harness\n"),
                    (snapshot, b"old harness\n"), (support, b"support\n"),
                    (test, b"test\n")):
                path.write_bytes(value)
            descriptor = {
                "harness_file": harness.name,
                "harness_sha256": common.sha256_file(harness),
                "support_files": [{
                    "file": support.name,
                    "sha256": common.sha256_file(support),
                }],
                "test_file": test.name,
                "test_sha256": common.sha256_file(test),
            }
            harness.write_bytes(b"changed canonical harness\n")
            paths = common.descriptor_repository_inputs(root, descriptor,
                {harness.name: snapshot})
            self.assertIn(snapshot.resolve(), paths)
            self.assertNotIn(harness.resolve(), paths)

    def test_python_descriptor_requires_distribution_contract(self):
        descriptor = {
            "authority": "tool",
            "harness_file": "harness.py",
            "harness_sha256": "0" * 64,
            "harness_status": "planned",
            "platform": "macos-15-arm64",
            "python_distribution_sha256": "0" * 64,
            "python_distribution_url": "https://example.invalid/python.tar.gz",
            "python_executable_sha256": "0" * 64,
            "python_tree_policy": "extract-strip-site-packages-bytecode-v1",
            "python_tree_sha256": "0" * 64,
            "python_version": "3.12.8",
            "status": "planned",
            "support_files": [],
            "test_file": "test.py",
            "test_sha256": "0" * 64,
            "wheels": [],
        }
        for field in ("python_distribution_url",
                "python_distribution_sha256", "python_tree_policy"):
            incomplete = dict(descriptor)
            incomplete.pop(field)
            self.assert_harness_error(common.verify_python_descriptor,
                incomplete, ".", "tool")

    def test_output_cannot_alias_inputs_or_corpus(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            source = root / "source.jsonl"
            source.write_text("{}\n")
            corpus = root / "corpus"
            corpus.mkdir()
            self.assert_harness_error(common.reject_output_alias, source,
                [source], [corpus])
            self.assert_harness_error(common.reject_output_alias,
                corpus / "evidence.jsonl", [source], [corpus])
            common.reject_output_alias(root / "evidence.jsonl", [source],
                [corpus])
            alias = root / "hardlink.jsonl"
            alias.hardlink_to(source)
            self.assert_harness_error(common.reject_output_alias, alias,
                [source], [corpus])

    def test_output_must_equal_manifest_target(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory).resolve()
            parent = root / "test" / "conformance" / "n6" / "evidence"
            parent.mkdir(parents=True)
            target = {
                "file": "test/conformance/n6/evidence/tool.normalized.jsonl",
            }
            expected = parent / "tool.normalized.jsonl"
            self.assertEqual(common.manifest_output_path(root, target,
                expected), expected)
            self.assert_harness_error(common.manifest_output_path, root,
                target, root / "other.jsonl")
            alias = root / "evidence-link"
            alias.symlink_to(parent, target_is_directory=True)
            self.assert_harness_error(common.manifest_output_path, root,
                target, alias / expected.name)

    def test_evidence_bytes_enforces_limits(self):
        record = {"record": "run", "schema_version": 2}
        value = common.evidence_bytes([record], 128, 128, 1)
        self.assertTrue(value.endswith(b"\n"))
        self.assert_harness_error(common.evidence_bytes, [], 128, 128, 1)
        self.assert_harness_error(common.evidence_bytes, [record, record],
            256, 128, 1)
        self.assert_harness_error(common.evidence_bytes, [record], 128, 8, 1)
        self.assert_harness_error(common.evidence_bytes, [record], 8, 128, 1)

    def test_wheel_import_root_requires_exact_safe_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            wheel = root / "demo-1.0-cp312-cp312-macosx_12_0_arm64.whl"
            with zipfile.ZipFile(wheel, "w", zipfile.ZIP_DEFLATED) as archive:
                archive.writestr("demo/__init__.py", "version = '1.0'\n")
                archive.writestr("demo-1.0.dist-info/RECORD", "")
                archive.writestr("demo-1.0.dist-info/WHEEL", "Wheel-Version: 1.0\n")
            descriptor = {"wheels": [{
                "name": wheel.name,
                "sha256": common.sha256_file(wheel),
            }]}
            with common.wheel_import_root(wheel, descriptor) as extracted:
                self.assertEqual((extracted / "demo" / "__init__.py").read_text(),
                    "version = '1.0'\n")
            wheel.write_bytes(wheel.read_bytes() + b"changed")
            with self.assertRaises(common.HarnessError):
                with common.wheel_import_root(wheel, descriptor):
                    pass

            unsafe = root / "unsafe.whl"
            with zipfile.ZipFile(unsafe, "w") as archive:
                archive.writestr("../escape", "no")
                archive.writestr("unsafe.dist-info/RECORD", "")
                archive.writestr("unsafe.dist-info/WHEEL", "Wheel-Version: 1.0\n")
            unsafe_descriptor = {"wheels": [{
                "name": unsafe.name,
                "sha256": common.sha256_file(unsafe),
            }]}
            with self.assertRaises(common.HarnessError):
                with common.wheel_import_root(unsafe, unsafe_descriptor):
                    pass

    def test_exact_wheel_module_owns_origins_and_cleans_all_modules(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            wheel = root / "duckdb-1.0-py3-none-any.whl"
            with zipfile.ZipFile(wheel, "w", zipfile.ZIP_DEFLATED) as archive:
                archive.writestr("duckdb/__init__.py",
                    "import _duckdb\n__version__ = '1.0'\n")
                archive.writestr("_duckdb.py", "value = 1\n")
                archive.writestr("duckdb-1.0.dist-info/RECORD", "")
                archive.writestr("duckdb-1.0.dist-info/WHEEL",
                    "Wheel-Version: 1.0\n")
            descriptor = {"wheels": [{
                "name": wheel.name,
                "sha256": common.sha256_file(wheel),
            }]}
            with common.exact_wheel_module(wheel, descriptor, "duckdb",
                    "1.0") as module:
                self.assertEqual(module.__version__, "1.0")
                self.assertIn("_duckdb", sys.modules)
            self.assertNotIn("duckdb", sys.modules)
            self.assertNotIn("_duckdb", sys.modules)
            with self.assertRaises(common.HarnessError):
                with common.exact_wheel_module(wheel, descriptor, "duckdb",
                        "0.0"):
                    pass
            self.assertNotIn("duckdb", sys.modules)
            self.assertNotIn("_duckdb", sys.modules)
            for name in ("duckdb", "_duckdb"):
                sys.modules[name] = types.ModuleType(name)
                try:
                    with self.assertRaises(common.HarnessError):
                        with common.exact_wheel_module(wheel, descriptor,
                                "duckdb", "1.0"):
                            pass
                    self.assertIsInstance(sys.modules[name], types.ModuleType)
                finally:
                    del sys.modules[name]
            with self.assertRaises(common.HarnessError):
                with common.exact_wheel_module(wheel, descriptor, "duckdb",
                        "1.0"):
                    sys.modules["duckdb.injected"] = types.ModuleType(
                        "duckdb.injected")
            self.assertNotIn("duckdb", sys.modules)
            self.assertNotIn("duckdb.injected", sys.modules)
            self.assertNotIn("_duckdb", sys.modules)

    def test_wheel_owned_modules_include_native_extension_names(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            (root / "duckdb").mkdir()
            (root / "duckdb" / "__init__.py").write_text("")
            (root / "_duckdb.cpython-312-darwin.so").write_bytes(b"")
            (root / "duckdb.libs").mkdir()
            self.assertEqual(common._wheel_owned_modules(root),
                {"duckdb", "_duckdb"})

    def test_exact_wheel_module_rejects_an_outside_owned_origin(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            outside = root / "outside"
            outside.mkdir()
            (outside / "_demo.py").write_text("value = 1\n")
            wheel = root / "demo-1.0-py3-none-any.whl"
            with zipfile.ZipFile(wheel, "w", zipfile.ZIP_DEFLATED) as archive:
                archive.writestr("demo/__init__.py",
                    "import sys\nsys.path.pop(0)\nimport _demo\n"
                    "__version__ = '1.0'\n")
                archive.writestr("_demo.py", "value = 2\n")
                archive.writestr("demo-1.0.dist-info/RECORD", "")
                archive.writestr("demo-1.0.dist-info/WHEEL",
                    "Wheel-Version: 1.0\n")
            descriptor = {"wheels": [{
                "name": wheel.name,
                "sha256": common.sha256_file(wheel),
            }]}
            sys.path.insert(0, str(outside))
            try:
                with self.assertRaises(common.HarnessError):
                    with common.exact_wheel_module(wheel, descriptor, "demo",
                            "1.0"):
                        pass
            finally:
                sys.path.remove(str(outside))
            self.assertNotIn("demo", sys.modules)
            self.assertNotIn("_demo", sys.modules)

    def test_python_runtime_matches_descriptor(self):
        macos = platform.mac_ver()[0].split(".", 1)[0]
        descriptor = {
            "platform": f"macos-{macos}-{platform.machine()}",
            "python_distribution_sha256": "1" * 64,
            "python_distribution_url":
                "https://github.com/astral-sh/python-build-standalone/"
                "releases/download/example/python.tar.gz",
            "python_executable_sha256": common.sha256_file(
                pathlib.Path(sys.executable).resolve(strict=True)),
            "python_tree_sha256": common.tree_sha256(sys.base_prefix),
            "python_tree_policy": "extract-strip-site-packages-bytecode-v1",
            "python_version": ".".join(str(value)
                for value in sys.version_info[:3]),
        }
        if common.runtime_tree_policy_violations(sys.base_prefix):
            self.assert_harness_error(common.verify_python_runtime, descriptor)
        else:
            common.verify_python_runtime(descriptor)
        descriptor["python_version"] = "0.0.0"
        self.assert_harness_error(common.verify_python_runtime, descriptor)

    def test_python_runtime_policy_rejects_generated_and_installed_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            library = root / "lib" / "python3.12"
            library.mkdir(parents=True)
            (library / "module.py").write_text("value = 1\n")
            self.assertEqual(common.runtime_tree_policy_violations(root), [])
            site = library / "site-packages"
            site.mkdir()
            self.assertTrue(common.runtime_tree_policy_violations(root))
            site.rmdir()
            cache = library / "__pycache__"
            cache.mkdir()
            self.assertTrue(common.runtime_tree_policy_violations(root))
            cache.rmdir()
            bytecode = library / "module.pyc"
            bytecode.write_bytes(b"generated")
            self.assertTrue(common.runtime_tree_policy_violations(root))

    def test_python_runtime_rejects_missing_isolation_flags(self):
        path = pathlib.Path(common.__file__).resolve()
        version = ".".join(str(value) for value in sys.version_info[:3])
        template = textwrap.dedent("""
            import importlib.util
            import pathlib
            import sys
            specification = importlib.util.spec_from_file_location(
                "tested_common", pathlib.Path({path!r}))
            module = importlib.util.module_from_spec(specification)
            specification.loader.exec_module(module)
            {mutation}
            try:
                module.verify_python_runtime({{"python_version": {version!r}}})
            except module.HarnessError as error:
                if {message!r} not in str(error):
                    raise
            else:
                raise SystemExit("runtime flag was accepted")
        """)
        checks = (
            (("-I", "-B", "-S"), "sys.dont_write_bytecode = False",
                "must use -B"),
            (("-B", "-S"), "", "must use -I"),
            (("-I", "-B"), "", "must use -S"),
        )
        for flags, mutation, message in checks:
            script = template.format(path=str(path), mutation=mutation,
                version=version, message=message)
            result = subprocess.run([sys.executable, *flags, "-c", script],
                check=False, capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0,
                result.stdout + result.stderr)

    def test_harness_authenticates_read_only_sources_before_common_import(self):
        source_root = pathlib.Path(common.__file__).resolve().parent
        environment = os.environ.copy()
        environment.pop("PARQUET_N6_AUTHENTICATED_SNAPSHOT", None)
        for harness_name in ("pyarrow.py", "duckdb.py"):
            with self.subTest(harness=harness_name), \
                    tempfile.TemporaryDirectory() as directory:
                root = pathlib.Path(directory).resolve()
                harness = root / harness_name
                support = root / "common.py"
                harness.write_bytes((source_root / harness_name).read_bytes())
                support.write_bytes(pathlib.Path(common.__file__).read_bytes())
                descriptor = root / "descriptor.toml"
                descriptor.write_text(
                    f'harness_file = "{harness_name}"\n'
                    f'harness_sha256 = "{common.sha256_file(harness)}"\n'
                    'support_files = [{ file = "common.py", sha256 = '
                    f'"{common.sha256_file(support)}" }}]\n')
                command = [sys.executable, "-I", "-B", "-S", str(harness),
                    "--repository", str(root), "--descriptor",
                    str(descriptor), "--help"]
                result = subprocess.run(command, check=False,
                    capture_output=True, text=True, timeout=30,
                    env=environment)
                self.assertEqual(result.returncode, 0,
                    result.stdout + result.stderr)
                self.assertIn("usage:", result.stdout)
                marker = root / "common-executed"
                with support.open("a") as stream:
                    stream.write(f"\npathlib.Path({str(marker)!r}).write_text('bad')\n")
                result = subprocess.run(command, check=False,
                    capture_output=True, text=True, timeout=30,
                    env=environment)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(marker.exists())


if __name__ == "__main__":
    unittest.main()
