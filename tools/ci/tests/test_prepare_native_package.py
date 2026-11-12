# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import importlib.util
import json
from pathlib import Path
import struct
import sys
import zipfile

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "lightgbm" / "prepare_native_package.py"
SPEC = importlib.util.spec_from_file_location("prepare_native_package", SCRIPT)
PACKAGE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PACKAGE)
JNI = PACKAGE.CLASS_PREFIX + "lightgbmlibJNI.class"
CLASS_BYTES = b"\xca\xfe\xba\xbe" + struct.pack(">HH", 0, 52)
LIBRARIES = ["test/lib_lightgbm.so", "test/lib_lightgbm_swig.so"]


def jar_at(path, classes=None, libraries=None):
    PACKAGE.write_jar(
        path,
        {
            **({JNI: CLASS_BYTES} if classes is None else classes),
            **(
                {name: b"native" for name in LIBRARIES}
                if libraries is None
                else libraries
            ),
        },
    )
    return path


def test_cached_download_requires_exact_hash(tmp_path):
    path = tmp_path / "cached.jar"
    path.write_bytes(b"artifact")
    assert PACKAGE.fetch("unused", path, PACKAGE.digest(b"artifact")) == path
    with pytest.raises(ValueError, match="Checksum mismatch"):
        PACKAGE.fetch("unused", path, "0" * 64)
    assert path.read_bytes() == b"artifact"


def test_failed_download_does_not_leave_artifact(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.write_bytes(b"wrong")
    path = tmp_path / "download.jar"
    with pytest.raises(ValueError, match="Checksum mismatch"):
        PACKAGE.fetch(source.as_uri(), path, "0" * 64)
    assert not path.exists()
    monkeypatch.setattr(PACKAGE, "MAX_DOWNLOAD_BYTES", 1)
    with pytest.raises(ValueError, match="size limit"):
        PACKAGE.fetch(source.as_uri(), path, PACKAGE.digest(b"wrong"))
    assert not path.exists()


def test_jar_is_reproducible_and_contains_exact_bytes(tmp_path):
    first = jar_at(tmp_path / "first.jar")
    second = jar_at(tmp_path / "second.jar")
    assert first.read_bytes() == second.read_bytes()
    classes, resources = PACKAGE.read_jar(first, LIBRARIES)
    assert classes == {JNI: CLASS_BYTES}
    assert resources == {name: b"native" for name in LIBRARIES}


@pytest.mark.parametrize("missing", LIBRARIES)
def test_rejects_incomplete_native_pair(tmp_path, missing):
    libraries = {name: b"native" for name in LIBRARIES if name != missing}
    with pytest.raises(ValueError, match="Missing matched native"):
        PACKAGE.read_jar(jar_at(tmp_path / "test.jar", libraries=libraries), LIBRARIES)


@pytest.mark.parametrize("name", ["../bad", "/bad", "test\\bad", "unexpected.txt"])
def test_rejects_unexpected_entries(tmp_path, name):
    path = jar_at(tmp_path / "test.jar")
    with zipfile.ZipFile(path, "a") as jar:
        jar.writestr(name, b"unexpected")
    with pytest.raises(ValueError, match="ZIP entry"):
        PACKAGE.read_jar(path, LIBRARIES)


def test_rejects_duplicates_and_oversized_jars(tmp_path, monkeypatch):
    path = jar_at(tmp_path / "test.jar")
    monkeypatch.setattr(PACKAGE, "MAX_EXPANDED_BYTES", 1)
    with pytest.raises(ValueError, match="Expanded JAR"):
        PACKAGE.read_jar(path, LIBRARIES)
    with zipfile.ZipFile(path, "a") as jar:
        with pytest.warns(UserWarning, match="Duplicate"):
            jar.writestr(JNI, CLASS_BYTES)
    with pytest.raises(ValueError, match="Duplicate"):
        PACKAGE.read_jar(path, LIBRARIES)


def fake_platforms(tmp_path, monkeypatch, mac_version=65):
    paths = {}
    assets = {}
    for platform, version in [("linux", 52), ("macos", mac_version)]:
        resources = [f"{platform}/{name}" for name in LIBRARIES]
        paths[platform] = jar_at(
            tmp_path / f"{platform}.jar",
            classes={JNI: CLASS_BYTES[:6] + struct.pack(">H", version)},
            libraries={name: platform.encode() for name in resources},
        )
        assets[platform] = {"resources": resources}
    monkeypatch.setattr(PACKAGE, "declarations", lambda *args: "same API")
    return paths, {"java_platform": "linux", "assets": assets}


def test_uses_java8_wrappers_and_keeps_each_platform_pair(tmp_path, monkeypatch):
    paths, lock = fake_platforms(tmp_path, monkeypatch)
    entries = PACKAGE.merge_jars(paths, lock, "unused")
    assert entries[JNI] == CLASS_BYTES
    assert len(entries) == 5
    assert entries["macos/" + LIBRARIES[0]] == b"macos"


def test_rejects_incompatible_platform_declarations(tmp_path, monkeypatch):
    paths, lock = fake_platforms(tmp_path, monkeypatch)
    monkeypatch.setattr(PACKAGE, "declarations", lambda path, *args: path.stem)
    with pytest.raises(ValueError, match="declarations differ"):
        PACKAGE.merge_jars(paths, lock, "unused")


def test_rejects_java21_as_common_wrapper(tmp_path, monkeypatch):
    paths, lock = fake_platforms(tmp_path, monkeypatch)
    lock["java_platform"] = "macos"
    with pytest.raises(ValueError, match="newer runtime"):
        PACKAGE.merge_jars(paths, lock, "unused")


def test_prepare_is_idempotent_and_rejects_overwrite(tmp_path, monkeypatch):
    commit = "a" * 40
    license_bytes = b"test license"
    source = tmp_path / "source"
    source.mkdir()
    paths, lock = fake_platforms(source, monkeypatch)
    lock.update(
        version="4.7.0",
        commit=commit,
        commit_file_sha256=PACKAGE.digest((commit + "\n").encode()),
        license_sha256=PACKAGE.digest(license_bytes),
    )
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / "commit.txt").write_bytes((commit + "\n").encode())
    (cache / "LICENSE").write_bytes(license_bytes)
    for platform, path in paths.items():
        lock["assets"][platform]["sha256"] = PACKAGE.digest(path.read_bytes())
        (cache / f"lightgbmlib_{platform}.jar").write_bytes(path.read_bytes())
    lock_file = tmp_path / "lock.json"
    lock_file.write_text(json.dumps(lock))
    monkeypatch.setattr(PACKAGE, "LOCK_FILE", lock_file)
    script_copy = tmp_path / "prepare_native_package.py"
    script_copy.write_bytes(b"# packaging script\n# second line\n")
    monkeypatch.setattr(PACKAGE, "__file__", str(script_copy))
    output = tmp_path / "repository"
    destination = PACKAGE.prepare(output, cache, sys.executable)
    script_copy.write_bytes(b"# packaging script\r\n# second line\r\n")
    assert PACKAGE.prepare(output, cache, sys.executable) == destination
    jar = destination / "lightgbmlib-4.7.0.jar"
    with zipfile.ZipFile(jar) as archive:
        provenance = json.loads(archive.read("META-INF/lightgbm-build.json"))
        assert provenance["commit"] == commit
        assert provenance["packaging_script_sha256"] == PACKAGE.digest(
            b"# packaging script\n# second line\n"
        )
        assert provenance["entries_sha256"][JNI] == PACKAGE.digest(CLASS_BYTES)
        assert archive.read("META-INF/LICENSE.LightGBM") == license_bytes
    for algorithm in ("sha1", "sha256", "sha512"):
        checksum = jar.with_suffix(".jar." + algorithm).read_text().strip()
        assert checksum == PACKAGE.digest(jar.read_bytes(), algorithm)
    jar.write_bytes(b"existing publication")
    with pytest.raises(ValueError, match="Refusing to replace"):
        PACKAGE.prepare(output, cache, sys.executable)
    assert jar.read_bytes() == b"existing publication"
