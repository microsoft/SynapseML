# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

"""Prepare a verified local Maven package. This tool never publishes artifacts."""

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import struct
import subprocess
import tempfile
import urllib.request
import zipfile

GROUP = "com.microsoft.ml.lightgbm"
ARTIFACT = "lightgbmlib"
CLASS_PREFIX = "com/microsoft/ml/lightgbm/"
MAX_DOWNLOAD_BYTES = 32 * 1024 * 1024
MAX_EXPANDED_BYTES = 128 * 1024 * 1024
LOCK_FILE = Path(__file__).with_name("native-package.json")


def digest(data, algorithm="sha256"):
    return hashlib.new(algorithm, data).hexdigest()


def fetch(url, destination, expected):
    if destination.exists():
        if digest(destination.read_bytes()) != expected:
            raise ValueError(f"Checksum mismatch for cached artifact: {destination}")
        return destination
    destination.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url, timeout=60) as response:
        data = response.read(MAX_DOWNLOAD_BYTES + 1)
    if len(data) > MAX_DOWNLOAD_BYTES:
        raise ValueError(f"Download exceeds size limit: {url}")
    if digest(data) != expected:
        raise ValueError(f"Checksum mismatch for {url}")
    with tempfile.TemporaryDirectory(dir=destination.parent) as temporary:
        candidate = Path(temporary) / destination.name
        candidate.write_bytes(data)
        os.replace(candidate, destination)
    return destination


def read_jar(path, expected_resources):
    classes, resources = {}, {}
    with zipfile.ZipFile(path) as jar:
        names = jar.namelist()
        if len(names) != len(set(names)):
            raise ValueError(f"Duplicate ZIP entries in {path}")
        if sum(entry.file_size for entry in jar.infolist()) > MAX_EXPANDED_BYTES:
            raise ValueError(f"Expanded JAR exceeds size limit: {path}")
        for name in names:
            entry = PurePosixPath(name)
            if entry.is_absolute() or ".." in entry.parts or "\\" in name:
                raise ValueError(f"Unsafe ZIP entry: {name}")
            if name.endswith("/") or name == "META-INF/MANIFEST.MF":
                continue
            if name in expected_resources:
                resources[name] = jar.read(name)
            elif name.startswith(CLASS_PREFIX) and name.endswith(".class"):
                classes[name] = jar.read(name)
            else:
                raise ValueError(f"Unexpected ZIP entry: {name}")
    if set(resources) != set(expected_resources):
        raise ValueError(f"Missing matched native libraries in {path}")
    if CLASS_PREFIX + "lightgbmlibJNI.class" not in classes:
        raise ValueError(f"Missing Java/JNI classes in {path}")
    return classes, resources


def declarations(jar, classes, javap):
    names = [name[:-6].replace("/", ".") for name in sorted(classes)]
    result = subprocess.run(
        [javap, "-private", "-s", "-classpath", str(jar), *names],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    return "\n".join(
        line
        for line in result.stdout.splitlines()
        if not line.startswith("Compiled from")
    )


def merge_jars(paths, lock, javap):
    parsed = {
        platform: read_jar(path, lock["assets"][platform]["resources"])
        for platform, path in paths.items()
    }
    java_platform = lock["java_platform"]
    classes = parsed[java_platform][0]
    for name, data in classes.items():
        if len(data) < 8 or data[:4] != b"\xca\xfe\xba\xbe":
            raise ValueError(f"Invalid Java class: {name}")
        if struct.unpack(">H", data[6:8])[0] > 52:
            raise ValueError(
                f"Java wrapper requires a newer runtime than Java 8: {name}"
            )
    signature = declarations(paths[java_platform], classes, javap)
    merged = dict(classes)
    for platform, (other_classes, resources) in parsed.items():
        if set(other_classes) != set(classes):
            raise ValueError(f"Java class inventory differs on {platform}")
        if declarations(paths[platform], other_classes, javap) != signature:
            raise ValueError(f"Java/JNI declarations differ on {platform}")
        overlap = set(merged).intersection(resources)
        if overlap:
            raise ValueError(f"Overlapping native resources: {sorted(overlap)}")
        merged.update(resources)
    return merged


def write_jar(path, entries):
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_STORED) as jar:
        for name, data in sorted(entries.items()):
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.create_system = 3
            info.external_attr = 0o100644 << 16
            jar.writestr(info, data)


def pom(version, commit):
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>{GROUP}</groupId>
  <artifactId>{ARTIFACT}</artifactId>
  <version>{version}</version>
  <packaging>jar</packaging>
  <name>LightGBM Java and native libraries</name>
  <description>Matched Java/JNI and CPU native libraries from the official LightGBM release.</description>
  <url>https://github.com/lightgbm-org/LightGBM</url>
  <licenses>
    <license>
      <name>MIT License</name>
      <url>https://github.com/lightgbm-org/LightGBM/blob/{commit}/LICENSE</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>https://github.com/lightgbm-org/LightGBM</url>
    <connection>scm:git:https://github.com/lightgbm-org/LightGBM.git</connection>
    <tag>{commit}</tag>
  </scm>
  <developers>
    <developer>
      <id>lightgbm</id>
      <name>LightGBM contributors</name>
      <url>https://github.com/lightgbm-org/LightGBM/graphs/contributors</url>
    </developer>
  </developers>
</project>
""".encode(
        "utf-8"
    )


def prepare(output, cache, javap):
    lock = json.loads(LOCK_FILE.read_text(encoding="utf-8"))
    version, commit = lock["version"], lock["commit"]
    if not re.fullmatch(r"\d+\.\d+\.\d+", version) or not re.fullmatch(
        r"[a-f0-9]{40}", commit
    ):
        raise ValueError("Invalid version or commit in native-package.json")
    release = f"https://github.com/lightgbm-org/LightGBM/releases/download/v{version}"
    commit_file = fetch(
        f"{release}/commit.txt", cache / "commit.txt", lock["commit_file_sha256"]
    )
    if commit_file.read_text().strip() != commit:
        raise ValueError("Release commit does not match the pinned source revision")
    license_file = fetch(
        f"https://raw.githubusercontent.com/lightgbm-org/LightGBM/{commit}/LICENSE",
        cache / "LICENSE",
        lock["license_sha256"],
    )
    paths = {
        platform: fetch(
            f"{release}/lightgbmlib_{platform}.jar",
            cache / f"lightgbmlib_{platform}.jar",
            asset["sha256"],
        )
        for platform, asset in lock["assets"].items()
    }
    entries = merge_jars(paths, lock, javap)
    provenance = {
        **lock,
        "release_url": f"https://github.com/lightgbm-org/LightGBM/releases/tag/v{version}",
        "packaging_script_sha256": digest(
            Path(__file__).read_text(encoding="utf-8").encode("utf-8")
        ),
        "entries_sha256": {
            name: digest(data) for name, data in sorted(entries.items())
        },
    }
    entries["META-INF/lightgbm-build.json"] = (
        json.dumps(provenance, indent=2, sort_keys=True) + "\n"
    ).encode()
    entries["META-INF/LICENSE.LightGBM"] = license_file.read_bytes()
    entries["META-INF/MANIFEST.MF"] = (
        f"Manifest-Version: 1.0\r\nImplementation-Version: {version}\r\n"
        f"LightGBM-Commit: {commit}\r\n\r\n"
    ).encode()
    pom_bytes = pom(version, commit)
    metadata = f"META-INF/maven/{GROUP}/{ARTIFACT}"
    entries[f"{metadata}/pom.xml"] = pom_bytes
    entries[f"{metadata}/pom.properties"] = (
        f"groupId={GROUP}\nartifactId={ARTIFACT}\nversion={version}\n"
    ).encode()
    destination = output / Path(*GROUP.split(".")) / ARTIFACT / version
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=destination.parent) as temporary:
        stage = Path(temporary) / version
        stage.mkdir()
        jar = stage / f"{ARTIFACT}-{version}.jar"
        write_jar(jar, entries)
        (stage / f"{ARTIFACT}-{version}.pom").write_bytes(pom_bytes)
        for path in list(stage.iterdir()):
            data = path.read_bytes()
            for algorithm in ("sha1", "sha256", "sha512"):
                path.with_suffix(path.suffix + "." + algorithm).write_bytes(
                    (digest(data, algorithm) + "\n").encode("ascii")
                )
        if destination.exists():
            existing = {p.name: p.read_bytes() for p in destination.iterdir()}
            prepared = {p.name: p.read_bytes() for p in stage.iterdir()}
            if existing != prepared:
                raise ValueError(
                    f"Refusing to replace different artifacts: {destination}"
                )
        else:
            stage.rename(destination)
    return destination


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path, required=True, help="Local Maven repository"
    )
    parser.add_argument(
        "--cache", type=Path, required=True, help="Verified download cache"
    )
    parser.add_argument(
        "--javap", default="javap", help="Path to the JDK javap executable"
    )
    args = parser.parse_args()
    javap = shutil.which(args.javap)
    if javap is None:
        parser.error("javap is required; select a JDK or pass --javap")
    result = prepare(args.output.resolve(), args.cache.resolve(), javap)
    print(f"Prepared local package: {result}")
    print(
        "Not published. Public Maven publication and runtime qualification are separate steps."
    )


if __name__ == "__main__":
    main()
