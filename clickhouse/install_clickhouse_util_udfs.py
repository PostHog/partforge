#!/usr/bin/env python3
import argparse
import json
import os
import pwd
import shutil
import sys
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import yaml


def normalize_arch(arch: str) -> str:
    if arch in ("amd64", "arm64"):
        return arch
    if arch == "x86_64":
        return "amd64"
    if arch == "aarch64":
        return "arm64"
    raise ValueError(f"unsupported architecture: {arch}")


def require_str(mapping: dict[str, Any], key: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"missing string field: {key}")
    return value


def url_for(base_url: str, name: str) -> str:
    return f"{base_url.rstrip('/')}/{name.lstrip('/')}"


def resolve_revision(repository: str, revision: str) -> str:
    url = f"https://api.github.com/repos/{repository}/commits/{revision}"
    try:
        with urllib.request.urlopen(url) as response:
            payload = json.load(response)
    except urllib.error.URLError as error:
        raise RuntimeError(f"failed to resolve {repository}@{revision}: {error}") from error

    commit = payload.get("sha")
    if not isinstance(commit, str) or not commit:
        raise ValueError(f"{repository}@{revision} did not resolve to a commit")
    return commit


def source_url(config: dict[str, Any]) -> str:
    repository = require_str(config, "repository")
    revision = resolve_revision(repository, require_str(config, "revision"))
    return f"https://raw.githubusercontent.com/{repository}/{revision}"


def download(url: str, destination: Path) -> None:
    try:
        with urllib.request.urlopen(url) as response, destination.open("wb") as output:
            shutil.copyfileobj(response, output)
    except urllib.error.URLError as error:
        raise RuntimeError(f"failed to download {url}: {error}") from error


def install_function_config(manifest_url: str, names: list[str], destination: Path) -> None:
    try:
        with urllib.request.urlopen(manifest_url) as response:
            source = ET.parse(response).getroot()
    except (urllib.error.URLError, ET.ParseError) as error:
        raise RuntimeError(f"failed to load UDF manifest {manifest_url}: {error}") from error

    functions = {function.findtext("name"): function for function in source.findall("function")}
    missing = [name for name in names if name not in functions]
    if missing:
        raise ValueError(f"UDF manifest is missing functions: {', '.join(missing)}")

    selected = ET.Element("functions")
    for name in names:
        selected.append(functions[name])
    ET.indent(selected)
    ET.ElementTree(selected).write(destination, encoding="unicode")


def chown_clickhouse(paths: list[Path]) -> None:
    try:
        clickhouse_user = pwd.getpwnam("clickhouse")
    except KeyError:
        return

    for path in paths:
        os.chown(path, clickhouse_user.pw_uid, clickhouse_user.pw_gid)


def load_config(config_path: Path) -> dict[str, Any]:
    with config_path.open() as config_file:
        config = yaml.safe_load(config_file)
    if not isinstance(config, dict):
        raise ValueError(f"{config_path} must contain a YAML object")
    return config


def install_udfs(config_path: Path, arch: str, config_dir: Path, data_path: Path) -> None:
    config = load_config(config_path)
    base_url = source_url(config)
    manifest_path = require_str(config, "manifest_path")
    binary_source_path = require_str(config, "binary_path")
    udfs = config.get("udfs")
    if not isinstance(udfs, list) or not udfs:
        raise ValueError("missing non-empty list field: udfs")

    config_d_dir = config_dir / "config.d"
    user_defined_dir = config_dir / "user_defined"
    user_scripts_dir = data_path / "user_scripts"
    for directory in (config_d_dir, user_defined_dir, user_scripts_dir):
        directory.mkdir(parents=True, exist_ok=True)

    installed_paths = [config_d_dir, user_defined_dir, user_scripts_dir]

    loader_config_path = config_d_dir / "clickhouse-util-udfs.xml"
    loader_config_path.write_text(
        "<clickhouse><user_defined_executable_functions_config>"
        "/etc/clickhouse-server/user_defined/*_function.xml"
        "</user_defined_executable_functions_config></clickhouse>\n"
    )
    loader_config_path.chmod(0o644)
    installed_paths.append(loader_config_path)

    function_names: list[str] = []
    binaries: list[str] = []
    for udf in udfs:
        if not isinstance(udf, dict):
            raise ValueError("each UDF entry must be a YAML object")

        binaries.append(require_str(udf, "binary_name"))
        names = udf.get("function_names")
        if not isinstance(names, list) or not names or not all(isinstance(name, str) and name for name in names):
            raise ValueError("each UDF entry must have non-empty string list field: function_names")
        function_names.extend(names)

    source_arch = {"amd64": "x86_64", "arm64": "aarch64"}[arch]
    for binary_name in binaries:
        binary_path = user_scripts_dir / binary_name
        download(url_for(base_url, f"{binary_source_path}/{binary_name}_{source_arch}"), binary_path)
        binary_path.chmod(0o550)
        installed_paths.append(binary_path)

    function_config_path = user_defined_dir / "clickhouse-util-udfs_function.xml"
    install_function_config(url_for(base_url, manifest_path), function_names, function_config_path)
    function_config_path.chmod(0o644)
    installed_paths.append(function_config_path)

    chown_clickhouse(installed_paths)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("config_path", type=Path)
    parser.add_argument("arch")
    args = parser.parse_args()

    try:
        install_udfs(
            config_path=args.config_path,
            arch=normalize_arch(args.arch),
            config_dir=Path(os.environ.get("CLICKHOUSE_CONFIG_DIR", "/etc/clickhouse-server")),
            data_path=Path(os.environ.get("CLICKHOUSE_DATA_PATH", "/var/lib/clickhouse")),
        )
    except Exception as error:
        print(error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
