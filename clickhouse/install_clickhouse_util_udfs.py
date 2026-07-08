#!/usr/bin/env python3
import argparse
import json
import os
import pwd
import shutil
import sys
import urllib.error
import urllib.request
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


def resolve_release(repository: str, release: str) -> str:
    if release != "latest":
        return release

    url = f"https://api.github.com/repos/{repository}/releases/latest"
    try:
        with urllib.request.urlopen(url) as response:
            payload = json.load(response)
    except urllib.error.URLError as error:
        raise RuntimeError(f"failed to resolve latest release for {repository}: {error}") from error

    tag_name = payload.get("tag_name")
    if not isinstance(tag_name, str) or not tag_name:
        raise ValueError(f"latest release for {repository} did not include tag_name")
    return tag_name


def source_urls(config: dict[str, Any]) -> tuple[str, str]:
    release_base_url = config.get("release_base_url")
    config_base_url = config.get("config_base_url")
    if isinstance(release_base_url, str) and isinstance(config_base_url, str):
        return release_base_url, config_base_url

    repository = require_str(config, "repository")
    release = resolve_release(repository, require_str(config, "release"))
    return (
        f"https://github.com/{repository}/releases/download/{release}",
        f"https://raw.githubusercontent.com/{repository}/{release}/udf",
    )


def download(url: str, destination: Path) -> None:
    try:
        with urllib.request.urlopen(url) as response, destination.open("wb") as output:
            shutil.copyfileobj(response, output)
    except urllib.error.URLError as error:
        raise RuntimeError(f"failed to download {url}: {error}") from error


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
    release_base_url, config_base_url = source_urls(config)
    loader_config_name = require_str(config, "loader_config_name")
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
    download(url_for(config_base_url, loader_config_name), loader_config_path)
    loader_config_path.chmod(0o644)
    installed_paths.append(loader_config_path)

    for udf in udfs:
        if not isinstance(udf, dict):
            raise ValueError("each UDF entry must be a YAML object")

        binary_name = require_str(udf, "binary_name")
        config_name = require_str(udf, "config_name")
        config_dest_name = require_str(udf, "config_dest_name")

        binary_path = user_scripts_dir / binary_name
        config_path = user_defined_dir / config_dest_name
        download(url_for(release_base_url, f"{binary_name}-linux-{arch}"), binary_path)
        download(url_for(config_base_url, config_name), config_path)
        binary_path.chmod(0o550)
        config_path.chmod(0o644)
        installed_paths.extend([binary_path, config_path])

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
