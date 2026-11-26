#!/usr/bin/env python3
"""Use an independent Ubuntu mirror on ephemeral CI runners, retaining APT's signing policy."""

from pathlib import Path
import re

# Canonical's DNS outage on 2026-11-11 blocked all three default runner mirrors.
# Keep independent HTTPS mirrors ahead of Canonical's archive so mirror+file lists
# fail over across distinct operators instead of collapsing onto a single host.
UBUNTU_MIRRORS = (
    "https://mirrors.edge.kernel.org/ubuntu",
    "https://mirrors.ocf.berkeley.edu/ubuntu",
    "http://azure.archive.ubuntu.com/ubuntu",
    "https://archive.ubuntu.com/ubuntu",
)
UBUNTU_MIRROR = UBUNTU_MIRRORS[0]
APT_NETWORK_CONF = (
    'Acquire::ForceIPv4 "true";\n'
    'Acquire::Retries "3";\n'
    'Acquire::http::Timeout "10";\n'
    'Acquire::https::Timeout "10";\n'
)
MIRROR_LIST = "".join(
    f"{mirror}\tpriority:{priority}\n"
    for priority, mirror in enumerate(UBUNTU_MIRRORS, start=1)
)
DEFAULT_ARCHIVE = re.compile(
    r"https?://(?:azure\.archive|archive|security)\.ubuntu\.com/ubuntu/?(?=\s|$)"
)


def configure_mirror(apt_directory: Path) -> int:
    conf_dir = apt_directory / "apt.conf.d"
    conf_dir.mkdir(parents=True, exist_ok=True)
    conf_file = conf_dir / "99culprit-network"
    if not conf_file.is_file() or conf_file.read_text() != APT_NETWORK_CONF:
        conf_file.write_text(APT_NETWORK_CONF)
    sources = [apt_directory / "sources.list"]
    sources.extend(apt_directory.glob("sources.list.d/*.list"))
    sources.extend(apt_directory.glob("sources.list.d/*.sources"))
    changed = 0
    for source in sources:
        if not source.is_file():
            continue
        original = source.read_text()
        updated = DEFAULT_ARCHIVE.sub(UBUNTU_MIRROR, original)
        if updated != original:
            source.write_text(updated)
            changed += 1
    # GitHub runner images also use mirror+file URIs backed by *mirrors*.txt lists.
    for mirror_file in apt_directory.glob("*mirrors*.txt"):
        if not mirror_file.is_file():
            continue
        original = mirror_file.read_text()
        if original == MIRROR_LIST:
            continue
        if DEFAULT_ARCHIVE.search(original) or any(mirror in original for mirror in UBUNTU_MIRRORS):
            mirror_file.write_text(MIRROR_LIST)
            changed += 1
    return changed


if __name__ == "__main__":
    changed = configure_mirror(Path("/etc/apt"))
    print(f"Ubuntu mirror: {UBUNTU_MIRROR}; {changed} source files updated; APT signature policy unchanged")
