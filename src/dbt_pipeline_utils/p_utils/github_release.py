"""
Zero-dependency GitHub release asset download (stdlib only).

Every source repository is expected to publish its DD files directly as
named release assets (e.g. "patients_dd.csv" attached to a GitHub release) -
the release is the source of truth. This just pulls those bytes.
"""

import json
import urllib.request
from urllib.parse import quote, urlparse

from dbt_pipeline_utils.p_utils.release_cache import cached_pull


def _github_repository_parts(repository_url: str) -> tuple[str, str]:
    parsed = urlparse(repository_url.rstrip("/"))
    if parsed.netloc not in {"github.com", "www.github.com"}:
        raise ValueError("repository_url must be a GitHub repository URL")

    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) != 2:
        raise ValueError(
            "repository_url must look like https://github.com/<owner>/<repository>"
        )

    return parts[0], parts[1].removesuffix(".git")


def _request(url: str, token: str | None = None) -> bytes:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "dbt-pipeline-utils",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request) as response:
        return response.read()


def _release_asset_url(
    repository_url: str,
    asset_name: str,
    *,
    tag: str = "latest",
    token: str | None = None,
) -> str:
    owner, repository = _github_repository_parts(repository_url)
    release_path = "latest" if tag == "latest" else f"tags/{quote(tag, safe='')}"
    api_url = f"https://api.github.com/repos/{owner}/{repository}/releases/{release_path}"
    release = json.loads(_request(api_url, token=token))

    for asset in release.get("assets", []):
        if asset.get("name") == asset_name:
            return asset["browser_download_url"]

    available = ", ".join(asset.get("name", "") for asset in release.get("assets", []))
    raise FileNotFoundError(
        f"Release asset '{asset_name}' was not found in {repository_url}@{tag}. "
        f"Available assets: {available or '(none)'}"
    )


def pull_release_asset_to_memory(
    repository_url: str,
    asset_name: str,
    *,
    tag: str = "latest",
    token: str | None = None,
    refresh: bool = False,
) -> bytes:
    """
    Download one named GitHub release asset and return its bytes - nothing
    written to disk.

    The result is cached in memory per (repository_url, asset_name, tag), so
    repeated calls don't re-hit the network. Pass refresh=True to force a
    fresh pull, e.g. if the asset was re-published under the same tag.
    """

    def _pull() -> bytes:
        asset_url = _release_asset_url(repository_url, asset_name, tag=tag, token=token)
        return _request(asset_url, token=token)

    return cached_pull(("asset", repository_url, asset_name, tag), refresh, _pull)
