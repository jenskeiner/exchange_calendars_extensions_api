import os
import re
from copy import replace
from typing import Any
from collections.abc import Iterable

from packaging.version import Version
from versioningit import VCSDescription

INTEGRATION_BRANCHES = ("develop", "dev")
RELEASE_BRANCHES = ("master", "main")
FEATURE_BRANCH_PREFIX = "feature/"
BUGFIX_BRANCH_PREFIX = "bugfix/"
RELEASE_BRANCH_PREFIX = "release/"
HOTFIX_BRANCH_PREFIX = "hotfix/"
SUPPORT_BRANCH_PREFIX = "support/"
RELEASE_BRANCH_PREFIXES = (
    RELEASE_BRANCH_PREFIX,
    HOTFIX_BRANCH_PREFIX,
    SUPPORT_BRANCH_PREFIX,
)


def sanitize(x: str) -> str:
    # Replace non-alphanumeric characters with "."
    return re.sub(r"[^a-zA-Z0-9]", ".", x)


def to_local(local: Iterable[str]) -> str:
    return ".".join(local)


def get_version(
    *,
    description: VCSDescription,
    base_version: str,
    next_version: str,
    params: dict[str, Any],
) -> str:
    params = {"rc_num_env_var": "RC", "version_override": None, **params}

    if params["version_override"]:
        return params["version_override"]

    local: list[str] = []

    if not description.branch:
        raise ValueError("Unable to format version without branch name.")

    version: Version | None

    # Check if we are on a branch that represents a release or that will be used to create a release.
    if description.branch in RELEASE_BRANCHES or any(
        description.branch.startswith(p) for p in RELEASE_BRANCH_PREFIXES
    ):
        version_str = None

        # Extract release version, depending on case.
        if description.branch in RELEASE_BRANCHES:
            if description.tag and description.state == "exact":
                version_str = description.tag
            else:
                raise ValueError(
                    f"Release branch '{description.branch}' has nearest tag '{description.tag}' but state is '{description.state}'."
                )
        else:
            # Find first release branch prefix that matches and extract version number from branch name.
            for prefix in RELEASE_BRANCH_PREFIXES:
                if description.branch.startswith(prefix):
                    version_str = description.branch[len(prefix) :] + ".rc"
                    if os.environ.get(params["rc_num_env_var"]):
                        version_str = version_str + os.environ.get(
                            params["rc_num_env_var"]
                        )
                    else:
                        version_str = version_str + "0"
                        local.append(f"rev.{description.fields['revision']}")
                    break

        if not version_str:
            raise ValueError(
                f"Unable to determine version from branch name '{description.branch}'."
            )

        try:
            version = Version(version_str)
        except Exception as e:
            raise ValueError(
                f"Version '{version_str}' determined from branch '{description.branch}' is not a valid package version."
            ) from e
    else:
        version = Version("0.dev0")
        local.append(f"branch.{sanitize(description.branch)}")
        local.append(f"rev.{description.fields['revision']}")

    if len(local) > 0:
        version = replace(version, local=to_local(local))

    return str(version)
