# Agent Instructions

## Version bumps

When incrementing the version number in `pyproject.toml`, always run `uv sync` afterwards so that `uv.lock` is updated to reflect the new version. Include the updated `uv.lock` in the same commit.
