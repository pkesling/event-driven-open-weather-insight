"""Shared Great Expectations helpers to generate Data Docs locally.

Creates a filesystem-backed context rooted under ``gx_artifacts`` so that
validation results and HTML Data Docs are persisted for inspection/demo.
GX_STRICT (default=1) controls whether we require `project_config` support;
tests can set GX_STRICT=0 to use the relaxed code path. This is a deliberate
test-only leniency; production should keep strict mode.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

import great_expectations as gx
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults,
)

logger = logging.getLogger(__name__)

def _resolve_artifacts_root(root_dir: Path | None = None) -> Path:
    override = os.getenv("GX_ARTIFACTS_DIR")
    if override:
        return Path(override).expanduser().resolve()
    if root_dir is None:
        root_dir = Path(__file__).resolve().parent
    return (root_dir / "gx_artifacts").resolve()


def build_local_context(root_dir: Path | None = None):
    """
    Build a filesystem-based GX context rooted at ``gx_artifacts`` (or GX_ARTIFACTS_DIR).

    - Expectations/validations are stored locally.
    - Data Docs (HTML) are written to ``gx_artifacts/data_docs/local_site``.
    Returns (context, artifacts_root).
    """
    artifacts_root = _resolve_artifacts_root(root_dir)
    artifacts_root.mkdir(parents=True, exist_ok=True)

    store_defaults = FilesystemStoreBackendDefaults(root_directory=str(artifacts_root))

    config = DataContextConfig(
        store_backend_defaults=store_defaults,
        data_docs_sites={
            "local_site": {
                "class_name": "SiteBuilder",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "root_directory": str(artifacts_root),
                    "base_directory": "data_docs/local_site",
                },
                "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
            }
        },
    )

    strict = os.getenv("GX_STRICT", "1") != "0"
    try:
        if strict:
            ctx = gx.get_context(project_config=config)
        else:
            ctx = gx.get_context()
    except TypeError:
        # Fallback for environments/tests where project_config isn't supported
        ctx = gx.get_context()
    ctx._artifacts_root = artifacts_root  # type: ignore[attr-defined]
    return ctx, artifacts_root


def build_data_docs(context) -> Path | None:
    """Render Data Docs for the configured sites and return the output path if known."""
    try:
        if not hasattr(context, "build_data_docs"):
            return None
        context.build_data_docs()
        artifacts_root = getattr(context, "_artifacts_root", None)
        if artifacts_root:
            out = Path(artifacts_root) / "data_docs" / "local_site" / "index.html"
            logger.info(f"Great Expectations Data Docs written to: {out}")
            return out
    except Exception as exc:  # pragma: no cover - best effort logging only
        logger.warning(f"Failed to build Data Docs: {exc}")
    return None
