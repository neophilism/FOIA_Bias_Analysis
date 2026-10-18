"""Validate immutable corpus snapshot configuration without network access."""
from datetime import date
from pathlib import Path
import json


def validate_corpus_config(data):
    if not isinstance(data, dict):
        raise ValueError("Config must be a mapping")
    corpus = data.get("corpus")
    if not isinstance(corpus, dict) or corpus.get("mode") != "snapshot":
        raise ValueError("corpus.mode must be snapshot")
    path = corpus.get("manifest_path")
    if not isinstance(path, str) or not path.strip():
        raise ValueError("corpus.manifest_path is required")
    if (Path(path).is_absolute() or ".." in Path(path).parts or "://" in path
            or "\\\\" in path or "\\x00" in path):
        raise ValueError("manifest_path must be relative")
    if not isinstance(corpus.get("fixture_mode"), bool):
        raise ValueError("corpus.fixture_mode must be boolean")
    batch = corpus.get("batch_size")
    if type(batch) is not int or not 1 <= batch <= 10000:
        raise ValueError("corpus.batch_size must be 1..10000")
    cohort = data.get("cohort")
    if not isinstance(cohort, dict):
        raise ValueError("cohort is required")
    try:
        start = date.fromisoformat(cohort["start_date"])
        end = date.fromisoformat(cohort["end_date"])
    except (ValueError, KeyError, TypeError) as exc:
        raise ValueError("cohort requires ISO dates") from exc
    if start != date(1993, 1, 20) or end != date(2025, 1, 20):
        raise ValueError("cohort dates must match approved completed administrations")
    if cohort.get("include_incomplete_administrations") is not False:
        raise ValueError("incomplete administrations must be excluded")
    return data


def load_config(path):
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)
    if path.suffix.lower() in (".yaml", ".yml"):
        import yaml
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    elif path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
    else:
        raise ValueError("Unsupported config format")
    return validate_corpus_config(data)
