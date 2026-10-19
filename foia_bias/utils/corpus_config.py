"""Validate immutable corpus snapshot configuration without network access."""
from datetime import date
from pathlib import Path
import re

MAX_CONFIG_BYTES = 64 * 1024
MAX_YAML_DEPTH = 32


def validate_corpus_config(data):
    if not isinstance(data, dict):
        raise ValueError("Config must be a mapping")
    corpus = data.get("corpus")
    if not isinstance(corpus, dict) or corpus.get("mode") != "snapshot":
        raise ValueError("corpus.mode must be snapshot")
    if set(data) != {"corpus", "cohort"}:
        raise ValueError("Unsupported top-level settings; legacy collection is disabled")
    if set(corpus) != {"mode", "manifest_path", "fixture_mode", "batch_size"}:
        raise ValueError("Unsupported corpus settings; population caps are not allowed")
    path = corpus.get("manifest_path")
    if not isinstance(path, str) or not path.strip():
        raise ValueError("corpus.manifest_path is required")
    if (Path(path).is_absolute() or ".." in Path(path).parts or ":" in path
            or chr(92) in path or chr(0) in path):
        raise ValueError("manifest_path must be relative")
    if not isinstance(corpus.get("fixture_mode"), bool):
        raise ValueError("corpus.fixture_mode must be boolean")
    batch = corpus.get("batch_size")
    if type(batch) is not int or not 1 <= batch <= 10000:
        raise ValueError("corpus.batch_size must be 1..10000")
    cohort = data.get("cohort")
    if not isinstance(cohort, dict):
        raise ValueError("cohort is required")
    if set(cohort) != {"start_date", "end_date", "include_incomplete_administrations"}:
        raise ValueError("Unsupported cohort settings")
    if any(not isinstance(cohort[key], str)
           or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", cohort[key])
           for key in ("start_date", "end_date")):
        raise ValueError("cohort requires canonical ISO dates (YYYY-MM-DD)")
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



def _strict_yaml(text):
    # Optional parser remains lazy: JSON/help never imports third-party code.
    import yaml

    class ConfigLoader(yaml.SafeLoader):
        def construct_mapping(self, node, deep=False):
            if not isinstance(node, yaml.nodes.MappingNode):
                raise ValueError("YAML configuration mapping is invalid")
            result = {}
            for key_node, value_node in node.value:
                if (not isinstance(key_node, yaml.nodes.ScalarNode)
                        or key_node.tag != "tag:yaml.org,2002:str"):
                    raise ValueError("YAML mapping keys must be strings; merge keys unsupported")
                key = self.construct_object(key_node, deep=deep)
                if key in result:
                    raise ValueError("duplicate YAML key")
                result[key] = self.construct_object(value_node, deep=deep)
            return result

    try:
        # This flat configuration never needs object sharing or recursive aliases.
        # Scan parser events first, before constructing any YAML object graph.
        depth = 0
        for event in yaml.parse(text, Loader=yaml.SafeLoader):
            if getattr(event, "anchor", None) is not None:
                raise ValueError("YAML anchors and aliases are not supported")
            if isinstance(event, (yaml.events.MappingStartEvent, yaml.events.SequenceStartEvent)):
                depth += 1
                if depth > MAX_YAML_DEPTH:
                    raise ValueError("YAML nesting exceeds configuration boundary")
            elif isinstance(event, (yaml.events.MappingEndEvent, yaml.events.SequenceEndEvent)):
                depth -= 1
        return yaml.load(text, Loader=ConfigLoader)
    except (yaml.YAMLError, RecursionError) as exc:
        # Parser diagnostics may contain source lines; never echo configuration.
        raise ValueError("Malformed YAML configuration") from exc


def load_config(path):
    path = Path(path)
    suffix = path.suffix.lower()
    if suffix not in (".json", ".yaml", ".yml"):
        raise ValueError("Unsupported config format")
    with path.open("rb") as stream:
        raw = stream.read(MAX_CONFIG_BYTES + 1)
    if len(raw) > MAX_CONFIG_BYTES:
        raise ValueError("configuration exceeds 64 KiB boundary")
    text = raw.decode("utf-8")
    if suffix in (".yaml", ".yml"):
        data = _strict_yaml(text)
    else:
        from foia_bias.corpus import strict_json
        data = strict_json(text)
    return validate_corpus_config(data)
