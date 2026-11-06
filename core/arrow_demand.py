"""Resolve small per-consumer requests once per offset publication."""
import json
from pathlib import Path


def publication_rows(folder, configured, *, enabled=False, minimum=None):
    if not enabled:
        return configured
    # Preserve unregistered consumers unless the operator explicitly sets a
    # common floor. 0 means full history, never a smaller numeric minimum.
    floor = configured if minimum is None else minimum
    if type(floor) is not int or floor < 0:
        return 0
    folder = Path(folder)/'.arrow-demand'
    if not folder.exists():
        return configured
    depths = []
    try:
        for owner in folder.iterdir():
            if not owner.is_dir():
                continue
            for path in owner.iterdir():
                if path.suffix == '.tmp':
                    return 0
                if path.suffix != '.json':
                    continue
                value = json.loads(path.read_text(encoding='utf8'))
                rows = value.get('rows')
                if value.get('version') != 1 or type(rows) is not int or (rows != 0 and rows < 4050):
                    return 0
                if rows == 0:
                    return 0
                depths.append(rows)
    except (OSError, ValueError, AttributeError):
        return 0
    if not depths:
        return configured
    return 0 if floor == 0 else max(floor, *depths)
