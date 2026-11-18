"""Declared-output helper, restored from the browser job contract."""
import json
from pathlib import Path
INPUT_DIR = Path('/sandbox/input')
OUTPUT_DIR = Path('/sandbox/work')
WORK_DIR = OUTPUT_DIR
_CONFIG = json.loads((INPUT_DIR / '__kernel__/job.json').read_text())
_DECLARED_OUTPUTS = frozenset(_CONFIG['outputs'])
def _canonical(path):
    if not isinstance(path, str) or not path or '\\' in path or any(ord(c) < 32 for c in path):
        raise ValueError('path must be a canonical relative string')
    if path.startswith('/') or any(p in ('', '.', '..') for p in path.split('/')):
        raise ValueError('path must be canonical and relative')
    return path

def input_path(path):
    value = INPUT_DIR / _canonical(path)
    if not value.is_file():
        raise FileNotFoundError(path)
    return value

def output_path(path):
    path = _canonical(path)
    if path not in _DECLARED_OUTPUTS:
        raise ValueError('output path was not declared by the job')
    value = OUTPUT_DIR / path
    value.parent.mkdir(parents=True, exist_ok=True)
    return value

def list_inputs():
    return tuple(sorted(_CONFIG['inputs']))

def environment():
    import importlib.metadata, sys
    return {'execution': 'server-container', 'python': sys.version,
            'installed': {d.metadata['Name']: d.version for d in importlib.metadata.distributions() if d.metadata['Name']}}
