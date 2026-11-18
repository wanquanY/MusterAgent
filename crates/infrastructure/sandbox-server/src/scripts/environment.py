"""Prepare an immutable receipt for the host-supplied Python environment.
No image mutation: unavailable packages/constraints are explicit failures.
"""
import hashlib
import importlib.metadata
import json
import platform
from pathlib import Path
from pip._vendor.packaging.requirements import Requirement
from pip._vendor.packaging.specifiers import SpecifierSet
from pip._vendor.packaging.utils import canonicalize_name

def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()

def prepare(spec, runtime, installation, scope):
    if not SpecifierSet(spec.get('requiresPython') or '').contains(platform.python_version()):
        raise RuntimeError('python_environment_unavailable: Python version constraint')
    packages = {}
    expanded = set()
    queue = list(spec['requirements'])
    while queue:
        requirement = Requirement(queue.pop(0))
        if requirement.url:
            raise RuntimeError('python_environment_unavailable: URL requirement')
        if requirement.marker and not requirement.marker.evaluate():
            continue
        name = canonicalize_name(requirement.name)
        try:
            dist = importlib.metadata.distribution(name)
        except importlib.metadata.PackageNotFoundError as error:
            raise RuntimeError('python_environment_unavailable: ' + name) from error
        if not requirement.specifier.contains(dist.version):
            raise RuntimeError('python_environment_unavailable: version for ' + name)
        expansion = (name, frozenset(requirement.extras))
        if expansion in expanded:
            continue
        expanded.add(expansion)
        if name not in packages and len(packages) >= 128:
            raise RuntimeError('python_environment_unavailable: package count')
        if not dist.files:
            raise RuntimeError('python_environment_unavailable: package has no file manifest')
        digest = hashlib.sha256()
        for entry in sorted(dist.files or [], key=str):
            path = Path(dist.locate_file(entry))
            if path.is_file() and path.suffix != '.pyc':
                digest.update(str(entry).encode() + b'\0')
                with path.open('rb') as file:
                    while chunk := file.read(65536):
                        digest.update(chunk)
        packages[name] = {'name': name, 'version': dist.version, 'sha256': digest.hexdigest()}
        for dependency in dist.requires or []:
            parsed = Requirement(dependency)
            extras = requirement.extras or {''}
            if not parsed.marker or any(parsed.marker.evaluate({'extra': extra}) for extra in extras):
                queue.append(str(parsed).split(';')[0])
    material = {'schema': 'musteragent.python-environment/1', 'runtimeHash': hashlib.sha256((runtime + '\0' + platform.python_version()).encode()).hexdigest(),
                'scopeKey': scope, 'requirements': spec, 'packages': [packages[n] for n in sorted(packages)]}
    return dict(material, installationId=installation, revisionHash=hashlib.sha256(canonical(material)).hexdigest())

def verify(actual, expected):
    if actual != expected:
        raise RuntimeError('python_environment_unavailable: prepared material changed')
