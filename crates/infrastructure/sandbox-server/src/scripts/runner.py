"""Job bootstrap. All paths and configuration originate in the immutable input mount."""
import errno
import json
import os
from pathlib import Path
import runpy
import sys
import traceback

config = json.loads(Path('/sandbox/input/__kernel__/job.json').read_text())
sys.path.insert(0, '/sandbox/input/__kernel__')
try:
    if config.get('environment'):
        from environment import prepare, verify
        try:
            receipt = prepare(config['environment'], config['runtime'], config['installation'], config['scope'])
        except Exception as error:
            raise RuntimeError('python_environment_unavailable: ' + str(error)) from error
        if config.get('expected'):
            verify(receipt, config['expected'])
        if config['kind'] == 'environment':
            body = json.dumps(receipt, separators=(',', ':'), ensure_ascii=False)
            Path('/sandbox/work/environment-lock.json').write_text(body)
            print(body)
            sys.exit(0)
    os.chdir(config['cwd'])
    sys.path.insert(0, config['cwd'])
    sys.argv = [config['entrypoint']]
    runpy.run_path(config['entrypoint'], run_name='__main__')
except OSError as error:
    traceback.print_exc()
    sys.exit(122 if error.errno in (errno.ENOSPC, errno.EDQUOT, errno.EFBIG) else 1)
except Exception:
    traceback.print_exc()
    sys.exit(1)
