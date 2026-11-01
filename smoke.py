# Smoke (CPU): imports + servidor --cpu e /object_info com os nos exigidos (N_GEN opcional via env).
# Imprime JSON; exit 1 se faltar no exigido.
import json, subprocess, sys, time, urllib.request

import os
REQ = [n for n in [os.environ.get('N_GEN')] if n] + ['UNETLoader', 'CLIPLoader', 'LoraLoaderModelOnly', 'VAEDecodeAudio', 'CreateVideo', 'SaveVideo']
r = {}
import torch
r['torch'] = torch.__version__; r['cuda'] = torch.cuda.is_available()
for m in ('nunchaku', 'sageattention'):
  try: r[m] = getattr(__import__(m), '__version__', 'ok')
  except Exception as e: r[m] = f'ERR {type(e).__name__}: {str(e)[:200]}'

log = open('/tmp/smoke_srv.log', 'w')
p = subprocess.Popen([sys.executable, 'main.py', '--cpu', '--listen', '127.0.0.1', '--port', '8188'],
                     cwd='/opt/app', stdout=log, stderr=subprocess.STDOUT)
info = None
for _ in range(120):
  if p.poll() is not None: break
  try: info = json.loads(urllib.request.urlopen('http://127.0.0.1:8188/object_info', timeout=30).read()); break
  except Exception: time.sleep(5)
p.terminate()
try: p.wait(30)
except subprocess.TimeoutExpired: p.kill()

names = set(info or ())
r['srv_up'] = info is not None
r['missing'] = [n for n in REQ if n not in names]
r['nunchaku_nodes'] = sorted(n for n in names if n.lower().startswith('nunchaku'))
r['import_failed'] = [l.strip() for l in open('/tmp/smoke_srv.log') if 'IMPORT FAILED' in l]
r['ok'] = r['srv_up'] and not r['missing']
print(json.dumps(r, indent=1))
if not r['srv_up']: print(open('/tmp/smoke_srv.log').read()[-5000:])
sys.exit(0 if r['ok'] else 1)
