# Bench pos-worker no mesmo host: se QUEUE_PREFIX/post_bench.json existir ({"file": "<k do manifest>", "max_s": 150}),
# baixa o arquivo do R2 (16 conexoes) e depois do HF para /tmp, mede MB/s, apaga. Nunca falha o pod (sai 0).
import json, os, sys, time
sys.argv = [sys.argv[0]]
import fetch_models as F
Q = os.environ['QUEUE_PREFIX'].rstrip('/') + '/'
try: cfg = json.loads(F.s3.get_object(Bucket=F.B, Key=Q + 'post_bench.json')['Body'].read())
except Exception: sys.exit(0)
k = cfg['file']; mx = float(cfg.get('max_s', 150))
for nm, cls in (('R2', F.R2), ('HF', F.HF)):
  dst = f'/tmp/pb_{nm}.bin'
  try:
    src = cls(k); size = src.size()
    with open(dst, 'wb') as f: f.truncate(size)
    cov = []; st = {}; t = time.time()
    m = F.fetch(src, dst, size, cov, deadline=time.time() + mx, stats=st, nconn=16)
    dt = time.time() - t; got = F.covered(cov, size)
    F.log('PHASE postbench', f'origem={nm}', 'conn=16', f'arquivo={k}', f'bytes={got}', f'size={size}', f'{dt:.1f}s', f'MB/s={got / max(dt, 1e-3) / 1e6:.0f}', f'completo={got == size}', f'res={m}')
  except Exception as e: F.log('WARN postbench', nm, type(e).__name__, str(e)[:200])
  finally:
    try: os.remove(dst)
    except Exception: pass
F.log('PHASE postbench_fim', f'r2_requests={F.R2REQ[0]}')
