# Baixa os arquivos do manifest (MODELS_PREFIX) para models/, com reserva opcional HF_REPO.
# R2: ranged GET paralelo com ate DL_RESUMES (2) retomadas por arquivo apos STALL_S parado.
# Saida: 0 ok, 3 stall (bytes parados por STALL_S), 4 falha de verificacao/download.
import hashlib, json, os, sys, threading, time
import boto3
from boto3.s3.transfer import TransferConfig

E = os.environ
M = '/opt/app/models/'
P = E['MODELS_PREFIX'].rstrip('/') + '/'
B = E['R2_BUCKET']
HF_REPO = E.get('HF_REPO', '')
STALL_S = int(E.get('STALL_S', '180'))
SKIP = [s.strip() for s in E.get('MODELS_SKIP', '').split(',') if s.strip()]
s3 = boto3.client('s3', endpoint_url=E['R2_EP'], aws_access_key_id=E['R2_ACCESS_KEY_ID'],
                  aws_secret_access_key=E['R2_SECRET_ACCESS_KEY'], region_name='auto')
cfg = TransferConfig(multipart_chunksize=64 << 20, max_concurrency=32)


def log(*a): print(*a, flush=True)


def disk_bytes():
  # bytes realmente gravados (st_blocks) sob models/, inclui temporarios do boto3 e .incomplete do HF
  n = 0
  for d, _, fs in os.walk(M):
    for f in fs:
      try: n += os.stat(os.path.join(d, f)).st_blocks * 512
      except OSError: pass
  return n


downloading = threading.Event()


def watchdog():
  last, t = disk_bytes(), time.time()
  while True:
    time.sleep(10)
    now = disk_bytes()
    if now != last or not downloading.is_set(): last, t = now, time.time(); continue
    if time.time() - t > STALL_S:
      log(f'ABORT stall {STALL_S}s sem bytes novos'); os._exit(3)


hash_fail = []


def check_hash(path, want, k):
  h = hashlib.sha256()
  with open(path, 'rb') as f:
    for c in iter(lambda: f.read(16 << 20), b''): h.update(c)
  ok = h.hexdigest() == want
  log('HASH', k, 'ok' if ok else 'FAIL')
  if not ok: hash_fail.append(k)


PART = 64 << 20
RESUMES = int(E.get('DL_RESUMES', '2'))
SUB = 8 << 20; TAIL = int(E.get('DL_TAIL_MB', '64')) << 20; NCONN = 16
SLOW_BPS = float(E.get('DL_SLOW_MBPS', '5')) * 1e6; SLOW_S = float(E.get('DL_SLOW_S', '10'))
from botocore.config import Config as BCfg
s3dl = boto3.client('s3', endpoint_url=E['R2_EP'], aws_access_key_id=E['R2_ACCESS_KEY_ID'],
                    aws_secret_access_key=E['R2_SECRET_ACCESS_KEY'], region_name='auto',
                    config=BCfg(read_timeout=60, connect_timeout=20, retries={'max_attempts': 3}, max_pool_connections=40))


def split(o, end, sz):
  return [(x, min(x + sz, end + 1) - 1) for x in range(o, end + 1, sz)]


def gaps(cov, size):
  out, p = [], 0
  for o, e in sorted(cov):
    if o > p: out.append((p, o - 1))
    p = max(p, e + 1)
  if p < size: out.append((p, size - 1))
  return out


def from_r2(k, dst):
  # ranged GET paralelo (16 conexoes). Cauda (ultimos TAIL bytes) em partes de SUB baixadas em duplicado (hedged, vale a 1a).
  # Range < SLOW_BPS por > SLOW_S: cancela e repede o restante da faixa em outra conexao (redividido em SUB).
  # Parado STALL_S: cancela tudo e retoma so as lacunas (ate RESUMES vezes). sha256 checado depois.
  size = s3dl.head_object(Bucket=B, Key=P + k)['ContentLength']
  with open(dst, 'ab') as f: f.truncate(size)
  cut = max(0, size - TAIL) // PART * PART
  body = split(0, cut - 1, PART) if cut else []
  tail = split(cut, size - 1, SUB)
  cov = []; lock = threading.Lock(); got, last = [0], [time.time()]; st = {'slow': 0, 'hedge_win': 0}
  t_tail = [None, None]
  for tent in range(RESUMES + 1):
    if tent == 0: q = body + [r for r in tail for _ in (0, 1)]
    else:
      q = [r for g in gaps(cov, size) for r in split(g[0], g[1], SUB)]
      if not q: break
      log('RESUME', k, f'tentativa={tent}', f'ranges={len(q)}', f'{sum(e - o + 1 for o, e in cov) >> 20}MB')
    cancel = threading.Event(); prog = [time.time()]; ativos = [0]; feitos = set()
    def worker():
      fd = os.open(dst, os.O_WRONLY)
      try:
        while not cancel.is_set():
          with lock:
            r = None
            while q:
              r = q.pop(0)
              if r not in feitos: break
              r = None
            if r is None:
              if ativos[0] == 0: return
            else:
              ativos[0] += 1
              if r[0] >= cut and t_tail[0] is None: t_tail[0] = time.time()
          if r is None: time.sleep(0.3); continue
          o, end = r; pos = o; t0 = time.time()
          try:
            b = s3dl.get_object(Bucket=B, Key=P + k, Range=f'bytes={o}-{end}')['Body']
            for c in b.iter_chunks(1 << 20):
              if cancel.is_set() or r in feitos: break
              os.pwrite(fd, c, pos); pos += len(c)
              with lock: got[0] += len(c); prog[0] = time.time()
              el = time.time() - t0
              if el > SLOW_S and (pos - o) / el < SLOW_BPS and pos <= end:
                with lock:
                  cov.append((o, pos - 1)) if pos > o else None
                  q[:0] = split(pos, end, SUB); st['slow'] += 1
                log('SLOW', k, f'range {o >> 20}MB {(pos - o) / el / 1e6:.1f}MB/s -> repede {end + 1 - pos >> 20}MB')
                break
            else:
              if pos == end + 1:
                with lock:
                  if r not in feitos:
                    feitos.add(r); cov.append(r)
                    if r[0] >= cut and r in tail: st['hedge_win'] += 0
          except Exception as e:
            log('WARN range', k, o, type(e).__name__, str(e)[:120])
            with lock:
              if pos > o: cov.append((o, pos - 1))
          finally:
            with lock: ativos[0] -= 1
      finally: os.close(fd)
    ths = [threading.Thread(target=worker, daemon=True) for _ in range(NCONN)]
    for t in ths: t.start()
    while any(t.is_alive() for t in ths):
      time.sleep(1)
      if not gaps(cov, size): cancel.set(); break
      if time.time() - last[0] > 20: last[0] = time.time(); log('dl', k, got[0] >> 20, 'MB', f'{sum(e - o + 1 for o, e in cov) * 100 // size}%')
      if time.time() - prog[0] > STALL_S:
        log(f'STALL {STALL_S}s', k); cancel.set(); break
    for t in ths: t.join(90)
    if not gaps(cov, size): break
  t_tail[1] = time.time()
  if gaps(cov, size): raise RuntimeError(f'download incompleto apos {RESUMES} retomadas: lacunas={len(gaps(cov, size))}')
  log('TAIL', k, f'{(t_tail[1] - (t_tail[0] or t_tail[1])):.1f}s desde 1a parte da cauda', f'slow_repedidos={st["slow"]}')


def from_hf(k, dst):
  if not HF_REPO: raise RuntimeError('HF_REPO nao definido')
  os.environ.setdefault('HF_HUB_ENABLE_HF_TRANSFER', '1')
  from huggingface_hub import hf_hub_download
  mp = json.loads(E.get('HF_FILE_MAP', '{}'))  # {arquivo: 'repo:caminho'} opcional
  stop = threading.Event()
  def prog():
    while not stop.wait(20): log('dl', k, disk_bytes() >> 20, 'MB (models/ total)')
  threading.Thread(target=prog, daemon=True).start()
  repo, path = (mp[k].split(':', 1) if k in mp else (HF_REPO, k))
  try:
    f = hf_hub_download(repo, path, local_dir=M)
    if os.path.abspath(f) != os.path.abspath(dst): os.replace(f, dst)
  finally: stop.set()
  if not os.path.exists(dst): raise FileNotFoundError(dst)


def main():
  try: man = json.loads(s3.get_object(Bucket=B, Key=P + 'manifest.json')['Body'].read())
  except Exception as e: log('ABORT manifest', type(e).__name__, e); sys.exit(4)
  threading.Thread(target=watchdog, daemon=True).start()
  ths, failed = [], []
  for k, v in man.items():
    if any(s in k for s in SKIP): log('skip', k); continue
    dst = M + k; os.makedirs(os.path.dirname(dst), exist_ok=True)
    if os.path.exists(dst) and os.path.getsize(dst) == v['size']:
      origin, dt = 'cache', 0.0
    else:
      origin = None
      for name, fn in (('R2', from_r2), ('HF', from_hf)):
        t = time.time()
        if name == 'HF': downloading.set()
        try:
          fn(k, dst)
          if os.path.getsize(dst) != v['size']: raise ValueError(f'size {os.path.getsize(dst)} != {v["size"]}')
          origin, dt = name, time.time() - t; break
        except Exception as e:
          log('WARN', k, f'origem={name}', type(e).__name__, str(e)[:300])
        finally: downloading.clear()
      if not origin: failed.append(k); continue
    log('PHASE dl_model', k, f'origem={origin}', f'{dt:.0f}s', f'{v["size"] / max(dt, 1e-3) / 1e6:.0f}MB/s', 'size_ok')
    th = threading.Thread(target=check_hash, args=(dst, v['sha256'], k)); th.start(); ths.append(th)
  for th in ths: th.join()
  if failed or hash_fail:
    log('PHASE models_verified FAIL', 'dl=' + ','.join(failed), 'hash=' + ','.join(hash_fail)); sys.exit(4)
  log('PHASE models_verified ok')


if __name__ == '__main__': main()
