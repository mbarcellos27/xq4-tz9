# Baixa os arquivos do manifest (MODELS_PREFIX) para models/, com reserva opcional HF_REPO.
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


def from_r2(k, dst):
  got, last = [0], [time.time()]
  def cb(n):
    got[0] += n
    if time.time() - last[0] > 20: last[0] = time.time(); log('dl', k, got[0] >> 20, 'MB')
  s3.download_file(B, P + k, dst, Config=cfg, Callback=cb)


def from_hf(k, dst):
  if not HF_REPO: raise RuntimeError('HF_REPO nao definido')
  from huggingface_hub import hf_hub_download
  stop = threading.Event()
  def prog():
    while not stop.wait(20): log('dl', k, disk_bytes() >> 20, 'MB (models/ total)')
  threading.Thread(target=prog, daemon=True).start()
  try: hf_hub_download(HF_REPO, k, local_dir=M)
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
        t = time.time(); downloading.set()
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
