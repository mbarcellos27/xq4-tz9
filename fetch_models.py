# Baixa os arquivos do manifest (MODELS_PREFIX) para models/, origens R2 e HF (HF_REPO / HF_FILE_MAP por env).
# Downloader unico por faixas (ranged GET, 16 conexoes, partes de 64 MB; cauda de 64 MB em 8 MB duplicada/hedged).
# Bench: no 1o arquivo (maior) baixa ~BENCH_S s por origem (faixas reais, gravadas no destino) e escolhe a mais rapida
# para o resto de todos os arquivos; a outra fica de reserva. HF: GET ranged HTTP direto na URL resolve (hf_transfer
# nao baixa faixa parcial). Faixa lenta: < 30% da mediana das outras conexoes ativas por SLOW_S, ou < 1 MB/s => repede.
# 3 min sem bytes => troca de origem / retoma lacunas. sha256 obrigatorio. Saida: 0 ok, 4 falha.
import hashlib, json, os, sys, threading, time, statistics, urllib.request, urllib.error
import boto3
from botocore.config import Config as BCfg

E = os.environ
M = E.get('MODELS_DIR', '/opt/app/models/')
P = E['MODELS_PREFIX'].rstrip('/') + '/'
B = E['R2_BUCKET']
HF_REPO = E.get('HF_REPO', '')
HF_MAP = json.loads(E.get('HF_FILE_MAP', '{}') or '{}')  # {arquivo: 'repo:caminho'}
STALL_S = int(E.get('STALL_S', '180'))
BENCH_S = float(E.get('BENCH_S', '35'))
PART = 64 << 20; SUB = 8 << 20; TAIL = int(E.get('DL_TAIL_MB', '64')) << 20; NCONN = int(E.get('DL_CONN', '16'))
MAXERR = int(E.get('DL_MAXERR', '200')); SLOW_S = float(E.get('DL_SLOW_S', '10')); SLOW_REL = 0.30; SLOW_FLOOR = 1e6
SKIP = [s.strip() for s in E.get('MODELS_SKIP', '').split(',') if s.strip()]
s3 = boto3.client('s3', endpoint_url=E['R2_EP'], aws_access_key_id=E['R2_ACCESS_KEY_ID'],
                  aws_secret_access_key=E['R2_SECRET_ACCESS_KEY'], region_name='auto',
                  config=BCfg(read_timeout=60, connect_timeout=20, retries={'max_attempts': 3}, max_pool_connections=80))


def log(*a): print(*a, flush=True)


def split(o, end, sz): return [(x, min(x + sz, end + 1) - 1) for x in range(o, end + 1, sz)]


def gaps(cov, size):
  out, p = [], 0
  for o, e in sorted(cov):
    if o > p: out.append((p, o - 1))
    p = max(p, e + 1)
  if p < size: out.append((p, size - 1))
  return out


def covered(cov, size): return size - sum(e - o + 1 for o, e in gaps(cov, size))


R2REQ = [0]; ULT = ['?']


class R2:
  name = 'R2'
  def __init__(self, k): self.k = P + k
  def size(self): R2REQ[0] += 1; return s3.head_object(Bucket=B, Key=self.k)['ContentLength']
  def chunks(self, o, end):
    R2REQ[0] += 1
    return s3.get_object(Bucket=B, Key=self.k, Range=f'bytes={o}-{end}')['Body'].iter_chunks(1 << 20)


class HF:
  name = 'HF'
  def __init__(self, k):
    if k in HF_MAP: repo, path = HF_MAP[k].split(':', 1)
    elif HF_REPO: repo, path = HF_REPO, k
    else: raise RuntimeError('sem HF_REPO/HF_FILE_MAP')
    self.base = f'https://huggingface.co/{repo}/resolve/main/{path}'; self.url = None; self.lock = threading.Lock()
    tok = E.get('HF_TOKEN'); self.h = {'Authorization': 'Bearer ' + tok} if tok else {}
  def resolve(self, force=False):
    with self.lock:
      if self.url and not force: return self.url
      r = urllib.request.urlopen(urllib.request.Request(self.base, method='HEAD', headers=self.h), timeout=30)
      self.url, self._size = r.geturl(), int(r.headers.get('Content-Length') or 0)
      return self.url
  def size(self): self.resolve(True); return self._size
  def chunks(self, o, end):
    for i in range(2):
      try:
        r = urllib.request.urlopen(urllib.request.Request(self.resolve(i > 0), headers=dict(self.h, Range=f'bytes={o}-{end}')), timeout=60)
        if r.status != 206: raise RuntimeError(f'HF sem range (status {r.status})')
        break
      except urllib.error.HTTPError as e:
        if i or e.code not in (403, 410): raise
    def it():
      while True:
        c = r.read(1 << 20)
        if not c: return
        yield c
    return it()


def fetch(src, dst, size, cov, deadline=None, stats=None, nconn=None, part=None):
  nconn = nconn or NCONN; part = part or PART
  # baixa as lacunas de cov via src; devolve quando completo, parado STALL_S, ou deadline. cov e atualizado.
  cut = max(0, size - TAIL) // part * part
  q = []
  for g0, g1 in gaps(cov, size):
    if g1 < cut: q += split(g0, g1, part)
    else:
      if g0 < cut: q += split(g0, cut - 1, part); g0 = cut
      q += [r for r in split(g0, g1, SUB) for _ in (0, 1)]  # cauda duplicada (hedged)
  lock = threading.Lock(); cancel = threading.Event(); prog = [time.time()]; ativos = {}; feitos = set(); nerr = [0]
  st = stats if stats is not None else {}; st.setdefault('slow', 0); st.setdefault('bytes', 0)
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
          if r is None and not ativos: return
          me = object()
          if r: ativos[me] = 0.0
        if r is None: time.sleep(0.3); continue
        o, end = r; pos = o; t0 = time.time()
        try:
          for c in src.chunks(o, end):
            if cancel.is_set() or r in feitos: break
            n = min(len(c), end + 1 - pos)
            os.pwrite(fd, c[:n], pos); pos += n
            el = time.time() - t0; v = (pos - o) / max(el, 1e-3)
            with lock:
              st['bytes'] += n; prog[0] = time.time(); ativos[me] = v
              outros = [x for kk, x in ativos.items() if kk is not me and x > 0]
            if el > SLOW_S and pos <= end:
              med = statistics.median(outros) if len(outros) >= 3 else 0
              if v < SLOW_FLOOR or (med and v < SLOW_REL * med):
                with lock:
                  if pos > o: cov.append((o, pos - 1))
                  q[:0] = split(pos, end, SUB); st['slow'] += 1
                break
            if pos > end: break
          else: pass
          if pos == end + 1:
            with lock:
              if r not in feitos: feitos.add(r); cov.append(r)
          elif pos > o:
            with lock: cov.append((o, pos - 1))
        except Exception as e:
          log('WARN faixa', src.name, o, type(e).__name__, str(e)[:120])
          with lock:
            if pos > o: cov.append((o, pos - 1))
            nerr[0] += 1
            if pos <= end and nerr[0] <= MAXERR: q.append((pos, end))  # repede o resto da faixa (mesma origem)
          time.sleep(1)
        finally:
          with lock: ativos.pop(me, None)
    finally: os.close(fd)
  ths = [threading.Thread(target=worker, daemon=True) for _ in range(nconn)]
  for t in ths: t.start()
  motivo = 'ok'; last = time.time()
  while any(t.is_alive() for t in ths):
    time.sleep(0.5)
    with lock: falta = gaps(cov, size)
    if not falta: break
    if deadline and time.time() > deadline: motivo = 'deadline'; break
    if time.time() - prog[0] > STALL_S: motivo = 'stall'; log(f'STALL {STALL_S}s', src.name); break
    if time.time() - last > 20: last = time.time(); log('dl', src.name, os.path.relpath(dst, M), covered(cov, size) >> 20, 'MB')
  cancel.set()
  fim = time.time() + 70
  for t in ths: t.join(max(0.1, fim - time.time()))
  if gaps(cov, size) and motivo == 'ok': motivo = 'incompleto'
  return motivo


ORDEM = [('R2', 16, 64 << 20), ('HF', 16, 64 << 20)]  # (origem, conexoes, parte); bench reordena
# HF primeiro: em 01/11 o R2 completou o arquivo de 20 GB durante o bench e o HF nunca foi medido
FONTE = E.get('MODELS_SOURCE', 'bench').lower()  # bench | hf | r2 (a outra so como reserva apos 3 min parado/erro)
if FONTE == 'hf': ORDEM = [('HF', 16, 64 << 20), ('R2', 16, 64 << 20)]
elif FONTE == 'r2': ORDEM = [('R2', 16, 64 << 20), ('HF', 16, 64 << 20)]
BENCH_CFG = [('HF', 16, 64 << 20), ('R2', 16, 64 << 20), ('R2', 32, 64 << 20), ('R2', 64, 32 << 20)]


def get_file(k, dst, size, bench=False):
  global ORDEM
  with open(dst, 'ab') as f: f.truncate(size)
  cov = []; mk = {'R2': R2, 'HF': HF}; t0 = time.time(); stats = {}
  temhf = bool(HF_REPO or k in HF_MAP)
  if bench and FONTE == 'bench':
    res = {}
    for cfg in BENCH_CFG:
      nm, nc, pt = cfg
      if nm == 'HF' and not temhf: continue
      st = {}; tb = time.time()
      try: fetch(mk[nm](k), dst, size, cov, deadline=time.time() + BENCH_S, stats=st, nconn=nc, part=pt)
      except Exception as e: log('WARN bench', nm, type(e).__name__, str(e)[:150])
      res[cfg] = st.get('bytes', 0) / max(time.time() - tb, 1e-3) / 1e6
      log('PHASE bench', f'origem={nm}', f'conn={nc}', f'parte={pt >> 20}MB', f'MB/s={res[cfg]:.0f}')
      if not gaps(cov, size): break
    best = max(res, key=res.get)
    outra = max([c for c in res if c[0] != best[0]], key=res.get, default=None)
    ORDEM = [best] + ([outra] if outra else [])
    log('PHASE bench_escolha', f'origem={best[0]} conn={best[1]} parte={best[2] >> 20}MB', 'reserva=' + (outra[0] if outra else '-'))
  tent = 0
  ordem = ORDEM if temhf else [c for c in ORDEM if c[0] == 'R2'] or [('R2', 16, 64 << 20)]
  while gaps(cov, size) and tent < 4:
    nm, nc, pt = ordem[tent % len(ordem)]; tent += 1
    ULT[0] = nm
    try: m = fetch(mk[nm](k), dst, size, cov, stats=stats, nconn=nc, part=pt)
    except Exception as e: m = f'erro {type(e).__name__}: {str(e)[:150]}'
    if m != 'ok': log('RESUME', k, f'origem={nm}', m, f'{covered(cov, size) >> 20}MB')
  if gaps(cov, size): raise RuntimeError(f'incompleto apos {tent} tentativas')
  return time.time() - t0, stats.get('slow', 0)


hash_fail = []


def check_hash(path, want, k):
  h = hashlib.sha256()
  with open(path, 'rb') as f:
    for c in iter(lambda: f.read(16 << 20), b''): h.update(c)
  ok = h.hexdigest() == want
  log('HASH', k, 'ok' if ok else 'FAIL')
  if not ok: hash_fail.append(k)


def main():
  try: man = json.loads(s3.get_object(Bucket=B, Key=P + 'manifest.json')['Body'].read())
  except Exception as e: log('ABORT manifest', type(e).__name__, e); sys.exit(4)
  ths, failed, first = [], [], True; T0 = time.time()
  log('PHASE fonte', FONTE, 'ordem=' + ','.join(f'{o}/{c}' for o, c, _ in ORDEM))
  for k, v in sorted(man.items(), key=lambda kv: -kv[1]['size']):
    if any(s in k for s in SKIP): log('skip', k); continue
    dst = M + k; os.makedirs(os.path.dirname(dst), exist_ok=True)
    try:
      dt, slow = get_file(k, dst, v['size'], bench=first); first = False
    except Exception as e:
      log('WARN', k, type(e).__name__, str(e)[:300]); failed.append(k); continue
    log('PHASE dl_model', k, f'origem={ULT[0]}', f'{dt:.0f}s', f'{v["size"] / max(dt, 1e-3) / 1e6:.0f}MB/s', f'slow_repedidos={slow}', 'size_ok')
    th = threading.Thread(target=check_hash, args=(dst, v['sha256'], k)); th.start(); ths.append(th)
  for th in ths: th.join()
  if failed or hash_fail:
    log('PHASE r2_requests', f'total={R2REQ[0]}')
    log('PHASE models_verified FAIL', 'dl=' + ','.join(failed), 'hash=' + ','.join(hash_fail)); sys.exit(4)
  log('PHASE r2_requests', f'total={R2REQ[0]}')
  tot = sum(v['size'] for k, v in man.items() if not any(x in k for x in SKIP))
  log('PHASE models_total', f'{time.time() - T0:.0f}s', f'{tot / max(time.time() - T0, 1e-3) / 1e6:.0f}MB/s')
  log('PHASE models_verified ok')


if __name__ == '__main__': main()
