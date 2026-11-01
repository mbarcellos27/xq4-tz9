# Consome QUEUE_PREFIX/queue.json via servidor local; sobe out/<name>.mp4 + .json a cada job.
# Sai 0 quando nao ha mais job pendente sem saida; 5 se todos os jobs tentados falharem.
import glob, json, os, sys, time, traceback, urllib.error, urllib.request
import boto3

E = os.environ
B = E['R2_BUCKET']
Q = E['QUEUE_PREFIX'].rstrip('/') + '/'
GPU = E.get('GPU', '')
# nomes de modelos/no vem do ambiente (obrigatorios, sem padrao)
DEF = dict(width=480, height=864, steps=8, unet=E['M_UNET'])
M_TE, M_TE_TYPE, M_VAE_V, M_VAE_A, M_LORA, N_GEN = (E[k] for k in ('M_TE', 'M_TE_TYPE', 'M_VAE_V', 'M_VAE_A', 'M_LORA', 'N_GEN'))
s3 = boto3.client('s3', endpoint_url=E['R2_EP'], aws_access_key_id=E['R2_ACCESS_KEY_ID'],
                  aws_secret_access_key=E['R2_SECRET_ACCESS_KEY'], region_name='auto')


def log(*a): print(*a, flush=True)


def api(p, d=None):
  req = urllib.request.Request('http://127.0.0.1:8188' + p, json.dumps(d).encode() if d else None,
                               {'Content-Type': 'application/json'})
  try: return json.loads(urllib.request.urlopen(req, timeout=60).read())
  except urllib.error.HTTPError as e: raise RuntimeError(f'HTTP {e.code} {p}: {e.read().decode()[:2000]}')


def out_exists(n): return s3.list_objects_v2(Bucket=B, Prefix=Q + 'out/' + n + '.mp4').get('KeyCount', 0) > 0


def run(w):
  pid = api('/prompt', {'prompt': w})['prompt_id']
  while True:
    h = api('/history/' + pid)
    if pid in h and h[pid].get('status', {}).get('completed') is not None: return h[pid]['status']
    time.sleep(5)


def workflow(j, imgs, p):
  w = {"1": {"class_type": "UNETLoader", "inputs": {"unet_name": p['unet'], "weight_dtype": "default"}},
       "2": {"class_type": "CLIPLoader", "inputs": {"clip_name": M_TE, "type": M_TE_TYPE, "device": "default"}},
       "3": {"class_type": "VAELoader", "inputs": {"vae_name": M_VAE_V}},
       "4": {"class_type": "VAELoader", "inputs": {"vae_name": M_VAE_A}},
       "L": {"class_type": "LoraLoaderModelOnly", "inputs": {"model": ["1", 0], "lora_name": M_LORA, "strength_model": 1.0}}}
  ri = {}
  for i, n in enumerate(imgs):
    w[f"i{i}"] = {"class_type": "LoadImage", "inputs": {"image": n}}; ri[f"ref_images.ref_image_{i}"] = [f"i{i}", 0]
  w["5"] = {"class_type": N_GEN, "inputs": {"clip": ["2", 0], "vae": ["3", 0], "audio_vae": ["4", 0], "prompt": j['prompt'],
            "width": p['width'], "height": p['height'], "length": j['length'], "ref_image_size": "match", **ri}}
  w.update({"6": {"class_type": "RandomNoise", "inputs": {"noise_seed": j['seed']}},
            "7": {"class_type": "KSamplerSelect", "inputs": {"sampler_name": "res_multistep"}},
            "8": {"class_type": "BasicScheduler", "inputs": {"model": ["L", 0], "scheduler": "simple", "steps": p['steps'], "denoise": 1.0}},
            "9": {"class_type": "BasicGuider", "inputs": {"model": ["L", 0], "conditioning": ["5", 0]}},
            "10": {"class_type": "SamplerCustomAdvanced", "inputs": {"noise": ["6", 0], "guider": ["9", 0], "sampler": ["7", 0], "sigmas": ["8", 0], "latent_image": ["5", 1]}},
            "11": {"class_type": "VAEDecode", "inputs": {"samples": ["10", 0], "vae": ["3", 0]}},
            "12": {"class_type": "VAEDecodeAudio", "inputs": {"samples": ["10", 0], "vae": ["4", 0]}},
            "13": {"class_type": "CreateVideo", "inputs": {"images": ["11", 0], "fps": 24.0, "audio": ["12", 0]}},
            "14": {"class_type": "SaveVideo", "inputs": {"video": ["13", 0], "filename_prefix": "raw/" + j['name'], "format": "auto", "codec": "auto"}}})
  return w


def do_job(j):
  t = time.time()
  p = {k: j.get(k, v) for k, v in DEF.items()}
  imgs = []
  for n in j['refs']:
    b = os.path.basename(n); s3.download_file(B, Q + 'refs/' + n, '/opt/app/input/' + b); imgs.append(b)
  st = run(workflow(j, imgs, p)); gen = time.time() - t
  log('gen', j['name'], st.get('status_str'), f'{gen:.0f}s')
  if st.get('status_str') != 'success': raise RuntimeError(json.dumps(st.get('messages'))[-1500:])
  src = max(glob.glob(f"/opt/app/output/raw/{j['name']}*.mp4"), key=os.path.getmtime)
  s3.upload_file(src, B, Q + 'out/' + j['name'] + '.mp4')
  meta = dict(gen_s=round(gen), total_s=round(time.time() - t), gpu=GPU, seed=j['seed'], length=j['length'], **p)
  s3.put_object(Bucket=B, Key=Q + 'out/' + j['name'] + '.json', Body=json.dumps(meta).encode())


def main():
  tried, ok, fail = set(), 0, 0
  while True:
    jobs = json.loads(s3.get_object(Bucket=B, Key=Q + 'queue.json')['Body'].read())
    todo = [j for j in jobs if j.get('status') == 'pending' and j['name'] not in tried and not out_exists(j['name'])]
    if not todo: break
    j = todo[0]; tried.add(j['name'])
    try:
      do_job(j); ok += 1
      if ok == 1: log('PHASE first_item', j['name'])
    except Exception as e:
      fail += 1; err = f'{type(e).__name__}: {e}'
      log('ERROR job', j['name'], err[-1500:])
      try: s3.put_object(Bucket=B, Key=Q + 'out/' + j['name'] + '.error.json',
                         Body=json.dumps(dict(error=err, trace=traceback.format_exc()[-4000:], gpu=GPU)).encode())
      except Exception as e2: log('WARN upload erro', j['name'], e2)
  log(f'fila vazia ok={ok} fail={fail}')
  sys.exit(5 if fail and not ok else 0)


if __name__ == '__main__': main()
