# Termina o proprio pod via RunPod GraphQL (podTerminate). Nunca imprime a chave.
import json, os, sys, time, urllib.request

E = os.environ
if E.get('NO_DESTROY') == '1': print('selfdestroy: NO_DESTROY=1, pod mantido', flush=True); sys.exit(0)
key, pod = E.get('RUNPOD_API_KEY'), E.get('RUNPOD_POD_ID')
if not key or not pod: print('selfdestroy: sem RUNPOD_API_KEY/RUNPOD_POD_ID, nada a fazer', flush=True); sys.exit(0)

body = json.dumps({'query': 'mutation($id:String!){podTerminate(input:{podId:$id})}', 'variables': {'id': pod}}).encode()
req = urllib.request.Request('https://api.runpod.io/graphql', body,
                             {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + key})
for i in range(5):
  try:
    r = json.loads(urllib.request.urlopen(req, timeout=30).read())
    if not r.get('errors'): print('selfdestroy: podTerminate ok', pod, flush=True); sys.exit(0)
    print('selfdestroy: erro', json.dumps(r['errors'])[:500], flush=True)
  except Exception as e:
    print('selfdestroy: falha', type(e).__name__, str(e)[:300], flush=True)
  time.sleep(5)
sys.exit(1)
