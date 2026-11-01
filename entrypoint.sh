#!/bin/bash
# Entrypoint: rede -> modelos -> servidor -> worker -> DONE -> selfdestroy.
set -u
export PYTHONUNBUFFERED=1 TZ=UTC
N=/opt/w
[ "${SMOKE:-0}" = 1 ] && exec python $N/smoke.py

L=/w.log
lg(){ local s; printf -v s '%(%H:%M:%S)T +%ss %s' -1 "$SECONDS" "$*"; echo "$s"; echo "$s" >> $L; }
# roda comando com saida prefixada no log; devolve o codigo do comando
run(){ "$@" 2>&1 | while IFS= read -r l; do lg "$l"; done; return "${PIPESTATUS[0]}"; }
PY="import boto3,os,sys;s3=boto3.client('s3',endpoint_url=os.environ['R2_EP'],aws_access_key_id=os.environ['R2_ACCESS_KEY_ID'],aws_secret_access_key=os.environ['R2_SECRET_ACCESS_KEY'],region_name='auto')"
up(){ python -c "$PY;s3.upload_file(sys.argv[1],os.environ['R2_BUCKET'],sys.argv[2])" "$1" "$2" 2>/dev/null; }
up_log(){ up $L "${Q}pod.log"; }
destroy(){ run python $N/selfdestroy.py; up_log; }

for v in R2_EP R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY R2_BUCKET QUEUE_PREFIX MODELS_PREFIX; do
  [ -n "${!v:-}" ] || { lg "ABORT env $v ausente"; [ -n "${QUEUE_PREFIX:-}" ] && Q=${QUEUE_PREFIX%/}/ && up_log; python $N/selfdestroy.py; exit 2; }
done
Q=${QUEUE_PREFIX%/}/

HB=; WD=
fin(){
  local rc=$?; trap - EXIT ERR
  kill $HB $WD 2>/dev/null
  lg "EXIT rc=$rc"; [ $rc != 0 ] && [ -f /srv.log ] && tail -40 /srv.log | while IFS= read -r l; do lg "srv| $l"; done
  up_log
  if [ "${NO_DESTROY:-0}" = 1 ]; then lg "NO_DESTROY=1: pod mantido"; up_log; sleep infinity; fi
  destroy; exit $rc
}
trap fin EXIT
trap 'rc=$?; lg "ABORT err linha $LINENO rc=$rc"; exit $rc' ERR

(while :; do up_log; sleep 10; done) & HB=$!  # log ao R2 desde o 1o segundo
HARD=${HARD_TIMEOUT_S:-3600}
(sleep "$HARD"; lg "ABORT hard_timeout ${HARD}s"; up_log; destroy) & WD=$!

lg "BUILD $(tr '\n' ' ' < /opt/BUILD_INFO 2>/dev/null)"
export GPU=$(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null | head -1)
lg "PHASE pod_up ${GPU:-sem_gpu}"

# DNS/R2; fallback resolv.conf 1.1.1.1 (como no setup antigo)
H=${R2_EP#https://}; H=${H%%/*}
nt(){ getent hosts "$H" >/dev/null && curl -s -m 10 -o /dev/null -w '%{http_code}' "https://$H/" | grep -qE '^[1-5][0-9][0-9]$'; }
ok=0; for k in $(seq 1 12); do if nt; then ok=1; break; fi; sleep 5; done
if [ $ok = 0 ]; then
  lg "net falhou, resolv.conf -> 1.1.1.1"
  printf 'nameserver 1.1.1.1\nnameserver 8.8.8.8\n' > /etc/resolv.conf
  for k in $(seq 1 6); do if nt; then ok=1; break; fi; sleep 5; done
fi
lg "PHASE net ok=$ok"
[ $ok = 1 ] || { lg "ABORT net"; exit 6; }

run python -c "import torch,sys;ok=torch.cuda.is_available();print('torch',torch.__version__,'cuda',ok,torch.cuda.get_device_capability() if ok else '');sys.exit(0 if ok else 1)" \
  || { lg "ABORT cuda indisponivel"; exit 7; }

lg "PHASE fetch"
run python $N/fetch_models.py || { rc=$?; lg "ABORT fetch rc=$rc"; exit $rc; }

FLAGS=
if [ "${SAGE:-0}" = 1 ]; then
  if python -c "import sageattention" 2>/dev/null; then FLAGS=--use-sage-attention; else lg "SAGE=1 mas sageattention ausente; seguindo sem"; fi
fi
cd /opt/app
python main.py --listen 127.0.0.1 --port 8188 $FLAGS > /srv.log 2>&1 & CP=$!
up_ok=0
for i in $(seq 1 120); do
  kill -0 $CP 2>/dev/null || break
  if curl -sf -m 5 -o /dev/null http://127.0.0.1:8188/system_stats; then up_ok=1; break; fi
  sleep 5
done
[ $up_ok = 1 ] || { lg "ABORT srv nao subiu"; exit 8; }
lg "PHASE srv_up $FLAGS"

run python $N/worker.py || { rc=$?; lg "ABORT worker rc=$rc"; exit $rc; }
lg "PHASE postbench"; run python $N/post_bench.py || lg "WARN postbench rc=$?"
lg "PHASE done"
date -u +%FT%TZ > /DONE; up /DONE "${Q}DONE" || lg "WARN upload DONE falhou"
exit 0
