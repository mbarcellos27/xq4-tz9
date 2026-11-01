FROM runpod/pytorch:1.0.2-cu1281-torch280-ubuntu2404

ARG NUNCHAKU_WHEEL=https://github.com/nunchaku-tech/nunchaku/releases/download/v1.2.1/nunchaku-1.2.1+cu12.8torch2.8-cp312-cp312-linux_x86_64.whl
ARG NUNCHAKU_NODE_TAG=v1.2.1
ARG SAGE_SPEC=sageattention
ARG GIT_SHA=unknown
ARG APP_REPO
ARG NODE_REPO

ENV PIP_BREAK_SYSTEM_PACKAGES=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 PYTHONUNBUFFERED=1
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# torch da base fica travado: constraints com as versoes instaladas
RUN { command -v python >/dev/null || ln -s "$(command -v python3)" /usr/local/bin/python; } && \
    { command -v git >/dev/null || { apt-get update && apt-get install -y --no-install-recommends git && apt-get clean && rm -rf /var/lib/apt/lists/*; }; } && \
    python -m pip list --format=freeze | grep -iE '^(torch|torchvision|torchaudio)==' > /opt/torch-constraints.txt && \
    cat /opt/torch-constraints.txt && grep -qi '^torch==' /opt/torch-constraints.txt && \
    { echo "image_git_sha=$GIT_SHA"; echo "built=$(date -u +%FT%TZ)"; echo "python=$(python -V 2>&1 | cut -d' ' -f2)"; sed 's/==/=/' /opt/torch-constraints.txt; } > /opt/BUILD_INFO

RUN git clone --depth 1 "$APP_REPO" /opt/app && \
    echo "app_commit=$(git -C /opt/app rev-parse HEAD)" >> /opt/BUILD_INFO && \
    python -m pip install -r /opt/app/requirements.txt -c /opt/torch-constraints.txt

# wheel pronto (sem compilar) + extensao
RUN python -m pip install "$NUNCHAKU_WHEEL" -c /opt/torch-constraints.txt && \
    git clone --depth 1 --branch "$NUNCHAKU_NODE_TAG" "$NODE_REPO" /opt/app/custom_nodes/nk && \
    { [ ! -f /opt/app/custom_nodes/nk/requirements.txt ] || \
      python -m pip install -r /opt/app/custom_nodes/nk/requirements.txt -c /opt/torch-constraints.txt; } && \
    echo "nunchaku=$(python -c 'import importlib.metadata as m;print(m.version("nunchaku"))') node=$NUNCHAKU_NODE_TAG" >> /opt/BUILD_INFO

# sageattention opcional: sem wheel/erro o build segue
RUN if python -m pip install "$SAGE_SPEC" -c /opt/torch-constraints.txt >&2; then \
      echo "sageattention=$(python -c 'import importlib.metadata as m;print(m.version("sageattention"))')"; \
    else echo "sageattention=none"; fi >> /opt/BUILD_INFO

RUN python -m pip install boto3 "huggingface_hub[cli]" hf_transfer requests -c /opt/torch-constraints.txt && \
    { python -m pip cache purge >/dev/null 2>&1 || true; } && rm -rf /root/.cache /tmp/* /var/lib/apt/lists/* && \
    cat /opt/BUILD_INFO

COPY --chmod=755 entrypoint.sh fetch_models.py worker.py selfdestroy.py smoke.py post_bench.py /opt/w/
WORKDIR /opt/app
ENTRYPOINT ["/opt/w/entrypoint.sh"]
CMD []
