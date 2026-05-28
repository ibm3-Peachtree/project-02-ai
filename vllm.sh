#!/bin/bash
# export HF_HUB_ENABLE_HF_TRANSFER=1
# export TRANSFORMERS_VERIFY_ARCHITECTURE=0 # <- 유효성 검사 해제

# echo "[vLLM] kakaocorp/kanana-1.5-2.1b-instruct-2505 시작합니다..."

# vllm serve kakaocorp/kanana-1.5-2.1b-instruct-2505 \
#     --port 8001 \
#     --trust-remote-code \
#     --max-model-len 4096

# echo "2.1B 로딩을 위해 60초간 대기합니다..."
# sleep 60

echo "[vLLM] kakaocorp/kanana-1.5-8b-instruct-2505 시작합니다..."

vllm serve kakaocorp/kanana-1.5-8b-instruct-2505 \
    --port 8002 \
    --trust-remote-code \
    --max-model-len 20000