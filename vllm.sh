#!/bin/bash
vllm serve kakaocorp/kanana-1.5-8b-instruct-2505 \
    --port 8002 \
    --trust-remote-code \
    --max-model-len 20000

# vllm serve skt/A.X-3.1-Light \
#     --port 8002 \
#     --trust-remote-code \
#     --max-model-len 10000