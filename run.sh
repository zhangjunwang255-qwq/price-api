#!/bin/bash
# price-api/run.sh  — 环境变量由调用方配置

cd "$(dirname "$0")"
python -m app.main
