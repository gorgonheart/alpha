#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
PYTHON="${ALPHA_PYTHON:-/home/ubuntu/miniconda3/envs/alpha311/bin/python}"
if [[ ! -x "$PYTHON" ]]; then echo "Set ALPHA_PYTHON to the alpha311 interpreter" >&2; exit 1; fi
build() {
  for entry in build.py momentum_v2_build.py illq_build.py cci_build.py; do
    "$PYTHON" -X utf8 -B "factors/c_ext/$entry"
  done
}
verify() {
  "$PYTHON" -X utf8 -B tools/run_deployment_tests.py \
    tests/test_deployment_config.py tests/test_coin_cap_proxy.py tests/test_deployment_test_directory.py tests/test_dispatch_prewarm.py \
    tests/test_realtime_recovery.py tests/test_exchange_info_reuse.py \
    tests/test_shared_factor_pool.py tests/test_module8_arrow_publish.py \
    tests/test_arrow_single_snapshot.py \
    tests/test_module9_embedded_factors.py \
    tests/test_module10_preprocess_pipeline.py tests/test_corrected_momentum.py \
    tests/test_illq_nonfinite_oracle.py tests/test_module_cci.py
}
case "${1:-status}" in
  build) build ;;
  verify) verify ;;
  update)
    if pgrep -f "$(pwd)/realtime_data.py" >/dev/null; then
      echo 'Stop only this installation and wait for its children before updating.' >&2; exit 1
    fi
    if [[ -n "$(git status --porcelain --untracked-files=no)" ]]; then
      echo 'Tracked files have local changes; inspect them before updating.' >&2; exit 1
    fi
    git fetch origin
    git switch main
    git merge --ff-only origin/main
    build
    verify
    ;;
  status) git status --short; git log -1 --format='%h %s'; "$PYTHON" --version ;;
  *) echo 'Usage: bash tools/deploy_alpha.sh {status|build|verify|update}' >&2; exit 2 ;;
esac
