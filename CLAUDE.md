# CLAUDE.md

Current cross-project unified entry point: [统一开发入口与全局摘要](../alphaonline/docs/README.md)（专题知识库见 `D:/alphaonline/docs/kb/`，流水日志见 [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md)）. Read it first; it reflects the current 1.1.12 / 2.3.3 baseline and supersedes historical architecture descriptions below.

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

BMAC (Binance Marketdata Async Client) — a crypto market **data center / producer**. It runs a continuous 5-minute loop that fetches Binance spot + USDT-perp K-lines (via Binance REST and the Quantclass Data API), resamples 5m base candles into 1h candles at multiple minute-offsets, preprocesses them into per-batch `{symbol: DataFrame}` files, embeds precomputed factor columns, and publishes snapshots for a downstream consumer (`d:/alphaonline`) to read.

This repo is the **producer side** of a two-project contract. Changes to storage format, factor columns, or the publish gate can break the downstream consumer — see `docs/superpowers/specs/2026-06-27-producer-io-and-arrow-publish-design.md` for the cross-project contract (§5).

## Environment & commands

**All Python must run in conda env `alpha311`.** The reliable invocation (plain `conda run` crashes intermittently on this machine's conda 4.9.2):

```powershell
conda run --no-capture-output -n alpha311 python <script>
```

Run the producer:
```powershell
conda run --no-capture-output -n alpha311 python realtime_data.py
```

Run tests (pytest, real-data-grounded):
```powershell
conda run --no-capture-output -n alpha311 python -m pytest
conda run --no-capture-output -n alpha311 python -m pytest tests/test_module8_arrow_publish.py        # single file
conda run --no-capture-output -n alpha311 python -m pytest tests/test_module9_embedded_factors.py::test_name   # single test
```

Real-data equivalence validation (not a unit test — reads live production pickles under `data/preprocess_1h_resample/`):
```powershell
conda run --no-capture-output -n alpha311 python tests/validate_against_real_data.py
```

**C extensions must be compiled per-platform** (cffi → `.pyd`/`.so`). The committed `.pyd` is Windows-only; rebuild on the Linux deploy target. Three historical extensions (plus the new versioned momentum V2 extension described below):
```powershell
conda run --no-capture-output -n alpha311 python factors/c_ext/build.py        # momentum -> _momentum_core
conda run --no-capture-output -n alpha311 python factors/c_ext/illq_build.py    # ILLQ
conda run --no-capture-output -n alpha311 python factors/c_ext/cci_build.py     # CCI
```

Deploy (see `部署命令.md`): `git archive main` → scp tarball to server → rebuild the 3 C extensions there → set `proxy = None` in config → `nohup ... realtime_data.py`. `.gitattributes` export-ignore strips tests/docs/`.pyd` from the production archive.

## Architecture

### Run loop (`realtime_data.py`)
`main()` builds a `BmacHandle` from config, then loops: `init_history()` (download + resample full history on startup/restart) → inner loop of `update_all(handle, run_time)` once per base interval. `update_all` skips run_times whose minute isn't in `enabled_hour_offsets`. Within a cycle it uses a `ProcessPoolExecutor` to run Binance fetch, Data API fetch, and coin-cap update concurrently; **preprocessing kicks off as soon as the first data source succeeds** (`as_completed`). Errors are caught at the top level, pushed to a WeChat-work webhook, and the whole thing restarts after 60s.

### Config → Handle pattern (`config.py`, `core/handle.py`)
`config.py` is the single source of all tunables (API keys, `enabled_hour_offsets`, `kline_count_1h_spot/swap`, `precompute_factor_configs`, `preprocess_num_batch`, `publish_kline_num`, `proxy`). A `config.json` next to it can override a whitelisted subset at runtime. `create_bmac_handle_from_config()` turns config into a `BmacHandle` dataclass tree (`BinanceHandle` ×2, `DataApiHandle`, `PreprocessHandle`, `CoinCapHandle`). **Pass `handle` everywhere; read `config` only at construction.** Most functions take `(handle, run_time, ...)`.

### Storage layer (`core/candle_manager.py`)
`CandleFileManager` / `CandleFileReader` own all on-disk K-line I/O (pickle default; feather/parquet supported). Key convention: **data files and a separate `.ready` gate flag** (`{symbol}_{int(run_time.timestamp())}.ready`). `set_candle` = `save_data_file` + `save_ready`; `save_ready` globs and removes stale `.ready` files so only the latest run's flag survives. **The consumer gates on `.ready`, never on the data file** — so a crash that leaves a data file without its flag safely triggers a full rebuild rather than serving partial data. Data is partitioned by minute-offset subdir (e.g. `preprocess_1h_resample/10m/`).

### Incremental update (the "Module 1–5" design, see `修改内容.md`)
Each heavy rebuild step has an incremental twin that processes only new rows and **falls back to the full path** on any anomaly (no cache, overlapping/duplicate timestamps, symbol-set change, large gaps):
- Module 1 `update_resample_candle_incremental` (`resample_candle.py`)
- Module 2 `update_candle_incremental` (`candle_manager.py`)
- Module 3 `gen_batch_data_incremental` (`preprocess.py`) — appends only `candle_begin_time > last_ts` rows per symbol, inherits `first_candle_time`
- Module 4 `gen_pivot_incremental` (`preprocess.py`)
- Module 5 — `preprocess_type_data` wired to use the incremental twins; no longer wipes batch caches each cycle

Contract for every incremental fn: first run (no cache) is byte-identical to the full fn; later runs equal re-running the full fn on combined data. Full history must be retained (`kline_count_1h ≈ 14060`) because the largest factor window (13608) needs it.

### Factor system (`factors/`, `core/preprocess.py`)
Each factor module exposes `signal_multi_params(df, param_list) -> {str(param): pd.Series}`. The hot loop is a **pure, stateless C extension** (`factors/c_ext/*_wrapper.py` bridges to the compiled core); output is a deterministic function of the full input arrays, so recomputing on full history each cycle == a from-scratch full computation. Configured in `precompute_factor_configs = {factor_module_name: [params]}`; columns are named `{factor_name}_{param}`.

`apply_precomputed_factors(df, factor_modules_params)` in `preprocess.py` is the **single source of truth** for factor embedding — both the legacy two-step `gen_factor_data` path and the one-step `gen_batch_data_incremental(with_factors=True)` path call it, guaranteeing identical values/dtypes. `_load_factor_modules_params()` resolves config into `(module, name, params)` tuples.

Note (`factors_old/` vs `factors/`): the C-backed `factors/` versions are validated against pure-Python references. Momentum diverges for symbols whose row count < window `n` (rolling window never fills) — this is by design, not a pipeline bug, and is the same in both the full and embedded paths.

### Write-once + race fix + Arrow publish (current design, `2026-06-27` spec)
`preprocess_type_data` ordering matters and is deliberate:
1. Generate each batch with `gen_batch_data_incremental(..., with_factors=True)` — persist raw history once; return an in-memory dictionary with factors embedded for Arrow, with no read-back/rewrite.
2. `publish_arrow_snapshot_for_type(...)` — merges batches into one atomic Arrow IPC snapshot `{trade_type}_dict_{epoch}.arrow` (one record batch per symbol + a JSON symbol manifest in schema metadata; tmp→fsync→`os.replace`). **Purely additive**: wrapped in try/except, failure only logs and never blocks the pkl path; consumer auto-falls-back to pickles.
3. `save_ready('{trade_type}_dict', run_time)` **last** — the consumer's gate flag is written only after factor columns are on disk and the snapshot is published (the §3.2 race fix). Arrow publish uses `write_ready=False` so the pipeline owns the single market-dict gate flag.

`core/arrow_publish.py` (`publish_arrow_snapshot` / `read_arrow_snapshot`) is standalone and additive. `publish_kline_num` (default 5000) tail-trims each symbol **after** factors are computed; must stay ≥ the consumer's needs (4050 + run_time filter margin).

**Two independent cross-project artifacts, two independent `.ready` gates — do not conflate them:**
1. **Market dict** — batch PKL keeps raw full retained history; Arrow carries precomputed factors and a recent window. The consumer prefers Arrow and **still falls back to batch PKL**. New momentum V2 missing from an old snapshot triggers full-history PKL computation before trimming. Do not remove that fallback source.
2. **Pivot** — year-partitioned market_pivot PKL is a live independent contract and has its own ready gate. The consumer uses the two-argument PKL reader directly; consumer-side Arrow pivot reconstruction was removed.

Current factor migration: the configured momentum is `动量210401_双项异常隔离V2`, built with `factors/c_ext/momentum_v2_build.py`. The old momentum extension remains available only for historical compatibility/tests. CciV3 and ILLQ are unchanged. The consumer's abnormal CCI is a different factor and is not CciV3.

## Testing conventions
Tests combine **real BTCUSDT history** with synthetic anomaly/boundary fixtures. The corrected momentum suite uses the independently supplied consumer Python implementation as its oracle. Harnesses live in `tests/support/` (`harness_common.py` has `assert_dataframes_strict_equal` with `check_dtype=True, check_exact=True`, and `benchmark_callable`; per-module harnesses build cases from real pickles). The dominant test pattern is **equivalence**: prove the new/incremental path produces output identical (exact, NaN-aware) to the legacy/full path. Numbered `test_moduleN_*.py` files map to the modules above.
