#ifndef MOMENTUM_CORE_H
#define MOMENTUM_CORE_H

#ifdef __cplusplus
extern "C" {
#endif

/*
 * compute_momentum: 计算单个 symbol 单个 param 的动量因子
 *
 * close/high/low : 全量 K 线数组，长度 len
 * n              : span 参数（如 168）
 * len            : 数组长度（全量）
 * trunc_len      : 截取末尾条数（52560），若 len < trunc_len 则用 len
 * out            : 输出数组，长度 len，前 (len-trunc_len) 个填 NaN
 */
void compute_momentum(
    const double *close,
    const double *high,
    const double *low,
    int n,
    int len,
    int trunc_len,
    double *out
);

#ifdef __cplusplus
}
#endif

#endif /* MOMENTUM_CORE_H */
