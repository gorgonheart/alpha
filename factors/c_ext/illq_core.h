/* factors/c_ext/illq_core.h */
#ifndef ILLQ_CORE_H
#define ILLQ_CORE_H

#ifdef __cplusplus
extern "C" {
#endif

/*
 * compute_illq: ILLQStdBbi45208 factor for one symbol, one param.
 *
 * open/high/low/close/qvol : price and quote_volume arrays, all length len
 * n                        : param (rolling window)
 * len                      : array length
 * out                      : output array, length len
 */
void compute_illq(
    const double *open,
    const double *high,
    const double *low,
    const double *close,
    const double *qvol,
    int n,
    int len,
    double *out
);

#ifdef __cplusplus
}
#endif

#endif /* ILLQ_CORE_H */
