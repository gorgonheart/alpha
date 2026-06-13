/* factors/c_ext/cci_core.h */
#ifndef CCI_CORE_H
#define CCI_CORE_H

#ifdef __cplusplus
extern "C" {
#endif

/*
 * compute_cci: CciV3 factor for one symbol, one param.
 *
 * open/high/low/close : price arrays, all length len
 * n                   : span parameter (alpha = 2/(n+1))
 * window              : ewm_fixed window (15000 in Python)
 * len                 : array length
 * out                 : output array, length len
 */
void compute_cci(
    const double *open,
    const double *high,
    const double *low,
    const double *close,
    int n,
    int window,
    int len,
    double *out
);

#ifdef __cplusplus
}
#endif

#endif /* CCI_CORE_H */
