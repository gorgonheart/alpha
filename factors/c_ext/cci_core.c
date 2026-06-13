/* factors/c_ext/cci_core.c */
#include <math.h>
#include <stdlib.h>
#include "cci_core.h"

/*
 * ewm_fixed_inner: matches Python ewm_fixed(arr, window, span=n, min_periods=2).
 *
 * EWM initializes from first non-NaN element of arr (adjust=False behaviour).
 * Correction formula (for t >= window when both y[t-window] and arr[t-window+1] are valid):
 *   out[t] = y[t] - beta_w * y[t-window] + beta_w * arr[t-window+1]
 * Otherwise:
 *   out[t] = y[t]   (residual = 0, i.e. Python fillna(0) applied)
 * min_periods=2: out[0] = NaN unconditionally.
 */
static void ewm_fixed_inner(
    const double *arr,
    double alpha, double beta_w, int window,
    int len, double *out
) {
    double *y = (double *)malloc(len * sizeof(double));

    /* Standard EWM with adjust=False: init from first non-NaN */
    double prev = NAN;
    for (int t = 0; t < len; t++) {
        if (isnan(arr[t])) {
            y[t] = NAN;
        } else if (isnan(prev)) {
            y[t] = prev = arr[t];            /* first valid value initialises EWM */
        } else {
            y[t] = prev = alpha * arr[t] + (1.0 - alpha) * prev;
        }
    }

    /* Apply windowed correction */
    for (int t = 0; t < len; t++) {
        if (isnan(y[t])) {
            out[t] = NAN;
        } else if (t >= window
                   && !isnan(y[t - window])
                   && !isnan(arr[t - window + 1])) {
            out[t] = y[t] - beta_w * y[t - window] + beta_w * arr[t - window + 1];
        } else {
            out[t] = y[t];   /* t < window or NaN in residual → fillna(0) */
        }
    }

    out[0] = NAN;   /* min_periods=2 */
    free(y);
}

void compute_cci(
    const double *open, const double *high, const double *low, const double *close,
    int n, int window, int len, double *out
) {
    double alpha  = 2.0 / (n + 1.0);
    double beta_w = pow(1.0 - alpha, (double)window);

    double *oma      = (double *)malloc(len * sizeof(double));
    double *hma      = (double *)malloc(len * sizeof(double));
    double *lma      = (double *)malloc(len * sizeof(double));
    double *cma      = (double *)malloc(len * sizeof(double));
    double *tp       = (double *)malloc(len * sizeof(double));
    double *ma       = (double *)malloc(len * sizeof(double));
    double *abs_diff = (double *)malloc(len * sizeof(double));
    double *md       = (double *)malloc(len * sizeof(double));

    ewm_fixed_inner(open,  alpha, beta_w, window, len, oma);
    ewm_fixed_inner(high,  alpha, beta_w, window, len, hma);
    ewm_fixed_inner(low,   alpha, beta_w, window, len, lma);
    ewm_fixed_inner(close, alpha, beta_w, window, len, cma);

    for (int i = 0; i < len; i++)
        tp[i] = (oma[i] + hma[i] + lma[i] + cma[i]) / 4.0;

    ewm_fixed_inner(tp, alpha, beta_w, window, len, ma);

    for (int i = 0; i < len; i++)
        abs_diff[i] = (isnan(cma[i]) || isnan(ma[i])) ? NAN : fabs(cma[i] - ma[i]);

    ewm_fixed_inner(abs_diff, alpha, beta_w, window, len, md);

    for (int i = 0; i < len; i++) {
        if (isnan(tp[i]) || isnan(ma[i]) || isnan(md[i]) || fabs(md[i]) < 1e-15)
            out[i] = NAN;
        else
            out[i] = (tp[i] - ma[i]) / md[i];
    }

    free(oma); free(hma); free(lma); free(cma);
    free(tp);  free(ma);  free(abs_diff); free(md);
}
