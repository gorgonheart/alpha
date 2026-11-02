/* factors/c_ext/illq_core.c */
#include <math.h>
#include <stdlib.h>
#include "illq_core.h"

/* rolling_mean: window w, min_periods=1, skip NaN — matches pandas rolling(w,min_periods=1).mean() */
static void roll_mean(const double *x, int n, int w, double *out) {
    double sum = 0.0;
    int    cnt = 0;
    for (int i = 0; i < n; i++) {
        if (!isnan(x[i])) { sum += x[i]; cnt++; }
        if (i >= w && !isnan(x[i - w])) { sum -= x[i - w]; cnt--; }
        out[i] = (cnt >= 1) ? sum / cnt : NAN;
    }
}

/* rolling_std: window w, ddof=1, min_periods=1 — matches pandas rolling(w,min_periods=1).std() */
static void roll_std(const double *x, int n, int w, double *out) {
    /* Compensated online central moment; subtracting squared raw sums loses
       precision when the level is large relative to the variance. Follow the
       remove-before-add order used by pandas' fixed rolling windows. */
    double mean = 0.0, moment = 0.0, add_error = 0.0, remove_error = 0.0;
    double last = NAN;
    int cnt = 0, repeated = 0;
    for (int i = 0; i < n; i++) {
        if (i >= w && isfinite(x[i - w])) {
            double v = x[i - w];
            cnt--;
            if (cnt) {
                double old = mean - remove_error;
                double corrected = v - remove_error;
                double delta = corrected - mean;
                remove_error = (delta + mean) - corrected;
                mean -= delta / cnt;
                moment -= (v - old) * (v - mean);
            } else { mean = moment = 0.0; }
        }
        /* pandas rolling converts +/-Inf to missing before accumulating. */
        if (isfinite(x[i])) {
            double v = x[i];
            cnt++;
            repeated = (v == last) ? repeated + 1 : 1;
            last = v;
            double old = mean - add_error;
            double corrected = v - add_error;
            double delta = corrected - mean;
            add_error = (delta + mean) - corrected;
            mean += delta / cnt;
            moment += (v - old) * (v - mean);
        }
        double variance = repeated >= cnt ? 0.0 : moment / (cnt - 1);
        out[i] = cnt < 2 ? NAN : (variance > 0.0 ? sqrt(variance) : 0.0);
    }
}

void compute_illq(
    const double *open, const double *high, const double *low,
    const double *close, const double *qvol,
    int n, int len, double *out
) {
    double *ilq_sum  = (double *)malloc(len * sizeof(double));
    double *bbi1     = (double *)malloc(len * sizeof(double));
    double *bbi2     = (double *)malloc(len * sizeof(double));
    double *bbi3     = (double *)malloc(len * sizeof(double));
    double *bbi4     = (double *)malloc(len * sizeof(double));
    double *bbi      = (double *)malloc(len * sizeof(double));
    double *bbi_bias = (double *)malloc(len * sizeof(double));
    double *std_ilq  = (double *)malloc(len * sizeof(double));

    /* ilq_sum = ilq + ilq2 element-wise (IEEE 754: x/0 = ±Inf, 0/0 = NaN — same as pandas) */
    for (int i = 0; i < len; i++) {
        double zhenfu = (high[i] - low[i]) / open[i];
        double ilq2   = qvol[i] / zhenfu;
        double route  = (2.0 * (high[i] - low[i]) - fabs(open[i] - close[i])) / open[i];
        double ilq    = qvol[i] / route;
        ilq_sum[i]    = ilq + ilq2;
    }

    /* BBI = average of rolling means at windows n, 2n, 4n, 6n */
    roll_mean(close, len, n,       bbi1);
    roll_mean(close, len, 2 * n,   bbi2);
    roll_mean(close, len, 4 * n,   bbi3);
    roll_mean(close, len, 6 * n,   bbi4);
    for (int i = 0; i < len; i++) {
        bbi[i]      = (bbi1[i] + bbi2[i] + bbi3[i] + bbi4[i]) / 4.0;
        bbi_bias[i] = close[i] / bbi[i];
    }

    /* factor = rolling_std(ilq_sum, n) * bbi_bias */
    roll_std(ilq_sum, len, n, std_ilq);
    for (int i = 0; i < len; i++)
        out[i] = std_ilq[i] * bbi_bias[i];

    free(ilq_sum); free(bbi1); free(bbi2); free(bbi3); free(bbi4);
    free(bbi); free(bbi_bias); free(std_ilq);
}
