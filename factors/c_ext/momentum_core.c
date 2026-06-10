/* factors/c_ext/momentum_core.c
 *
 * 动量因子核心算法 C 实现。
 * 算法与 Python 原版（factors/动量因子210401修正.py）完全对应，
 * 通过 validate.py 逐元素对比验证。
 */
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include "momentum_core.h"

/* ── 单调双端队列（用于 rolling min/max）─────────────────────── */
#define DQ_CAP 65536

typedef struct {
    int    idx[DQ_CAP];
    double val[DQ_CAP];
    int    head, tail;
} Deque;

static void dq_clear(Deque *d)               { d->head = d->tail = 0; }
static int  dq_empty(const Deque *d)         { return d->head == d->tail; }
static void dq_push_back(Deque *d, int i, double v) {
    d->idx[d->tail] = i; d->val[d->tail] = v;
    d->tail = (d->tail + 1) % DQ_CAP;
}
static void dq_pop_back(Deque *d)  { d->tail = (d->tail - 1 + DQ_CAP) % DQ_CAP; }
static void dq_pop_front(Deque *d) { d->head = (d->head + 1) % DQ_CAP; }
static double dq_front_val(const Deque *d)   { return d->val[d->head]; }
static int    dq_front_idx(const Deque *d)   { return d->idx[d->head]; }
static double dq_back_val(const Deque *d) {
    return d->val[(d->tail - 1 + DQ_CAP) % DQ_CAP];
}

/* rolling min，window 大小 w，min_periods=1 */
static void roll_min(const double *x, int n, int w, double *out) {
    Deque dq; dq_clear(&dq);
    for (int i = 0; i < n; i++) {
        while (!dq_empty(&dq) && dq_front_idx(&dq) <= i - w) dq_pop_front(&dq);
        if (!isnan(x[i])) {
            while (!dq_empty(&dq) && dq_back_val(&dq) >= x[i]) dq_pop_back(&dq);
            dq_push_back(&dq, i, x[i]);
        }
        out[i] = dq_empty(&dq) ? NAN : dq_front_val(&dq);
    }
}

/* rolling max，window 大小 w，min_periods=1 */
static void roll_max(const double *x, int n, int w, double *out) {
    Deque dq; dq_clear(&dq);
    for (int i = 0; i < n; i++) {
        while (!dq_empty(&dq) && dq_front_idx(&dq) <= i - w) dq_pop_front(&dq);
        if (!isnan(x[i])) {
            while (!dq_empty(&dq) && dq_back_val(&dq) <= x[i]) dq_pop_back(&dq);
            dq_push_back(&dq, i, x[i]);
        }
        out[i] = dq_empty(&dq) ? NAN : dq_front_val(&dq);
    }
}

/* ── Phase 1：计算 tp, h4min, l4max, pctchange, is_anomaly ──── */
static void phase1(
    const double *c, const double *h, const double *l,
    int tlen, int n,
    double *tp, double *h4min, double *l4max,
    double *pctchg, int *is_anom
) {
    /* tp = (c+h+l)/3, tp[0]=tp[1]（与 Python 原版对应） */
    for (int i = 0; i < tlen; i++) tp[i] = (c[i] + h[i] + l[i]) / 3.0;
    if (tlen >= 2) tp[0] = tp[1];

    /* h4min = rolling(4).min(h), l4max = rolling(4).max(l) */
    roll_min(h, tlen, 4, h4min);
    roll_max(l, tlen, 4, l4max);

    /* pctchange = (h4min - l4max) / l4max */
    for (int i = 0; i < tlen; i++) {
        double denom = l4max[i];
        pctchg[i] = (isnan(denom) || denom == 0.0) ? NAN
                    : (h4min[i] - denom) / denom;
    }

    /* rolling mean + std (Welford ring-buffer, window n, min_periods=1) */
    double *ring = (double *)malloc(n * sizeof(double));
    double  wsum = 0.0, wsum2 = 0.0;
    int     wpos = 0, wcount = 0;
    for (int i = 0; i < tlen; i++) {
        double x = pctchg[i];
        double mean_i = NAN, std_i = NAN;
        if (!isnan(x)) {
            if (wcount == n) {
                double old = ring[wpos];
                wsum -= old; wsum2 -= old * old; wcount--;
            }
            ring[wpos] = x; wpos = (wpos + 1) % n;
            wsum += x; wsum2 += x * x; wcount++;
            mean_i = wsum / wcount;
            if (wcount > 1) {
                double var = (wsum2 - wsum * wsum / wcount) / (wcount - 1);
                std_i = (var > 0.0) ? sqrt(var) : 0.0;
            } else {
                std_i = 0.0;
            }
        }
        /* anomaly: |zscore| > 25 AND (pct > 0.2 OR pct < -0.5) */
        is_anom[i] = 0;
        if (!isnan(mean_i) && !isnan(std_i) && std_i > 0.0) {
            double z = (x - mean_i) / std_i;
            if (fabs(z) > 25.0 && (x > 0.2 || x < -0.5)) {
                is_anom[i] = 1;
                tp[i] = NAN;   /* 与 Python tpn.loc[anomaly_indices]=NaN 一致 */
            }
        }
    }
    free(ring);
}

/* ── Phase 2：对一个 segment [start, end) 计算 EWM+归一化 ───── */
static void process_segment(
    const double *tp_trunc,
    int start, int end,
    double alpha, int n,
    double *o
) {
    int seg_len = end - start;
    if (seg_len <= 168) {
        for (int i = start; i < end; i++) o[i] = NAN;
        return;
    }
    double ewm_val = NAN;
    int valid_count = 0;
    Deque min_dq, max_dq;
    dq_clear(&min_dq); dq_clear(&max_dq);

    for (int i = start; i < end; i++) {
        if (isnan(tp_trunc[i])) { o[i] = NAN; continue; }

        /* EWM(span=n, adjust=False): 首个有效值初始化为自身 */
        if (isnan(ewm_val)) ewm_val = tp_trunc[i];
        else                ewm_val = alpha * tp_trunc[i] + (1.0 - alpha) * ewm_val;

        double diff = tp_trunc[i] - ewm_val;

        /* rolling min（窗口 n，min_periods=167） */
        while (!dq_empty(&min_dq) && dq_front_idx(&min_dq) <= i - n) dq_pop_front(&min_dq);
        while (!dq_empty(&min_dq) && dq_back_val(&min_dq) >= diff)    dq_pop_back(&min_dq);
        dq_push_back(&min_dq, i, diff);

        /* rolling max（窗口 n） */
        while (!dq_empty(&max_dq) && dq_front_idx(&max_dq) <= i - n) dq_pop_front(&max_dq);
        while (!dq_empty(&max_dq) && dq_back_val(&max_dq) <= diff)    dq_pop_back(&max_dq);
        dq_push_back(&max_dq, i, diff);

        valid_count++;
        if (valid_count >= 167) {
            double dmin  = dq_front_val(&min_dq);
            double dmax  = dq_front_val(&max_dq);
            double range = dmax - dmin;
            o[i] = (range < 1e-10) ? NAN : (diff - dmin) / range;
        } else {
            o[i] = NAN;
        }
    }
}

/* ── 主函数 ──────────────────────────────────────────────────── */
void compute_momentum(
    const double *close, const double *high, const double *low,
    int n, int len, int trunc_len, double *out
) {
    int offset = len - trunc_len;
    if (offset < 0) { offset = 0; trunc_len = len; }
    int tlen = trunc_len;

    /* 前段填 NaN */
    for (int i = 0; i < offset; i++) out[i] = NAN;
    double *o = out + offset;

    const double *c = close + offset;
    const double *h = high  + offset;
    const double *l = low   + offset;

    /* 分配工作数组 */
    double *tp      = (double *)malloc(tlen * sizeof(double));
    double *h4min   = (double *)malloc(tlen * sizeof(double));
    double *l4max   = (double *)malloc(tlen * sizeof(double));
    double *pctchg  = (double *)malloc(tlen * sizeof(double));
    double *maxh    = (double *)malloc(tlen * sizeof(double));
    double *minl    = (double *)malloc(tlen * sizeof(double));
    int    *is_anom = (int    *)calloc(tlen, sizeof(int));

    /* Phase 1 */
    phase1(c, h, l, tlen, n, tp, h4min, l4max, pctchg, is_anom);

    /* Phase 2：按 segment 处理
     * anomaly 位置是前一段的最后一个元素（tp[i]=NaN），
     * 与 Python np.split(tpn, anomaly_idx+1) 逻辑一致 */
    double alpha = 2.0 / (n + 1.0);
    int seg_start = 0;
    for (int i = 0; i < tlen; i++) {
        if (is_anom[i]) {
            process_segment(tp, seg_start, i + 1, alpha, n, o);
            seg_start = i + 1;
        }
    }
    process_segment(tp, seg_start, tlen, alpha, n, o);  /* 最后一段 */

    /* Phase 3：长窗口分量
     * max_high = rolling(4368,min_periods=1).max(h4min)
     * min_low  = rolling(4368,min_periods=1).min(l4max)
     * o[i] += (tp[i] - min_low[i]) / (max_high[i] - min_low[i]) */
    roll_max(h4min, tlen, 26 * 168, maxh);
    roll_min(l4max, tlen, 26 * 168, minl);
    for (int i = 0; i < tlen; i++) {
        if (!isnan(tp[i]) && !isnan(o[i]) && !isnan(minl[i]) && !isnan(maxh[i])) {
            double range = maxh[i] - minl[i];
            if (range > 1e-10)
                o[i] += (tp[i] - minl[i]) / range;
        }
    }

    free(tp); free(h4min); free(l4max); free(pctchg);
    free(maxh); free(minl); free(is_anom);
}
