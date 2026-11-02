/* Versioned causal momentum. Oracle: 动量210401_双项异常隔离V2.py.
 * No global state, no input writes, no history truncation. O(length + n).
 */
#include <math.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

/* IEEE-754 float64, as required by the numpy bridge. memcpy avoids aliasing UB.
 * In particular MSVC's isfinite may call the CRT for every scalar in the loop.
 */
static int finite_double(double value) {
    uint64_t bits;
    memcpy(&bits, &value, sizeof(bits));
    return (bits & UINT64_C(0x7ff0000000000000)) != UINT64_C(0x7ff0000000000000);
}

typedef struct { int *idx; int head, tail; } Queue;
static double extreme(Queue *q, const double *values, int i, int window, int maximum) {
    while (q->head < q->tail && q->idx[q->head] <= i - window) q->head++;
    double value = values[i];
    if (finite_double(value)) {
        while (q->head < q->tail && (maximum ? values[q->idx[q->tail-1]] <= value
                                                       : values[q->idx[q->tail-1]] >= value)) q->tail--;
        q->idx[q->tail++] = i;
    }
    return q->head < q->tail ? values[q->idx[q->head]] : NAN;
}

/* Diagnostic pointers are either all non-NULL, or all NULL for signal-only calls.
 * Separate exported name prevents an old binary from receiving NULL outputs.
 */
int compute_momentum_v2_fast(const double *high, const double *low, const double *close,
                        const long long *times, int timed, int length, int n,
                        int location_window, int warmup, int cooldown,
                        double *trend, double *location, double *signal,
                        unsigned char *anomaly, unsigned char *invalid,
                        unsigned char *blocked, long long *counts) {
    if (!length) return 0;
    int ring_size = n < length ? n : length;
    double *work = malloc(((size_t)ring_size + 3*(size_t)length) * sizeof(double));
    int *indices = malloc((size_t)length * 4 * sizeof(int));
    if (!work || !indices) {
        free(work); free(indices); return -1;
    }
    double *ring = work, *delta = ring+ring_size, *h4 = delta+length, *l4 = h4+length;
    Queue qmin = {indices, 0, 0}, qmax = {indices+length, 0, 0};
    Queue qhigh = {indices+2*(size_t)length, 0, 0}, qlow = {indices+3*(size_t)length, 0, 0};
    for (int j = 0; j < ring_size; j++) ring[j] = NAN;
    int count = 0, last_reset = 0, last_event = -1, segment_start = 1, slot = -1;
    int diagnostic = trend != NULL;
    double mean = 0, m2 = 0, ewm = NAN, alpha = 2.0/(n+1.0);
    for (int i = 0; i < length; i++) {
        signal[i] = NAN;
        if (diagnostic) {
            trend[i] = location[i] = NAN;
            anomaly[i] = invalid[i] = blocked[i] = 0; counts[i] = 0;
        }
        if (++slot == ring_size) slot = 0;
        double previous = ring[slot]; ring[slot] = NAN;
        if (finite_double(previous)) {
            if (count == 1) {count=0; mean=0; m2=0;}
            else {
                double next_mean = (count*mean-previous)/(count-1);
                m2 = m2-(previous-mean)*(previous-next_mean);
                m2 = m2 > 0.0 ? m2 : 0.0;
                mean=next_mean; count--;
            }
        }
        if (!i) continue;
        int is_invalid = 0, is_anomaly = 0;
        double h = NAN, l = NAN;
        if (!finite_double(high[i]) || !finite_double(low[i]) || !finite_double(close[i]) ||
            low[i] <= 0 || close[i] <= 0 || high[i] < low[i]) is_invalid=1;
        else if (i-last_reset >= 4) {
            /* Since last_reset every price is finite. Reuse this window in location. */
            h = high[i]; l = low[i];
            for (int j=i-3; j<i; j++) {
                h = h < high[j] ? h : high[j];
                l = l > low[j] ? l : low[j];
            }
            double value=(h-l)/l;
            if (!finite_double(value)) is_invalid=1;
            else {
                int nc=count+1;
                double d=value-mean, nm=mean+d/nc;
                double nm2=m2+d*(value-nm);
                nm2 = nm2 > 0.0 ? nm2 : 0.0;
                /* Short-circuit only: retain original sqrt/division at the threshold. */
                if ((value>0.2 || value< -0.5) && nc>1 && nm2>0 &&
                    fabs(value-nm)/sqrt(nm2/(nc-1))>25.0) is_anomaly=1;
                else {ring[slot]=value; count=nc; mean=nm; m2=nm2;}
            }
        }
        if (is_anomaly || is_invalid) {
            last_reset=last_event=i; segment_start=i+1; ewm=NAN;
            qmin.head=qmin.tail=qmax.head=qmax.tail=0;
            qhigh.head=qhigh.tail=qlow.head=qlow.tail=0;
            if (diagnostic) {anomaly[i]=is_anomaly; invalid[i]=is_invalid; blocked[i]=1;}
            continue;
        }
        double price=(high[i]+low[i]+close[i])/3.0;
        ewm=(ewm != ewm) ? price : (1.0-alpha)*ewm+alpha*price;
        delta[i]=price-ewm;
        double bottom=extreme(&qmin,delta,i,n,0), top=extreme(&qmax,delta,i,n,1);
        int normal_count=i-segment_start+1;
        double trend_value=NAN, location_value=NAN;
        if (normal_count>=warmup && top!=bottom) trend_value=(delta[i]-bottom)/(top-bottom);
        h4[i]=h; l4[i]=l;
        double ceiling=extreme(&qhigh,h4,i,location_window,1);
        double floor=extreme(&qlow,l4,i,location_window,0);
        if (ceiling!=floor) location_value=(price-floor)/(ceiling-floor);
        int is_blocked=0;
        if (last_event>=0) is_blocked=timed ?
            (times[i]-times[last_event] <= (long long)cooldown*3600000000000LL) :
            (i-last_event<=cooldown);
        double value=trend_value+location_value;
        if (!is_blocked && normal_count>=warmup && finite_double(value)) signal[i]=value;
        if (diagnostic) {
            trend[i]=trend_value; location[i]=location_value;
            counts[i]=normal_count; blocked[i]=is_blocked;
        }
    }
    free(work); free(indices);
    return 0;
}
