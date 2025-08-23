package com.github.benmanes.caffeine.cache.simulator.policy.prefix.chunk_manager;

import com.github.benmanes.caffeine.cache.simulator.policy.prefix.Consts;

import java.util.*;
import java.util.stream.Collectors;

/* ======================= RNG: PCG64 (compact) ======================= */
final class PCG64 {
  private long state_lo;
  private long state_hi;
  private final long inc_lo;
  private final long inc_hi;

  public PCG64(long seedLo, long seedHi) {
    inc_lo = (seedLo << 1) | 1L;
    inc_hi = seedHi;
    long l = nextLong();
    state_lo += seedLo;
    state_hi += seedHi;
    l = nextLong();
  }

  private void step() {
    final long MUL_LO = 0x2360ed051fc65da4L, MUL_HI = 0x4385df649fccf645L;
    long sLo = state_lo, sHi = state_hi;
    long lo_lo = sLo * MUL_LO;
    long lo_hi = mulHi(sLo, MUL_LO);
    long mid1_lo = sLo * MUL_HI;
    long mid1_hi = mulHi(sLo, MUL_HI);
    long mid2_lo = sHi * MUL_LO;
    long mid2_hi = mulHi(sHi, MUL_LO);
    long hi_lo = sHi * MUL_HI;
    long carry = 0L;
    long sum = lo_hi + mid1_lo + mid2_lo;
    carry += ((sum ^ lo_hi) & (sum ^ mid1_lo)) < 0 ? 1L : 0L;
    long newHi = hi_lo + mid1_hi + mid2_hi + carry;
    long nLo = lo_lo + inc_lo;
    long c1 = Long.compareUnsigned(nLo, lo_lo) < 0 ? 1L : 0L;
    long nHi = newHi + inc_hi + c1;
    state_lo = nLo;
    state_hi = nHi;
  }

  private static long mulHi(long x, long y) {
    long x0 = x & 0xffffffffL, x1 = x >>> 32;
    long y0 = y & 0xffffffffL, y1 = y >>> 32;
    long z0 = x0 * y0;
    long t = x1 * y0 + (z0 >>> 32);
    long z1 = t & 0xffffffffL;
    long z2 = t >>> 32;
    z1 += x0 * y1;
    return x1 * y1 + z2 + (z1 >>> 32);
  }

  public long nextLong() {
    step();
    long x = state_hi ^ state_lo;
    int rot = (int) (state_hi >>> 58);
    return Long.rotateRight(x, rot);
  }

  public double nextDouble() {
    long r = nextLong() >>> 11;
    return r * (1.0 / (1L << 53));
  }

  public double nextGaussian() {
    double u1 = Math.max(1e-12, nextDouble()), u2 = nextDouble();
    double r = Math.sqrt(-2.0 * Math.log(u1)), t = 2.0 * Math.PI * u2;
    return r * Math.cos(t);
  }
}

/* ======================= Welford per item ======================= */
final class Welford {
  long n = 0;
  double mean = 0.0, M2 = 0.0;

  void update(double x) {
    n++;
    double d = x - mean;
    mean += d / n;
    M2 += d * (x - mean);
  }

  double std() {
    return n > 1 ? Math.sqrt(M2 / (n - 1)) : 0.0;
  }
}

/* ======================= Cluster in 2D (mean,std) ======================= */
final class C2 {
  double[] mu;
  long n = 0;
  long seen = 0;
  double[] bufMean = null, bufStd = null;
  int bufHead = 0, bufSize = 0;

  C2(double[] mu) {
    this.mu = mu.clone();
  }

  C2(double[] mu, long n) {
    this.mu = mu.clone();
    this.n = n;
  }

  void ensureBuf(int B) {
    if (bufMean == null) {
      bufMean = new double[B];
      bufStd = new double[B];
    }
  }

  void pushToBuf(double m, double s, int B) {
    ensureBuf(B);
    bufMean[bufHead] = m;
    bufStd[bufHead] = s;
    bufHead = (bufHead + 1) % bufMean.length;
    if (bufSize < bufMean.length) bufSize++;
  }

  void update(double[] x, int B) {
    seen++;
    n++;
    mu[0] += (x[0] - mu[0]) / n;
    mu[1] += (x[1] - mu[1]) / n;
    pushToBuf(x[0], x[1], B);
  }
}

/* ======================= Sequential X-means 2D ======================= */
final class SXMeans2D {
  final int B, MIN, CHECK, MERGE_EVERY, MIN_COUNT, nInit, maxIter;
  final double BIC_GAIN, mergeTol;
  final PCG64 rng;
  long t = 0;
  final ArrayList<C2> cs = new ArrayList<>();

  SXMeans2D(int B, int MIN, int CHECK, double BIC_GAIN, int MERGE_EVERY, int MIN_COUNT,
            double mergeTol, int nInit, int maxIter, long seedLo, long seedHi) {
    this.B = B;
    this.MIN = MIN;
    this.CHECK = CHECK;
    this.BIC_GAIN = BIC_GAIN;
    this.MERGE_EVERY = MERGE_EVERY;
    this.MIN_COUNT = MIN_COUNT;
    this.mergeTol = mergeTol;
    this.nInit = nInit;
    this.maxIter = maxIter;
    this.rng = new PCG64(seedLo, seedHi);
  }

  int assign(double[] x) {
    if (cs.isEmpty()) {
      cs.add(new C2(x));
      return 0;
    }
    int best = 0;
    double bd = dist2(x, cs.get(0).mu);
    for (int i = 1; i < cs.size(); i++) {
      double d = dist2(x, cs.get(i).mu);
      if (d < bd) {
        bd = d;
        best = i;
      }
    }
    return best;
  }

  void partialFit(double[] x) {
    t++;
    int j = assign(x);
    cs.get(j).update(x, B);
    trySplit(j);
    if (t % MERGE_EVERY == 0) pruneAndMerge();
  }

  List<double[]> centroids() {
    return cs.stream().sorted(Comparator.comparingDouble(c -> c.mu[0]))
      .map(c -> c.mu.clone()).collect(Collectors.toList());
  }

  void trySplit(int idx) {
    C2 c = cs.get(idx);
    if (c.bufSize < MIN || c.seen < CHECK) return;

    int m = c.bufSize;
    double[][] xs = new double[m][2];
    int cap = c.bufMean.length;
    int pos = (c.bufHead - m) % cap;
    if (pos < 0) pos += cap;
    for (int i = 0; i < m; i++) {
      xs[i][0] = c.bufMean[pos];
      xs[i][1] = c.bufStd[pos];
      pos++;
      if (pos == cap) pos = 0;
    }

    int[] labels1 = new int[m];
    double bic1 = bic(xs, labels1);

    int[] bestLabels = null;
    double bestBic = -Double.MAX_VALUE;
    for (int r = 0; r < nInit; r++) {
      KMeans2 km2 = new KMeans2(xs, 2, maxIter, rng);
      km2.fit();
      double b = bic(xs, km2.labels);
      if (b > bestBic) {
        bestBic = b;
        bestLabels = km2.labels.clone();
      }
    }
    if (bestBic > bic1 + BIC_GAIN) {
      ArrayList<double[]> a = new ArrayList<>(), b = new ArrayList<>();
      for (int i = 0; i < m; i++)
        if (bestLabels[i] == 0) a.add(xs[i]);
        else b.add(xs[i]);
      if (!a.isEmpty() && !b.isEmpty()) {
        cs.remove(idx);
        cs.add(idx, makeFrom(b));
        cs.add(idx, makeFrom(a));
      }
    }
    c.seen = 0;
  }

  C2 makeFrom(List<double[]> pts) {
    int m = pts.size();
    double m0 = 0, m1 = 0;
    for (double[] p : pts) {
      m0 += p[0];
      m1 += p[1];
    }
    m0 /= m;
    m1 /= m;
    C2 out = new C2(new double[]{m0, m1}, m);
    int keep = Math.min(B, m), start = m - keep;
    out.bufMean = new double[B];
    out.bufStd = new double[B];
    out.bufHead = 0;
    out.bufSize = 0;
    for (int i = start; i < m; i++) {
      double[] p = pts.get(i);
      out.pushToBuf(p[0], p[1], B);
    }
    return out;
  }

  void pruneAndMerge() {
    cs.removeIf(c -> c.n < MIN_COUNT);
    cs.sort(Comparator.comparingDouble(c -> c.mu[0]));
    int i = 0;
    while (i < cs.size() - 1) {
      C2 a = cs.get(i), b = cs.get(i + 1);
      double d = Math.hypot(a.mu[0] - b.mu[0], a.mu[1] - b.mu[1]);
      if (d < mergeTol) {
        long n = a.n + b.n;
        if (n == 0) {
          cs.remove(i + 1);
          continue;
        }
        double m0 = (a.mu[0] * a.n + b.mu[0] * b.n) / n;
        double m1 = (a.mu[1] * a.n + b.mu[1] * b.n) / n;

        C2 merged = new C2(new double[]{m0, m1}, n);
        merged.bufMean = new double[B];
        merged.bufStd = new double[B];
        merged.bufHead = 0;
        merged.bufSize = 0;
        appendRingOldestFirst(merged, a);
        appendRingOldestFirst(merged, b);
        cs.set(i, merged);
        cs.remove(i + 1);
      } else i++;
    }
  }

  void appendRingOldestFirst(C2 dst, C2 src) {
    if (src.bufMean == null || src.bufSize == 0) return;
    int cap = src.bufMean.length;
    int pos = (src.bufHead - src.bufSize) % cap;
    if (pos < 0) pos += cap;
    for (int k = 0; k < src.bufSize; k++) {
      if (dst.bufSize >= B) break;
      dst.pushToBuf(src.bufMean[pos], src.bufStd[pos], B);
      pos++;
      if (pos == cap) pos = 0;
    }
  }

  static double dist2(double[] a, double[] b) {
    double dx = a[0] - b[0], dy = a[1] - b[1];
    return dx * dx + dy * dy;
  }

  static double bic(double[][] X, int[] labels) {
    int n = X.length;
    if (n == 0) return -Double.MAX_VALUE;
    int k = 1 + Arrays.stream(labels).max().orElse(0);
    double ll = 0.0;
    int params = 0;
    for (int j = 0; j < k; j++) {
      int nj = 0;
      double m0 = 0, m1 = 0;
      for (int i = 0; i < n; i++)
        if (labels[i] == j) {
          nj++;
          m0 += X[i][0];
          m1 += X[i][1];
        }
      if (nj == 0) continue;
      m0 /= nj;
      m1 /= nj;
      double s00 = 0, s01 = 0, s11 = 0;
      for (int i = 0; i < n; i++)
        if (labels[i] == j) {
          double dx = X[i][0] - m0, dy = X[i][1] - m1;
          s00 += dx * dx;
          s01 += dx * dy;
          s11 += dy * dy;
        }
      s00 /= nj;
      s01 /= nj;
      s11 /= nj;
      double eps = 1e-10;
      s00 += eps;
      s11 += eps;
      double det = s00 * s11 - s01 * s01;
      if (det <= 0) return -Double.MAX_VALUE;
      double inv00 = s11 / det, inv01 = -s01 / det, inv11 = s00 / det;
      double quad = 0.0;
      for (int i = 0; i < n; i++)
        if (labels[i] == j) {
          double dx = X[i][0] - m0, dy = X[i][1] - m1;
          quad += dx * (inv00 * dx + inv01 * dy) + dy * (inv01 * dx + inv11 * dy);
        }
      ll += -0.5 * (nj * (2 * Math.log(2 * Math.PI) + Math.log(det)) + quad);
      params += 5;
    }
    params += (k - 1);
    return ll - 0.5 * params * Math.log(n);
  }

  /* ----- tiny KMeans(k=2) with k-means++ and Lloyd ----- */
  static final class KMeans2 {
    final double[][] X;
    final int n, maxIter;
    final PCG64 rng;
    double[][] centers;
    int[] labels;

    KMeans2(double[][] X, int kIgnored, int maxIter, PCG64 rng) {
      this.X = X;
      this.n = X.length;
      this.maxIter = maxIter;
      this.rng = rng;
      this.labels = new int[n];
    }

    void fit() {
      initPlusPlus();
      for (int it = 0; it < maxIter; it++) {
        boolean ch = assignAndUpdate();
        if (!ch) break;
      }
    }

    void initPlusPlus() {
      centers = new double[2][2];
      int first = (int) Math.floor(rng.nextDouble() * n);
      centers[0] = X[first].clone();
      double[] d2 = new double[n];
      for (int i = 0; i < n; i++) d2[i] = sqDist(X[i], centers[0]);
      double sum = 0;
      for (double v : d2) sum += v;
      double r = rng.nextDouble() * sum;
      double acc = 0;
      int second = 0;
      for (int i = 0; i < n; i++) {
        acc += d2[i];
        if (acc >= r) {
          second = i;
          break;
        }
      }
      centers[1] = X[second].clone();
    }

    boolean assignAndUpdate() {
      boolean ch = false;
      for (int i = 0; i < n; i++) {
        double d0 = sqDist(X[i], centers[0]), d1 = sqDist(X[i], centers[1]);
        int lab = (d0 <= d1) ? 0 : 1;
        if (lab != labels[i]) {
          labels[i] = lab;
          ch = true;
        }
      }
      double m00 = 0, m01 = 0;
      int c0 = 0;
      double m10 = 0, m11 = 0;
      int c1 = 0;
      for (int i = 0; i < n; i++) {
        if (labels[i] == 0) {
          m00 += X[i][0];
          m01 += X[i][1];
          c0++;
        } else {
          m10 += X[i][0];
          m11 += X[i][1];
          c1++;
        }
      }
      if (c0 > 0) {
        centers[0][0] = m00 / c0;
        centers[0][1] = m01 / c0;
      }
      if (c1 > 0) {
        centers[1][0] = m10 / c1;
        centers[1][1] = m11 / c1;
      }
      return ch;
    }

    static double sqDist(double[] a, double[] b) {
      double dx = a[0] - b[0], dy = a[1] - b[1];
      return dx * dx + dy * dy;
    }
  }
}

/* ======================= ClusterBasedChunkManager ======================= */
public class ClusterBasedChunkManager {

  private final SXMeans2D model;
  private final Map<Long, Welford> items = new HashMap<>();
  private final int minObs;
  private final double bandwidth;

  // scratch to avoid allocations in hot path
  private final double[] point = new double[2];

  public ClusterBasedChunkManager() {
    this.bandwidth = Consts.BANDWIDTH;
    this.minObs = Consts.MIN_OBS_FOR_CLUSTER;
    this.model = new SXMeans2D(
      512,
      128,
      256,
      8.0,
      1500,
      30,
      0.02,
      10,
      100,
      42L,
      17L
    );
  }

  private Welford statFor(long itemKey) {
    return items.computeIfAbsent(itemKey, k -> new Welford());
  }

  /**
   * Add one latency sample for an item (equivalent to online partial_fit on (mean,std)).
   */
  public synchronized void addDelay(long itemKey, double delaySeconds) {
    Welford st = statFor(itemKey);
    st.update(delaySeconds);
    if (st.n >= minObs) {
      point[0] = st.mean;
      point[1] = Math.max(st.std(), Consts.EPS_STD);
      model.partialFit(point);
    }
  }

  /**
   * Return chunk size in bytes for a request to itemKey of given size.
   * We (re)assign the item to the CURRENT nearest centroid using its latest (mean,std),
   * then compute (μ + 2·σ) * BANDWIDTH. Clamp to [0, itemSize].
   * This handles the case where the item was previously in c1 but, after other traffic,
   * c1 split/merged and the nearest centroid is now c2.
   */
  public synchronized long getChunkSize(long itemKey, long itemSize) {
    Welford st = items.get(itemKey);

    double mu, sigma;

    if (st != null && st.n > 0 && !model.cs.isEmpty()) {
      // assign to nearest CURRENT centroid using latest per-item (mean,std)
      double m = st.mean;
      double s = Math.max(st.std(), Consts.EPS_STD);
      point[0] = m;
      point[1] = s;

      int idx = model.assign(point); // nearest now (no centroid update)
      mu = model.cs.get(idx).mu[0];
      sigma = model.cs.get(idx).mu[1];
    } else if (!model.cs.isEmpty()) {
      // No per-item history yet – fall back to the most common centroid (closest to overall mean)
      double best = Double.POSITIVE_INFINITY;
      int idx = 0;
      for (int i = 0; i < model.cs.size(); i++) {
        double d = SXMeans2D.dist2(model.cs.get(i).mu, new double[]{model.cs.get(i).mu[0], model.cs.get(i).mu[1]});
        if (d < best) {
          best = d;
          idx = i;
        }
      }
      mu = model.cs.get(idx).mu[0];
      sigma = model.cs.get(idx).mu[1];
    } else {
      // No clusters yet – conservative default
      mu = Consts.DEFAULT_LATENCY_S;
      sigma = 0.0;
    }

    double estSeconds = mu + 2.0 * sigma;
    long bytes = Math.max(0L, Math.round(estSeconds * bandwidth));
    if (itemSize > 0) bytes = Math.min(bytes, itemSize);
    return bytes;
  }

  /* helpers */
  public synchronized List<double[]> getCentroids() {
    return model.centroids();
  }

  public synchronized int getClusterCount() {
    return model.cs.size();
  }

  /**
   * Return the current nearest-centroid label for a (mean,std) point (no updates).
   */
  public synchronized int assignCurrent(double mean, double std) {
    point[0] = mean;
    point[1] = Math.max(std, Consts.EPS_STD);
    return model.assign(point);
  }

}
