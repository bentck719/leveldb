#include "db/pre_l0/latency_injector.h"

#include <time.h>

#if defined(__x86_64__) || defined(__i386__)
#include <x86intrin.h>
#endif

namespace leveldb {

namespace {

inline uint64_t RdtscSerial() {
#if defined(__x86_64__) || defined(__i386__)
  unsigned int aux;
  return __rdtscp(&aux);
#else
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
  return static_cast<uint64_t>(ts.tv_sec) * 1000000000ull + ts.tv_nsec;
#endif
}

inline double TsDiffNs(struct timespec a, struct timespec b) {
  return (b.tv_sec - a.tv_sec) * 1e9 + (b.tv_nsec - a.tv_nsec);
}

double CalibrateTscGhz() {
  struct timespec t0, t1;
  clock_gettime(CLOCK_MONOTONIC_RAW, &t0);
  uint64_t c0 = RdtscSerial();
  double ns;
  do {
    clock_gettime(CLOCK_MONOTONIC_RAW, &t1);
    ns = TsDiffNs(t0, t1);
  } while (ns < 200e6);
  uint64_t c1 = RdtscSerial();
#if defined(__x86_64__) || defined(__i386__)
  return static_cast<double>(c1 - c0) / ns;
#else
  (void)c0;
  (void)c1;
  return 1.0;
#endif
}

uint64_t MeasureOverhead(double fixed_ns, double inv_bytes_per_ns,
                         double tsc_ghz) {
  uint64_t best = UINT64_MAX;
  volatile size_t vb = 64;
  for (int i = 0; i < 200000; i++) {
    uint64_t s = RdtscSerial();
    double ns = fixed_ns + static_cast<double>(vb) * inv_bytes_per_ns;
    volatile uint64_t target = static_cast<uint64_t>(ns * tsc_ghz);
    uint64_t e = RdtscSerial();
    (void)target;
    uint64_t d = e - s;
    if (d < best) best = d;
  }
  return best;
}

}  // namespace

void LatencyInjector::Init(double bytes_per_ns, double fixed_ns) {
  if (bytes_per_ns > 0) bytes_per_ns_ = bytes_per_ns;
  if (fixed_ns >= 0) fixed_ns_ = fixed_ns;
  inv_bytes_per_ns_ = 1.0 / bytes_per_ns_;
  tsc_ghz_ = CalibrateTscGhz();
  overhead_cyc_ = MeasureOverhead(fixed_ns_, inv_bytes_per_ns_, tsc_ghz_);
}

void LatencyInjector::Inject(size_t bytes) const {
  if (tsc_ghz_ <= 0.0) return;
  double ns = fixed_ns_ + static_cast<double>(bytes) * inv_bytes_per_ns_;
  uint64_t target = static_cast<uint64_t>(ns * tsc_ghz_);
  if (target <= overhead_cyc_) return;
  target -= overhead_cyc_;
  uint64_t start = RdtscSerial();
  while ((RdtscSerial() - start) < target);
}

}  // namespace leveldb
