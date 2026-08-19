#ifndef STORAGE_LEVELDB_DB_PRE_L0_LATENCY_INJECTOR_H_
#define STORAGE_LEVELDB_DB_PRE_L0_LATENCY_INJECTOR_H_

#include <cstddef>
#include <cstdint>

namespace leveldb {

class LatencyInjector {
 public:
  LatencyInjector() = default;

  void Init(double bytes_per_ns, double fixed_ns);

  void Inject(size_t bytes) const;

  double tsc_ghz() const { return tsc_ghz_; }

  static constexpr double kLinkBytesPerNs = 53.5;
  static constexpr double kLinkFixedNs = 57.0;
  static constexpr double kInnerBytesPerNs = 51.2;
  static constexpr double kInnerFixedNs = 0.0;

 private:
  double tsc_ghz_ = 0.0;
  uint64_t overhead_cyc_ = 0;
  double bytes_per_ns_ = kLinkBytesPerNs;
  double fixed_ns_ = kLinkFixedNs;
  double inv_bytes_per_ns_ = 1.0 / kLinkBytesPerNs;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_PRE_L0_LATENCY_INJECTOR_H_
