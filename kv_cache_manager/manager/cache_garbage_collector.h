#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <vector>

#include "kv_cache_manager/common/error_code.h"
#include "kv_cache_manager/common/loop_thread.h"
#include "kv_cache_manager/manager/schedule_plan_executor.h"
#include "kv_cache_manager/meta/common.h"
#include "kv_cache_manager/meta/types.h"
#include "kv_cache_manager/metrics/metrics_registry.h"

namespace kv_cache_manager {

inline constexpr int64_t kMinCacheGcOrphanWritingGracePeriodMs = 60LL * 60 * 1000;

#ifndef KVCM_COUNTER_METRICS_FOR_CACHE_GC
#define KVCM_COUNTER_METRICS_FOR_CACHE_GC(name)                                                                        \
public:                                                                                                                \
    DECLARE_METRICS_NAME_(cache_gc, name);                                                                             \
    DEFINE_GET_METRICS_COUNTER_(cache_gc, name)                                                                        \
                                                                                                                       \
private:                                                                                                               \
    DECLARE_METRICS_COUNTER_(cache_gc, name);
#endif

#ifndef KVCM_GAUGE_METRICS_FOR_CACHE_GC
#define KVCM_GAUGE_METRICS_FOR_CACHE_GC(name)                                                                          \
public:                                                                                                                \
    DECLARE_METRICS_NAME_(cache_gc, name);                                                                             \
    DEFINE_GET_METRICS_GAUGE_(cache_gc, name)                                                                          \
                                                                                                                       \
private:                                                                                                               \
    DECLARE_METRICS_GAUGE_(cache_gc, name);
#endif

class CacheLocation;
class DataStorageManager;
class EventReportBackend;
class MetaIndexerManager;
class MetaSearcher;
class MetaSearcherManager;
class MigrationManager;
class RegistryManager;
class RequestContext;
struct MaintenanceScanBatch;

class CacheGarbageCollector {
public:
    struct Config {
        bool enabled{false};
        int64_t scan_interval_ms{1000};
        int64_t round_pause_ms{24LL * 60 * 60 * 1000};
        size_t scan_batch_size{256};
        int64_t orphan_writing_grace_period_ms{24LL * 60 * 60 * 1000};
        size_t max_inflight_delete_requests{2};
        bool event_report_cleanup_enabled{false};
    };

    CacheGarbageCollector() = delete;
    CacheGarbageCollector(Config config,
                          std::shared_ptr<RegistryManager> registry_manager,
                          std::shared_ptr<MetaIndexerManager> meta_indexer_manager,
                          std::shared_ptr<MetaSearcherManager> meta_searcher_manager,
                          std::shared_ptr<DataStorageManager> data_storage_manager,
                          std::shared_ptr<SchedulePlanExecutor> schedule_plan_executor,
                          std::shared_ptr<MetricsRegistry> metrics_registry,
                          std::shared_ptr<MigrationManager> migration_manager);
    ~CacheGarbageCollector();

    CacheGarbageCollector(const CacheGarbageCollector &) = delete;
    CacheGarbageCollector(CacheGarbageCollector &&) = delete;
    CacheGarbageCollector &operator=(const CacheGarbageCollector &) = delete;
    CacheGarbageCollector &operator=(CacheGarbageCollector &&) = delete;

    [[nodiscard]] ErrorCode Validate() const noexcept;
    [[nodiscard]] ErrorCode Start() noexcept;
    void RequestStop() noexcept;
    void Join() noexcept;
    void Stop() noexcept;
    [[nodiscard]] bool IsRunning() const noexcept;
    [[nodiscard]] bool IsEnabled() const noexcept { return config_.enabled; }
    [[nodiscard]] bool IsEventReportCleanupEnabled() const noexcept {
        return config_.enabled && config_.event_report_cleanup_enabled;
    }

    bool RegisterSnapshotCleanupIntent(const std::string &instance_id,
                                       DataStorageType storage_type,
                                       const std::string &host_ip_port,
                                       const std::string &committed_version,
                                       uint64_t snapshot_attempt_epoch,
                                       uint64_t lifecycle_generation,
                                       const std::shared_ptr<EventReportBackend> &backend) noexcept;
    bool RegisterHostCleanupIntent(const std::string &instance_id,
                                   DataStorageType storage_type,
                                   const std::string &host_ip_port,
                                   uint64_t lifecycle_generation,
                                   const std::shared_ptr<EventReportBackend> &backend) noexcept;
    void CancelHostCleanupIntent(const std::string &instance_id,
                                 DataStorageType storage_type,
                                 const std::string &host_ip_port) noexcept;

private:
    using Clock = std::chrono::steady_clock;

    struct InstanceScanEntry {
        std::string instance_group;
        std::string instance_id;

        bool operator<(const InstanceScanEntry &other) const {
            return std::tie(instance_group, instance_id) < std::tie(other.instance_group, other.instance_id);
        }
        bool operator==(const InstanceScanEntry &other) const {
            return instance_group == other.instance_group && instance_id == other.instance_id;
        }
    };

    struct PendingLocationKey {
        std::string instance_id;
        int64_t block_key{0};
        std::string location_id;

        bool operator<(const PendingLocationKey &other) const noexcept {
            return std::tie(instance_id, block_key, location_id) <
                   std::tie(other.instance_id, other.block_key, other.location_id);
        }
    };

    struct InflightDelete {
        uint64_t round_id{0};
        std::string instance_id;
        size_t target_count{0};
        Clock::time_point submitted_at;
        std::vector<PendingLocationKey> pending_locations;
        std::future<PlanExecuteResult> future;
    };

    enum class EventReportIntentType {
        kStaleSnapshot,
        kDownHost,
        kRecoveryAbsentHost,
    };

    struct EventReportIntentKey {
        std::string instance_id;
        DataStorageType storage_type{DataStorageType::DATA_STORAGE_TYPE_UNKNOWN};
        std::string host_ip_port;

        bool operator<(const EventReportIntentKey &other) const noexcept {
            return std::tie(instance_id, storage_type, host_ip_port) <
                   std::tie(other.instance_id, other.storage_type, other.host_ip_port);
        }
    };

    struct EventReportCleanupIntent {
        EventReportIntentType type{EventReportIntentType::kStaleSnapshot};
        uint64_t revision{0};
        Clock::time_point created_at;
        std::weak_ptr<EventReportBackend> backend;
        std::string committed_version;
        uint64_t snapshot_attempt_epoch{0};
        uint64_t lifecycle_generation{0};
    };

    struct EventReportScanState {
        std::string cursor{SCAN_BASE_CURSOR};
        uint64_t pass_id{0};
        std::map<EventReportIntentKey, EventReportCleanupIntent> pass_intents;
        bool discovery_pass{false};
        bool sync_barrier_complete{false};
        bool revisit_required{false};
        uint32_t consecutive_failures{0};
        Clock::time_point next_retry_at{};
        Clock::time_point pass_started_at{};
        std::shared_ptr<RequestContext> context;
    };

    struct RecoveryObservation {
        Clock::time_point first_seen_absent;
        std::weak_ptr<EventReportBackend> backend;
    };

    struct EventReportDeleteTarget {
        EventReportIntentKey intent_key;
        EventReportIntentType reason{EventReportIntentType::kStaleSnapshot};
        uint64_t intent_revision{0};
        KeyType block_key{0};
        std::string location_id;
        std::string expected_location_value;
    };

    struct EventReportRetryBatch {
        std::string instance_id;
        uint64_t pass_id{0};
        std::vector<EventReportDeleteTarget> targets;
        MaintenanceLocationDeleteResults prior_results;
        uint32_t consecutive_failures{0};
        Clock::time_point next_retry_at{};
    };

    void RunOneTick() noexcept;
    bool RunEventReportTick();
    bool RefreshRecoveryDiscoverySnapshot();
    bool BeginEventReportPass(const std::string &instance_id, EventReportScanState &state);
    bool RunEventReportInstanceBatch(const std::string &instance_id, EventReportScanState &state);
    std::vector<EventReportDeleteTarget>
    BuildEventReportDeleteTargets(const std::string &instance_id,
                                  const MaintenanceScanBatch &batch,
                                  const std::map<EventReportIntentKey, EventReportCleanupIntent> &pass_intents,
                                  bool discovery_pass,
                                  bool &out_truncated,
                                  bool &out_retry_required);
    bool ExecuteEventReportDeleteBatch(const std::string &instance_id,
                                       uint64_t pass_id,
                                       const std::vector<EventReportDeleteTarget> &targets,
                                       const MaintenanceLocationDeleteResults *prior_results,
                                       MaintenanceLocationDeleteResults &out_results,
                                       bool &out_revisit_required);
    void PromoteExpiredRecoveryObservations();
    void CompleteEventReportPass(const std::string &instance_id, EventReportScanState &state);
    void FailEventReportPass(const std::string &instance_id, EventReportScanState &state, const std::string &stage);
    std::shared_ptr<EventReportBackend> LookupEventReportBackend(const std::string &instance_id,
                                                                 DataStorageType storage_type) const;
    void UpdateEventReportMetrics() noexcept;
    void RecordEventReportIntentUpdate(EventReportIntentType type, const std::string &result) noexcept;
    void RecordEventReportDeleteResult(EventReportIntentType type, const std::string &status, size_t count) noexcept;
    void RecordEventReportRetry(const std::string &stage) noexcept;
    void PollInflightDeletes() noexcept;
    void UpdateInflightDeleteMetrics() noexcept;
    void ReleasePendingLocations(const InflightDelete &inflight) noexcept;
    bool BeginRound();
    void CompleteRound() noexcept;
    void AdvanceInstance() noexcept;
    CacheLocationDelRequest
    BuildDeleteRequest(const std::string &instance_id, const MaintenanceScanBatch &batch, int64_t now_us);
    bool
    IsOrphanWriting(const std::string &map_location_id, const CacheLocation &location, int64_t now_us) const noexcept;
    void ResetWorkerState() noexcept;
    void RegisterMetrics();
    void RecordCandidateCount(const std::string &reason, size_t count) noexcept;
    void RecordOperationError(const std::string &stage) noexcept;
    void RecordDeleteResult(const std::string &status) noexcept;

    const Config config_;
    const std::shared_ptr<RegistryManager> registry_manager_;
    const std::shared_ptr<MetaIndexerManager> meta_indexer_manager_;
    const std::shared_ptr<MetaSearcherManager> meta_searcher_manager_;
    const std::shared_ptr<DataStorageManager> data_storage_manager_;
    const std::shared_ptr<SchedulePlanExecutor> schedule_plan_executor_;
    const std::shared_ptr<MetricsRegistry> metrics_registry_;
    const std::shared_ptr<MigrationManager> migration_manager_;

    mutable std::mutex lifecycle_mutex_;
    std::shared_ptr<LoopThread> worker_;
    std::atomic<bool> stop_requested_{true};
    bool running_{false};

    // Worker-thread-only state.
    std::vector<InstanceScanEntry> instances_;
    size_t instance_index_{0};
    std::string cursor_;
    uint64_t round_id_{0};
    bool round_active_{false};
    Clock::time_point round_started_at_;
    std::optional<Clock::time_point> next_round_at_;
    std::shared_ptr<RequestContext> round_context_;
    std::vector<InflightDelete> inflight_deletes_;
    std::set<PendingLocationKey> pending_locations_;

    // Event threads only mutate the compact intent/observation registry.
    // Cursor/pass/retry state remains owned by the GC LoopThread.
    mutable std::mutex event_report_intent_mutex_;
    std::map<EventReportIntentKey, EventReportCleanupIntent> event_report_intents_;
    std::map<EventReportIntentKey, RecoveryObservation> recovery_observations_;
    uint64_t next_event_report_intent_revision_{1};
    std::atomic<bool> event_report_intent_admission_open_{false};
    std::map<std::string, EventReportScanState> event_report_scan_states_;
    std::map<std::string, bool> recovery_discovery_instances_;
    bool recovery_final_snapshot_seen_{false};
    bool recovery_discovery_complete_{false};
    uint32_t recovery_list_failures_{0};
    Clock::time_point recovery_next_retry_at_{};
    std::string last_event_report_instance_;
    size_t consecutive_event_report_ticks_{0};
    std::map<std::string, EventReportRetryBatch> event_report_retry_batches_;

    KVCM_COUNTER_METRICS_FOR_CACHE_GC(scan_round_count)
    KVCM_COUNTER_METRICS_FOR_CACHE_GC(scan_key_count)
    KVCM_COUNTER_METRICS_FOR_CACHE_GC(candidate_count)
    KVCM_COUNTER_METRICS_FOR_CACHE_GC(delete_target_count)
    KVCM_GAUGE_METRICS_FOR_CACHE_GC(inflight_delete_count)
    KVCM_GAUGE_METRICS_FOR_CACHE_GC(inflight_delete_age_ms)
    KVCM_GAUGE_METRICS_FOR_CACHE_GC(round_duration_ms)
};

#undef KVCM_COUNTER_METRICS_FOR_CACHE_GC
#undef KVCM_GAUGE_METRICS_FOR_CACHE_GC

} // namespace kv_cache_manager
