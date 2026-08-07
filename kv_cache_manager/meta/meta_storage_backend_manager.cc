#include "kv_cache_manager/meta/meta_storage_backend_manager.h"

#include <algorithm>
#include <cassert>
#include <climits>
#include <utility>

#include "kv_cache_manager/common/error_code.h"
#include "kv_cache_manager/common/logger.h"
#include "kv_cache_manager/common/request_context.h"
#include "kv_cache_manager/common/standard_uri.h"
#include "kv_cache_manager/common/timestamp_util.h"
#include "kv_cache_manager/config/meta_storage_backend_config.h"
#include "kv_cache_manager/meta/common.h"
#include "kv_cache_manager/meta/meta_storage_backend_factory.h"
#include "kv_cache_manager/metrics/metrics_collector.h"

namespace kv_cache_manager {

namespace {
constexpr int64_t kRecoverScanBatchSize = 1000;
constexpr int kRecoverMaxConsecutiveFailures = 3;

// Collects keys where results[i] == EC_NOENT. Returns {missing_keys, missing_indices}.
std::pair<KeyTypeVec, std::vector<size_t>> CollectMissingKeys(const KeyVector &keys,
                                                              const std::vector<ErrorCode> &results) {
    KeyTypeVec missing_keys;
    std::vector<size_t> missing_indices;
    for (size_t i = 0; i < keys.size(); ++i) {
        if (results[i] == EC_NOENT) {
            missing_keys.push_back(keys[i]);
            missing_indices.push_back(i);
        }
    }
    return {std::move(missing_keys), std::move(missing_indices)};
}
} // namespace

MetaStorageBackendManager::~MetaStorageBackendManager() {
    // Defensive cleanup in case Close was never called explicitly.
    is_closed_.store(true, std::memory_order_release);
    if (recover_thread_.joinable()) {
        recover_thread_.join();
    }
}

ErrorCode MetaStorageBackendManager::Init(const std::string &instance_id,
                                          const std::shared_ptr<MetaStorageBackendConfig> &config) noexcept {
    if (instance_id.empty()) {
        KVCM_LOG_ERROR("init meta storage backend manager failed, empty instance id");
        return EC_BADARGS;
    }
    if (!config) {
        KVCM_LOG_ERROR("init meta storage backend manager failed, null storage backend config");
        return EC_BADARGS;
    }
    instance_id_ = instance_id;

    const std::string &storage_uri = config->GetStorageUri();
    if (config->GetStorageType() != META_CACHED_BACKEND_TYPE_STR) {
        // Single-backend mode: one backend serves every read/write directly.
        persistent_backend_ = MetaStorageBackendFactory::CreateAndInitStorageBackend(instance_id_, config);
        if (!persistent_backend_) {
            KVCM_LOG_ERROR("fail to create persistent backend uri[%s]", storage_uri.c_str());
            return EC_ERROR;
        }
        KVCM_LOG_INFO("meta storage backend manager init ok in single-backend mode, instance[%s] type[%s]",
                      instance_id_.c_str(),
                      config->GetStorageType().c_str());
        return EC_OK;
    }

    assert((config->GetStorageType() == META_CACHED_BACKEND_TYPE_STR));
    std::string persistent_type;
    std::string cache_type;
    if (!storage_uri.empty()) {
        StandardUri uri = StandardUri::FromUri(storage_uri);
        if (!uri.Valid()) {
            KVCM_LOG_ERROR("invalid storage uri[%s]", storage_uri.c_str());
            return EC_BADARGS;
        }
        persistent_type = uri.GetParam("persistent_type");
        cache_type = uri.GetParam("cache_type");
    }
    // default to redis / local
    persistent_type = persistent_type.empty() ? META_REDIS_BACKEND_TYPE_STR : persistent_type;
    cache_type = cache_type.empty() ? META_LOCAL_BACKEND_TYPE_STR : cache_type;

    auto persistent_config = std::make_shared<MetaStorageBackendConfig>(persistent_type);
    persistent_config->SetStorageUri(storage_uri);
    persistent_backend_ = MetaStorageBackendFactory::CreatePersistentBackend(instance_id_, persistent_config);
    if (!persistent_backend_) {
        KVCM_LOG_ERROR("fail to create persistent backend uri[%s]", storage_uri.c_str());
        return EC_ERROR;
    }
    auto cache_config = std::make_shared<MetaStorageBackendConfig>(cache_type);
    cache_config->SetStorageUri(storage_uri);
    cache_backend_ = MetaStorageBackendFactory::CreateCacheBackend(instance_id_, cache_config);
    if (!cache_backend_) {
        KVCM_LOG_ERROR("fail to create cache backend uri[%s]", storage_uri.c_str());
        return EC_ERROR;
    }
    KVCM_LOG_INFO("meta storage backend manager init ok, instance[%s] cache[%s] persistent[%s]",
                  instance_id_.c_str(),
                  cache_type.c_str(),
                  persistent_type.c_str());
    return EC_OK;
}

ErrorCode MetaStorageBackendManager::Open() noexcept {
    if (!persistent_backend_) {
        KVCM_LOG_ERROR("persistent backend not inited! instance[%s]", instance_id_.c_str());
        return EC_ERROR;
    }

    ErrorCode ec = persistent_backend_->Open();
    if (ec != EC_OK) {
        KVCM_LOG_ERROR("open persistent failed, instance[%s] ec[%d]", instance_id_.c_str(), ec);
        return ec;
    }
    is_closed_.store(false, std::memory_order_release);

    if (!cache_backend_) {
        recover_state_.store(RecoverState::kRunning, std::memory_order_release);
        KVCM_LOG_INFO("meta storage backend manager opened in single-backend mode, instance[%s]", instance_id_.c_str());
        return EC_OK;
    }

    ec = cache_backend_->Open();
    if (ec != EC_OK) {
        KVCM_LOG_ERROR("open cache failed, instance[%s] ec[%d]", instance_id_.c_str(), ec);
        return ec;
    }
    recover_state_.store(RecoverState::kRecover, std::memory_order_release);
    recover_thread_ = std::thread(&MetaStorageBackendManager::AsyncRecoverTask, this);
    KVCM_LOG_INFO("meta storage backend manager opened, instance[%s], async recover started", instance_id_.c_str());
    return EC_OK;
}

ErrorCode MetaStorageBackendManager::Close() noexcept {
    is_closed_.store(true, std::memory_order_release);
    if (recover_thread_.joinable()) {
        recover_thread_.join();
    }

    ErrorCode cache_ec = EC_OK;
    ErrorCode persistent_ec = EC_OK;
    if (cache_backend_) {
        cache_ec = cache_backend_->Close();
    }
    if (persistent_backend_) {
        persistent_ec = persistent_backend_->Close();
    }
    if (cache_ec != EC_OK) {
        KVCM_LOG_ERROR("close cache failed, instance[%s] ec[%d]", instance_id_.c_str(), cache_ec);
        return cache_ec;
    }
    if (persistent_ec != EC_OK) {
        KVCM_LOG_ERROR("close persistent failed, instance[%s] ec[%d]", instance_id_.c_str(), persistent_ec);
        return persistent_ec;
    }
    KVCM_LOG_INFO("meta storage backend manager closed, instance[%s]", instance_id_.c_str());
    return EC_OK;
}

void MetaStorageBackendManager::AsyncRecoverTask() noexcept {
    KVCM_LOG_INFO("meta storage backend manager async recover started, instance[%s]", instance_id_.c_str());
    std::string cursor = SCAN_BASE_CURSOR;
    int64_t total_backfilled_keys = 0;
    int consecutive_failures = 0;
    std::string next_cursor;
    KeyTypeVec scanned_keys;
    FieldMapVec field_maps;
    do {
        if (is_closed_.load(std::memory_order_acquire)) {
            KVCM_LOG_INFO("async recover aborted due to close, instance[%s]", instance_id_.c_str());
            return;
        }

        scanned_keys.clear();
        field_maps.clear();
        ErrorCode scan_ec =
            persistent_backend_->ListKeys(nullptr, cursor, kRecoverScanBatchSize, next_cursor, scanned_keys);
        if (scan_ec != EC_OK) {
            ++consecutive_failures;
            KVCM_LOG_ERROR("async recover scan failed, instance[%s] cursor[%s] ec[%d] attempt[%d/%d]",
                           instance_id_.c_str(),
                           cursor.c_str(),
                           scan_ec,
                           consecutive_failures,
                           kRecoverMaxConsecutiveFailures);
            if (consecutive_failures >= kRecoverMaxConsecutiveFailures) {
                KVCM_LOG_ERROR("async recover giving up after %d consecutive failures, "
                               "forcing transition to Running, instance[%s]",
                               kRecoverMaxConsecutiveFailures,
                               instance_id_.c_str());
                break;
            }
            continue;
        }

        consecutive_failures = 0;
        cursor = next_cursor;
        if (scanned_keys.empty()) {
            continue;
        }
        CacheLocationMapVector locations;
        PropertyMapVector properties;
        std::vector<ErrorCode> get_error_codes = persistent_backend_->Get(nullptr, scanned_keys, locations, properties);
        for (size_t i = 0; i < scanned_keys.size(); ++i) {
            if (get_error_codes[i] != EC_OK && get_error_codes[i] != EC_NOENT) {
                KVCM_LOG_WARN("async recover key[%ld] get failed ec[%d]", scanned_keys[i], get_error_codes[i]);
            }
        }
        total_backfilled_keys += BackfillKeysToCache(scanned_keys, locations, properties, get_error_codes);
    } while (cursor != SCAN_BASE_CURSOR);

    if (consecutive_failures == 0) {
        KVCM_LOG_INFO("async recover completed instance[%s] total_backfilled_keys[%ld]",
                      instance_id_.c_str(),
                      total_backfilled_keys);
    } else {
        KVCM_LOG_WARN("async recover partial, instance[%s] total_backfilled_keys[%ld], forcing transition to Running",
                      instance_id_.c_str(),
                      total_backfilled_keys);
    }
    recover_state_.store(RecoverState::kRunning, std::memory_order_release);
    {
        std::lock_guard<std::mutex> lock(deleted_keys_mutex_);
        deleted_keys_.clear();
    }
}

void MetaStorageBackendManager::EnsureKeyInCache(RequestContext *request_context, const KeyTypeVec &keys) noexcept {
    if (keys.empty()) {
        return;
    }
    std::vector<bool> exists_vec;
    std::vector<ErrorCode> exists_results = cache_backend_->Exists(request_context, keys, exists_vec);
    KeyTypeVec missing_keys;
    for (size_t i = 0; i < keys.size(); ++i) {
        if (exists_results[i] != EC_OK || !exists_vec[i]) {
            missing_keys.emplace_back(keys[i]);
        }
    }
    if (missing_keys.empty()) {
        return;
    }

    CacheLocationMapVector locations;
    PropertyMapVector properties;
    std::vector<ErrorCode> get_results = persistent_backend_->Get(request_context, missing_keys, locations, properties);
    std::vector<ErrorCode> put_results =
        cache_backend_->Put(request_context, missing_keys, locations, properties, get_results);
    for (size_t i = 0; i < missing_keys.size(); ++i) {
        if (put_results[i] != EC_OK && get_results[i] == EC_OK) {
            KVCM_LOG_WARN("ensure key[%ld] in cache failed, ec[%d]", missing_keys[i], put_results[i]);
        }
    }
}

int64_t MetaStorageBackendManager::BackfillKeysToCache(const KeyTypeVec &keys,
                                                       const CacheLocationMapVector &locations,
                                                       const PropertyMapVector &properties,
                                                       const std::vector<ErrorCode> &get_error_codes) noexcept {
    std::lock_guard<std::mutex> lock(deleted_keys_mutex_);

    // Merge get errors and deleted-key tombstones into a single error vector.
    assert(get_error_codes.size() == keys.size());
    int64_t valid_count = 0;
    std::vector<ErrorCode> merged_error_codes = get_error_codes;
    for (size_t i = 0; i < keys.size(); ++i) {
        if (merged_error_codes[i] == EC_OK && deleted_keys_.count(keys[i]) > 0) {
            merged_error_codes[i] = EC_NOENT;
        }
        valid_count += (merged_error_codes[i] == EC_OK);
    }
    if (valid_count == 0) {
        return 0;
    }

    std::vector<ErrorCode> put_results =
        cache_backend_->PutIfAbsent(nullptr, keys, locations, properties, merged_error_codes);
    int64_t backfilled_count = 0;
    for (size_t i = 0; i < keys.size(); ++i) {
        if (put_results[i] == EC_OK) {
            ++backfilled_count;
        } else if (merged_error_codes[i] == EC_OK && put_results[i] != EC_NOENT) {
            KVCM_LOG_WARN("backfill PutIfAbsent failed key[%ld] ec[%d]", keys[i], put_results[i]);
        }
    }
    return backfilled_count;
}

std::vector<ErrorCode> MetaStorageBackendManager::Put(RequestContext *request_context, BatchMetaData &batch) noexcept {
    const KeyVector &keys = batch.batch_keys;
    batch.EnsureLocationsAndPropertiesResized();
    CacheLocationMapVector &locations = batch.batch_locations;
    PropertyMapVector &properties = batch.batch_properties;
    std::vector<ErrorCode> persistent_results = persistent_backend_->Put(request_context, keys, locations, properties);
    if (!cache_backend_) {
        return persistent_results;
    }
    const int64_t cache_begin = TimestampUtil::GetCurrentTimeUs();
    auto results = cache_backend_->Put(request_context, keys, locations, properties, persistent_results);
    if (request_context) {
        auto *mc = dynamic_cast<ServiceMetricsCollector *>(request_context->metrics_collector());
        KVCM_METRICS_COLLECTOR_SET_METRICS(
            mc, meta_indexer, cache_backend_put_time_us, TimestampUtil::GetCurrentTimeUs() - cache_begin);
    }
    return results;
}

std::vector<ErrorCode> MetaStorageBackendManager::Upsert(RequestContext *request_context,
                                                         BatchMetaData &batch) noexcept {
    const KeyVector &keys = batch.batch_keys;
    batch.EnsureLocationsAndPropertiesResized();
    CacheLocationMapVector &locations = batch.batch_locations;
    PropertyMapVector &properties = batch.batch_properties;

    // Upsert may touch only a subset of fields, so Recover-time hydration
    // is needed to avoid overwriting unmentioned fields with empty values.
    if (cache_backend_ && recover_state_.load(std::memory_order_acquire) == RecoverState::kRecover) {
        EnsureKeyInCache(request_context, keys);
    }
    std::vector<ErrorCode> persistent_results =
        persistent_backend_->Upsert(request_context, keys, locations, properties);
    if (!cache_backend_) {
        return persistent_results;
    }
    const int64_t cache_begin = TimestampUtil::GetCurrentTimeUs();
    auto results = cache_backend_->Upsert(request_context, keys, locations, properties, persistent_results);
    if (request_context) {
        auto *mc = dynamic_cast<ServiceMetricsCollector *>(request_context->metrics_collector());
        KVCM_METRICS_COLLECTOR_SET_METRICS(
            mc, meta_indexer, cache_backend_upsert_time_us, TimestampUtil::GetCurrentTimeUs() - cache_begin);
    }
    return results;
}

std::vector<ErrorCode> MetaStorageBackendManager::Delete(RequestContext *request_context,
                                                         const KeyVector &keys) noexcept {
    std::vector<ErrorCode> persistent_results = persistent_backend_->Delete(request_context, keys);
    if (!cache_backend_) {
        return persistent_results;
    }
    if (recover_state_.load(std::memory_order_acquire) == RecoverState::kRecover) {
        // Tombstone to prevent Recover backfill from resurrecting deleted keys.
        std::lock_guard<std::mutex> lock(deleted_keys_mutex_);
        for (const auto &key : keys) {
            deleted_keys_.insert(key);
        }
    }
    const int64_t cache_begin = TimestampUtil::GetCurrentTimeUs();
    auto results = cache_backend_->Delete(request_context, keys, persistent_results);
    if (request_context) {
        auto *mc = dynamic_cast<ServiceMetricsCollector *>(request_context->metrics_collector());
        KVCM_METRICS_COLLECTOR_SET_METRICS(
            mc, meta_indexer, cache_backend_delete_time_us, TimestampUtil::GetCurrentTimeUs() - cache_begin);
    }
    return results;
}

std::vector<ErrorCode> MetaStorageBackendManager::Delete(RequestContext *request_context,
                                                         const KeyVector &keys,
                                                         const LocationIdsPerKey &location_ids,
                                                         int32_t &out_reclaimed_count) noexcept {
    out_reclaimed_count = 0;
    if (keys.empty()) {
        return {};
    }
    assert(location_ids.size() == keys.size());

    // Partial-delete during Recover: hydrate cache from persistent first so
    // the conditional mirror write below has the full pre-restart field set
    // to delete against (and async backfill cannot later overwrite us).
    if (cache_backend_ && recover_state_.load(std::memory_order_acquire) == RecoverState::kRecover) {
        EnsureKeyInCache(request_context, keys);
    }

    std::vector<ErrorCode> persistent_results =
        persistent_backend_->DeleteLocations(request_context, keys, location_ids);
    std::vector<ErrorCode> results;
    if (!cache_backend_) {
        results = std::move(persistent_results);
    } else {
        const int64_t cache_begin = TimestampUtil::GetCurrentTimeUs();
        results = cache_backend_->DeleteLocations(request_context, keys, location_ids, persistent_results);
        if (request_context) {
            auto *mc = dynamic_cast<ServiceMetricsCollector *>(request_context->metrics_collector());
            KVCM_METRICS_COLLECTOR_SET_METRICS(
                mc, meta_indexer, cache_backend_delete_time_us, TimestampUtil::GetCurrentTimeUs() - cache_begin);
        }
    }

    out_reclaimed_count = MaybeReclaimEmptyKeys(request_context, keys, results);
    return results;
}

MaintenanceLocationDeleteResults MetaStorageBackendManager::CompareDeleteLocationsForMaintenance(
    RequestContext *request_context,
    const KeyVector &keys,
    const LocationIdsPerKey &location_ids,
    const std::vector<std::vector<std::string>> &expected_location_values) noexcept {
    MaintenanceLocationDeleteResults results(keys.size());
    if (keys.size() != location_ids.size() || keys.size() != expected_location_values.size()) {
        return {};
    }
    for (size_t i = 0; i < keys.size(); ++i) {
        if (location_ids[i].size() != expected_location_values[i].size()) {
            return {};
        }
        results[i].resize(location_ids[i].size());
    }
    if (keys.empty()) {
        return results;
    }

    // Capture layer-local key contents before mutation. Besides avoiding the
    // ordinary touching read path, this lets us enqueue whole-key deletion
    // immediately after deleting the last Location while the caller still
    // owns the MetaIndexer shard lock.
    LocationIdsPerKey persistent_location_ids;
    const std::vector<ErrorCode> persistent_id_ecs =
        persistent_backend_->GetLocationIdsForMaintenance(request_context, keys, persistent_location_ids);

    LocationIdsPerKey hot_location_ids;
    std::vector<ErrorCode> hot_id_ecs;
    if (cache_backend_) {
        hot_id_ecs = cache_backend_->GetLocationIdsForMaintenance(request_context, keys, hot_location_ids);
    }

    LocationsPerKey hot_locations;
    std::vector<std::vector<ErrorCode>> hot_ecs;
    if (cache_backend_) {
        hot_ecs = cache_backend_->GetLocationsForMaintenance(request_context, keys, location_ids, hot_locations);
        if (hot_ecs.size() != keys.size() || hot_locations.size() != keys.size()) {
            for (auto &per_key : results) {
                for (auto &result : per_key) {
                    result.ec = EC_ERROR;
                }
            }
            return results;
        }
    }

    LocationIdsPerKey persistent_target_ids(keys.size());
    std::vector<std::vector<size_t>> persistent_target_indexes(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        if (cache_backend_ &&
            (hot_ecs[i].size() != location_ids[i].size() || hot_locations[i].size() != location_ids[i].size())) {
            for (auto &result : results[i]) {
                result.ec = EC_ERROR;
            }
            continue;
        }
        for (size_t j = 0; j < location_ids[i].size(); ++j) {
            if (!cache_backend_ || hot_ecs[i][j] == EC_NOENT) {
                persistent_target_ids[i].push_back(location_ids[i][j]);
                persistent_target_indexes[i].push_back(j);
            }
        }
    }

    LocationsPerKey persistent_locations;
    const std::vector<std::vector<ErrorCode>> persistent_ecs = persistent_backend_->GetLocationsForMaintenance(
        request_context, keys, persistent_target_ids, persistent_locations);
    if (persistent_ecs.size() != keys.size() || persistent_locations.size() != keys.size()) {
        for (size_t i = 0; i < keys.size(); ++i) {
            for (const size_t j : persistent_target_indexes[i]) {
                results[i][j].ec = EC_ERROR;
            }
        }
        return results;
    }

    std::vector<std::vector<size_t>> authorized_indexes(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        if (persistent_ecs[i].size() != persistent_target_ids[i].size() ||
            persistent_locations[i].size() != persistent_target_ids[i].size()) {
            for (const size_t j : persistent_target_indexes[i]) {
                results[i][j].ec = EC_ERROR;
            }
            continue;
        }
        size_t persistent_index = 0;
        for (size_t j = 0; j < location_ids[i].size(); ++j) {
            ErrorCode read_ec = EC_NOENT;
            CacheLocationConstPtr location;
            if (cache_backend_ && hot_ecs[i][j] != EC_NOENT) {
                read_ec = hot_ecs[i][j];
                location = hot_locations[i][j];
            } else {
                read_ec = persistent_ecs[i][persistent_index];
                location = persistent_locations[i][persistent_index];
                ++persistent_index;
            }

            if (read_ec == EC_NOENT) {
                results[i][j].ec = EC_NOENT;
                continue;
            }
            if (read_ec != EC_OK || !location) {
                results[i][j].ec = read_ec == EC_OK ? EC_ERROR : read_ec;
                continue;
            }
            if (location->ToJsonString() != expected_location_values[i][j]) {
                results[i][j].ec = EC_MISMATCH;
                continue;
            }
            authorized_indexes[i].push_back(j);
        }
    }

    auto layer_has_no_unauthorized_locations = [&](size_t key_index,
                                                   const std::vector<ErrorCode> &id_ecs,
                                                   const LocationIdsPerKey &ids) {
        if (id_ecs.size() != keys.size() || ids.size() != keys.size()) {
            return false;
        }
        if (id_ecs[key_index] == EC_NOENT) {
            return true;
        }
        if (id_ecs[key_index] != EC_OK) {
            return false;
        }
        for (const auto &existing_id : ids[key_index]) {
            const auto target_it =
                std::find(location_ids[key_index].begin(), location_ids[key_index].end(), existing_id);
            if (target_it == location_ids[key_index].end()) {
                return false;
            }
            const size_t target_index = static_cast<size_t>(target_it - location_ids[key_index].begin());
            if (std::find(authorized_indexes[key_index].begin(), authorized_indexes[key_index].end(), target_index) ==
                authorized_indexes[key_index].end()) {
                return false;
            }
        }
        return true;
    };

    // Whole-key reclamation must be a decision over both cache layers. A
    // concurrent async mutation may already be visible in hot cache while its
    // persistent write is still queued (or vice versa). Deleting an apparently
    // last Location from only one layer must not erase a different Location
    // that is visible in the other layer.
    std::vector<bool> delete_whole_keys(keys.size(), false);
    for (size_t i = 0; i < keys.size(); ++i) {
        if (authorized_indexes[i].empty()) {
            continue;
        }
        const bool persistent_safe = layer_has_no_unauthorized_locations(i, persistent_id_ecs, persistent_location_ids);
        const bool hot_safe = !cache_backend_ || layer_has_no_unauthorized_locations(i, hot_id_ecs, hot_location_ids);
        delete_whole_keys[i] = persistent_safe && hot_safe;
    }

    auto target_existed_in_layer =
        [&](size_t key_index, size_t target_index, const std::vector<ErrorCode> &id_ecs, const LocationIdsPerKey &ids) {
            return id_ecs.size() == keys.size() && ids.size() == keys.size() && id_ecs[key_index] == EC_OK &&
                   std::find(ids[key_index].begin(), ids[key_index].end(), location_ids[key_index][target_index]) !=
                       ids[key_index].end();
        };

    struct LayerDeletePlan {
        size_t key_index{0};
        bool delete_whole_key{false};
    };

    auto execute_layer = [&](MetaStorageBackend *backend,
                             const std::vector<ErrorCode> &layer_id_ecs,
                             const LocationIdsPerKey &layer_ids,
                             const std::vector<size_t> &eligible_key_indexes,
                             bool persistent_layer,
                             std::vector<bool> &out_accepted) {
        std::vector<LayerDeletePlan> whole_plans;
        std::vector<LayerDeletePlan> location_plans;
        KeyVector whole_keys;
        KeyVector location_keys;
        LocationIdsPerKey ids_to_delete;
        for (const size_t key_index : eligible_key_indexes) {
            const bool delete_whole_key = delete_whole_keys[key_index];
            if (delete_whole_key) {
                whole_keys.push_back(keys[key_index]);
                whole_plans.push_back({key_index, true});
                continue;
            }
            location_keys.push_back(keys[key_index]);
            location_plans.push_back({key_index, false});
            auto &ids = ids_to_delete.emplace_back();
            ids.reserve(authorized_indexes[key_index].size());
            for (const size_t target_index : authorized_indexes[key_index]) {
                ids.push_back(location_ids[key_index][target_index]);
            }
        }

        std::vector<ErrorCode> whole_ecs;
        if (!whole_keys.empty()) {
            whole_ecs = backend->Delete(request_context, whole_keys);
        }
        if (whole_ecs.size() != whole_plans.size()) {
            whole_ecs.assign(whole_plans.size(), EC_ERROR);
        }
        std::vector<ErrorCode> location_ecs;
        if (!location_keys.empty()) {
            location_ecs = backend->DeleteLocationsForMaintenance(request_context, location_keys, ids_to_delete);
        }
        if (location_ecs.size() != location_plans.size()) {
            location_ecs.assign(location_plans.size(), EC_ERROR);
        }

        auto apply_result = [&](const LayerDeletePlan &plan, ErrorCode ec) {
            const bool accepted = ec == EC_OK || ec == EC_NOENT;
            out_accepted[plan.key_index] = accepted;
            bool reclaim_recorded = false;
            for (const size_t target_index : authorized_indexes[plan.key_index]) {
                auto &target_result = results[plan.key_index][target_index];
                if (!accepted) {
                    target_result.ec = ec;
                    continue;
                }
                target_result.ec = EC_OK;
                const bool removed =
                    ec == EC_OK && target_existed_in_layer(plan.key_index, target_index, layer_id_ecs, layer_ids);
                if (persistent_layer) {
                    target_result.removed_from_persistent = removed;
                    if (plan.delete_whole_key && ec == EC_OK && !reclaim_recorded) {
                        target_result.reclaimed_persistent_key = true;
                        reclaim_recorded = true;
                    }
                } else {
                    target_result.removed_from_hot = removed;
                    if (plan.delete_whole_key && ec == EC_OK && !reclaim_recorded) {
                        target_result.reclaimed_hot_key = true;
                        reclaim_recorded = true;
                    }
                }
            }
        };
        for (size_t i = 0; i < whole_plans.size(); ++i) {
            apply_result(whole_plans[i], whole_ecs[i]);
        }
        for (size_t i = 0; i < location_plans.size(); ++i) {
            apply_result(location_plans[i], location_ecs[i]);
        }
    };

    std::vector<size_t> persistent_eligible_keys;
    persistent_eligible_keys.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        if (!authorized_indexes[i].empty()) {
            persistent_eligible_keys.push_back(i);
        }
    }
    std::vector<bool> persistent_accepted(keys.size(), false);
    execute_layer(persistent_backend_.get(),
                  persistent_id_ecs,
                  persistent_location_ids,
                  persistent_eligible_keys,
                  true,
                  persistent_accepted);

    if (cache_backend_) {
        std::vector<size_t> hot_eligible_keys;
        hot_eligible_keys.reserve(persistent_eligible_keys.size());
        for (const size_t key_index : persistent_eligible_keys) {
            if (persistent_accepted[key_index]) {
                hot_eligible_keys.push_back(key_index);
            }
        }
        std::vector<bool> hot_accepted(keys.size(), false);
        execute_layer(cache_backend_.get(), hot_id_ecs, hot_location_ids, hot_eligible_keys, false, hot_accepted);
    }
    return results;
}

int32_t MetaStorageBackendManager::MaybeReclaimEmptyKeys(RequestContext *request_context,
                                                         const KeyVector &keys,
                                                         const std::vector<ErrorCode> &delete_results) noexcept {
    KeyVector candidate_keys;
    for (size_t i = 0; i < keys.size(); ++i) {
        if (delete_results[i] == EC_OK) {
            candidate_keys.push_back(keys[i]);
        }
    }
    if (candidate_keys.empty()) {
        return 0;
    }

    std::vector<bool> has_locations;
    std::vector<ErrorCode> exists_ecs;
    if (cache_backend_) {
        exists_ecs = cache_backend_->ExistsLocation(request_context, candidate_keys, has_locations);
    } else {
        exists_ecs = persistent_backend_->ExistsLocation(request_context, candidate_keys, has_locations);
    }

    KeyVector reclaimed_keys;
    for (size_t i = 0; i < candidate_keys.size(); ++i) {
        if (exists_ecs[i] == EC_OK && !has_locations[i]) {
            reclaimed_keys.push_back(candidate_keys[i]);
        }
    }
    if (reclaimed_keys.empty()) {
        return 0;
    }

    std::vector<ErrorCode> whole_ecs = Delete(request_context, reclaimed_keys);
    int32_t reclaimed = 0;
    for (const ErrorCode ec : whole_ecs) {
        if (ec == EC_OK || ec == EC_NOENT) {
            ++reclaimed;
        }
    }
    return reclaimed;
}

std::vector<ErrorCode> MetaStorageBackendManager::Get(RequestContext *request_context,
                                                      const KeyVector &keys,
                                                      CacheLocationMapVector &out_locations,
                                                      PropertyMapVector &out_properties) noexcept {
    if (!cache_backend_) {
        return persistent_backend_->Get(request_context, keys, out_locations, out_properties);
    }

    std::vector<ErrorCode> results = cache_backend_->Get(request_context, keys, out_locations, out_properties);
    if (recover_state_.load(std::memory_order_acquire) == RecoverState::kRunning) {
        return results;
    }

    auto [missing_keys, missing_indices] = CollectMissingKeys(keys, results);
    if (missing_keys.empty()) {
        return results;
    }

    CacheLocationMapVector persistent_locations;
    PropertyMapVector persistent_properties;
    std::vector<ErrorCode> persistent_results =
        persistent_backend_->Get(request_context, missing_keys, persistent_locations, persistent_properties);
    if (missing_keys.size() != persistent_locations.size() || missing_keys.size() != persistent_properties.size()) {
        KVCM_LOG_ERROR("persistent Get size mismatch: locations[%lu] properties[%lu] vs keys[%lu]",
                       persistent_locations.size(),
                       persistent_properties.size(),
                       missing_keys.size());
        for (size_t i = 0; i < missing_keys.size(); ++i) {
            results[missing_indices[i]] = EC_ERROR;
        }
        return results;
    }
    for (size_t i = 0; i < missing_keys.size(); ++i) {
        const size_t original_idx = missing_indices[i];
        results[original_idx] = persistent_results[i];
        if (persistent_results[i] == EC_OK) {
            out_locations[original_idx] = std::move(persistent_locations[i]);
            out_properties[original_idx] = std::move(persistent_properties[i]);
        }
    }
    return results;
}

std::vector<ErrorCode> MetaStorageBackendManager::GetLocations(RequestContext *request_context,
                                                               const KeyVector &keys,
                                                               CacheLocationMapVector &out_location_maps) noexcept {
    if (!cache_backend_) {
        return persistent_backend_->GetLocations(request_context, keys, out_location_maps);
    }

    std::vector<ErrorCode> results = cache_backend_->GetLocations(request_context, keys, out_location_maps);
    if (recover_state_.load(std::memory_order_acquire) == RecoverState::kRunning) {
        return results;
    }

    auto [missing_keys, missing_indices] = CollectMissingKeys(keys, results);
    if (missing_keys.empty()) {
        return results;
    }

    CacheLocationMapVector persistent_locations;
    std::vector<ErrorCode> persistent_results =
        persistent_backend_->GetLocations(request_context, missing_keys, persistent_locations);
    if (missing_keys.size() != persistent_locations.size()) {
        KVCM_LOG_ERROR(
            "persistent_locations size[%lu] mismatch keys's[%lu]", persistent_locations.size(), missing_keys.size());
        for (size_t i = 0; i < missing_keys.size(); ++i) {
            results[missing_indices[i]] = EC_ERROR;
        }
        return results;
    }
    for (size_t i = 0; i < missing_keys.size(); ++i) {
        const size_t original_idx = missing_indices[i];
        results[original_idx] = persistent_results[i];
        if (persistent_results[i] == EC_OK) {
            out_location_maps[original_idx] = std::move(persistent_locations[i]);
        }
    }
    return results;
}

std::vector<ErrorCode> MetaStorageBackendManager::GetLocationsFromPersistent(
    RequestContext *request_context, const KeyVector &keys, CacheLocationMapVector &out_location_maps) noexcept {
    out_location_maps.clear();
    if (keys.empty()) {
        return {};
    }
    if (!persistent_backend_) {
        KVCM_LOG_ERROR("persistent GetLocations failed, backend is null, instance[%s]", instance_id_.c_str());
        out_location_maps.resize(keys.size());
        return std::vector<ErrorCode>(keys.size(), EC_ERROR);
    }
    std::vector<ErrorCode> results = persistent_backend_->GetLocations(request_context, keys, out_location_maps);
    if (results.size() != keys.size() || out_location_maps.size() != keys.size()) {
        KVCM_LOG_ERROR("persistent GetLocations shape mismatch, instance[%s] keys[%zu] results[%zu] locations[%zu]",
                       instance_id_.c_str(),
                       keys.size(),
                       results.size(),
                       out_location_maps.size());
        out_location_maps.resize(keys.size());
        return std::vector<ErrorCode>(keys.size(), EC_ERROR);
    }
    return results;
}

std::vector<ErrorCode> MetaStorageBackendManager::RefreshCacheFromPersistent(RequestContext *request_context,
                                                                             const KeyVector &keys) noexcept {
    if (keys.empty()) {
        return {};
    }
    if (!cache_backend_) {
        return std::vector<ErrorCode>(keys.size(), EC_OK);
    }

    CacheLocationMapVector locations;
    PropertyMapVector properties;
    std::vector<ErrorCode> persistent_results = persistent_backend_->Get(request_context, keys, locations, properties);
    if (persistent_results.size() != keys.size() || locations.size() != keys.size() ||
        properties.size() != keys.size()) {
        KVCM_LOG_ERROR("persistent refresh shape mismatch, instance[%s] keys[%zu] results[%zu] locations[%zu] "
                       "properties[%zu]",
                       instance_id_.c_str(),
                       keys.size(),
                       persistent_results.size(),
                       locations.size(),
                       properties.size());
        return std::vector<ErrorCode>(keys.size(), EC_ERROR);
    }

    std::vector<ErrorCode> results = persistent_results;
    const std::vector<ErrorCode> put_results =
        cache_backend_->Put(request_context, keys, locations, properties, persistent_results);
    if (put_results.size() != keys.size()) {
        KVCM_LOG_ERROR("cache refresh Put shape mismatch, instance[%s] keys[%zu] results[%zu]",
                       instance_id_.c_str(),
                       keys.size(),
                       put_results.size());
        return std::vector<ErrorCode>(keys.size(), EC_ERROR);
    }

    KeyVector missing_keys;
    for (size_t i = 0; i < keys.size(); ++i) {
        if (persistent_results[i] == EC_OK) {
            results[i] = put_results[i];
        } else if (persistent_results[i] == EC_NOENT) {
            // Do not let a stale hot-cache entry resurrect metadata that has
            // already disappeared from the source of truth.
            missing_keys.push_back(keys[i]);
        }
    }
    if (!missing_keys.empty()) {
        (void)cache_backend_->Delete(request_context, missing_keys);
    }
    return results;
}

std::vector<std::vector<ErrorCode>> MetaStorageBackendManager::GetLocations(RequestContext *request_context,
                                                                            const KeyVector &keys,
                                                                            const LocationIdsPerKey &location_ids,
                                                                            LocationsPerKey &out_locations) noexcept {
    if (!cache_backend_) {
        return persistent_backend_->GetLocations(request_context, keys, location_ids, out_locations);
    }

    std::vector<std::vector<ErrorCode>> results =
        cache_backend_->GetLocations(request_context, keys, location_ids, out_locations);
    if (recover_state_.load(std::memory_order_acquire) == RecoverState::kRunning) {
        return results;
    }

    KeyTypeVec missing_keys;
    std::vector<size_t> missing_indices;
    LocationIdsPerKey missing_location_ids;
    for (size_t i = 0; i < keys.size(); ++i) {
        if (!results[i].empty() && results[i][0] == EC_NOENT) {
            missing_keys.push_back(keys[i]);
            missing_indices.push_back(i);
            missing_location_ids.push_back(location_ids[i]);
        }
    }
    if (missing_keys.empty()) {
        return results;
    }

    LocationsPerKey persistent_locations;
    std::vector<std::vector<ErrorCode>> persistent_results =
        persistent_backend_->GetLocations(request_context, missing_keys, missing_location_ids, persistent_locations);
    if (missing_keys.size() != persistent_results.size()) {
        KVCM_LOG_ERROR(
            "persistent results size[%lu] mismatch keys's[%lu]", persistent_results.size(), missing_keys.size());
        for (size_t i = 0; i < missing_keys.size(); ++i) {
            results[missing_indices[i]].assign(location_ids[missing_indices[i]].size(), EC_ERROR);
        }
        return results;
    }
    for (size_t i = 0; i < missing_keys.size(); ++i) {
        const size_t original_idx = missing_indices[i];
        results[original_idx] = std::move(persistent_results[i]);
        out_locations[original_idx] = std::move(persistent_locations[i]);
    }
    return results;
}

std::vector<ErrorCode> MetaStorageBackendManager::GetLocationIds(RequestContext *request_context,
                                                                 const KeyVector &keys,
                                                                 LocationIdsPerKey &out_location_ids) noexcept {
    if (!cache_backend_) {
        return persistent_backend_->GetLocationIds(request_context, keys, out_location_ids);
    }

    std::vector<ErrorCode> results = cache_backend_->GetLocationIds(request_context, keys, out_location_ids);
    if (recover_state_.load(std::memory_order_acquire) == RecoverState::kRunning) {
        return results;
    }

    auto [missing_keys, missing_indices] = CollectMissingKeys(keys, results);
    if (missing_keys.empty()) {
        return results;
    }

    LocationIdsPerKey persistent_location_ids;
    std::vector<ErrorCode> persistent_results =
        persistent_backend_->GetLocationIds(request_context, missing_keys, persistent_location_ids);
    if (missing_keys.size() != persistent_location_ids.size()) {
        KVCM_LOG_ERROR("persistent_location_ids size[%lu] mismatch keys's[%lu]",
                       persistent_location_ids.size(),
                       missing_keys.size());
        for (size_t i = 0; i < missing_keys.size(); ++i) {
            results[missing_indices[i]] = EC_ERROR;
        }
        return results;
    }
    for (size_t i = 0; i < missing_keys.size(); ++i) {
        const size_t original_idx = missing_indices[i];
        results[original_idx] = persistent_results[i];
        if (persistent_results[i] == EC_OK) {
            out_location_ids[original_idx] = std::move(persistent_location_ids[i]);
        }
    }
    return results;
}

std::vector<ErrorCode> MetaStorageBackendManager::GetProperties(RequestContext *request_context,
                                                                const KeyVector &keys,
                                                                const std::vector<std::string> &field_names,
                                                                PropertyMapVector &out_properties) noexcept {
    if (!cache_backend_) {
        return persistent_backend_->GetProperties(request_context, keys, field_names, out_properties);
    }

    std::vector<ErrorCode> results = cache_backend_->GetProperties(request_context, keys, field_names, out_properties);
    if (recover_state_.load(std::memory_order_acquire) == RecoverState::kRunning) {
        return results;
    }

    auto [missing_keys, missing_indices] = CollectMissingKeys(keys, results);
    if (missing_keys.empty()) {
        return results;
    }

    PropertyMapVector persistent_properties;
    std::vector<ErrorCode> persistent_results =
        persistent_backend_->GetProperties(request_context, missing_keys, field_names, persistent_properties);
    if (missing_keys.size() != persistent_properties.size()) {
        KVCM_LOG_ERROR(
            "persistent_properties size[%lu] mismatch keys's[%lu]", persistent_properties.size(), missing_keys.size());
        for (size_t i = 0; i < missing_keys.size(); ++i) {
            results[missing_indices[i]] = EC_ERROR;
        }
        return results;
    }
    for (size_t i = 0; i < missing_keys.size(); ++i) {
        const size_t original_idx = missing_indices[i];
        results[original_idx] = persistent_results[i];
        if (persistent_results[i] == EC_OK) {
            out_properties[original_idx] = std::move(persistent_properties[i]);
        }
    }
    return results;
}

std::vector<ErrorCode> MetaStorageBackendManager::Exists(RequestContext *request_context,
                                                         const KeyVector &keys,
                                                         std::vector<bool> &out_is_exist_vec) noexcept {
    if (!cache_backend_) {
        return persistent_backend_->Exists(request_context, keys, out_is_exist_vec);
    }
    std::vector<ErrorCode> results = cache_backend_->Exists(request_context, keys, out_is_exist_vec);
    if (recover_state_.load(std::memory_order_acquire) == RecoverState::kRunning) {
        return results;
    }

    KeyTypeVec missing_keys;
    std::vector<size_t> missing_indices;
    for (size_t i = 0; i < keys.size(); ++i) {
        if ((results[i] == EC_OK && !out_is_exist_vec[i]) || results[i] == EC_NOENT) {
            missing_keys.push_back(keys[i]);
            missing_indices.push_back(i);
        }
    }
    if (missing_keys.empty()) {
        return results;
    }

    std::vector<bool> persistent_exists;
    std::vector<ErrorCode> persistent_results =
        persistent_backend_->Exists(request_context, missing_keys, persistent_exists);
    if (missing_keys.size() != persistent_exists.size()) {
        KVCM_LOG_ERROR(
            "persistent_exists size[%lu] mismatch missing_keys's[%lu]", persistent_exists.size(), missing_keys.size());
        for (size_t i = 0; i < missing_keys.size(); ++i) {
            results[missing_indices[i]] = EC_ERROR;
        }
        return results;
    }
    for (size_t i = 0; i < missing_keys.size(); ++i) {
        const size_t original_idx = missing_indices[i];
        results[original_idx] = persistent_results[i];
        out_is_exist_vec[original_idx] = persistent_exists[i];
    }
    return results;
}

ErrorCode MetaStorageBackendManager::ListKeys(RequestContext *request_context,
                                              const std::string &cursor,
                                              const int64_t limit,
                                              std::string &out_next_cursor,
                                              KeyTypeVec &out_keys) noexcept {
    if (cache_backend_ && recover_state_.load(std::memory_order_acquire) == RecoverState::kRunning) {
        return cache_backend_->ListKeys(request_context, cursor, limit, out_next_cursor, out_keys);
    }
    return persistent_backend_->ListKeys(request_context, cursor, limit, out_next_cursor, out_keys);
}

ErrorCode MetaStorageBackendManager::ScanLocationsForMaintenance(RequestContext *request_context,
                                                                 const std::string &cursor,
                                                                 const int64_t limit,
                                                                 MaintenanceScanBatch &out) noexcept {
    out.Clear();
    if (!persistent_backend_) {
        KVCM_LOG_ERROR("maintenance scan failed, persistent backend is null, instance[%s]", instance_id_.c_str());
        return EC_ERROR;
    }

    MaintenanceScanBatch batch;
    ErrorCode ec = persistent_backend_->ScanLocationsForMaintenance(request_context, cursor, limit, batch);
    if (ec != EC_OK) {
        return ec;
    }
    if (batch.next_cursor.empty() || batch.keys.size() != batch.locations.size() ||
        batch.keys.size() != batch.location_results.size()) {
        KVCM_LOG_ERROR(
            "maintenance scan result invalid, instance[%s] cursor_empty[%d] keys[%zu] locations[%zu] results[%zu]",
            instance_id_.c_str(),
            batch.next_cursor.empty(),
            batch.keys.size(),
            batch.locations.size(),
            batch.location_results.size());
        return EC_ERROR;
    }
    out = std::move(batch);
    return EC_OK;
}

ErrorCode MetaStorageBackendManager::RandomSample(RequestContext *request_context,
                                                  const int64_t count,
                                                  KeyTypeVec &out_keys) noexcept {
    if (cache_backend_ && recover_state_.load(std::memory_order_acquire) == RecoverState::kRunning) {
        return cache_backend_->RandomSample(request_context, count, out_keys);
    }
    return persistent_backend_->RandomSample(request_context, count, out_keys);
}

ErrorCode MetaStorageBackendManager::SampleReclaimKeys(RequestContext *request_context,
                                                       const int64_t count,
                                                       KeyTypeVec &out_keys) noexcept {
    if (count <= 0) {
        return EC_OK;
    }
    // Until recover finishes, the cache backend has not seen every key yet,
    // so we still go to persistent to avoid biased reclamation. In single-
    // backend mode (no cache) we always go to persistent.
    if (cache_backend_ && recover_state_.load(std::memory_order_acquire) == RecoverState::kRunning) {
        return cache_backend_->SampleReclaimKeys(request_context, count, out_keys);
    }
    return persistent_backend_->SampleReclaimKeys(request_context, count, out_keys);
}

ErrorCode MetaStorageBackendManager::PutMetaData(const FieldMap &field_maps) noexcept {
    return persistent_backend_->PutMetaData(field_maps);
}

ErrorCode MetaStorageBackendManager::GetMetaData(FieldMap &field_maps) noexcept {
    return persistent_backend_->GetMetaData(field_maps);
}

bool MetaStorageBackendManager::Sync(const KeyVector &keys) noexcept {
    if (!persistent_backend_) {
        return true;
    }
    return persistent_backend_->Sync(keys);
}

bool MetaStorageBackendManager::SyncAll() noexcept {
    if (!persistent_backend_) {
        return true;
    }
    return persistent_backend_->SyncAll();
}

MetaStorageBackend::AsyncWriteStats MetaStorageBackendManager::GetAsyncWriteStats() noexcept {
    if (!persistent_backend_) {
        return {};
    }
    return persistent_backend_->GetAsyncWriteStats();
}

size_t MetaStorageBackendManager::GetMemUsage() const noexcept {
    if (cache_backend_) {
        return cache_backend_->GetMemUsage();
    }
    return 0;
}

int64_t MetaStorageBackendManager::GetOldestAccessTime() const noexcept {
    if (cache_backend_) {
        return cache_backend_->GetOldestAccessTime();
    }
    return INT64_MAX;
}

void MetaStorageBackendManager::SetRevisitHistogram(std::shared_ptr<RevisitIntervalHistogram> histogram) {
    // 无条件调用两个 backend 的 SetRevisitHistogram
    // 基类默认实现为空操作（no-op），只有支持重访间隔统计的 backend 才会重写
    if (persistent_backend_) {
        persistent_backend_->SetRevisitHistogram(histogram);
    }
    if (cache_backend_) {
        cache_backend_->SetRevisitHistogram(histogram);
    }
}

} // namespace kv_cache_manager
