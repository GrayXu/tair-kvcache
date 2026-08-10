#include <memory>

#include "kv_cache_manager/common/unittest.h"
#include "kv_cache_manager/config/registry_manager.h"
#include "kv_cache_manager/manager/cache_manager.h"
#include "kv_cache_manager/meta/meta_indexer.h"
#include "kv_cache_manager/metrics/kmonitor_metrics_reporter.h"
#include "kv_cache_manager/metrics/metrics_collector.h"

using namespace kv_cache_manager;

class KmonitorMetricsReporterTest : public TESTBASE {
protected:
    void SetUp() override {
        metrics_registry_ = std::make_shared<MetricsRegistry>();
        registry_manager_ = std::make_shared<RegistryManager>("", metrics_registry_);
        cache_manager_ = std::make_shared<CacheManager>(metrics_registry_, registry_manager_);
        reporter_ = std::make_unique<KmonitorMetricsReporter>();
        reporter_->Init(cache_manager_, metrics_registry_, "");
    }

    void TearDown() override {}

    std::shared_ptr<MetricsRegistry> metrics_registry_;
    std::shared_ptr<RegistryManager> registry_manager_;
    std::shared_ptr<CacheManager> cache_manager_;
    std::unique_ptr<KmonitorMetricsReporter> reporter_;
};

TEST_F(KmonitorMetricsReporterTest, TestConstructorAndDestructor) { EXPECT_NE(reporter_, nullptr); }

TEST_F(KmonitorMetricsReporterTest, TestInitWithNull) {
    EXPECT_FALSE(reporter_->Init(nullptr, nullptr, ""));

    EXPECT_FALSE(reporter_->Init(nullptr, metrics_registry_, ""));

    EXPECT_FALSE(reporter_->Init(cache_manager_, nullptr, ""));

    EXPECT_TRUE(reporter_->Init(cache_manager_, metrics_registry_, ""));
}

TEST_F(KmonitorMetricsReporterTest, TestReportPerQuery) {
    EXPECT_NO_THROW(reporter_->ReportPerQuery(nullptr));

    {
        DummyMetricsCollector collector;
        collector.Init();
        EXPECT_NO_FATAL_FAILURE(reporter_->ReportPerQuery(&collector));
        EXPECT_EQ(3, metrics_registry_->GetSize());
    }

    {
        EventReportMetricsCollector collector(metrics_registry_, {{"event_type", "block_snapshot"}});
        ASSERT_TRUE(collector.Init());
        SET_METRICS_(&collector, event_report, request_rt_us, 123.);
        SET_METRICS_(&collector, event_report, error_code, 1.);
        EXPECT_NO_FATAL_FAILURE(reporter_->ReportPerQuery(&collector));
        EXPECT_EQ(1, collector.get_event_report_request_counter_metrics());
        EXPECT_EQ(1, collector.get_event_report_error_counter_metrics());
    }

    {
        // simulate the uninitialised case 1
        reporter_->cache_manager_ = nullptr;
        reporter_->metrics_registry_ = std::make_shared<MetricsRegistry>();

        ServiceMetricsCollector collector{metrics_registry_};
        collector.Init();
        EXPECT_NO_FATAL_FAILURE(reporter_->ReportPerQuery(&collector));

        DataStorageMetricsCollector collector2{metrics_registry_};
        collector2.Init();
        EXPECT_NO_FATAL_FAILURE(reporter_->ReportPerQuery(&collector));
    }

    {
        // simulate the uninitialised case 2
        reporter_->cache_manager_ = std::make_shared<CacheManager>(metrics_registry_, registry_manager_);
        reporter_->metrics_registry_ = nullptr;

        ServiceMetricsCollector collector{metrics_registry_};
        collector.Init();
        EXPECT_NO_FATAL_FAILURE(reporter_->ReportPerQuery(&collector));

        DataStorageMetricsCollector collector2{metrics_registry_};
        collector2.Init();
        EXPECT_NO_FATAL_FAILURE(reporter_->ReportPerQuery(&collector));
    }

    {
        // simulate the uninitialised case 3
        reporter_->cache_manager_ = nullptr;
        reporter_->metrics_registry_ = nullptr;

        ServiceMetricsCollector collector{metrics_registry_};
        collector.Init();
        EXPECT_NO_FATAL_FAILURE(reporter_->ReportPerQuery(&collector));

        DataStorageMetricsCollector collector2{metrics_registry_};
        collector2.Init();
        EXPECT_NO_FATAL_FAILURE(reporter_->ReportPerQuery(&collector));
    }
}

TEST_F(KmonitorMetricsReporterTest, TestReportInterval) {
    {
        metrics_registry_->GetCounter("cache_gc.scan_round_count") += 1;
        metrics_registry_->GetCounter("cache_gc.candidate_count", {{"reason", "storage_missing"}}) += 2;
        metrics_registry_->GetCounter("cache_gc.delete_result_count", {{"status", "0"}}) += 1;
        metrics_registry_->GetCounter("cache_gc.operation_error_count", {{"stage", "scan"}}) += 1;
        metrics_registry_->GetGauge("cache_gc.inflight_delete_count") = 2;
        metrics_registry_->GetGauge("cache_gc.event_report_intent_count", {{"reason", "down_host"}}) = 3;
        metrics_registry_->GetCounter("cache_gc.event_report_intent_update_count",
                                      {{"reason", "down_host"}, {"result", "created"}}) += 1;
        metrics_registry_->GetGauge("cache_gc.event_report_active_instance_count") = 2;
        metrics_registry_->GetGauge("cache_gc.event_report_oldest_intent_age_ms") = 100;
        metrics_registry_->GetGauge("cache_gc.event_report_recovery_pending_instance_count") = 1;
        metrics_registry_->GetGauge("cache_gc.event_report_recovery_observation_count") = 2;
        metrics_registry_->GetGauge("cache_gc.event_report_recovery_complete") = 0;
        metrics_registry_->GetCounter("cache_gc.event_report_scan_batch_count") += 1;
        metrics_registry_->GetCounter("cache_gc.event_report_scan_key_count") += 8;
        metrics_registry_->GetCounter("cache_gc.event_report_scan_pass_count", {{"status", "complete"}}) += 1;
        metrics_registry_->GetCounter("cache_gc.event_report_delete_location_count",
                                      {{"reason", "down_host"}, {"status", "deleted"}}) += 2;
        metrics_registry_->GetCounter("cache_gc.event_report_retry_count", {{"stage", "sync"}}) += 1;
        metrics_registry_->GetGauge("cache_gc.event_report_last_pass_duration_ms") = 10;
        metrics_registry_->GetGauge("cache_gc.event_report_last_action_duration_ms") = 3;
        metrics_registry_->GetGauge("cache_gc.event_report_last_action_shard_lock_wait_us") = 7;
        metrics_registry_->GetGauge("cache_gc.event_report_last_action_shard_lock_hold_us") = 9;
        metrics_registry_->GetGauge("cache_gc.event_report_sync_retry_batch_count") = 1;
        EXPECT_NO_FATAL_FAILURE(reporter_->ReportInterval());
    }

    {
        // simulate the uninitialised case 1
        reporter_->cache_manager_ = nullptr;
        reporter_->metrics_registry_ = std::make_shared<MetricsRegistry>();
        EXPECT_NO_FATAL_FAILURE(reporter_->ReportInterval());
    }

    {
        // simulate the uninitialised case 2
        reporter_->cache_manager_ = std::make_shared<CacheManager>(metrics_registry_, registry_manager_);
        reporter_->metrics_registry_ = nullptr;
        EXPECT_NO_FATAL_FAILURE(reporter_->ReportInterval());
    }

    {
        // simulate the uninitialised case 3
        reporter_->cache_manager_ = nullptr;
        reporter_->metrics_registry_ = nullptr;
        EXPECT_NO_FATAL_FAILURE(reporter_->ReportInterval());
    }
}
