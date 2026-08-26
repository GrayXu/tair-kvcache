#include "kv_cache_manager/common/jsonizable.h"

#include <cstddef>

#include "kv_cache_manager/common/logger.h"

namespace kv_cache_manager {

namespace {

constexpr size_t kRetainedJsonBufferSize = 64 * 1024;

// Serialization is sequential within each async Redis consumer. Reuse one
// bounded buffer per calling thread, while nested calls fall back to a local
// buffer so they cannot overwrite an outer serialization in progress.
struct JsonSerializationScratch {
    rapidjson::StringBuffer buffer;
    bool in_use = false;
};

thread_local JsonSerializationScratch tls_json_scratch;

class JsonScratchUseGuard {
public:
    explicit JsonScratchUseGuard(bool &in_use) noexcept : in_use_(in_use) {
        in_use_ = true;
    }

    ~JsonScratchUseGuard() { in_use_ = false; }

private:
    bool &in_use_;
};

std::string SerializeJsonObject(const Jsonizable &value, rapidjson::StringBuffer &buffer) noexcept {
    buffer.Clear();
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    writer.StartObject();
    value.ToRapidWriter(writer);
    writer.EndObject();
    return std::string(buffer.GetString(), buffer.GetSize());
}

} // namespace

Jsonizable::~Jsonizable() = default;

size_t Jsonizable::GetThreadLocalJsonBufferCapacityForTest() noexcept {
    const auto &stack = tls_json_scratch.buffer.stack_;
    // RapidJSON computes capacity by subtracting its internal pointers. Avoid
    // that operation while the lazily allocated stack still holds nulls.
    return stack.Bottom<char>() == nullptr ? 0 : stack.GetCapacity();
}

bool Jsonizable::Parse(rapidjson::Document &doc, const std::string &str) {
    doc.Parse(str.c_str());
    if (doc.HasParseError()) {
        KVCM_LOG_WARN("invalid json error code [%d], str [%s]", static_cast<int>(doc.GetParseError()), str.c_str());
        return false;
    }
    return true;
}

bool Jsonizable::FromJsonString(const std::string &str) {
    rapidjson::Document doc;
    if (!Parse(doc, str)) {
        return false;
    }
    return FromRapidValue(doc);
}

std::string Jsonizable::ToJsonString() const noexcept {
    if (tls_json_scratch.in_use) {
        rapidjson::StringBuffer local_buffer;
        return SerializeJsonObject(*this, local_buffer);
    }

    JsonScratchUseGuard guard(tls_json_scratch.in_use);
    std::string result = SerializeJsonObject(*this, tls_json_scratch.buffer);
    if (tls_json_scratch.buffer.stack_.GetCapacity() > kRetainedJsonBufferSize) {
        tls_json_scratch.buffer = rapidjson::StringBuffer();
    }
    return result;
}

} // namespace kv_cache_manager
