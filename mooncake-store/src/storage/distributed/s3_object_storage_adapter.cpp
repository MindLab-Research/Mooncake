#include "storage/distributed/s3_object_storage_adapter.h"

#include <cstdlib>
#include <cstring>
#include <limits>
#include <string_view>
#include <glog/logging.h>

namespace mooncake {

S3ObjectStorageConfig S3ObjectStorageConfig::FromEnvironment() {
    S3ObjectStorageConfig config;
    auto read = [](const char* name, std::string& value) {
        if (const char* env = std::getenv(name)) value = env;
    };
    read("MOONCAKE_AWS_S3_ENDPOINT", config.endpoint);
    read("MOONCAKE_AWS_BUCKET_NAME", config.bucket);
    read("MOONCAKE_AWS_REGION", config.region);
    read("MOONCAKE_S3_KEY_PREFIX", config.key_prefix);
    return config;
}

bool S3ObjectStorageConfig::Validate() const {
    return !bucket.empty() && !region.empty() && !key_prefix.empty() &&
           key_prefix.size() <= 909 &&
           key_prefix.find('\0') == std::string::npos &&
           bucket.find('\0') == std::string::npos &&
           endpoint.find('@') == std::string::npos;
}

}  // namespace mooncake

#ifdef HAVE_AWS_SDK
#include "utils/s3_helper.h"
#include <aws/core/utils/HashingUtils.h>

namespace mooncake {
namespace {
ErrorCode MapError(const S3RequestError& error) {
    LOG(WARNING) << "S3 operation failed: " << error.message;
    switch (error.kind) {
        case S3RequestErrorKind::kNotFound:
            return ErrorCode::FILE_NOT_FOUND;
        case S3RequestErrorKind::kPermissionDenied:
            return ErrorCode::DFS_PERMISSION_DENIED;
        case S3RequestErrorKind::kTimeout:
            return ErrorCode::DFS_NETWORK_TIMEOUT;
        case S3RequestErrorKind::kUnavailable:
            return ErrorCode::DFS_SERVICE_UNAVAILABLE;
        default:
            return ErrorCode::INTERNAL_ERROR;
    }
}
int Unhex(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    return -1;
}
}  // namespace

S3ObjectStorageAdapter::S3ObjectStorageAdapter(S3ObjectStorageConfig config)
    : config_(std::move(config)) {}

S3ObjectStorageAdapter::~S3ObjectStorageAdapter() {
    helper_.reset();
    if (api_acquired_) S3Helper::ShutdownAPI();
}

tl::expected<void, ErrorCode> S3ObjectStorageAdapter::Init() {
    std::lock_guard lock(init_mutex_);
    if (initialized_) return {};
    if (!config_.Validate())
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    if (!api_acquired_) {
        S3Helper::InitAPI();
        api_acquired_ = true;
    }
    helper_ = std::make_unique<S3Helper>(config_.endpoint, config_.bucket,
                                         config_.region);
    // Startup must fail for a missing bucket or missing List permission.
    auto list = helper_->ListObjectsV2Detailed(PhysicalPrefix());
    if (!list) return tl::make_unexpected(MapError(list.error()));
    initialized_ = true;
    return {};
}

std::string S3ObjectStorageAdapter::PhysicalPrefix() const {
    return config_.key_prefix + "/objects/";
}

tl::expected<DurableObjectStorageNamespace, ErrorCode>
S3ObjectStorageAdapter::GetDurableDeleteNamespace() const {
    if (!initialized_) return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    // Endpoint userinfo/query fragments are not routing metadata and might
    // contain credentials. Never publish them in a capability response.
    if (!config_.Validate() ||
        config_.endpoint.find_first_of("@?#\r\n") != std::string::npos ||
        config_.endpoint.find('\0') != std::string::npos) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const auto prefix_size =
        config_.key_prefix.size() + std::string_view("/deletions/").size();
    if (prefix_size + 2 > 1024) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return DurableObjectStorageNamespace{
        .protocol_version = kDurableReadFenceProtocolVersion,
        .backend = "s3",
        .endpoint = config_.endpoint,
        .bucket = config_.bucket,
        .region = config_.region,
        .key_prefix = config_.key_prefix,
        .max_scoped_key_bytes =
            static_cast<uint32_t>((1024 - prefix_size) / 2)};
}

tl::expected<std::string, ErrorCode> S3ObjectStorageAdapter::PhysicalKey(
    const std::string& key) const {
    auto result = PhysicalPrefix();
    if (result.size() > 1024 || key.size() > (1024 - result.size()) / 2)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    constexpr char hex[] = "0123456789abcdef";
    for (unsigned char ch : key) {
        result += hex[ch >> 4];
        result += hex[ch & 15];
    }
    return result;
}

tl::expected<void, ErrorCode> S3ObjectStorageAdapter::Put(
    const std::string& key, std::span<const char> data) {
    if (!initialized_) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    auto physical = PhysicalKey(key);
    if (!physical) return tl::make_unexpected(physical.error());
    auto writer = BeginUpload(key);
    if (!writer) return tl::make_unexpected(writer.error());
    auto result = helper_->UploadBytes(*physical, data);
    // Even an error can follow a committed upload (for example a lost reply).
    // Reconcile a concurrent permanent intent before returning to the writer.
    auto finalized = FinishUpload(key);
    // A failed upload may still be executing remotely. Its durable admission
    // is never expired or removed just because this process saw an error.
    if (result &&
        (finalized || finalized.error() == ErrorCode::FILE_NOT_FOUND)) {
        auto released = ReleaseUpload(*writer);
        if (!released) return released;
    }
    if (!finalized) return finalized;
    if (!result) return tl::make_unexpected(MapError(result.error()));
    return {};
}

std::string S3ObjectStorageAdapter::WriterPrefix(const std::string& key) const {
    const auto digest = Aws::Utils::HashingUtils::CalculateSHA256(
        Aws::String(key.data(), key.size()));
    return config_.key_prefix + "/writers/" +
           std::string(Aws::Utils::HashingUtils::HexEncode(digest)) + "/";
}

tl::expected<std::string, ErrorCode> S3ObjectStorageAdapter::BeginUpload(
    const std::string& key) {
    const auto writer = WriterPrefix(key) + UuidToString(generate_uuid());
    if (writer.size() > 1024)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    constexpr char admission[] = "mooncake-upload-admission-v1";
    auto registered = helper_->UploadBytes(
        writer, std::span<const char>(admission, sizeof(admission) - 1));
    if (!registered) return tl::make_unexpected(MapError(registered.error()));
    // Admission must exist before checking the permanent fence. With strongly
    // consistent LIST/HEAD, a deleter either sees this writer or this writer
    // sees the fence before sending data. No timed lease can prove IO stopped.
    auto visible = CheckNotDeleted(key);
    if (!visible) {
        auto released = ReleaseUpload(writer);  // No data request was issued.
        if (!released) return tl::make_unexpected(released.error());
        return tl::make_unexpected(visible.error());
    }
    return writer;
}

tl::expected<void, ErrorCode> S3ObjectStorageAdapter::ReleaseUpload(
    const std::string& writer) {
    auto removed = helper_->DeleteObjectChecked(writer);
    if (!removed) return tl::make_unexpected(MapError(removed.error()));
    return {};
}

tl::expected<std::string, ErrorCode> S3ObjectStorageAdapter::DeletionIntentKey(
    const std::string& key) const {
    auto physical = PhysicalKey(key);
    if (!physical) return tl::make_unexpected(physical.error());
    // Separate from objects/ so ordinary metadata discovery never exposes an
    // intent as an object. Keep the same encoded tenant-scoped logical key.
    auto result = config_.key_prefix + "/deletions/" +
                  physical->substr(PhysicalPrefix().size());
    if (result.size() > 1024)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    return result;
}

tl::expected<void, ErrorCode> S3ObjectStorageAdapter::MarkDeletion(
    const std::string& key) {
    if (!initialized_) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    auto intent = DeletionIntentKey(key);
    if (!intent) return tl::make_unexpected(intent.error());
    // Identical content makes concurrent/retried intent writes idempotent.
    // This record survives provider/Master restart and is never cleared here.
    constexpr char marker[] = "mooncake-delete-intent-v1";
    auto result = helper_->UploadBytes(
        *intent, std::span<const char>(marker, sizeof(marker) - 1));
    if (!result) return tl::make_unexpected(MapError(result.error()));
    auto writers = helper_->ListObjectsV2Detailed(WriterPrefix(key));
    if (!writers) return tl::make_unexpected(MapError(writers.error()));
    if (!writers->empty())
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    return {};
}

tl::expected<bool, ErrorCode> S3ObjectStorageAdapter::HasDeletionIntent(
    const std::string& key) {
    if (!initialized_) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    auto intent = DeletionIntentKey(key);
    if (!intent) return tl::make_unexpected(intent.error());
    auto result = helper_->ObjectExists(*intent);
    if (!result) return tl::make_unexpected(MapError(result.error()));
    return *result;
}

tl::expected<void, ErrorCode> S3ObjectStorageAdapter::CheckNotDeleted(
    const std::string& key) {
    auto deleted = HasDeletionIntent(key);
    if (!deleted) return tl::make_unexpected(deleted.error());
    if (*deleted) return tl::make_unexpected(ErrorCode::FILE_NOT_FOUND);
    return {};
}

tl::expected<void, ErrorCode> S3ObjectStorageAdapter::FinishUpload(
    const std::string& key) {
    auto deleted = HasDeletionIntent(key);
    // A failed intent lookup is not proof of deletion. Preserve the bytes and
    // propagate the error so the coordinator can reconcile uncertain work.
    if (!deleted) return tl::make_unexpected(deleted.error());
    if (!*deleted) return {};
    // An upload admitted before the permanent intent may finish after the
    // deleting provider's DELETE. Immutable keys cannot be reused, so these
    // late bytes must be removed before this writer reports completion.
    auto cleanup = Delete(key);
    if (!cleanup) return cleanup;
    return tl::make_unexpected(ErrorCode::FILE_NOT_FOUND);
}

tl::expected<void, ErrorCode> S3ObjectStorageAdapter::PutV(
    const std::string& key, const iovec* iov, int count) {
    if (count < 0 || (count > 0 && !iov))
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    size_t size = 0;
    for (int i = 0; i < count; ++i) {
        if ((iov[i].iov_len && !iov[i].iov_base) ||
            iov[i].iov_len > std::numeric_limits<size_t>::max() - size)
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        size += iov[i].iov_len;
    }
    if (!initialized_) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    auto physical = PhysicalKey(key);
    if (!physical) return tl::make_unexpected(physical.error());
    std::vector<std::span<const char>> slices;
    slices.reserve(count);
    for (int i = 0; i < count; ++i) {
        if (iov[i].iov_len)
            slices.emplace_back(static_cast<const char*>(iov[i].iov_base),
                                iov[i].iov_len);
    }
    auto writer = BeginUpload(key);
    if (!writer) return tl::make_unexpected(writer.error());
    auto result = helper_->UploadSlices(*physical, slices);
    auto finalized = FinishUpload(key);
    if (result &&
        (finalized || finalized.error() == ErrorCode::FILE_NOT_FOUND)) {
        auto released = ReleaseUpload(*writer);
        if (!released) return released;
    }
    if (!finalized) return finalized;
    if (!result) return tl::make_unexpected(MapError(result.error()));
    return {};
}

tl::expected<size_t, ErrorCode> S3ObjectStorageAdapter::Get(
    const std::string& key, void* buffer, size_t size) {
    if (!initialized_) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    auto physical = PhysicalKey(key);
    if (!physical) return tl::make_unexpected(physical.error());
    auto result = helper_->DownloadBytes(*physical, buffer, size);
    if (!result) return tl::make_unexpected(MapError(result.error()));
    auto visible = CheckNotDeleted(key);
    if (!visible) return tl::make_unexpected(visible.error());
    return *result;
}

tl::expected<bool, ErrorCode> S3ObjectStorageAdapter::Exists(
    const std::string& key) {
    if (!initialized_) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    auto physical = PhysicalKey(key);
    if (!physical) return tl::make_unexpected(physical.error());
    auto result = helper_->ObjectExists(*physical);
    if (!result) return tl::make_unexpected(MapError(result.error()));
    auto deleted = HasDeletionIntent(key);
    if (!deleted) return tl::make_unexpected(deleted.error());
    if (*deleted) return false;
    return *result;
}

tl::expected<void, ErrorCode> S3ObjectStorageAdapter::Delete(
    const std::string& key) {
    if (!initialized_) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    auto physical = PhysicalKey(key);
    if (!physical) return tl::make_unexpected(physical.error());
    auto result = helper_->DeleteObjectChecked(*physical);
    if (!result) return tl::make_unexpected(MapError(result.error()));
    return {};
}

tl::expected<std::vector<KeyInfo>, ErrorCode>
S3ObjectStorageAdapter::ListKeys() {
    if (!initialized_) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    auto result = helper_->ListObjectsV2Detailed(PhysicalPrefix());
    if (!result) return tl::make_unexpected(MapError(result.error()));
    std::vector<KeyInfo> keys;
    const auto prefix = PhysicalPrefix();
    for (const auto& object : *result) {
        if (!object.key.starts_with(prefix)) continue;
        auto encoded = object.key.substr(prefix.size());
        if (encoded.size() % 2) continue;
        std::string key;
        bool valid = true;
        for (size_t i = 0; i < encoded.size(); i += 2) {
            int hi = Unhex(encoded[i]), lo = Unhex(encoded[i + 1]);
            if (hi < 0 || lo < 0) {
                valid = false;
                break;
            }
            key += static_cast<char>((hi << 4) | lo);
        }
        if (valid) {
            auto deleted = HasDeletionIntent(key);
            if (!deleted) return tl::make_unexpected(deleted.error());
            if (*deleted) continue;
            keys.push_back({std::move(key), static_cast<size_t>(object.size)});
        }
    }
    return keys;
}
}  // namespace mooncake
#endif
