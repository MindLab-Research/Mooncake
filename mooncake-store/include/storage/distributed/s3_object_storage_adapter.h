#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>

#include "storage/distributed/object_storage_adapter.h"

namespace mooncake {

struct S3ObjectStorageConfig {
    std::string endpoint;
    std::string bucket;
    std::string region = "us-east-1";
    std::string key_prefix = "mooncake-store/v1";

    bool Validate() const;
    static S3ObjectStorageConfig FromEnvironment();
};

#ifdef HAVE_AWS_SDK

class S3Helper;

/**
 * @brief S3-compatible implementation of Mooncake's logical object adapter.
 *
 * Logical keys are opaque bytes. They are hex encoded below one configured
 * prefix so ListKeys can scan only the owned namespace and recover the exact
 * tenant-scoped key.
 */
class S3ObjectStorageAdapter final : public ObjectStorageAdapter {
   public:
    explicit S3ObjectStorageAdapter(S3ObjectStorageConfig config);
    ~S3ObjectStorageAdapter() override;

    tl::expected<void, ErrorCode> Put(const std::string& logical_key,
                                      std::span<const char> data) override;
    tl::expected<void, ErrorCode> PutV(const std::string& logical_key,
                                       const iovec* iov, int iovcnt) override;
    tl::expected<size_t, ErrorCode> Get(const std::string& logical_key,
                                        void* buf, size_t len) override;
    tl::expected<bool, ErrorCode> Exists(
        const std::string& logical_key) override;
    tl::expected<void, ErrorCode> Delete(
        const std::string& logical_key) override;
    // A committed intent is permanent for an immutable logical key. Adapter
    // reads/discovery hide it and subsequent writes reject it. The caller must
    // still fence/drain in-flight writers and invalidate Master replicas before
    // acknowledging deletion; an adapter intent is not a cluster-wide lease.
    tl::expected<void, ErrorCode> MarkDeletion(
        const std::string& logical_key) override;
    tl::expected<bool, ErrorCode> HasDeletionIntent(
        const std::string& logical_key) override;
    tl::expected<DurableObjectStorageNamespace, ErrorCode>
    GetDurableDeleteNamespace() const override;
    tl::expected<std::vector<KeyInfo>, ErrorCode> ListKeys() override;
    tl::expected<void, ErrorCode> Init() override;
    const char* GetName() const override { return "s3"; }

   private:
    tl::expected<std::string, ErrorCode> PhysicalKey(
        const std::string& logical_key) const;
    std::string PhysicalPrefix() const;
    tl::expected<std::string, ErrorCode> DeletionIntentKey(
        const std::string& logical_key) const;
    tl::expected<void, ErrorCode> CheckNotDeleted(const std::string& key);
    tl::expected<void, ErrorCode> FinishUpload(const std::string& key);
    std::string WriterPrefix(const std::string& key) const;
    tl::expected<std::string, ErrorCode> BeginUpload(const std::string& key);
    tl::expected<void, ErrorCode> ReleaseUpload(const std::string& writer);

    const S3ObjectStorageConfig config_;
    std::unique_ptr<S3Helper> helper_;
    std::mutex init_mutex_;
    std::atomic<bool> initialized_{false};
    bool api_acquired_ = false;
};

#endif  // HAVE_AWS_SDK

}  // namespace mooncake
