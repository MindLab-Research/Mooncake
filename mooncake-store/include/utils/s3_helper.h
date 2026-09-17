#pragma once

#ifndef HAVE_AWS_SDK
#error \
    "s3_helper.h requires AWS SDK. Please define HAVE_AWS_SDK or do not include this header."
#endif

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <cstdint>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <vector>
#include <ylt/util/tl/expected.hpp>

namespace mooncake {

enum class S3RequestErrorKind {
    kNotFound,
    kPermissionDenied,
    kTimeout,
    kUnavailable,
    kInvalidResponse,
    kOther,
};

struct S3RequestError {
    S3RequestErrorKind kind = S3RequestErrorKind::kOther;
    int http_status = 0;
    std::string message;
};

struct S3ListedObject {
    std::string key;
    uint64_t size = 0;
};

class S3Helper {
   public:
    static void InitAPI();

    static void ShutdownAPI();

   private:
    static size_t api_refcount_;
    static std::mutex api_mutex_;
    static Aws::SDKOptions options_;

   public:
    explicit S3Helper(const std::string &endpoint = "",
                      const std::string &bucket = "",
                      const std::string &region = "us-east-1");

    ~S3Helper();

    // Get connection info
    [[nodiscard]] const std::string &GetConnectionInfo() const {
        return connection_info_;
    }

    // Memory upload
    tl::expected<void, std::string> UploadBuffer(
        const std::string &key, const std::vector<uint8_t> &buffer);

    tl::expected<void, std::string> UploadBufferMultipart(
        const std::string &key, const std::vector<uint8_t> &buffer);

    tl::expected<void, std::string> UploadString(const std::string &key,
                                                 const std::string &data);

    // Memory download
    tl::expected<void, std::string> DownloadBuffer(
        const std::string &key, std::vector<uint8_t> &buffer);

    tl::expected<void, std::string> DownloadBufferMultipart(
        const std::string &key, std::vector<uint8_t> &buffer);

    tl::expected<void, std::string> DownloadString(const std::string &key,
                                                   std::string &data);

    // Delete object
    tl::expected<void, std::string> DeleteObject(const std::string &key);

    // Batch delete objects
    tl::expected<void, std::string> DeleteObjects(
        const std::vector<std::string> &keys);

    tl::expected<void, std::string> UploadFile(const Aws::String &file_path,
                                               const Aws::String &key);

    tl::expected<void, std::string> DownloadFile(const Aws::String &file_path,
                                                 const Aws::String &key);

    // New interface: list objects with specified prefix
    tl::expected<void, std::string> ListObjectsWithPrefix(
        const std::string &prefix, std::vector<std::string> &object_keys);

    tl::expected<void, std::string> DeleteObjectsWithPrefix(
        const std::string &prefix);

    tl::expected<void, std::string> InspectObject(
        const std::string &key, uint64_t &stored_size,
        std::optional<uint32_t> &crc32c);

    // Typed operations used by Store data backends. Unlike the legacy string
    // errors above, these preserve not-found, permission, timeout, and service
    // failures so callers never turn an authorization or network error into a
    // cache miss.
    tl::expected<void, S3RequestError> UploadBytes(const std::string &key,
                                                   std::span<const char> data);
    tl::expected<void, S3RequestError> UploadSlices(
        const std::string &key,
        const std::vector<std::span<const char>> &slices);
    // Durable writer admission binds a server-side multipart capability.
    tl::expected<std::string, S3RequestError> CreateUpload(
        const std::string &key);
    tl::expected<void, S3RequestError> AbortUpload(
        const std::string &key, const std::string &upload_id);
    tl::expected<void, S3RequestError> UploadAdmittedSlices(
        const std::string &key, const std::string &upload_id,
        const std::vector<std::span<const char>> &slices);
    tl::expected<size_t, S3RequestError> DownloadBytes(const std::string &key,
                                                       void *buffer,
                                                       size_t capacity);
    tl::expected<uint64_t, S3RequestError> HeadObjectSize(
        const std::string &key);
    tl::expected<bool, S3RequestError> ObjectExists(const std::string &key);
    tl::expected<void, S3RequestError> DeleteObjectChecked(
        const std::string &key);
    tl::expected<std::vector<S3ListedObject>, S3RequestError>
    ListObjectsV2Detailed(const std::string &prefix);

   private:
    tl::expected<void, S3RequestError> UploadSlicesImpl(
        const std::string &key,
        const std::vector<std::span<const char>> &slices,
        const std::optional<std::string> &admitted_upload);
    Aws::S3::S3Client s3_client_;
    std::string bucket_;
    std::string connection_info_;
};
}  // namespace mooncake
