#include "durable_delete_journal.h"

#include <boost/crc.hpp>
#include <cerrno>
#include <filesystem>
#include <sstream>
#include <stdexcept>
#include <sys/file.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

namespace mooncake {
namespace {
// Tombstones are permanent. Do not impose a lifetime key-count ceiling.
// Recovery streams the journal; capacity is provisioned as persistent metadata.
constexpr size_t kMaxFieldBytes = 1024;
constexpr size_t kMaxRecordBytes = 4 * kMaxFieldBytes * 2 + 128;

std::string Hex(const std::string& value) {
    constexpr char digits[] = "0123456789abcdef";
    std::string result;
    for (unsigned char c : value) {
        result += digits[c >> 4];
        result += digits[c & 15];
    }
    return result;
}

std::string Unhex(const std::string& value) {
    if (value.empty() || value.size() % 2 ||
        value.size() > kMaxFieldBytes * 2) {
        throw std::runtime_error("Invalid deletion journal field");
    }
    auto nibble = [](char c) -> unsigned {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        throw std::runtime_error("Invalid deletion journal hex");
    };
    std::string result;
    for (size_t i = 0; i < value.size(); i += 2) {
        result +=
            static_cast<char>((nibble(value[i]) << 4) | nibble(value[i + 1]));
    }
    return result;
}

std::string Encode(const DurableDeleteJournal::Record& record) {
    auto body = std::string(record.reader_grace_ms ? "v3 " : "v2 ") +
                Hex(record.tenant) + " " + Hex(record.key) + " " +
                Hex(record.operation) + " " + (record.completed ? "1" : "0") +
                " " + Hex(record.namespace_identity);
    if (record.reader_grace_ms)
        body += " " + std::to_string(*record.reader_grace_ms);
    boost::crc_32_type crc;
    crc.process_bytes(body.data(), body.size());
    return body + " " + std::to_string(crc.checksum()) + "\n";
}

bool Valid(const DurableDeleteJournal::Record& record) {
    return (!record.reader_grace_ms || *record.reader_grace_ms <= 86400000) &&
           !record.tenant.empty() && !record.key.empty() &&
           !record.operation.empty() && !record.namespace_identity.empty() &&
           record.namespace_identity.size() <= kMaxFieldBytes &&
           record.tenant.size() <= kMaxFieldBytes &&
           record.key.size() <= kMaxFieldBytes &&
           record.operation.size() <= kMaxFieldBytes;
}
}  // namespace

DurableDeleteJournal::DurableDeleteJournal(const std::string& path) {
    const std::filesystem::path file(path);
    if (!file.is_absolute()) {
        throw std::invalid_argument("Deletion journal needs an absolute path");
    }
    fd_ = open(path.c_str(),
               O_RDWR | O_APPEND | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (fd_ < 0) throw std::runtime_error("Cannot open deletion journal");
    try {
        struct stat status{};
        if (fstat(fd_, &status) || !S_ISREG(status.st_mode) ||
            flock(fd_, LOCK_EX | LOCK_NB)) {
            throw std::runtime_error("Cannot exclusively own deletion journal");
        }
        // Persist the directory entry as well as file contents before use.
        const int parent = open(file.parent_path().c_str(),
                                O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (parent < 0)
            throw std::runtime_error("Cannot open journal directory");
        const bool synced = fsync(fd_) == 0 && fsync(parent) == 0;
        close(parent);
        if (!synced)
            throw std::runtime_error("Cannot persist journal directory");
        Replay();
    } catch (...) {
        close(fd_);
        fd_ = -1;
        throw;
    }
}

DurableDeleteJournal::~DurableDeleteJournal() {
    if (fd_ >= 0) close(fd_);
}

void DurableDeleteJournal::Replay() {
    off_t pos = 0;
    std::string pending;
    bool eof = false;
    while (true) {
        auto end = pending.find('\n');
        while (end == std::string::npos && !eof) {
            char chunk[4096];
            const auto n =
                pread(fd_, chunk, sizeof(chunk), pos + pending.size());
            if (n < 0 && errno == EINTR) continue;
            if (n < 0) throw std::runtime_error("Cannot read deletion journal");
            eof = n == 0;
            pending.append(chunk, n);
            end = pending.find('\n');
            if (end == std::string::npos && pending.size() > kMaxRecordBytes)
                throw std::runtime_error("Deletion journal record too large");
        }
        if (pending.empty()) break;
        // Only an incomplete final append is recoverable. Commit never
        // acknowledges it. Complete malformed records still fail closed.
        if (end == std::string::npos) {
            if (ftruncate(fd_, pos) != 0 || fsync(fd_) != 0)
                throw std::runtime_error(
                    "Cannot persist journal tail recovery");
            break;
        }
        if (end + 1 > kMaxRecordBytes)
            throw std::runtime_error("Deletion journal record too large");
        const auto line = pending.substr(0, end + 1);
        pending.erase(0, end + 1);
        std::istringstream input(line);
        std::string version, tenant, key, operation, phase, scope, checksum,
            extra;
        if (!(input >> version >> tenant >> key >> operation >> phase >>
              scope) ||
            (version != "v2" && version != "v3") ||
            (phase != "0" && phase != "1")) {
            throw std::runtime_error("Invalid deletion journal record");
        }
        Record record{Unhex(tenant), Unhex(key), Unhex(operation), phase == "1",
                      Unhex(scope)};
        if (version == "v3") {
            uint64_t grace;
            if (!(input >> grace))
                throw std::runtime_error("Invalid deletion reader grace");
            record.reader_grace_ms = grace;
        }
        if (!(input >> checksum) || input >> extra || !Valid(record) ||
            Encode(record) != line) {
            throw std::runtime_error("Deletion journal checksum mismatch");
        }
        const auto identity = std::make_pair(record.tenant, record.key);
        auto old = records_.find(identity);
        if (old == records_.end()) {
            if (record.completed) {
                throw std::runtime_error("Invalid deletion journal admission");
            }
        } else if (old->second.operation != record.operation ||
                   old->second.namespace_identity !=
                       record.namespace_identity ||
                   old->second.reader_grace_ms != record.reader_grace_ms ||
                   (old->second.completed && !record.completed)) {
            throw std::runtime_error("Conflicting deletion journal operation");
        }
        records_[identity] = std::move(record);
        pos += end + 1;
    }
}

bool DurableDeleteJournal::Commit(const Record& record) {
    std::lock_guard lock(mutex_);
    if (failed_ || !Valid(record)) return false;
    const auto identity = std::make_pair(record.tenant, record.key);
    auto old = records_.find(identity);
    if (old == records_.end()) {
        if (record.completed) return false;
    } else {
        if (old->second.operation != record.operation ||
            old->second.namespace_identity != record.namespace_identity ||
            old->second.reader_grace_ms != record.reader_grace_ms ||
            (old->second.completed && !record.completed))
            return false;
        if (old->second.completed == record.completed) return true;
    }
    const auto line = Encode(record);
    size_t written = 0;
    while (written < line.size()) {
        const auto n = write(fd_, line.data() + written, line.size() - written);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) {
            failed_ = true;
            return false;
        }
        written += n;
    }
    if (fsync(fd_) != 0) {
        failed_ = true;
        return false;
    }
    records_[identity] = record;
    return true;
}

std::vector<DurableDeleteJournal::Record> DurableDeleteJournal::Records()
    const {
    std::lock_guard lock(mutex_);
    std::vector<Record> result;
    for (const auto& [key, record] : records_) result.push_back(record);
    return result;
}
}  // namespace mooncake
