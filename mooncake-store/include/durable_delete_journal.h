#pragma once

#include <map>
#include <mutex>
#include <optional>
#include <cstdint>
#include <string>
#include <vector>

namespace mooncake {

// Standalone Master journal. The parent directory must already exist on
// persistent local storage. An exclusive file lock prevents split ownership.
// An incomplete final append is truncated and fsynced during recovery.
// Complete-record corruption, missing permissions and I/O failure fail closed.
// Tombstones remain permanent; provision disk and RAM for lifetime key count.
// This is not an
// HA/replicated log and must never be used to acknowledge HA deletion.
class DurableDeleteJournal {
   public:
    struct Record {
        std::string tenant;
        std::string key;
        std::string operation;
        bool completed = false;
        std::string namespace_identity;
        // Missing in v2 records: metadata-free recovery must then fail closed.
        std::optional<uint64_t> reader_grace_ms;
    };
    explicit DurableDeleteJournal(const std::string& path);
    ~DurableDeleteJournal();
    DurableDeleteJournal(const DurableDeleteJournal&) = delete;
    DurableDeleteJournal& operator=(const DurableDeleteJournal&) = delete;

    bool Commit(const Record& record);
    std::vector<Record> Records() const;

   private:
    void Replay();
    int fd_ = -1;
    bool failed_ = false;
    mutable std::mutex mutex_;
    std::map<std::pair<std::string, std::string>, Record> records_;
};

}  // namespace mooncake
