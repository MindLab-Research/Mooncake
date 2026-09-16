#include "durable_delete_journal.h"
#include "storage/distributed/object_storage_namespace.h"

#include <gtest/gtest.h>
#include <atomic>
#include <filesystem>
#include <fstream>
#include <thread>
#include <sys/wait.h>
#include <sys/resource.h>
#include <csignal>
#include <unistd.h>

namespace mooncake::test {
class DurableDeleteJournalTest : public ::testing::Test {
   protected:
    void SetUp() override {
        char temporary[] = "/tmp/mooncake-delete-journal-XXXXXX";
        const auto created = mkdtemp(temporary);
        ASSERT_NE(created, nullptr);
        directory = created;
        path = directory + "/journal";
    }
    void TearDown() override { std::filesystem::remove_all(directory); }
    std::string directory, path;
};

TEST_F(DurableDeleteJournalTest, ConcurrentReplayAndConflict) {
    DurableDeleteJournal::Record record{"tenant", "key", "operation", false,
                                        "scope-a"};
    {
        DurableDeleteJournal journal(path);
        EXPECT_THROW(DurableDeleteJournal second(path), std::runtime_error);
        std::vector<std::thread> workers;
        std::atomic<int> successes{0};
        for (int i = 0; i < 8; ++i) {
            workers.emplace_back([&] {
                if (journal.Commit(record)) ++successes;
            });
        }
        for (auto& worker : workers) worker.join();
        EXPECT_EQ(successes, 8);
        auto conflict = record;
        conflict.operation = "different";
        EXPECT_FALSE(journal.Commit(conflict));
        conflict = record;
        conflict.namespace_identity = "scope-b";
        EXPECT_FALSE(journal.Commit(conflict));
        record.completed = true;
        EXPECT_TRUE(journal.Commit(record));
        EXPECT_TRUE(journal.Commit(record));
    }
    DurableDeleteJournal restored(path);
    ASSERT_EQ(restored.Records().size(), 1);
    EXPECT_TRUE(restored.Records()[0].completed);
    EXPECT_EQ(restored.Records()[0].namespace_identity, "scope-a");
    record.completed = false;
    EXPECT_FALSE(restored.Commit(record));
}

TEST_F(DurableDeleteJournalTest, ProcessExitPreservesAcknowledgedFence) {
    const auto pid = fork();
    ASSERT_GE(pid, 0);
    if (pid == 0) {
        DurableDeleteJournal journal(path);
        if (!journal.Commit({"tenant", "key", "operation", false, "scope-a"}))
            _exit(2);
        // Abrupt process exit: no destructor and no periodic snapshot.
        _exit(0);
    }
    int status;
    ASSERT_EQ(waitpid(pid, &status, 0), pid);
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 0);
    DurableDeleteJournal journal(path);
    ASSERT_EQ(journal.Records().size(), 1);
    EXPECT_FALSE(journal.Records()[0].completed);
    EXPECT_TRUE(
        journal.Commit({"tenant", "key", "operation", true, "scope-a"}));
}

TEST_F(DurableDeleteJournalTest, ReaderGraceSurvivesExitAndCannotBeShortened) {
    DurableDeleteJournal::Record record{"tenant", "key",     "operation",
                                        false,    "scope-a", 1500};
    const auto pid = fork();
    ASSERT_GE(pid, 0);
    if (pid == 0) {
        DurableDeleteJournal journal(path);
        _exit(journal.Commit(record) ? 0 : 2);
    }
    int status;
    ASSERT_EQ(waitpid(pid, &status, 0), pid);
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 0);
    DurableDeleteJournal recovered(path);
    ASSERT_EQ(recovered.Records().size(), 1);
    EXPECT_EQ(recovered.Records()[0].reader_grace_ms, 1500);
    auto changed = record;
    changed.reader_grace_ms = 0;
    EXPECT_FALSE(recovered.Commit(changed));
    changed.reader_grace_ms.reset();
    EXPECT_FALSE(recovered.Commit(changed));
    record.completed = true;
    EXPECT_TRUE(recovered.Commit(record));
}

TEST_F(DurableDeleteJournalTest, ReaderGraceBoundAndLegacyUnknown) {
    DurableDeleteJournal journal(path);
    DurableDeleteJournal::Record record{"tenant", "key",     "operation",
                                        false,    "scope-a", 86400001};
    EXPECT_FALSE(journal.Commit(record));
    record.reader_grace_ms.reset();
    ASSERT_TRUE(journal.Commit(record));
    EXPECT_FALSE(journal.Records()[0].reader_grace_ms.has_value());
    // A legacy record does not prove that no old readers exist.
    record.reader_grace_ms = 0;
    EXPECT_FALSE(journal.Commit(record));
}

TEST_F(DurableDeleteJournalTest,
       TornTailRecoversButCompleteRecordCorruptionFailsClosed) {
    {
        DurableDeleteJournal journal(path);
        ASSERT_TRUE(
            journal.Commit({"tenant", "key", "operation", false, "scope-a"}));
    }
    const auto length = std::filesystem::file_size(path);
    {
        std::ofstream file(path, std::ios::app);
        file << "v1 partial";
    }
    {
        DurableDeleteJournal journal(path);
        ASSERT_EQ(journal.Records().size(), 1);
        EXPECT_EQ(journal.Records()[0].key, "key");
        EXPECT_EQ(std::filesystem::file_size(path), length);
        ASSERT_TRUE(
            journal.Commit({"tenant", "next", "operation", false, "scope-a"}));
    }
    {
        DurableDeleteJournal journal(path);
        EXPECT_EQ(journal.Records().size(), 2);
    }
    {
        std::fstream file(path, std::ios::in | std::ios::out);
        file.put('x');
    }
    EXPECT_THROW(DurableDeleteJournal journal(path), std::runtime_error);
}

TEST_F(DurableDeleteJournalTest,
       PartialWriteCannotAcknowledgeAndRecoveryCanAppend) {
    const auto pid = fork();
    ASSERT_GE(pid, 0);
    if (pid == 0) {
        DurableDeleteJournal journal(path);
        // Actual kernel write failure after a short append, not a mock result.
        signal(SIGXFSZ, SIG_IGN);
        const rlimit limit{12, 12};
        if (setrlimit(RLIMIT_FSIZE, &limit)) _exit(2);
        if (journal.Commit({"tenant", "key", "operation", false, "scope-a"}))
            _exit(3);
        if (journal.Commit({"tenant", "other", "operation", false, "scope-a"}))
            _exit(4);
        _exit(0);
    }
    int status;
    ASSERT_EQ(waitpid(pid, &status, 0), pid);
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 0);
    DurableDeleteJournal journal(path);
    EXPECT_TRUE(journal.Records().empty());
    EXPECT_EQ(std::filesystem::file_size(path), 0);
    EXPECT_TRUE(journal.Commit(
        {"tenant", "after-crash", "operation", false, "scope-a"}));
}

TEST_F(DurableDeleteJournalTest, MoreThanTenThousandKeysSurviveRestart) {
    {
        DurableDeleteJournal journal(path);
        for (size_t i = 0; i < 10001; ++i) {
            ASSERT_TRUE(journal.Commit(
                {"tenant", std::to_string(i), "operation", false, "scope-a"}));
        }
    }
    DurableDeleteJournal journal(path);
    ASSERT_EQ(journal.Records().size(), 10001);
    EXPECT_TRUE(
        journal.Commit({"tenant", "10001", "operation", false, "scope-a"}));
}

TEST_F(DurableDeleteJournalTest, InvalidPathAndCompletionWithoutBeginRefused) {
    EXPECT_THROW(DurableDeleteJournal journal("relative.log"),
                 std::invalid_argument);
    EXPECT_THROW(DurableDeleteJournal journal(directory + "/missing/journal"),
                 std::runtime_error);
    DurableDeleteJournal journal(path);
    EXPECT_FALSE(
        journal.Commit({"tenant", "key", "operation", true, "scope-a"}));
    EXPECT_FALSE(journal.Commit({"tenant", "", "operation", false, "scope-a"}));
    EXPECT_FALSE(journal.Commit(
        {"tenant", std::string(1025, 'k'), "operation", false, "scope-a"}));
}
TEST_F(DurableDeleteJournalTest, NamespaceRejectsMalformedRoutingAndAliases) {
    DurableObjectStorageNamespace scope{
        1, "s3", "https://oss.example.test", "bucket", "region", "prefix", 128};
    ASSERT_TRUE(scope.IsValid());
    const auto original = scope.Identity();
    scope.key_prefix = "other-prefix";
    EXPECT_NE(scope.Identity(), original);
    for (const auto& endpoint : {"http://", "http:///path", "file:///tmp",
                                 "https://user:password@oss.example.test",
                                 "https://oss.example.test?secret=1"}) {
        scope.endpoint = endpoint;
        EXPECT_FALSE(scope.IsValid());
    }
    scope.endpoint = "https://oss.example.test";
    scope.max_scoped_key_bytes = 1024;
    EXPECT_FALSE(scope.IsValid());
    scope.max_scoped_key_bytes = 1;
    scope.key_prefix = std::string(1012, 'p');
    EXPECT_FALSE(scope.IsValid());
}

TEST_F(DurableDeleteJournalTest, EmptyNamespaceCannotAuthorizeDeletion) {
    DurableDeleteJournal journal(path);
    EXPECT_FALSE(journal.Commit({"tenant", "key", "operation", false, ""}));
    EXPECT_TRUE(journal.Records().empty());
}
TEST_F(DurableDeleteJournalTest, LegacyUnscopedJournalFailsClosed) {
    {
        std::ofstream file(path);
        file << "v1 74656e616e74 6b6579 6f7065726174696f6e 0 2139567417\n";
    }
    EXPECT_THROW(DurableDeleteJournal journal(path), std::runtime_error);
}
}  // namespace mooncake::test
