# 删除恢复与本轮协议修订

## multipart admission

新 provider 的 Put / PutV 一律通过 multipart 上传（包含小对象和空对象）。在发送任何 part / Complete 请求前，将服务端 upload ID 写入 `writers/<key-sha256>/<uuid>`，再检查永久删除标记。删除先写 `deletions/`，然后读取同 key 的 writer admission，对每个 ID 执行 AbortMultipartUpload，获得成功或明确 NoSuchUpload 后才清除 admission 并删除对象。权限、网络或无法识别的 admission 错误仍传播给 Master；不能据此移除 metadata。

Abort 撤销 upload ID，已中止的 ID 不能再次 Complete。若 Complete 已先成功，Abort 返回 NoSuchUpload，后续 Delete 移除已提交对象。若 provider 崩溃，另一 provider 仍能按持久 admission 中止上传，无需推断进程是否存活或等待任意 TTL。该协议依赖所用 S3 服务对 multipart ID 的服务端撤销，以及 LIST/HEAD 的强一致性，兼容服务必须先执行对应实测。

凭据除现有对象 Put/Get/Delete/List 权限外，还需允许 multipart 创建、part 上传、完成与中止；AWS S3 的中止操作需要 `s3:AbortMultipartUpload`，OSS 对应 `oss:AbortMultipartUpload`。创建 upload 后、写 admission 前崩溃可能留下尚未上传数据的空 multipart，可用 bucket 的未完成 multipart 生命周期清理；不能用生命周期或 TTL 代替删除时的 Abort 围栏。

旧 `mooncake-upload-admission-v1` 记录没有可撤销 ID，不能自动过期或静默清除。升级前停止旧 writer 并排空；如已有崩溃遗留，须通过基础设施隔离证明旧请求不可能继续写入，再由运维处理。混用旧 provider 时不得宣称旧记录具备自动恢复能力。现有对象布局与读取格式不变。

## TCP 超时恢复

取消仍先停止并 join 旧 I/O 执行器，完成所有在途 batch 的失败通知，再销毁旧 context 和回调。恢复创建全新的执行器并绑定原端口，不运行旧回调。关闭连接池的请求也使用受跟踪的异步队列，但每次成功后关闭连接，避免同步 DNS/connect 阻挡取消。新请求与恢复串行，旧 batch 的重复取消不会停止新一代请求。

非 TCP/Tent 缺少物理取消屏障时不能安全返回调用方。托管的 provider/sidecar 可显式设置 `MC_STORE_TRANSFER_FATAL_TIMEOUT=1`：`wait()` 在第一次状态查询前启动独立 watchdog，在传输期限后加 `MC_STORE_TRANSFER_ABORT_GRACE_MS`（默认 5000 ms）的宽限期。未确认完成时调用 `_Exit(124)`，不执行析构、atexit 或日志锁，不释放活动缓冲区后继续运行。watchdog 独立于状态查询和 abort；它们本身阻塞也不能阻挡进程退出。若开始等待时传输期限已过，仍给予完整宽限期以检查完成状态。

这是进程级故障隔离，不声称 RDMA/Tent 获得了物理取消 API，也不保证已发往远端的写被撤回。并发请求一起失败；写入响应丢失属于结果不确定，必须依靠既有幂等/删除围栏协调，不能当作未写入。进程退出后由操作系统回收本地执行上下文；不要在同进程中捕获超时再回收活动内存。通用库默认不启用退出策略。

只在独立受托管的 Store 进程启用，不对内嵌训练主进程默认注入。容器应使用 `restart: on-failure` 或 Kubernetes `restartPolicy: Always`，保留终止 exit code 和日志，设置重启退避及告警。provider 重启后重新发现 segment/offload endpoint，更新 sidecar 配置并重建；仅原地重启旧配置的 sidecar 不足以恢复动态路由。共享 Master 及持久 journal 不随 regional Store 重建。

该期限覆盖 Store 原生 batch `wait()`，不等于所有 SDK、OSS HTTP 或进程启动都有相同的端到端期限；外层 RPC 还应有独立 deadline。单独轮询 `isReady()` 不启动此 watchdog。

## journal 恢复

未确认的最终半行在 Master 启动时截断并 fsync。完整坏记录仍 fail closed；不能删除坏行、清空 journal 或用更旧的备份覆盖，否则可能丢失已经确认的永久围栏。

提供离线工具 `mooncake-store/tools/repair_delete_journal.py`。以下命令在与部署候选匹配的 Mooncake 源码 checkout 根目录运行，需要 Python 3；不要假设最小运行镜像包含源码或 Python。先停止持有 journal 的 Master；工具获得排他文件锁，运行中的 Master 会使操作被拒绝。

```bash
python3 mooncake-store/tools/repair_delete_journal.py /persistent/delete.journal
python3 mooncake-store/tools/repair_delete_journal.py /persistent/delete.journal \
  --mirror /independent-backup/delete.journal \
  --output /persistent/delete.journal.repaired
```

修复只接受**来自可信独立副本、记录条数相同、所有完好行完全一致**的 mirror，验证其全部 CRC、格式和状态转移。旧备份、缺行、有效记录冲突、完成状态回退及损坏 mirror 均拒绝。输出为新文件，原文件不变；工具不切换 Master、不覆盖任何现有文件。核对输出 SHA、备份来源及 snapshot 的 canonical fence 后，由运维切换已验证的 journal 并启动 Master，保留原件与修复输出作为审计记录。

没有匹配的独立完整副本时，该工具无法凭空重建记录，必须做权威删除记录的取证恢复。它不是容灾复制协议，也不承诺恢复所有介质损坏。

## 合并边界

本轮修复要重新绑定源码、动态库及镜像。历史两地域 PASS 不自动覆盖 multipart 协议变化；正式放行还需最终候选两地域 live 门禁及维护者评审。测试日志、运行包和备份继续保存在 Git 外部。
