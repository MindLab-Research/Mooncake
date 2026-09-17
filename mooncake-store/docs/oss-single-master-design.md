# OSS 单 Master 集成：设计审查与合并门禁

本设计对应 Mooncake PR #1 与 Mint PR #1314。仓库要求超过 500 行的架构变更经过 RFC；当前 fork 禁用了 Issues，因此提供此文档作为可审查的设计输入，由维护者确认采用 PR 内设计审查，或指定 RFC 位置。此文档不代表 RFC 已获批准。

## 范围与数据路径

北京和曼谷各自的 Mint 经本地 sidecar/C ABI 访问本地域 Store/provider。两个 provider 使用同一 authoritative Master 和相同 OSS endpoint/bucket/prefix。Master 管理对象位置、租约和删除协调；对象字节由 provider 与 OSS 交换。Mint API 的 artifact catalog 通过 export/import 交接，共享 Master 不同步业务数据库。

只验收单 Master、TCP、OSS 存储功能与旧 S3 兼容。GPU 训练、RDMA/Tent 硬件、控制面 HA、生产网络 SLA 不在此结论内。启用本地删除 journal 的 Master 拒绝 HA，不能由多个 Master 共用 journal 文件模拟一致性。

## 关键不变量

1. 写入成功不能仅靠 RAM Put ACK 判断。Mint 配置 require_offload，等待 offload 完成；C ABI replica bit 2 表示 offloaded，只有已经核验为 S3 的 provider 才能据此声称云端持久化。本接口不提供任意后端的自动认证。
2. 新上传先创建 multipart ID，再持久化 writer admission，然后检查永久删除 intent；只有检查通过才上传 part/Complete。删除先持久 intent，再 Abort 所有发现的 upload ID，最后 Delete 对象。依赖云端强一致 LIST/HEAD 和服务端 upload ID 撤销语义，不能推广到未验证的兼容服务。
3. Master 删除 admission 先 fsync journal，再允许 provider 执行；pending 阶段拒绝写入/重新发现并停止续租。provider 失败时保留 metadata 和围栏；只有所有计划成员成功且成员/分配仍匹配后才持久完成状态并移除 metadata。
4. 已发出的读不能被逻辑取消当作物理完成。Master 保留旧租约窗口；v3 provider 创建永久 intent 后额外等待协议读窗口。冷重启重放 journal，OSS discovery 过滤永久 intent，删除对象不可复活、不可复用原 key。
5. TCP 超时先停止 I/O、join、销毁旧回调，再重建执行器；不能先释放仍被原生 I/O 引用的缓冲区。无法取消时，仅显式启用的托管进程由独立 watchdog 退出124并重启；默认库不退出，远端已发生的写也不会因此撤销。

普通 `Remove` / `RemoveAll` / 按正则清 metadata 不等于 OSS durable-delete；本集成删除必须走 `mooncake_store_remove_durable` / `RemoveDurable`。运维不能用管理接口清 metadata 代替云端删除，否则未设永久 intent 的对象可能被 discovery 再发现。

## 升级、故障与容量

v3 provider 必须使用配置了持久 journal 的 Master，缺少能力即拒绝启动；S3 endpoint 必须显式配置为无凭据 origin。旧 v1 writer admission 没有可撤销 upload ID，升级须排空并隔离旧 writer，不按 TTL 猜测回收。永久 tombstone 没有自动 GC，需要持续监控 journal 磁盘和索引内存。

断尾 journal 只截断未完成的最后一条；完整损坏 fail closed。离线修复只接受可信、完整且匹配的 mirror；没有这样的副本不能宣称可恢复。详见 [删除恢复说明](durable-delete-recovery.md)。

provider 重启后动态 endpoint/segment 可变。自动重启策略只恢复进程，编排层还须重新发现新路由、更新 sidecar，并用实际 GetBlob 作为 readiness。部署 skill 是操作说明，不是已完成生产编排的安装器。回滚保留 journal、OSS namespace 与删除围栏，不能退到不理解当前协议的版本。

## 可复现验证

在已安装依赖、配置好的 Linux CPU CMake 构建目录执行：

```bash
bash mooncake-store/tests/run_oss_review_gate.sh /path/to/build
```

入口显式构建并检查 CTest 注册，运行 Master SSD/删除状态机、journal、provider RPC、TCP/Store 超时、object adapter、file storage、地域路由、metadata 配置，以及 Python journal 修复/云验证防误用测试。任何缺测试、编译错误或失败都会非零退出；全套 Master 测试包含租约等待。此门禁不调用云端；真实云测试另需 boto3 与获准的隔离凭据。事件模式需单独配置构建目录并运行对应传输测试；本入口不改变构建选项。

这个门禁不替代真实云端/跨地域实测。最终运行候选还必须绑定双向 Put/Get 与 OSS hash、源离线/目标空缓存冷读、两地删除及失败保护、catalog/download token、真实历史 S3 回退和镜像/FFI hash。流式错误注入、模拟不可取消 transport 与真实云端测试分别报告；证据包放 Git 外。

## 放行条件

- 代码、协议及兼容性由维护者审查；明确接受默认非 TCP 取消边界和 opt-in 退出策略。
- 原生门禁与两地域 live 门禁绑定运行源码；后续仅文档/测试入口变更可保持运行绑定，改运行代码必须重验。
- Mint 配套提交必须可合并；其冲突解决若改变运行代码，应补验受影响集成，不能继承旧 head 的全部结果。
- 正式生产接入前发布可拉取的配套镜像 digest，在目标网络完成路由刷新/readiness、凭据、持久卷、容量与回滚演练。本地镜像 ID 不等于 registry 发布完成。
