---
name: mooncake-store-deploy
description: 供运维同事将已审核的 OSS-backed Mooncake Store 接入生产 Mint 集群：部署共享 Master、地域 provider 与同 Pod sidecar，验证跨地域读写并执行升级回滚。
---

# Mint 接入 OSS-backed Mooncake Store

用于单 authoritative Master、每个业务节点或地域部署 Store/provider 的接入。Master 管对象元数据和路由；对象字节由 Store/provider 与 OSS 交换，不经过 Master。Mint 经 sidecar/C ABI 访问 Store，不直接持有 OSS 凭据。

## 交付状态与使用方式

本 skill 是部署和验收操作说明，不是已接入 Mint 生产编排的一键安装器。开发机单 Master、两地 Store 的存储门禁已通过；正式镜像仓库发布、生产服务配置，以及 provider 重启后的自动路由刷新和实际 GetBlob readiness 仍需在目标集群落实并演练。Docker 自动重启仅恢复进程，不能自动更新 sidecar 的动态 endpoint。不要把上述开发机 PASS 表述为生产发布已经完成。

## 先确定版本和网络

记录 Mint/Mooncake commit、C header、Rust FFI 声明、动态库、Master/provider/sidecar/API SHA-256 和实际加载文件。2026-09-16 最新开发机镜像 live 验收绑定的运行源码为 Mooncake `db65d41a4cac63ec055806922b15981aea1314a2`、Mint `aa9a1e67cbd2a740a2569b92a563cb7663d83d29`。后续纯文档提交不改变运行候选；如果修改运行代码，重跑受影响门禁并重新绑定，不能借用旧证据。

原始报告、日志、verifier 和二进制证据包应从交付人提供的外部验收包获取，不提交 Git。历史提交已清理重写，旧 SHA 仅用于识别归档构建；部署当前候选前必须拿到与其源码及镜像 digest 绑定的新验收清单。当前采用显式启用的原生传输进程隔离恢复；该运行候选的两地 live 验收和本地镜像绑定已通过。退出 124/自动重启通过故障注入验证，两地 provider 异常退出后的路由刷新与真实读取也通过；未测试 RDMA 硬件或启用 Tent 的构建。配套镜像尚未发布 ACR，生产放行仍需维护者审核、镜像发布及目标集群门禁。C ABI 没有数字版本查询函数；用全部 12 个 Mint 所需符号、声明、库 hash 及真实调用证明一致性。

每个节点必须能够访问同一个 Master RPC 和 HTTP metadata 服务；Master/客户端也必须能回连 provider offload RPC、Transfer Engine 和 TCP 数据端口。只通 50481 不够。动态端口要使用可双向路由的专网地址或测试 overlay；不要将公网任意端口全部开放。

共享 Master 独立于 Store 生命周期。每个节点使用唯一 `MOONCAKE_HOST_ID`，provider `--host` 和 sidecar `local_hostname` 使用可回连地址。设置 `MC_TCP_BIND_ADDRESS` 为同一地址，防止自动选到不通的 eth0 地址。不要设置 `MC_LEGACY_RPC_PORT_BINDING`：本次实际 runtime 会发生重复 bind。

完整 OSS 环境变量、依赖检查、provider 命令和 sidecar TOML 见 [启动参考](references/provider-launch.md)。

## 启动顺序

1. 在控制面主机启动一个 Master。记录 RPC、HTTP metadata 地址和 Master PID。OSS durable-delete 候选应带持久 journal 路径，并验证重启恢复能力；旧版本缺此功能时不能报告删除门禁通过。
2. 每个节点启动 provider，传入同一 Master/metadata 地址、本节点 host、segment 和 buffer 容量以及独立端口。OSS 配置仅通过 provider 的受保护环境文件注入。
3. 确认 provider 日志出现 S3 backend、segment 挂载和 offload RPC 启动。从本次 provider 的 `LocalDiskDescriptor.transport_endpoint` 取得真实 offload endpoint，将 Mooncake 原生选项 `MC_STORE_REQUIRED_OFFLOAD_ENDPOINT` 注入 sidecar 进程，再启动 sidecar。该选项不是 Mint TOML 字段，也不是 Master 地址；provider 端口变化后必须更新并重启 sidecar。某些 provider 先启动 Unix IPC，稍后才启动配置的 TCP 端口；不能把端口短暂未监听当作 Master 网络断开。
4. 确认两端 sidecar 能访问自己的 Store，进程环境不带 provider 的云凭据（历史 S3 回退使用 sidecar 独立挂载的凭据文件）；配置相同 bucket/endpoint/测试 prefix 才能验证共享 OSS 场景。TOS/R2 不同后端不因共享 Master 自动互通，必须另验数据路由。

provider 关键参数（值从部署配置提供）：

```text
--host=<NODE_ROUTABLE_IP>
--master_server_address=<MASTER_IP>:50481
--metadata_server=http://<MASTER_IP>:28482/metadata
--protocol=tcp
--global_segment_size=<BYTES>
--local_buffer_size=<BYTES>
--port=<PROVIDER_PORT>
--enable_offload=true
--start_offload_rpc_server=true
```

sidecar 的 `[store.mooncake]` 配置（preferred_segments 填本次 provider 实际注册的 segment 名称）：

```toml
master_server_addr = "<MASTER_IP>:50481"
metadata_server = "http://<MASTER_IP>:28482/metadata"
local_hostname = "<NODE_ROUTABLE_IP>:<UNIQUE_SIDECAR_ID_PORT>"
protocol = "tcp"
require_offload = true
replica_num = 1
preferred_segments = ["<NODE_ROUTABLE_IP>:<REGISTERED_SEGMENT_PORT>"]
global_segment_size = 0
local_buffer_size = 536870912
```

容量、timeout、read limit 按模型对象大小及并发量设置；示例容量不是生产统一值。配置格式以当前 Mint 源码为准。确保 `mint://` 走 Mooncake，同时保留 `s3://` 的 S3Persistence 读取和回退。

API 的 Mooncake 下载需要 `[server] public_base_url` 及独立的 `[auth] download_signing_secret`（或对应的 `download_signing_secret_file`）。签名密钥仅保存在 API 服务端，不能发给业务客户端或管理工具；它与业务 `admin_token`、管理 `admin_management_token` 都必须不同。下载 capability 使用仅 API 持有的专用密钥及 HMAC domain 签名；缺少专用签名密钥时下载接口返回 503，绝不退回业务凭据签名。

## 真实验收

用 sidecar PutBlob/Publish 写新的唯一对象，保存 URI、字节数、SHA-256 和返回时间。OSS hash 可由独立审计工具核对；目标读取必须用真实 sidecar，不以 boto3 下载替代。

本 skill 的 `scripts/read_probe.py` 只调用真实 GetBlob，需目标环境的 grpc 和生成的 protobuf 模块，以及包含 `stub_path`、`ports.sidecar` 的 TOML：

```bash
python scripts/read_probe.py --config node.toml \
  --key mint/<object-key> --sha256 <expected-sha256> \
  --bytes <expected-size> --result read-result.json
```

双向执行。为了证明 OSS 冷读，停止源端 provider/sidecar，保留共享 Master；目标端使用新 Store 进程、新 segment 和空 sidecar 缓存目录。保存停止状态、PID、实际配置、provider 回源日志及结果。随后交换源和目标重复。每一阶段确认 Master PID未被区域 Store 停启改变。

记录多次 RPC/Put/Get 的 p50/p95、对象大小、并发和失败率。单次 1–4 MiB 成功不代表大 checkpoint、GPU optimizer 恢复或生产性能验收。新部署按下述门禁做上线验证。下文 live 结果只适用于其绑定的运行版本；PR 后续运行代码变更须补充受影响门禁，不可直接继承通过结论。

## 验收结论与使用边界

2026-09-16 镜像绑定候选（Mooncake `db65d41a`、Mint `aa9a1e67`）已完成双向 1/4/96 MiB Put/Get、源离线冷读、OSS hash、两地删除与失败保护、实际 catalog import/download、token 和真实 S3 回退。模型归档使用 CPU 重放；不声称 GPU optimizer、续训或 sampling 已验收。流式截断/过长/中途失败是实际 HTTP API 后注入 gRPC 故障，不能当作 OSS 故障实测。

删除必须先完成 OSS durable delete，再清除 Master metadata。失败保留 metadata；两地验收还需并发重复删除、冷启动不复活及未删除正控可读。既有读租约约 900 秒，删除可能等待该窗口并需要重试；保留首次结果及累计时延，不得缩短保护窗口以加速验收。

`download_url` 要验证合法下载、签名篡改、错误密钥、过期、跨 artifact 重放，以及长度/截断错误；不得泄露 OSS/Mooncake 凭据。S3 门禁必须用真实后端验证历史 `s3://` 读取和 presign、切换 Mooncake 后 `mint://` 新写读、切回 S3 后继续新写。保留 `S3Persistence` 与旧 presign 路径。

## 新上传协议与 journal 恢复

新 provider 的 Put/PutV 使用持久 multipart upload ID，删除通过 OSS/S3 的 AbortMultipartUpload 撤销残留 writer，禁止按 TTL 猜测可删除。凭据必须包含 multipart 中止权限。升级前排空旧 v1 writer；旧 admission 没有可撤销 ID，不能自动清除。journal 介质损坏时使用独立、完整且匹配的 mirror 离线修复，不得删除坏行强行启动。步骤与边界见[删除恢复说明](references/durable-delete-recovery.md)。

## 故障恢复

- `rc=-707`/namespace 不匹配：先核对两地 endpoint/bucket/prefix 一致，目标 provider 的 descriptor 已完成并可路由；不要绕过本地 offload endpoint 的 fail-closed 限制直接读 OSS。
- metadata 超时：检查双向网络和实际进程环境。本次 WAN provider/sidecar 使用 `MC_METADATA_HTTP_CONNECT_TIMEOUT_MS=10000`、`MC_METADATA_HTTP_TIMEOUT_MS=30000`，上游默认仍为 1500/3000 ms。OSS accelerate 只加速对象访问，不会自动加速 Master RPC 或 SSH。
- 源端停止后短暂旧 RAM route：记录即时失败及 client_ttl 收敛后的结果，不能静默重试后只报成功。
- TCP 原生传输卡住：当前库在 `MC_STORE_TRANSFER_TIMEOUT_MS`（默认 60000 ms）期限后终止 TCP I/O 并 join，确认不再触碰缓冲区才返回失败。同一旧执行器上的并发调用也会失败；新修复会销毁旧回调并在原端口创建全新 TCP 执行器，同一 Client 可继续新请求。若资源不足或重新绑定失败，transport 保持不可用。health 为 `HC_TRANSFER_UNAVAILABLE=3`，原生客户端显式启动 HTTP server 后的 `/health` 返回 503/`transfer_unavailable` 时，编排层应撤销 readiness 并重建受影响客户端/sidecar；进程未退出不代表健康。provider 同样受损时先恢复 provider，再重新获取 endpoint/segment 并启动 sidecar。不对同一失效客户端无限重试。
- 非 TCP/Tent 不支持物理取消时使用下述托管进程退出/重启策略；不能以缩短 timeout 后直接释放内存代替。Store deadline 不是所有网络/metadata 阶段的端到端 SLA。

## 开发基准与生产接入

开发基准的节点地址、运行目录和进程证据见仓库外的配套验收包。部署时填写本次获准的控制面及地域服务地址；用 `systemctl show -p MainPID -p ActiveState` 和 `/proc/PID/exe`、加载库核查，不以目录名字代替版本证据。

开发机 SSH TUN/BBR 是测试网络适配，不依赖 Mac 转发，也不是生产 WAN SLA。部署到实际 Mint 集群时替换为批准的双向网络、服务地址、密钥注入和持久卷；固定审核后的镜像/源码及 hash，在目标网络重跑本 skill 的门禁。共享 Master 的 durable-delete journal 必须持久化并保留；本次没有验收多 Master 或控制面 HA。

生产发布与回滚的执行顺序见 [生产接入参考](references/production-rollout.md)。

交付物包含节点无密钥配置、启动/回滚命令、版本 manifest、两方向原始读写/冷读/删除日志、token/S3 结果和时延定义。开发验收通过后交人工审核 PR，再安排生产部署；skill 不隐含合并或生产变更授权。

Mint 仓库中的配置模板、固定依赖和活体测试入口见 [Mint 验证入口](references/mint-validation.md)。

### 无法取消的托管传输

独立 provider/sidecar 启用 `MC_STORE_TRANSFER_FATAL_TIMEOUT=1`、`MC_STORE_TRANSFER_ABORT_GRACE_MS=5000`，并配置失败自动重启。原生 batch 等待超时后仍无物理停止证明时进程退出 124，不执行析构；同进程并发请求也失败。通用库默认不启用，禁止默认注入内嵌训练进程。该策略不撤回已到达远端的写，不把请求失败等同于对象不存在。provider 重启后必须重取 segment/offload endpoint 并更新 sidecar，共享 Master/journal 不重建。故障测试需保存 exit code、重启次数、新进程 ID 和恢复后的真实读取。详见 [协议与恢复说明](references/durable-delete-recovery.md)。
