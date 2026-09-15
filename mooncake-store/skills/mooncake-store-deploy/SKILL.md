---
name: mooncake-store-deploy
description: 在 Mint 节点部署接入 OSS 的 Mooncake Store/provider 与 sidecar，连接共享 Master，并验证跨节点读写和冷恢复。
---

# Mint 接入 OSS-backed Mooncake Store

用于单 authoritative Master、每个业务节点或地域部署 Store/provider 的接入。Master 管对象元数据和路由；对象字节由 Store/provider 与 OSS 交换，不经过 Master。Mint 经 sidecar/C ABI 访问 Store，不直接持有 OSS 凭据。

## 先确定版本和网络

记录 Mint/Mooncake commit、C header、动态库、Master/provider/sidecar SHA-256。不能把旧 runtime 的实验结果绑定成最新 PR 验收。本次已测试组合为 Mint 2fe97e16、Mooncake Master/动态库 39c34714、保留的 provider d86c547d。具体 hash 见随 PR 提交的验收 manifest；不能引用旧 runtime 结果替代新候选。

每个节点必须能够访问同一个 Master RPC 和 HTTP metadata 服务；Master/客户端也必须能回连 provider offload RPC、Transfer Engine 和 TCP 数据端口。只通 50481 不够。动态端口要使用可双向路由的专网地址或测试 overlay；不要将公网任意端口全部开放。

共享 Master 独立于 Store 生命周期。每个节点使用唯一 `MOONCAKE_HOST_ID`，provider `--host` 和 sidecar `local_hostname` 使用可回连地址。设置 `MC_TCP_BIND_ADDRESS` 为同一地址，防止自动选到不通的 eth0 地址。不要设置 `MC_LEGACY_RPC_PORT_BINDING`：本次实际 runtime 会发生重复 bind。

完整 OSS 环境变量、依赖检查、provider 命令和 sidecar TOML 见 [启动参考](references/provider-launch.md)。

## 启动顺序

1. 在控制面主机启动一个 Master。记录 RPC、HTTP metadata 地址和 Master PID。OSS durable-delete 候选应带持久 journal 路径，并验证重启恢复能力；旧版本缺此功能时不能报告删除门禁通过。
2. 每个节点启动 provider，传入同一 Master/metadata 地址、本节点 host、segment 和 buffer 容量以及独立端口。OSS 配置仅通过 provider 的受保护环境文件注入。
3. 确认 provider 日志出现 S3 backend、segment 挂载和 offload RPC 启动。从本次 provider 的 `LocalDiskDescriptor.transport_endpoint` 取得真实 offload endpoint，将 Mooncake 原生选项 `MC_STORE_REQUIRED_OFFLOAD_ENDPOINT` 注入 sidecar 进程，再启动 sidecar。该选项不是 Mint TOML 字段，也不是 Master 地址；provider 端口变化后必须更新并重启 sidecar。某些 provider 先启动 Unix IPC，稍后才启动配置的 TCP 端口；不能把端口短暂未监听当作 Master 网络断开。
4. 确认两端 sidecar 能访问自己的 Store，进程环境不带云凭据；配置相同 bucket/endpoint/测试 prefix 才能验证共享 OSS 场景。TOS/R2 不同后端不因共享 Master 自动互通，必须另验数据路由。

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
local_hostname = "<NODE_ROUTABLE_IP>:0"
protocol = "tcp"
require_offload = true
replica_num = 1
preferred_segments = ["<NODE_ROUTABLE_IP>:<REGISTERED_SEGMENT_PORT>"]
global_segment_size = 0
local_buffer_size = 536870912
```

容量、timeout、read limit 按模型对象大小及并发量设置；示例容量不是生产统一值。配置格式以当前 Mint 源码为准。确保 `mint://` 走 Mooncake，同时保留 `s3://` 的 S3Persistence 读取和回退。

## 真实验收

用 sidecar PutBlob/Publish 写新的唯一对象，保存 URI、字节数、SHA-256 和返回时间。OSS hash 可由独立审计工具核对；目标读取必须用真实 sidecar，不以 boto3 下载替代。

本 skill 的 `scripts/read_probe.py` 只调用真实 GetBlob，需目标环境的 grpc 和生成的 protobuf 模块，以及包含 `stub_path`、`ports.sidecar` 的 TOML：

```bash
python scripts/read_probe.py --config node.toml \
  --key mint/<object-key> --sha256 <expected-sha256> \
  --bytes <expected-size> --result read-result.json
```

双向执行。为了证明 OSS 冷读，停止源端 provider/sidecar，保留共享 Master；目标端使用新 Store 进程、新 segment 和空 sidecar 缓存目录。保存停止状态、PID、实际配置、provider 回源日志及结果。随后交换源和目标重复。每一阶段确认 Master PID未被区域 Store 停启改变。

记录多次 RPC/Put/Get 的 p50/p95、对象大小、并发和失败率。单次 1–4 MiB 成功不代表大 checkpoint、GPU optimizer 恢复或生产性能验收。生产合并前另外完成 download_url token、真实历史 s3 回退、durable-delete/并发/失败保护及最终候选绑定。

## 已实测的开发网络（2026-09-15）

北京开发机 `115.191.57.4` 的测试网络地址为 `10.254.254.1/30`；曼谷 `47.81.60.206` 为 `10.254.254.2/30`。Master 是 `10.254.254.1:50481`，metadata 是 `http://10.254.254.1:28482/metadata`。这些地址仅适用于本次环境，不应复制到任意 Mint 集群。

北京 `ctgg-mooncake-tunnel.service` 使用专用受限 SSH key 建立 `tun42`，曼谷 `ctgg-mooncake-tun-address.service` 配置对端地址；没有 Mac 转发依赖。两端只对另一开发机的公网 /32 路由配置 BBR。Cubic 测试出现反向 1 MiB 超时，BBR 后新 4 MiB 首读成功，但不是受控性能对比，也不是生产 WAN SLA。

北京 Master 服务为 `ctgg-shared-mooncake-master.service`；两地 Store 服务为 `ctgg-mint-mooncake-oss.service`。使用 `systemctl show ... -p MainPID -p ActiveState` 核查，不要以 unit 文件存在判断服务存活。仅停止本任务服务，不停止同机其他 Master。

SSH TUN 是开发网络适配。生产优先使用已批准的双向可路由网络，并重新验证 RTT、吞吐、故障恢复、认证和访问控制。部署 skill 本身不代表获准修改生产网络或合并 PR。

96 MiB 后续实测：启用 Mint preferred_segments，并统一两地 OSS accelerate endpoint 后，双向 sidecar 写入、OSS hash 核对及源端离线后的目标冷读均通过。早期未传 preferred_segments 和标准 endpoint 的失败日志仍保留；不能将不同配置的实验混为同一候选。2026-09-15 新增 WAN metadata timeout 修复后的复测见工作区 evidence/2026-09-15/shared-master-network/wan-*。

配套候选部署注意：durable-read namespace 比较包含 endpoint。标准 OSS endpoint 与 accelerate endpoint 即使指向同一 bucket，也会被当前 Master 视为不同 namespace 并返回 NOT_SUPPORTED；同一共享 Master 下先统一 provider 的 endpoint/bucket/prefix。`preferred_segments` 需填写本次 provider 实际注册名（例如 IP:动态端口），不可把示例值跨重启复用。优先分配可能回退，不能当作硬性地域隔离。

退出收敛：停止源端后，Master 在 client_ttl 窗口内可能仍返回已失效内存副本。验收需记录即时读取失败与副本清理后的回源结果；不能静默重试并只报告成功。当前默认 metadata HTTP connect timeout 为 1500 ms，在测试 WAN 中已出现真实超时，生产网络超时预算仍需验证。

WAN metadata 超时补丁保留默认 1500 ms 连接/3000 ms 总预算；本次两地 Store service 明确设置 `MC_METADATA_HTTP_CONNECT_TIMEOUT_MS=10000` 和 `MC_METADATA_HTTP_TIMEOUT_MS=30000`。必须给 provider 和 sidecar 实际进程继承这两个变量；仅改交互 shell 无效。新动态库/provider 在 `ctgg-wan-timeout-20260915`，运行目录还需携带 `libasio.so`。北京 Master 维持 `671db7ab` 原二进制；补丁未修改 C ABI 或 RPC 协议。该混合版本安排需如实记录，不能声称 Master 也已换为新编译文件。

## 当前范围和合并门禁

单 Master + 双地域 Store 的在线/源端离线冷读、live 删除、token 和历史 S3 回退已通过开发机验收。Master 只承担 metadata/routing，字节路径为 Store/provider ↔ OSS。读路由必须命中本地域已完成的 offload descriptor，缺失时 fail closed。

成功删除可能等待既有约 900 秒读租约；调用方应保留可重试状态，不得缩短 fence 来让门禁变绿。独立 native OSS 故障注入不等同于共享服务的线上故障测试。

开发验收 PASS 不等于代码审查通过。使用前核对 PR 中的审查报告与当前 CI；已知尚未关闭的通用启动兼容性、读 fence 路由和传输终止问题禁止据此直接宣称生产就绪。
