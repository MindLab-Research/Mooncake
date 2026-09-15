# Store/provider 配置与启动参考

这是已实测参数的可移植写法。`<...>` 必须替换为部署节点参数；不要直接复制本次开发机 IP 到生产。使用包含 S3 provider、durable C ABI 和 metadata timeout 和 TCP 安全终止补丁的候选构建，以及支持 preferred_segments 的 Mint sidecar。上游任意版本不保证支持这些参数。

## 运行文件

为候选建立独立目录，包含 `services/mooncake_client`、`lib/libmooncake_store.so`、`lib/libasio.so` 和 `mint-store-sidecar`。AWS SDK 依赖放在独立 lib 目录。执行 `LD_LIBRARY_PATH=<LIB_DIR>:<AWS_LIB_DIR> ldd <PROVIDER>`，对 sidecar 和动态库也执行同样检查；所有依赖必须解析成功。记录文件 SHA-256、源码提交和未提交 patch 的 hash。

## 唯一 Master

只在控制面节点执行一次，由独立服务管理器守护。以下参数与已验收服务一致；地址应限制在批准的内网，并给 journal 目录配置持久卷和写权限：

```bash
<MASTER> --rpc_port=50481 --rpc_address=<MASTER_BIND_IP> \
  --enable_http_metadata_server=true --http_metadata_server_port=28482 \
  --metrics_port=29433 --enable_offload=true --offload_on_evict=false \
  --default_kv_lease_ttl=900000 --client_ttl=30 \
  --durable_delete_journal_path=<PERSISTENT_STATE_DIR>/durable-delete.journal
```

Master 不需要 OSS 凭据；regional provider 负责对象字节。不要随 Store 重启或清缓存删除 journal。冷读验收保留共享 Master 在线，Master 崩溃恢复属于独立控制面恢复测试。

## provider 专属环境

用权限 0600 的文件保存以下环境配置。凭据来自获准的密钥管理渠道，禁止写入 skill、Git 或验收日志。只向 provider 进程注入该文件，不向 sidecar/Mint 注入。

```bash
MOONCAKE_AWS_ACCESS_KEY_ID=<ACCESS_KEY>
MOONCAKE_AWS_SECRET_ACCESS_KEY=<SECRET_KEY>
MOONCAKE_AWS_BUCKET_NAME=<BUCKET>
MOONCAKE_AWS_REGION=<REGION>
MOONCAKE_AWS_S3_ENDPOINT=https://oss-accelerate.aliyuncs.com
MOONCAKE_AWS_USE_VIRTUAL_ADDRESSING=true
MOONCAKE_AWS_USE_HTTPS=true
MOONCAKE_AWS_REQUEST_TIMEOUT_MS=60000
MOONCAKE_AWS_CONNECT_TIMEOUT_MS=10000
MOONCAKE_AWS_REQUEST_CHECKSUM_CALCULATION=when_required
MOONCAKE_AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
MOONCAKE_S3_KEY_PREFIX=<SHARED_PREFIX>
MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR=s3_object_storage_backend
MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=<NODE_LOCAL_STATE_DIR>
MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES=536870912
MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES=68719476736
MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS=1
MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_TTL_MS=120000
MOONCAKE_OFFLOAD_S3_METADATA_REFRESH_INTERVAL_SECONDS=3
```

两地域共享相同 endpoint/bucket/prefix；本地状态目录不同。accelerate 需要 bucket 已支持该访问方式。本次仅验收 Alibaba OSS，不能推定任意 S3 兼容后端可用。示例缓冲池适合单次 96 MiB 验证；生产需按同时恢复对象的总大小和持续时间配置。

## 公共运行环境与启动

provider 与 sidecar 均需下列非敏感环境。`MOONCAKE_HOST_ID` 每个节点唯一，`MC_TCP_BIND_ADDRESS` 是可被另一地域回连的地址。

```bash
export LD_LIBRARY_PATH=<LIB_DIR>:<AWS_LIB_DIR>
export MOONCAKE_HOST_ID=<UNIQUE_NODE_ID>
export MC_TCP_BIND_ADDRESS=<NODE_ROUTABLE_IP>
export MC_METADATA_HTTP_CONNECT_TIMEOUT_MS=10000
export MC_METADATA_HTTP_TIMEOUT_MS=30000
export MC_STORE_TRANSFER_TIMEOUT_MS=60000
```

provider 的服务管理器加载上述公共环境和 provider 专属文件，然后执行：

```bash
<PROVIDER> --host=<NODE_ROUTABLE_IP> \
  --master_server_address=<MASTER_IP>:50481 \
  --metadata_server=http://<MASTER_IP>:28482/metadata \
  --protocol=tcp --device_names= \
  --global_segment_size=536870912 --local_buffer_size=536870912 \
  --port=<PROVIDER_PORT> --threads=8 \
  --enable_offload=true --start_offload_rpc_server=true
```

首次启动等待日志确认 `object adapter=s3`、segment 挂载与 offload RPC 就绪。从**本次启动**的 `Successfully created client on port N` 日志取得 segment，填为 `<NODE_ROUTABLE_IP>:N`，不要使用上一代进程日志的动态端口。

sidecar 由独立服务管理器启动，只加载公共环境，并设置当前 provider 的原生读路由选项（每次 provider 重启重新取值）：

```bash
MC_STORE_REQUIRED_OFFLOAD_ENDPOINT="<CURRENT_PROVIDER_OFFLOAD_IP:PORT>" \
  <SIDECAR> --config <SIDECAR_TOML>
```

完整的开发验证 TOML 示例：

```toml
[base]
env = "dev"
base_dir = "<FRESH_NODE_CACHE_DIR>"
dev_user = "dev/validation"
[observability]
endpoint = ""
insecure = false
[listen]
addr = "127.0.0.1:17420"
[store]
kind = "mooncake"
[store.mooncake]
master_server_addr = "<MASTER_IP>:50481"
metadata_server = "http://<MASTER_IP>:28482/metadata"
local_hostname = "<NODE_ROUTABLE_IP>:0"
protocol = "tcp"
device_name = ""
global_segment_size = 0
local_buffer_size = 536870912
replica_num = 1
preferred_segments = ["<NODE_ROUTABLE_IP>:<CURRENT_SEGMENT_PORT>"]
enable_ssd_offload = false
require_offload = true
offload_timeout_secs = 180
put_retry_timeout_secs = 60
[cache]
max_bytes = 17179869184
[admission]
max_inflight_tar_ops = 4
[compression]
workers = 2
```

本示例监听 loopback，适用于同节点 Mint。容器/Pod 需明确 sidecar 的可达服务地址和访问控制；不要直接把 loopback 改成无认证公网监听。Mint 调用此 sidecar，不获得 provider 的云凭据。真实训练资源与业务调度配置是独立接入步骤，本存储测试不证明 GPU 训练闭环。

## 常驻与故障恢复

用 systemd 或集群原生编排分别管理唯一 Master 和 regional Store/sidecar。Master 不作为每个 Store 的子进程启动。provider 重启后重新发现 segment，并更新或重启其 sidecar；不能沿用旧 preferred_segments 或 MC_STORE_REQUIRED_OFFLOAD_ENDPOINT。只在当前 provider 的 readiness 通过后启动 sidecar。

对本次开发环境，现成 unit 为 `ctgg-shared-mooncake-master`（北京）、`ctgg-mint-mooncake-oss`（两地）。升级时保存旧配置/运行目录；只切换 regional provider 和库无需重启 Master。回滚时同样恢复成对的 provider/库，保留 namespace 和 durable-delete journal；不删除 OSS 对象或 journal。

验收结束保留两地 Store 常驻。状态证据至少包括真实 PID、`/proc/PID/exe`、加载库、无凭据的配置摘要和双向冷读结果。若出现 metadata 超时、旧 RAM route 或后台反复重启，记录失败，不能以最终一次成功覆盖。

## Mint API 接入与读取探针

在已有 Mint API 配置中设置本地域 sidecar 地址（其余数据库、认证、调度配置保留）：

```toml
[sidecar]
url = "http://127.0.0.1:17420"
```

这是 Mint API 的配置，不是 sidecar TOML。跨 Pod 时替换为本地域私有服务地址。API 的模型 catalog 不会因为共享 Master 自动同步；通过当前候选提供的 artifact catalog export/import API 交接描述信息，再由目标 sidecar 读取对象。模型导入要求 base_model/LoRA metadata 匹配，不使用无模型 metadata 的普通 tar 冒充模型 checkpoint。下载 token 密钥在 API 侧安全配置，不复用 OSS 凭据。

读取探针使用独立的部署参数 TOML（不要把下面字段塞入 sidecar TOML）：

```toml
stub_path = "/path/to/generated/python/protobuf"
external_master_addr = "<MASTER_IP>:50481"
[ports]
sidecar = 17420
```

Python 环境需要 Python 3.11+、grpcio、protobuf，以及从同一 Mint 候选的 `proto/store_sidecar.proto` 生成的 `store_sidecar_pb2.py` / `store_sidecar_pb2_grpc.py`。先用 `rg --files` 确认仓库中的 proto 实际位置，再通过 `python -m grpc_tools.protoc -I <PROTO_DIR> --python_out=<STUB_DIR> --grpc_python_out=<STUB_DIR> <PROTO_FILE>` 生成（构建环境还需 grpcio-tools）。该探针只读取已有 object_key，不负责写入；写入/Publish 与 API 验收使用报告中的对应实测脚本及其配置格式。
