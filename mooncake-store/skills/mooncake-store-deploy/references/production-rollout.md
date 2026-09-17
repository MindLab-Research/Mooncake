# 同事执行的生产接入与回滚

## 发布输入

采用 Mooncake #1 与 Mint #1314 审核后的配套源码，固定实际合并 commit 和构建镜像 digest。如果合并含运行代码变化，重新执行受影响的验收；仅文档变更可沿用已绑定运行包。确认发布单给出了：唯一 Master 地址、两地可双向回连的 provider 地址、稳定且唯一的 host identity、OSS endpoint/bucket/prefix、持久 journal 卷、节点 state/cache 卷、Mint Pod 放置方式、旧 S3 配置、catalog 签名/信任根、API 管理凭据与独立下载签名密钥。

发布前从当前 PR 获取最终代码状态：Mint #1314 与 Mooncake #1 分别核对 head、CI、自动审查和维护者批准。一个仓库通过不能替代另一个仓库；没有 check run 不等于检查通过。复制整个 `mooncake-store/skills/mooncake-store-deploy/` 目录供同事使用，保留 references 和 scripts，并记录它来自哪个 Mooncake commit。本机安装副本只用于辅助，仓库审核版本为准。

## 构建和装配

使用项目 Linux 构建环境和固定依赖。Mooncake 配置 `WITH_STORE=ON`、`WITH_STORE_C_SHARED=ON`、`BUILD_SHARED_LIBS=OFF`，通过 `CMAKE_PREFIX_PATH`/`AWSSDK_ROOT_DIR` 提供 AWS SDK，确认 CMake 输出 `AWS SDK found`（否则 S3 功能会被禁用；没有 `WITH_S3` 开关），产出 Master、mooncake_client 和 libmooncake_store；AWS SDK/libasio 等依赖与发布包一起固定。Mint sidecar 必须开启 `mooncake` feature：

```bash
MOONCAKE_STORE_LIB_DIR=<MOONCAKE_SHARED_LIB_DIR> \
  cargo build --release -p mint-store-sidecar --features mooncake
```

Rust 工具链使用 Mint 仓库要求。API 用集群既有生产构建流程，不能把验收时开启的 `mint-api/dev` 当作生产配置。装配后对 Master/provider/sidecar/API 和所有加载库记录 SHA-256，执行 ldd 并验证 12 个 FFI 符号及一次实际调用。目录名或单独的 Git SHA 不是运行版本证明。

## Mint 专用容器镜像

Mint 默认 `mint-store-sidecar` target 和 composer 镜像内附带的 sidecar 没有启用 `mooncake` feature；仅把 TOML 改为 Mooncake 不会获得原生能力。选择 `deploy/docker/Dockerfile` 的 **`mint-store-sidecar-mooncake`** target。从审核后的 Mint checkout 根目录构建，使用 Docker 27+ / BuildKit，构建平台为 Linux x86-64：

```bash
# Replace placeholders with the reviewed release inputs.
python3 scripts/tools/verify_mooncake_runtime.py \
  --runtime <NATIVE_BUNDLE_DIR> --manifest-sha256 <REVIEWED_MANIFEST_SHA256>
docker buildx build --platform linux/amd64 \
  -f deploy/docker/Dockerfile --target mint-store-sidecar-mooncake \
  --build-arg RUST_VERSION=<CHANNEL_FROM_rust-toolchain.stable.toml> \
  --build-context mooncake-runtime=<NATIVE_BUNDLE_DIR> \
  --build-arg MOONCAKE_MANIFEST_SHA256=<REVIEWED_MANIFEST_SHA256> \
  -t <SIDECAR_IMAGE:RELEASE_TAG> --load .
```

`RUST_VERSION` 必须是本次 checkout 的 `rust-toolchain.stable.toml` 中 channel 精确值；不要使用浮动 stable。镜像推送按集群现有发布流程执行，部署引用最终 registry digest。

原生包须包含 `SHA256SUMS`、`source.json`、`include/store_c.h`、`lib/libmooncake_store.so` 和所有非系统运行库。`source.json` 至少记录 `schema_version: 1`、完整 `mooncake_commit`、`durable_delete_protocol_version: 3`。`SHA256SUMS` 逐项覆盖除自身之外的所有文件，格式为 SHA-256、两个空格、相对路径；包内不能含符号链接，打包时将所需库解引用成普通文件。manifest digest 必须来自已审核的发布记录，不能临时对未知包计算一个 digest 就视为可信。verifier 检查包完整性和声明，镜像构建检查动态链接；真实 Put/Get 才验证服务能力。

专用镜像默认配置路径为 `/etc/mint/store-sidecar.toml`。现有 Compose 显式传入 `/mint/prd/config/mint-store-sidecar.toml`，以实际 command 为准并挂载匹配文件。保留镜像内适配器转换 venv、Mint 代码及共享 staging 卷，不用只含二进制的空镜像替换。

## 对接现有 Mint 编排

当前 `deploy/docker/compose.yaml` 使用 `network_mode: host`，sidecar 默认探测 `127.0.0.1:7100`；本 skill 的 `17420` 是隔离开发示例。沿用生产已有端口，或同时修改 sidecar `[listen].addr`、API `[sidecar].url`、composer 配置中的 sidecar URL 和健康探针。不要把业务全部切到跨地域 sidecar。

Compose 的 `store-sidecar.image` 必须覆盖为上述 Mooncake 专用镜像的 digest。API/composer 与 sidecar 使用同一宿主网络；Kubernetes 则让调用方与 sidecar 共享 Pod 网络。API 和 composer 分开部署时，每个调用方都必须具备本地可达的 sidecar 及所需共享 staging 路径。不要照搬 hostNetwork 给不受信任工作负载：loopback sidecar 无鉴权，网络命名空间是访问边界。

现有 Compose TCP healthcheck 只证明端口监听，不能发现原生 transport 已失效。部署方需把已知正控对象的实际 GetBlob 校验接入业务 readiness，并配置失败撤流与重建；本 skill 的 `read_probe.py` 默认 RPC 期限 240 秒，适合验收，不能直接作为每 5 秒运行的高频探针。探针所需 Python/grpc/stub 也不假定已在生产镜像内提供。恢复演练必须验证 provider 重启后重新发现 endpoint/segment、更新 sidecar，以及失效 transport 重建后正控读取恢复。

## 部署顺序

1. 挂载持久 journal，单独启动一个 Master。现有共享 Master 可复用，不为每个地域再创建一个。按启动参考配置内网 RPC/metadata 地址。
2. 两地各启动 provider，使用相同 OSS namespace、不同 host identity 和本地 state 目录；验证 Master 注册成功。保存当前进程日志和实际 offload endpoint/segment，生成本地 sidecar 配置。
3. 在每个调用方 Mint Pod 内放置 sidecar，使用 loopback RPC。只给 provider 挂载 OSS 凭据；需要历史 S3 的 sidecar 另外挂载其凭据文件。Pod 内 tar staging 路径按现有 Mint 部署共享可访问的卷，不能只配网络而丢失文件可见性。
4. 启动 sidecar 后执行真实小对象 Put/Get，核对 hash，再开放该 Mint 实例流量。sidecar 的 17420 是 gRPC，不存在可直接照搬的 HTTP `/health` 探针。原生 C ABI health 与其可选 HTTP server 也不是自动暴露在 sidecar gRPC 端口；readiness 应使用已知正控对象的真实 GetBlob 或已有平台的业务探针。
5. 使用独立验收 namespace 完成双向 1/4/96 MiB、源下线冷读、删除保护、token 与历史 S3 门禁后，再接入业务 namespace。冷读/删除故障注入在隔离验证实例执行，不能停止生产服务或删除业务 checkpoint 来验收。

## 同节点多实例与旧 S3 endpoint

同一可路由 IP 下部署多个 sidecar 时，为每个原生客户端配置不同的 `local_hostname`（例如 `<NODE_IP>:18638`、`<NODE_IP>:18639`），并记录本次实际 RPC/数据端口。不能全部使用 `<NODE_IP>:0`：`MOONCAKE_HOST_ID` 的不同值不足以区分 HTTP metadata 中的 `rpc_meta/<local_hostname>`，可能导致 `Duplicate rpc_meta key not allowed`。主示例要求显式填写 `UNIQUE_SIDECAR_ID_PORT`，不为多实例自动分配身份。滚动重建需先确认旧实例退出、旧 metadata 注册完成清理，再启新实例；不能让两代进程同时占用同一个身份。

旧 S3 回退 `[store] endpoint_internal` 还有 sidecar 启动前的裸 endpoint DNS/TCP 检查。不能直接把 provider 的 `https://oss-accelerate.aliyuncs.com` 填过去：该裸域名在部分环境无地址，而 provider 的 bucket 虚拟域名仍可用。本轮回退使用可解析的 `https://oss-cn-beijing.aliyuncs.com`；生产按实际旧服务 endpoint 配置，并在目标容器内检查 DNS/TCP、真实历史对象读取及 presign 下载。该限制不改变 Mooncake provider 使用 OSS accelerate 的配置。

## 常驻、重启与升级

由集群已有 supervisor/Kubernetes 管理进程。Master 和 provider/sidecar 分别管理；不能通过一个区域 Pod 的生命周期误停 Master。provider 重启后 endpoint/segment 可能变化，部署入口必须重新发现并更新依赖它的 sidecar；普通静态 ConfigMap 加 RestartPolicy 不能自动完成这一步。使用启动参考的注册日志/descriptor 获取本次值，确认新配置生效后重新开放流量。Docker 自动重启期间不能仅凭 `RestartCount` 增加就读取端口；须同时确认新 PID、变化后的 `StartedAt` 和运行状态，再只读取该次启动后的日志或当前 descriptor。跨代日志中的旧端口会导致 `rc=-702`；须以实际 GetBlob 正控成功作为重新开放流量条件。

升级按地域逐个撤流、等待在途调用结束、切换配套库/provider/sidecar、重新发现路由、验证正控读写再恢复流量。TCP 成功停止旧执行器后会在原端口重建，可继续新请求；若重建失败、transport 不可用，由业务探针撤流并重建进程。独立托管的 provider/sidecar 显式启用 `MC_STORE_TRANSFER_FATAL_TIMEOUT=1` 与 `MC_STORE_TRANSFER_ABORT_GRACE_MS=5000`，使无法停止的原生 batch 在宽限期后退出 124；配置失败重启、退避和告警，并按前述流程刷新 provider 路由。不要向内嵌训练主进程默认注入退出策略。按对象大小/并发调优缓冲池与期限，开发样本的时延不是生产 SLA。

## 回滚

保留上一套配套镜像、配置和 manifest。失败地域先撤流，停止其 sidecar/provider，恢复上一套兼容运行包与配置，再发现 endpoint/segment 并启动；以已有对象读取和新写验证后恢复流量。保留 OSS namespace、对象和 Master durable-delete journal；不要回滚到不理解当前 journal/删除语义的旧 Master。控制面若需升级，单独安排维护窗口，不能同时启动两个 authoritative Master。

S3 写入回退按启动参考执行，并明确已存在的 `mint://` 仍需要原 Mooncake 能力。出现持续错误时保持该地域撤流并保存日志；不要自动删除数据、清 journal 或无限重启。

## 交付记录

交付实际填好但无密钥的配置、镜像 digest/源码和库 hash、服务地址、当前 segment/endpoint、正控 URI/hash、双向门禁日志、监控与回滚命令。基础部署参数可移植；具体生产集群的调度资源、卷和网络由接入方填写，本文不把开发机 systemd unit 或 SSH overlay 当作生产部署清单。
