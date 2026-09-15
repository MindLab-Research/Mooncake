# 同事执行的生产接入与回滚

## 发布输入

采用 Mooncake #1 与 Mint #1314 审核后的配套源码，固定实际合并 commit 和构建镜像 digest。如果合并含运行代码变化，重新执行受影响的验收；仅文档变更可沿用已绑定运行包。确认发布单给出了：唯一 Master 地址、两地可双向回连的 provider 地址、稳定且唯一的 host identity、OSS endpoint/bucket/prefix、持久 journal 卷、节点 state/cache 卷、Mint Pod 放置方式、旧 S3 配置与 API 管理签名密钥。

## 构建和装配

使用项目 Linux 构建环境和固定依赖。Mooncake 配置 `WITH_STORE=ON`、`WITH_STORE_C_SHARED=ON`、`BUILD_SHARED_LIBS=OFF`，通过 `CMAKE_PREFIX_PATH`/`AWSSDK_ROOT_DIR` 提供 AWS SDK，确认 CMake 输出 `AWS SDK found`（否则 S3 功能会被禁用；没有 `WITH_S3` 开关），产出 Master、mooncake_client 和 libmooncake_store；AWS SDK/libasio 等依赖与发布包一起固定。Mint sidecar 必须开启 `mooncake` feature：

```bash
MOONCAKE_STORE_LIB_DIR=<MOONCAKE_SHARED_LIB_DIR> \
  cargo build --release -p mint-store-sidecar --features mooncake
```

Rust 工具链使用 Mint 仓库要求。API 用集群既有生产构建流程，不能把验收时开启的 `mint-api/dev` 当作生产配置。装配后对 Master/provider/sidecar/API 和所有加载库记录 SHA-256，执行 ldd 并验证 12 个 FFI 符号及一次实际调用。目录名或单独的 Git SHA 不是运行版本证明。

## 部署顺序

1. 挂载持久 journal，单独启动一个 Master。现有共享 Master 可复用，不为每个地域再创建一个。按启动参考配置内网 RPC/metadata 地址。
2. 两地各启动 provider，使用相同 OSS namespace、不同 host identity 和本地 state 目录；验证 Master 注册成功。保存当前进程日志和实际 offload endpoint/segment，生成本地 sidecar 配置。
3. 在每个调用方 Mint Pod 内放置 sidecar，使用 loopback RPC。只给 provider 挂载 OSS 凭据；需要历史 S3 的 sidecar 另外挂载其凭据文件。Pod 内 tar staging 路径按现有 Mint 部署共享可访问的卷，不能只配网络而丢失文件可见性。
4. 启动 sidecar 后执行真实小对象 Put/Get，核对 hash，再开放该 Mint 实例流量。sidecar 的 17420 是 gRPC，不存在可直接照搬的 HTTP `/health` 探针。原生 C ABI health 与其可选 HTTP server 也不是自动暴露在 sidecar gRPC 端口；readiness 应使用已知正控对象的真实 GetBlob 或已有平台的业务探针。
5. 使用独立验收 namespace 完成双向 1/4/96 MiB、源下线冷读、删除保护、token 与历史 S3 门禁后，再接入业务 namespace。冷读/删除故障注入在隔离验证实例执行，不能停止生产服务或删除业务 checkpoint 来验收。

## 常驻、重启与升级

由集群已有 supervisor/Kubernetes 管理进程。Master 和 provider/sidecar 分别管理；不能通过一个区域 Pod 的生命周期误停 Master。provider 重启后 endpoint/segment 可能变化，部署入口必须重新发现并更新依赖它的 sidecar；普通静态 ConfigMap 加 RestartPolicy 不能自动完成这一步。使用启动参考的注册日志/descriptor 获取本次值，确认新配置生效后重新开放流量。

升级按地域逐个撤流、等待在途调用结束、切换配套库/provider/sidecar、重新发现路由、验证正控读写再恢复流量。TCP transport 被安全终止后即使进程存活也不可继续复用；由业务探针识别并重建对应进程。按对象大小/并发调优缓冲池与期限，开发样本的时延不是生产 SLA。

## 回滚

保留上一套配套镜像、配置和 manifest。失败地域先撤流，停止其 sidecar/provider，恢复上一套兼容运行包与配置，再发现 endpoint/segment 并启动；以已有对象读取和新写验证后恢复流量。保留 OSS namespace、对象和 Master durable-delete journal；不要回滚到不理解当前 journal/删除语义的旧 Master。控制面若需升级，单独安排维护窗口，不能同时启动两个 authoritative Master。

S3 写入回退按启动参考执行，并明确已存在的 `mint://` 仍需要原 Mooncake 能力。出现持续错误时保持该地域撤流并保存日志；不要自动删除数据、清 journal 或无限重启。

## 交付记录

交付实际填好但无密钥的配置、镜像 digest/源码和库 hash、服务地址、当前 segment/endpoint、正控 URI/hash、双向门禁日志、监控与回滚命令。基础部署参数可移植；具体生产集群的调度资源、卷和网络由接入方填写，本文不把开发机 systemd unit 或 SSH overlay 当作生产部署清单。
