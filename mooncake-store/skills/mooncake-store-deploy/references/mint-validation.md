## 单 Master 验证配置

仓库提供 `configs/cross-region-single-master.example.toml`，将二进制路径、两地可路由地址、host identity、bucket 和 prefix 改为获准环境的值。两端必须使用相同 external_master_addr / external_metadata_url 与 OSS namespace；不要在 regional Store 中启动另一个 Master。

配置中的 prefer_local_provider 从本次 provider 日志提取动态 segment。它只是写入偏好，不能保证跨地域硬隔离。metadata timeout 的原生运行库配置与版本要求见启动参考；Mint 配置仍由 TOML 提供。

共享 Master 存活时，源 Store 退出后的路由可能在 client_ttl 窗口内保留旧 RAM 副本。冷读验收明确记录等待时间和首次失败；不把等待后的成功当成即时故障切换。当前候选的生产验收必须绑定最终提交与全部门禁，不引用开发机的旧成功替代。


配置示例 `configs/cross-region-single-master.example.toml` 不是已执行的证据。
复制后填写节点地址、OSS bucket、安装路径，并保持 `[runtime_pin]` 与实际构建配套；
控制器会在启动前核对 Master/provider/sidecar/库 SHA-256 并保存 runtime-binding.json。
在独立 Python 3.12 环境安装 `scripts/tools/mooncake-validation-requirements.txt`。
本仓库的 `make check-mooncake` 仅执行原生编译/单元测试；真实调用需通过
`make check-mooncake-live MOONCAKE_LIVE_CONFIG=/path/live.toml`，参照
`scripts/tools/mooncake-live-tests.example.toml` 为每个测试选独立的正确存储模式。
