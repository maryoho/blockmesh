# BlockMesh 第一版操作指南与验收计划

> 文档状态：跟随 `main` 更新。当前代码基线：2026-11-13。P4-1.3.5.3 Volume Catalog、P4-1.3.5.4 Unix socket RPC、P4-1.3.5.5 Pool 初始化与 Volume RW 激活/停用已有代码接线和 CI 单测。
>
> **重要：命令链路已接入不等于生产验收完成。当前仍缺跨节点 interrupted Pool bootstrap 的 OwnerRecord/恢复闭环、Volume Delete 的生产 Data Retire 后端、Pool Destroy、RO 动态 Reader 生命周期，以及真实 SAN/目标内核端到端验证。未通过这些验收前，不要在生产 LUN 上执行破坏性操作。**

## 1. 第一版交付目标（优先级最高）

第一版不再以“完成某个内部子模块”为验收终点，而以单节点、单 Pool、单 Volume 的完整可用闭环为终点：

1. 创建 Pool：自动识别设备容量/逻辑扇区，计算 Metadata 与 Data 布局，在同一个 LUN 上完成 Bootstrap、BMPOOL03、Bitmap、Checkpoint、Reserved Metadata allocator 初始化。
2. 创建 Volume：只分配持久化 Volume Descriptor、Arena 和生命周期元数据，不预分配 Volume 数据块。
3. 激活 RW：完成 Owner Lease、SCSI PR fencing、WAL recovery、DM Target 与 Metadata Service runtime 装配后，才开放 I/O。
4. 精简置备 I/O：未映射逻辑块读零；首次写入按 4 MiB 分配物理块，先清零并持久化，再通过 WAL 原子发布 Bitmap、Mapping、幂等记录和 Root；覆盖写执行 COW；Discard 删除映射并延迟回收。
5. 停用与删除：先关闭 I/O admission 并 drain，再释放租约；删除只允许在集群无活动 Owner/Reader 引用时执行，物理空间遵循 Reader generation 安全回收。

第一版先聚焦单节点 RW 数据路径和可恢复的 Pool/Volume 生命周期。多节点 RO 并发、自动 Owner takeover、跨节点故障切换和大规模性能优化继续保留为后续专项验收，但**不能为了赶进度绕过 Owner fencing、WAL 一致性或安全回收规则**。

## 2. 最终用户操作流程

安装完成并启动 `bm-metad` 后，普通运维只需要以下命令：

```bash
# 创建 Pool（会清空目标设备，必须二次确认）
bmctl pool create pool0 /dev/mapper/mpatha

# 查看 Pool
bmctl pool list
bmctl pool info pool0

# 创建 100 GiB 的精简置备 Volume（此时不分配 100 GiB 物理空间）
bmctl volume create pool0 vm-disk-01 100G

# 查看 Volume
bmctl volume list
bmctl volume info <volume-uuid>

# 激活为读写块设备
bmctl volume activate <volume-uuid> rw
ls -l /dev/mapper/

# 停用、删除
bmctl volume deactivate <volume-uuid>
bmctl volume delete <volume-uuid>

# 仅当 Pool 中没有 Volume 且所有节点均无活动引用时销毁
bmctl pool destroy pool0
```

约定：

- 容量后缀：K/M/G/T，按 KiB/MiB/GiB/TiB 解释。
- Pool 的 Metadata 容量由 BlockMesh 根据当前设备容量自动计算，不要求用户指定；设备扩容后由用户显式执行扩容命令，扩容量由后台自动计算。
- Metadata 与 Data 位于同一个 LUN。支持 multipath mapper、直连 iSCSI block device 和本地 block device；multipath 场景必须始终使用同一个 multipath mapper 作为设备入口。
- Volume create 只创建元数据，不做全量数据预分配。
- Volume 的物理分配粒度当前为 4 MiB。

## 3. 当前代码状态（不要混淆“内部实现”与“用户功能已可用”）

| 能力 | 当前状态 | 当前能否作为第一版用户功能 |
|---|---|---|
| BMPOOL03 Bootstrap / Extent Map / 动态几何 | 已实现并有测试 | 尚需由生命周期命令统一调用 |
| Pool Catalog 与按 LUN 只读发现 | 已实现 | 通过 `discovery_devices` 动态扫描 |
| `poolinit.Initialize` 自动初始化服务 | 已实现 | `bmctl pool create` 已接入 sanlock Pool Owner 锁、WWID 匹配的 SCSI PR 注册/Reservation 和初始化流程；跨节点中断恢复仍 fail-closed |
| Dynamic Volume Reserved allocator | 已实现 | 已供 Volume Catalog 使用 |
| Volume Catalog（Descriptor v5） | 已实现 create/list/find/delete 元数据 API、CreateToken 幂等和 cluster-safe WAL | `volume create/list/info` 已接入；生产 delete 的 Data Retire 后端仍未接入 |
| `bmctl` Cobra 生命周期命令树与参数解析 | 已实现 | 生命周期命令通过 Unix socket 请求 `bm-metad` |
| `bm-metad` 守护进程 | 已实现动态目录发现和运行时接线 | 仍保留兼容用静态 `volumes[]` 配置 |
| bm-metad Unix socket 生命周期 RPC | 已实现 | Pool/Volume list/info/create 与 RW activate/deactivate 已接入 |
| DM + Metadata Service 动态 Volume 激活/停用 | 已接入 RW 路径 | 已有 closed/fenced DM table、OwnerCoordinator、WAL recovery、gate admission、drain/remove；需真实 SAN 和目标内核验收 |
| Pool Destroy / Volume Delete / 动态 RO | 未完成生产后端 | 不可用 |
| 4 MiB 分配、清零、Mapping WAL 事务、COW、Discard retire | 底层数据路径已实现；稀疏 Mapping Vector 已增加 Unmapped 标记，Kernel 对未映射读区间执行 zero-fill | 仍需在目标内核和真实块设备上做端到端 I/O/重启验收 |
| 真实 SAN / multipath / SCSI PR 双节点故障测试 | 未在目标硬件完成 | 不可宣称生产验收通过 |

当前 `pool create/list/info`、`volume create/list/info`、`volume activate <uuid> rw` 和 `volume deactivate` 已有 RPC/backend 实现，但尚未完成真实 SAN 上的端到端验收。`pool destroy`、`volume delete` 和动态 `ro` 仍不可用。不要把 Go 单测或 CI 内核模块编译通过，等同于生产验收完成。

## 4. 当前可执行的开发验证

### 4.1 获取代码与测试

```bash
git clone git@github.com:maryoho/blockmesh.git
cd blockmesh
git checkout main
git pull --ff-only
git rev-parse HEAD

go test ./...
go test -race ./...
go vet ./...

mkdir -p bin
go build -o bin/bmctl ./cmd/bmctl
go build -o bin/bm-metad ./cmd/bm-metad
go build -o bin/bm-lock-helper ./cmd/bm-lock-helper
```

CI 的 Go 测试、Race 和 Linux 5.15 kernel build 结果以当前 commit 对应的 GitHub Actions 为准。CI 成功不代替目标发行版内核 ABI 和 SAN 设备验证。

### 4.2 Linux 5.15 内核模块

必须在目标主机上使用与运行内核完全匹配的 headers：

```bash
KDIR="/lib/modules/$(uname -r)/build"
test -e "$KDIR/Makefile"
make -C kernel/dm-blockmesh KDIR="$KDIR"

modinfo kernel/dm-blockmesh/dm-blockmesh.ko
modprobe dm_mod
insmod kernel/dm-blockmesh/dm-blockmesh.ko
ls -l /dev/blockmesh
dmsetup targets | grep -w blockmesh
dmesg | tail -n 100
```

若是厂商定制内核，必须在目标主机重新构建并加载验证，不能用通用 CI 编译结果替代。

### 4.3 当前低层 Pool 命令的边界

当前仍保留 `pool format`、`data-init`、`dynamic-format`、`dynamic-status`、`dynamic-recover` 等开发/恢复命令。它们使用 `/etc/blockmesh/pools/<id>.json` 和 sanlock/Owner helper 配置，属于内部调试与故障恢复接口，不是第一版简化运维流程。

特别注意：

- `pool format` 是破坏性操作，旧命令路径不等同于新的 `poolinit.Initialize` 自动初始化服务。
- `data-init` 是一次性低层步骤，需要指定 Data 容量；第一版 `pool create` 应自动完成该步骤，用户不应手工指定 Metadata/Data 比例。
- `dynamic-format` 当前会创建内部初始 Volume Descriptor；它不能替代未来的普通 `volume create`。
- 不要在业务 LUN 上试验这些低层命令，也不要通过手工写 Superblock、Bitmap、Extent Map 或手工打开 DM gate 绕过服务。

## 5. 精简置备的数据语义与必须通过的测试

### 5.1 预期语义

- 新建 Volume 的逻辑容量可以大于当前已分配物理容量；创建 Volume 不应清零整个逻辑容量。
- 未映射逻辑块读取必须返回全零，且不得分配物理块。
- 对未映射块首次写入：分配新的 4 MiB 物理块 → 清零整个新块 → Sync/flush → WAL 原子提交 Bitmap + Mapping + 幂等记录 + Root → 才允许原始 BIO 写入。
- 对已经映射的 4 MiB 块进行部分覆盖写：分配新块、复制旧块内容、修改新块，最后原子切换映射；旧块进入 Data Retire Queue，不能立即复用。
- Discard 删除逻辑映射，但只有在所有旧 Reader generation 安全前进或被 fencing 后，才能回收物理 Bitmap。
- WAL 结果不确定时必须先恢复 WAL 并重新打开 Volume，再使用稳定幂等键重试；不得重复分配。

### 5.2 第一版单机 RW 验收矩阵

以下是生命周期 RPC 和 DM 数据路径接通后的验收项；当前不能提前执行：

| 测试 | 操作 | 通过标准 |
|---|---|---|
| 空 Volume | 创建 100G Volume，查询分配量 | 已分配数据块为 0 |
| 未映射读取 | 对新 Volume 全零读取 | 数据全零，物理分配量不增加 |
| 首次写入 | 在一个 4K 偏移写入随机数据 | 对应 4MiB 块只分配一次；写后读校验一致 |
| 同块部分写 | 在同一 4MiB 块内多位置写入 | 原有数据保留，新数据正确 |
| 跨块写 | 在 4MiB 边界前后写入 | 两个块映射正确，无数据串扰 |
| 覆盖写/COW | 覆盖已映射块中的部分扇区 | 新映射原子发布，旧块进入 Retire |
| Discard | 对已写区域执行 discard | 读取语义为零；物理块在 Reader 安全后回收 |
| Flush/FUA | fio 使用 direct + fsync/flush | 返回成功的数据在重启后仍一致 |
| 服务重启 | 写入过程中停止/重启 bm-metad | 无重复分配、无已确认数据丢失，未决请求可恢复 |
| 内核模块重载 | 仅在测试环境、Volume 停用后执行 | 无残留队列引用和错误映射 |

推荐 fio 基础校验（仅在完整 I/O 闭环明确 Ready 后执行）：

```bash
fio --name=write-verify --filename=/dev/mapper/<volume-device> \
  --ioengine=libaio --direct=1 --rw=randwrite --bs=4k \
  --iodepth=16 --size=1G --verify=crc32c --do_verify=1 \
  --verify_fatal=1 --group_reporting

fio --name=read-verify --filename=/dev/mapper/<volume-device> \
  --ioengine=libaio --direct=1 --rw=randread --bs=4k \
  --iodepth=16 --size=1G --verify=crc32c --do_verify=1 \
  --verify_fatal=1 --group_reporting
```

不要对尚未完成 Owner/Reader 授权和 Metadata Service 消费链路的 DM 设备运行 fio、dd、mkfs 或挂载文件系统。

## 6. 环境与数据安全要求

测试前记录：

```bash
uname -a
uname -r
go version
dmsetup version
lsblk -o NAME,KNAME,TYPE,SIZE,RO,WWN,FSTYPE,MOUNTPOINTS
multipath -ll
```

选择独立、无数据、未挂载、未被 LVM/文件系统使用的测试 LUN。multipath 环境只使用 multipath mapper，不得在 mapper 与底层 path 之间混用。

禁止：

- 对有业务数据的 LUN 执行格式化或初始化。
- 在多个节点同时初始化同一个 Pool。
- 使用无操作的 Owner helper、伪造 OwnerEpoch 或本地锁替代分布式锁。
- 手动发送 `owner_enable` 绕过 OwnerCoordinator / SCSI PR fencing。
- 在 DM 尚有 inflight BIO 时释放 Owner/Reader lease。
- 在未完成真实故障注入前宣称双节点 SAN failover 已可用。

## 7. 第一版开发顺序（加速交付）

后续开发按可运行主链路优先，避免继续扩展与第一版无关的功能：

1. **P4-1.3.5.3 Volume Catalog：** Descriptor v5、持久化 Catalog API、CreateToken 幂等重试、AG 绑定容量校验已实现；删除 API 现在强制先执行 Data Retire 回收，并在同一个集群 fencing guard 下完成 Descriptor 释放。
2. **P4-1.3.5.4 Lifecycle RPC：** Unix socket 服务端、动态 Pool/Volume 发现和 CLI 接线已实现。
3. **P4-1.3.5.5 Pool/Volume 生命周期：** Pool create 初始化、Volume create/list/info、RW activate 已接入；Pool destroy、Volume delete 和 RO 仍待完成。
4. **P4-1.3.5.6 Deactivate/Recovery：** RW deactivate 的 drain、DM remove、lease release 顺序和 daemon 启动时 stale DM 清理已接入；跨节点 interrupted Pool bootstrap 的持久 OwnerRecord/恢复仍待实现。
5. **P4-1.3.5.7 第一版端到端验收：** 需在真实测试 LUN 执行 pool create → volume create → activate → fio（含稀疏零读、首次写、COW、discard）→ deactivate → daemon restart → activate。Volume delete 在生产 Retire 后端接入前不纳入可执行步骤。
6. **第一版发布门槛：** Go test/race/vet、Linux 5.15 模块构建、真实目标内核加载、单机真实块设备读写/重启恢复全部通过。双节点 SAN、RO 并发和故障切换作为独立验收，不以跳过安全机制换取进度。

## 8. 故障信息收集

每次测试至少保存：

```bash
date -Is
git rev-parse HEAD
uname -a
dmsetup table
dmsetup status
ls -l /dev/blockmesh
lsblk -o NAME,KNAME,TYPE,SIZE,RO,WWN,FSTYPE,MOUNTPOINTS
multipath -ll
dmesg --ctime | tail -n 300
journalctl -u bm-metad --since "-30 min" --no-pager
```

同时记录 Pool UUID、Volume UUID、设备 WWID、OwnerEpoch、ViewGeneration、AG/WAL 状态、fio job 文件和退出码。

## daemon startup and lifecycle readiness

`bm-metad` first acquires its singleton lock and opens/reconciles the
kernel Metadata Service device. It then fences and removes stale local
BlockMesh DM tables left by a previous daemon instance. Only after this
recovery completes does it start the request-consumer loop and expose the
lifecycle Unix socket. A client should treat a missing socket or a rejected
connection during startup as "daemon not ready" and retry; the daemon does
not intentionally accept Pool/Volume lifecycle actions before recovery.
