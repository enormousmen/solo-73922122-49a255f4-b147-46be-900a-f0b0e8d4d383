# TCP 故障代理 / 采样包上传卡顿复现平台

设备接入测试场景：复现 32 MiB 采样包上传卡顿，并判断超时发生在**上传阶段**还是
**回执阶段**。本项目用 **FastAPI + asyncio 单进程**实现一个 TCP 故障代理，固定转发到
本地接收端，可按连接、按方向在累计转发 N 字节后**暂停**，并支持查询、恢复、强断。

## 架构

```
 sender (FastAPI)  ──TCP:9000──▶  proxy (asyncio 转发 + FastAPI 控制面:8080)  ──▶  receiver (asyncio:9100)
   /docs 发起上传                    /docs 预设暂停/查询/恢复/强断                    收 EOF 才出回执(字节数+SHA-256)
   32MiB 后 write_eof                每方向独立 pump 任务                            回执 JSON 后反向半关闭
   读回执直到 EOF                    回执查询 HTTP:9101
```

三个组件各一个容器（`docker compose up`），也可在本机直接用脚本起三个进程。

| 组件 | 端口 | 说明 |
| --- | --- | --- |
| receiver | TCP 9100 / HTTP 9101 | 收满到 EOF 才计算并返回 `{bytes, sha256, ...}` 回执；HTTP 查最近回执；`POST /disconnect` 演示中途断连 |
| proxy | TCP 9000 / HTTP 8080 | 故障转发入口；8080 为控制 API 与 `/docs` |
| sender | HTTP 8081 | `/docs` 发起上传、查看 run 与回执 |

## 快速开始（Docker Compose）

```bash
docker compose up --build
# 控制文档
open http://127.0.0.1:8080/docs    # 故障代理：预设/连接/恢复/强断
open http://127.0.0.1:8081/docs    # 发送器：发起上传、查看回执
# receiver 回执
curl http://127.0.0.1:9101/receipts
```

无 Docker 时（本机已有 Python 3.11）：

```bash
scripts/run-local.sh        # 建 venv、装依赖、起三个进程（日志在 /tmp/fp-logs）
scripts/stop-local.sh       # 停止
python -I tests/smoke_test.py   # 端到端冒烟（52 项检查）
```

## 复现卡顿并定位超时阶段（标准操作流）

1. 在代理 `/docs` 调 `PUT /preset`，设 `c2r_pause_after=1048576`（上传 1 MiB 后暂停）。
2. 在发送器 `/docs` 调 `POST /send`（默认 32 MiB），拿到 `run_id`。
3. `GET /connections` 看到连接 `c2r` 方向 `paused=true`、`forwarded_bytes=1048576`、
   `fin` 未送达；发送器 run 停在 `connected`（drain 被 TCP 背压阻塞）——**说明超时在上传阶段**。
4. `POST /connections/{id}/resume`（`direction=c2r`），积压与 FIN 继续，接收端出回执，
   run 变 `done`，`hash_match=true`。
5. 要定位**回执阶段**超时：`PUT /preset` 设 `r2c_pause_after=16`，此时上传正常完成
   （`c2r.half_closed_delivered=true`），run 停在 `waiting-receipt`，恢复 `r2c` 后才收到回执。
6. `POST /connections/{id}/force-close` 可随时强断，立即打断暂停与阻塞写。
7. 演示**接收端中途断连**：在回执方向暂停（或上传进行中）时 `curl -X POST
   http://127.0.0.1:9101/disconnect`。代理会立即结束另一方向的暂停/阻塞，关闭两端，
   连接从活动列表移入已关闭记录并带 `error`（如 `c2r pump error: ConnectionResetError`），
   发送器 run 变 `failed`；控制 API 与后续新连接不受影响。

## 控制 API（proxy:8080）

| 方法/路径 | 作用 |
| --- | --- |
| `GET /health` | 健康状态、转发目标、每方向用户态缓冲上限（256 KiB） |
| `GET /preset` / `PUT /preset` | 查看/预设**新连接**每方向暂停阈值（`null` 取消） |
| `GET /connections` | 全部连接（活动在前、已关闭在后），含每方向转发字节、积压、暂停/FIN 状态 |
| `GET /connections/{id}` | 单连接详情 |
| `POST /connections/{id}/pause` | 手动暂停某方向（阈值之外的测试手段） |
| `POST /connections/{id}/resume` | 恢复指定方向；不传 direction 则恢复全部暂停方向 |
| `POST /connections/{id}/force-close` | 强断：abort 两端、取消 pump，任务退出且两端关闭后才返回 |

连接状态字段（每方向）：`forwarded_bytes`（已转发）、`pending_bytes`（暂停时暂扣的积压）、
`paused`、`fin_pending`、`half_closed_delivered`、`eof_seen`、`peer_closed` 等。

## 关键正确性设计与保证

- **恰好停在 N 字节（跨读取块）**：每次 `read(64KiB)` 后按阈值切分，`head` 转发、
  `tail` 作为该方向唯一的用户态积压（`pending`）；暂停期间不再读 socket。
- **积压与 FIN 不越过暂停**：暂停时不做任何读和 FIN 投递；读到 EOF 也只标记
  `eof_seen/fin_pending`，等恢复后先把积压发完再 `write_eof()`。
- **恢复后不丢、不重、不乱序**：恢复后先 flush `pending` 再继续读，字节顺序唯一；
  测试以双端 SHA-256 与字节数比对验证。
- **背压、无整包缓存、不落盘**：暂停即停读 → 内核接收缓冲填满 → TCP 流控反压到发送端，
  发送端阻塞在 `drain()`；每方向用户态数据 = StreamReader 缓冲（上限 256 KiB）+
  至多一个切分尾块，全程不写磁盘。
- **FIN 半关闭语义**：FIN 等本方向积压清空后才向对端半关闭；反向 pump 是独立任务，
  回执方向不受影响，可一直读到 EOF。
- **强断语义**：`transport.abort()` 让阻塞的 `drain()`/`read()` 立即抛错，两个 pump
  被取消并退出，supervisor 关闭两端 socket、连接从活动列表移除后 API 才报完成；
  其他连接与控制 API 不受影响（冒烟测试覆盖）。
- **双向故障收尾**：任一方向因读/写故障退出时，supervisor 立即唤醒另一方向的暂停
  等待、`abort` 两端 socket 打断另一方向可能阻塞的读/写并取消其任务，等两个 pump
  都退出后关闭两端，把连接移入已关闭记录并写入 `error`（含故障方向与异常类型）。
  正常上传半关闭（c2r FIN）**不触发**收尾，回执方向继续读到 EOF；手动强断同样等待
  两个任务退出后才返回。收尾不占用其他连接，控制 API 始终可用。
  暂停方向按设计不读 socket（以保证背压与不额外缓冲），因此等待恢复期间会周期性
  用 `SO_ERROR` 做只读存活探测（不读数据、不改变背压）：对端 RST 时暂停方向也能
  发现并触发收尾。另有一条兜底规则：若回执方向（r2c）先到 EOF 而上传方向尚未完成，
  视为目标提前关闭的故障并结束等待，避免“上传方向暂停 + 接收端断连”永久滞留。
  若接收端已**正常**完成并关闭（无 RST）、而回执方向被手动暂停，则回执字节保留在
  代理侧，`resume` 后完整送达（这是暂停语义，不是故障）。

## 测试

`tests/smoke_test.py`（纯标准库）覆盖：基线哈希一致、自动暂停精确停 N、积压/FIN 暂扣、
奇数 N 跨块、手动暂停/恢复、回执方向暂停、强断与隔离性、背压下用户态积压 ≤ 256 KiB、
32 MiB 级载荷恢复后哈希一致，以及**接收端中途断连**的两种对称情形：
（a）回执方向暂停时上传写入报错 → 另一方向暂停被解除、两端关闭、连接带 error 退休、
发送器快速失败、控制 API 与新连接可用；（b）上传方向暂停时接收端断连 → 回执方向先到
EOF、代理判定上传未完成的提前关闭为故障并同样完成收尾；另有客户端在暂停期间 RST，
由只读 `SO_ERROR` 存活探测触发同样的收尾。
可用环境变量 `PROXY_API/SENDER_API/RECEIVER_API/PROXY_TCP` 指向 Compose 暴露的端口直接
对容器跑。

```bash
python -I tests/smoke_test.py
# 52/52 checks passed
```

## 实测结果（本环境，Python 3.11.2 / loopback）

环境无 Docker 守护进程，故用等价的三进程本地部署验证（Compose 文件已就绪，逻辑与
容器部署一致，仅主机名改为 127.0.0.1）。连续多轮结果：

- 冒烟套件 **52/52 通过**（新增接收端断连两种对称场景与暂停期间 RST 探测共 15 项），
  并连跑多轮无回归。
- 精确停点：阈值 N=300000、100007（奇数，跨 64KiB 读块）、1048576、4194304、0
  均实测 `forwarded_bytes == N`，暂停期间计数冻结、FIN 不送达。
- 背压：16 MiB 载荷在 N=300000 暂停后，发送器实际卡在 `drain()`（仅发出约 3.75 MiB），
  代理 `pending_bytes` 峰值约 62 KiB，全程 **≤ 256 KiB**，接收端无回执。
- 恢复完整性：恢复后字节数与双端 SHA-256 全部一致（含 32 MiB 完整采样包，
  实测 33554432 字节、约 3–7 秒、`hash_match=true`、接收端 `eof_seen=true`）。
- 回执阶段：`r2c_pause_after=16` 时上传照常半关闭完成，run 停在 `waiting-receipt`，
  恢复 r2c 后才收到回执——可据此区分超时在上传还是回执阶段。
- 强断：暂停中的连接 `force-close` 约 20–40 ms 返回，发送端收到
  `ConnectionResetError`，连接从活动列表移除，控制 API 与后续新连接不受影响。
- 接收端断连：回执方向暂停时 `POST /disconnect`，上传方向写入立即
  `ConnectionResetError`，代理约 50 ms 内解除另一方向暂停、关闭两端并以
  `error="c2r pump error: ConnectionResetError: ..."` 退休连接；发送器 run 变
  `failed`，新连接照常完成上传且哈希一致。上传方向暂停时断连同样在数毫秒内以
  `error="r2c peer closed (target EOF) before upload finished"` 收尾；客户端在暂停
  期间发 RST 也在数毫秒内经只读 `SO_ERROR` 探测收尾。
- N=0 边界：立即暂停、暂扣全部字节与 FIN，恢复后 8 字节测试载荷与回执完整。

> 说明：发送器对回执读取带 `SENDER_RECEIPT_TIMEOUT`（默认 120s）超时保护，
> 回执阶段长期无 EOF 时 run 置 `failed` 并注明 receipt-phase stall，而非永久挂起。

## 目录

```
proxy/app.py       故障代理（asyncio 双向 pump + FastAPI 控制面）
sender/app.py      发送器（确定性 32MiB 采样流、半关闭、读回执，FastAPI /docs）
receiver/app.py    接收端（读到 EOF 算字节数/SHA-256，回执 + 极简 HTTP 查询）
tests/smoke_test.py 端到端冒烟
scripts/           本机启停脚本
Dockerfile docker-compose.yml requirements.txt
```
