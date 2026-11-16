# 地区直播按需中继系统

同一地区的多个观众共享**一条**上游媒体连接。中继只在首次观看时向源站拉流，
收到真实 libx264 编码流后再分发给本地区所有接收端；最后一个观众离开后，
按明确的保留期限回收上游，而不是每次页面刷新都新建永久连接。

仅依赖 Python 3 标准库 + `ffmpeg`（已自带静态二进制于 `bin/`，含 libx264）。

## 一、界面怎样启动

```bash
cd relay_live
./start_all.sh                 # 启动源站(每地区一个, 带监督重启) + 中继内核 + 控制页
```

启动脚本会等待所有端口就绪后打印：

```
control page: http://127.0.0.1:8080/
```

浏览器打开 **http://127.0.0.1:8080/** 即为流媒体控制页。停止整套环境：

```bash
./stop_all.sh
```

可选环境变量：`RELAY_PORT`（默认 8080）、`RETAIN_SECONDS`（默认 10，空闲保留期）。

控制页由**地区中继内核驱动**，不是孤立脚本：
- “增加订阅端 / 增加慢速订阅端” → `POST /api/subscribers`，内核按需建立上游并派生 viewer 进程；
- 每路的“停止” → `POST /api/subscribers/stop`，内核终止该 viewer 并在最后一人离开时进入保留期；
- “模拟源站短断” → 源站监督端口 `/control/die`，进程退出后由监督者自动重启，
  内核经历 `FAILED → PLAYING`，失败信息回显到页面的地区卡片；
- 结果区每秒轮询 `GET /api/topology`，展示各 viewer 的真实解码画面（JPEG 快照）、
  首帧时间、队列水位、丢帧，以及中继状态机与上游连接统计。

## 二、进程拓扑（单机多进程）

```
source.py --supervise (每地区一个, 端口 9100/9200)
   └─ ffmpeg libx264 编码器, 按 15fps 时钟产出 Annex-B H.264
relay.py (一个进程, 控制端口 8080)
   └─ RegionKernel × 地区数, 状态机 IDLE/CONNECTING/PLAYING/FAILED/RETAINING
        └─ worker.py (每个观众一个独立进程)
             └─ ffmpeg 实时解码 → JPEG 快照 → 心跳回报
```

## 三、关键机制与需求对应

| 需求 | 实现位置 |
| --- | --- |
| 首次观看触发拉流，共享一条上游 | `relay.py` `RegionKernel.join()` / `_connect()` |
| 并发加入合并启动，禁止重复实例 | 单飞 `_connect_task`，CONNECTING 时所有 join await 同一任务 |
| 连接中/可播/失败分别响应 | 状态机五态；FAILED 的 join 收到明确 JSON 错误，退避重试 |
| 最后一人离开按保留期回收 | `RETAINING` + `_retain_wait()`，保留期内再加入直接复用 |
| 新观众从可解码边界起播 | `_fanout()` 先补发 SPS/PPS，再从下一个 IDR 关键帧开始 |
| 慢接收端独立有界队列 | 每 viewer 独立 `asyncio.Queue(120)` + ACK 信用流控，满则仅丢本端旧帧 |
| 网页拓扑来自进程状态/流统计 | `/api/topology` 直接由内核与 viewer 心跳聚合 |
| 空闲回收后再次观看重建链路 | 回收回到 IDLE，再次 join 重新走完整连接与关键帧引导 |

### 慢接收端为什么不会拖累别人
viewer 每消费一个访问单元回一个 8 字节 ACK，中继按每 viewer 的信用窗口
（=队列容量）放行发送。慢速 viewer 欠 ACK，其发送协程阻塞、有界队列填满，
`_fanout` 只对该 viewer 丢帧；其他 viewer 的队列与发送完全独立，保持满速、零丢帧。

## 四、自动化验证

```bash
./start_all.sh
python3 -I verify.py
```

`verify.py` 通过公开控制 API 驱动真实多进程，断言六项（实测全部通过）：

1. 并发加入合并为**一条**上游（4 并发 → 上游 1、源站连接 1）
2. 各端首帧到达且始于关键帧边界（首帧约 0.9s）
3. 播放中再加观众不新增上游（页面刷新不新建永久连接）
4. 慢速端队列填满后仅本端丢帧，其他端零丢帧且持续解码
5. 源站短断后 `FAILED → PLAYING` 自动恢复，不产生重复上游
6. 最后一人离开 → 保留期 → IDLE 且源站连接归零，再次观看完整重建

另有 `python3 -I _selftest_framer.py` 对 H.264 访问单元分帧做单元自检。

## 五、文件说明

- `common.py` — Annex-B NAL 拆分、访问单元重组（关键帧/SPS/PPS 识别）、HTTP 工具
- `source.py` — 模拟直播源站（ffmpeg 编码 + TCP 服务 + 监督重启 + 控制 API）
- `relay.py` — 地区中继内核、状态机、单飞、保留期、有界分发、控制页 HTTP 服务
- `worker.py` — 订阅端进程，实时解码、JPEG 快照、ACK 流控、心跳回报
- `web/index.html` — 流媒体控制页（无第三方前端依赖）
- `verify.py` — 端到端六场景验证
- `config.json` — 地区到源站地址/控制端口的映射
