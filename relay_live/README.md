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

### 共享上游的三个成本项
中继把一条上游的访问单元分给同地区所有观众。成本拆成可观测的三段
（`/api/topology` 的 `copy_cost`，单位都是真实字节拷贝，不是“逻辑搬运”）：

| 字段 | 含义 | 优化前后 |
| --- | --- | --- |
| `parse_copy_bytes` | 分帧时每个访问单元一次 `b"".join` | 每条 AU 一次，不变 |
| `boundary_copy_bytes` | 新观众加入时的 `SPS/PPS + 关键帧` 前缀 | 仅入会那一次，不变 |
| `wire_copy_bytes` | 每观众发送时 `header + payload` 的拼接 | **每观众每帧一次 → 仅 8 字节头部** |

优化前的 `wire_copy_bytes` 把整帧再复制一遍给每个观众，30 人时约为上游字节的
27 倍；优化后发送路径改为先写 8 字节头、再直接写共享的 AU 缓冲区，复制量降到
约 1.36 倍（只剩每帧一次分帧拷贝）。缓冲复用：稳态下同一个 `bytes` 对象进入每个
观众的有界队列，队列满时只丢引用、不复制，内存上界为 `容量 × 单帧大小 × 观众数`
的引用本身（30 观众 × 120 槽 ≈ 31 KB 引用，而非 3.6 MB 帧副本）。

**计算顺序（先成本、后速度）**，基准严格按此顺序执行：

1. 先记录**上游连接数**与**缓存量**（`/stats` 的 `upstream_connections`、
   `cache_bytes` 的 `latest_keyframe + init_bytes + queued_bytes`）——它们是
   本次对照的不变量，先取初值，跑完再取终值；
2. 成本分解按 `parse_copy_bytes → boundary_copy_bytes → wire_copy_bytes` 三段
   各自累计，单位都是真实字节拷贝；三段之和除以同期上游字节得到复制放大倍数。
   内核计数是进程内累计值，因此基准在开跑前先快照一次，报告的是**差值**，不会
   把上一轮的结果算进本轮；
3. 成本分解完整取到（且非零）之后，才比较首帧延迟、丢帧与 CPU。任何一段缺失
   都会被当作“应答的不是本构建”直接 `BENCH_FAIL`，而不是按 0 处理。

发送路径拆成“8 字节头 + 帧”两次 write 后，单观众首帧一度比基线的“一次拼接
write”慢约 23 ms——环回上第二个小包会被 Nagle 延迟 ACK 拖住。因此
`_handle_stream` 对该 socket 显式开启 `TCP_NODELAY`。三臂对照（HEAD / 拆分写无
nodelay / 拆分写 + nodelay，各 5 轮交替顺序）首帧中位数 872 / 874 / 869 ms，
差异落在噪声内；复制放大倍数则保持约 1.02（单观众）和 1.36（30 观众），
相对 HEAD 的 2.02 / 27.0 大幅下降。若去掉 nodelay 后首帧出现系统性回退，就应
撤回拆分写这一优化。

`cache_bytes` 报告中继侧的缓存量：`latest_keyframe` + `init_bytes`（SPS/PPS，
按 NAL 类型去重后恒定）+ 各观众队列中的字节。参数集缓存改为按类型覆盖后，
即使编码器每个关键帧都重发 SPS/PPS，缓存也不会线性增长。

### 代际与重入
每次建立上游都会递增 `epoch`。分帧协程绑定自己那一代的 `epoch`，在 `read()`
返回后先校验代际：旧代的数据不会漏进新代，旧代的错误处理也不会关掉新连接。
保留期到期回收时先递增代际再关 socket，避免“本地关闭”被误判为源站故障。

## 四、自动化验证

```bash
./start_all.sh
python3 -I verify.py
python3 -I test_lifecycle.py
```

`verify.py` 通过公开控制 API 驱动真实多进程，断言六项（实测全部通过）：

1. 并发加入合并为**一条**上游（4 并发 → 上游 1、源站连接 1）
2. 各端首帧到达且始于关键帧边界（首帧约 0.9s）
3. 播放中再加观众不新增上游（页面刷新不新建永久连接）
4. 慢速端队列填满后仅本端丢帧，其他端零丢帧且持续解码
5. 源站短断后 `FAILED → PLAYING` 自动恢复，不产生重复上游
6. 最后一人离开 → 保留期 → IDLE 且源站连接归零，再次观看完整重建

`test_lifecycle.py` 补充生命周期回归（实测 3/3 通过）：回收后重入拿到新代并重新出画、
保留期内加入复用同一上游、慢观众独立背压且队列有界。

另有 `python3 -I _selftest_framer.py` 对 H.264 访问单元分帧做单元自检。

## 五、共享上游负载基准

`bench_relay.py` 只通过公开控制 API 驱动：N 个订阅端并发加入（前 M 个标记慢速），
稳定保持一段时间，每秒采样中继拓扑与源站 `/stats`，然后全部停止并等待保留期回收。
它记录上游连接数、缓存量、三段复制成本、中继进程 CPU 与解码子树 CPU、首帧延迟分布。

```bash
./start_all.sh
python3 -I bench_relay.py --subscribers 30 --slow 8 --hold 25
```

硬性不变量不通过时基准以非零退出（不以默认值吞掉错误）：同地区出现重复上游、
队列超出容量、普通观众丢帧、进入 IDLE 后上游仍连接、加入报错、或拿不到
`copy_cost`（说明应答的是旧构建/陈旧进程）都会直接 `BENCH_FAIL`。

`start_all.sh` 在 8080 已被占用时直接失败，`stop_all.sh` 会清扫其它检出路径下的
同名进程并在端口未释放时报错；这让“优化前/后”的对照不会打到错误的进程上。

## 六、地区中继的图形入口

浏览器打开 `http://127.0.0.1:8080/`。页面按钮直接调用中继的内核：
“增加订阅端 / 增加慢速订阅端”走 `POST /api/subscribers`，每张卡片的“停止”走
`POST /api/subscribers/stop`，“模拟源站短断”走后端控制端口。每张卡片显示的是该
worker 进程实时解码出的 JPEG 画面（来自真实 ffmpeg 解码，不是固定占位图）、首帧耗时、
已解码帧、本端丢帧与队列水位，地区卡片显示状态机与上游连接统计。

命令行脚本只作为地区中继的辅助：`verify.py` / `test_lifecycle.py` / `bench_relay.py`
都只调用与图形入口相同的公开 API。

## 七、文件说明

- `common.py` — Annex-B NAL 拆分、访问单元重组（关键帧/SPS/PPS 识别）、HTTP 工具
- `source.py` — 模拟直播源站（ffmpeg 编码 + TCP 服务 + 监督重启 + 控制 API）
- `relay.py` — 地区中继内核、状态机、单飞、代际、保留期、有界分发、控制页 HTTP 服务
- `worker.py` — 订阅端进程，实时解码、JPEG 快照、ACK 流控、心跳回报
- `web/index.html` — 流媒体控制页（无第三方前端依赖）
- `verify.py` — 端到端六场景验证
- `test_lifecycle.py` — 回收/重入/保留复用/慢观众背压回归
- `bench_relay.py` — 订阅风暴 + 慢观众的上游成本与负载基准
- `config.json` — 地区到源站地址/控制端口的映射
