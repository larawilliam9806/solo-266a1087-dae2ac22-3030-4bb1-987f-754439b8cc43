# 公交 GTFS 时刻表发布系统

FastAPI + SQLite + 本地文件的公交时刻表发布与查询网页。数据工程师一次性上传同一版本的
5 个 CSV（stops / routes / trips / stop_times / calendar），系统整版校验通过后才发布；
公众按「线路 + 运营日」查询有序停站时刻。

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

SQLite 数据库保存在命名卷 `gtfs-data` 中，容器重启数据不丢。

### 本地运行

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --port 8000
```

### 运行测试

```bash
pip install pytest httpx
pytest tests/ -q
```

## 网页功能

- **时刻查询**：选择运营日和线路，网页通过流式接口**分批自动显示**各班次的
  完整停站明细——首批班次无需等其余班次取齐即可显示；班次按
  「首站发车时刻 → 班次号」排序；跨午夜时刻显示为 `24:20:00` 并标注「+1天」。
  重新查询会自动取消上一次查询（旧查询停止取下一批，迟到的批次不会混入）；
  查询中断时已显示的班次保留，并提示「时刻表未查完」。
- **发布新版本**：一次上传 5 个 CSV + 版本名称。通过则立即生效；
  失败则列出全部错误行（文件 / 行号 / 字段 / 原因），当前正式版不受影响。
- **版本记录**：查看所有版本（含失败版本）及其错误明细。

也可以用 `sample_data/valid/`（可通过）和 `sample_data/invalid/`（含典型错误）
两组样例数据在网页上直接试。

## 校验规则

任一规则不满足，整版拒绝发布：

1. **结构**：必备列齐全；字段类型合法（非负整数序号、YYYYMMDD 日期、0/1 星期标志、
   HH:MM:SS 时刻）。
2. **表内唯一**：`stop_id` / `route_id` / `service_id` / `trip_id` 各自唯一；
   `stop_times` 内 `(trip_id, stop_sequence)` 唯一。
3. **跨表引用**：`trips.route_id → routes`、`trips.service_id → calendar`、
   `stop_times.trip_id → trips`、`stop_times.stop_id → stops`；
   每个 trip 至少有一条 stop_times。
4. **时刻递增**：同一 trip 按 `stop_sequence` 排序后，每行 `departure ≥ arrival`，
   且下一站到站时刻严格大于上一站发车时刻。
5. **跨午夜**：时刻允许 `HH ≥ 24`（如 `25:10:00`），内部统一换算为
  「自运营日 00:00 起的秒数」比较与存储；`24:10` 之后出现 `23:59` 视为倒退报错。

## 发布与并发语义

- **整版原子发布**：所有校验在内存中完成；通过后在**单个 SQLite 事务**内写入
  整版数据并拨动 `meta.current_version_id` 指针。查询方要么看到旧完整版本、
  要么看到新完整版本，绝不出现半新半旧。
- **失败保旧**：校验失败的版本只记录 `versions(status='failed')` 和错误明细，
  不写入任何时刻数据、不触碰指针。
- **版本不可变**：已发布版本的数据永不修改或删除；每次查询（含流式查询的
  整个流）在开始时读一次指针，之后所有批次都固定读取同一正式版——
  查询途中发布新版不影响进行中的查询。
- **流式分批**：流式接口按批现取现发，服务端最多缓冲一个批次的停站明细，
  缓冲量不随班次总数增长；客户端断开（取消）后不再产生下一批。
- SQLite 开启 **WAL**，发布写入不阻塞并发查询。

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/versions` | multipart 上传 `label` + 5 个 CSV；成功 201，失败 422 并返回错误行 |
| GET | `/api/versions` | 版本列表（含当前正式版指针） |
| GET | `/api/versions/current` | 当前正式版 |
| GET | `/api/versions/{id}` | 版本详情与错误明细 |
| GET | `/api/routes` | 当前版本的线路列表 |
| GET | `/api/routes/{route_id}/schedule?date=YYYY-MM-DD` | 该运营日的班次与有序停站时刻（整表 JSON，保持兼容） |
| GET | `/api/routes/{route_id}/schedule/stream?date=YYYY-MM-DD&batch_size=N` | 同上结果的分批流式版（NDJSON） |

### 流式查询（NDJSON）

`schedule/stream` 每行返回一个 JSON 帧，供网页分批渲染：

1. `{"type":"meta", ...}`：版本号、线路、运营日、班次总数；
2. `{"type":"batch", "trips":[...]}`：每帧 `batch_size` 个班次（默认 10，最大 100）
   的完整停站明细，班次按「首站发车时刻 → 班次号」排序；
3. `{"type":"done", "trip_count":N}`：正常结束；流中途异常则改发
   `{"type":"error", ...}`，已发出的批次仍然有效。

整个流固定使用开始时的正式版；客户端断开即停止产生后续批次。

### 命令行示例

```bash
# 发布
curl -F "label=2026冬季运行图" \
     -F "stops=@sample_data/valid/stops.csv" \
     -F "routes=@sample_data/valid/routes.csv" \
     -F "trips=@sample_data/valid/trips.csv" \
     -F "stop_times=@sample_data/valid/stop_times.csv" \
     -F "calendar=@sample_data/valid/calendar.csv" \
     http://localhost:8000/api/versions

# 查询夜班线路（含 24:00 之后的跨午夜班次）
curl "http://localhost:8000/api/routes/R2/schedule?date=2026-11-23"

# 分批流式查询（每行一个 JSON 帧，每批 10 个班次）
curl -N "http://localhost:8000/api/routes/R2/schedule/stream?date=2026-11-23&batch_size=10"
```

## 目录结构

```
app/
  main.py        FastAPI 端点（发布 / 查询 / 版本）
  gtfs.py        CSV 解析与整版校验（纯标准库）
  db.py          SQLite schema、原子发布、版本指针
  static/index.html  网页（查询 / 发布 / 版本记录）
tests/           pytest 单元与集成测试
sample_data/     可通过与含错误的样例 CSV
Dockerfile / docker-compose.yml
```
