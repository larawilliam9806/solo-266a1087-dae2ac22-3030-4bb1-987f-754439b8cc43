"""FastAPI HTTP 层：版本上传发布 + 时刻查询（整表 JSON 与分批流式）。"""
from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import asynccontextmanager
from datetime import date as date_type
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from . import db
from .gtfs import WEEKDAY_COLUMNS, format_gtfs_time, validate_version

STATIC_DIR = Path(__file__).parent / "static"

# 流式查询每批推送的班次数。服务端最多只缓冲一个批次的停站明细，
# 缓冲量不随班次总数增长；首批取齐即发送，无需等待其余班次。
DEFAULT_BATCH_SIZE = 10
MAX_BATCH_SIZE = 100


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    yield


app = FastAPI(title="公交 GTFS 时刻表发布", lifespan=lifespan)


# ---------------- 发布 ----------------

@app.post("/api/versions", status_code=201)
async def publish(
    label: str = Form(..., min_length=1, max_length=200),
    stops: UploadFile = File(...),
    routes: UploadFile = File(...),
    trips: UploadFile = File(...),
    stop_times: UploadFile = File(...),
    calendar: UploadFile = File(...),
):
    """上传同一版本的 5 个 CSV。整版校验通过才发布，否则记录错误并保持当前正式版。"""
    uploads = {"stops": stops, "routes": routes, "trips": trips,
               "stop_times": stop_times, "calendar": calendar}
    contents: dict[str, bytes] = {}
    for name, up in uploads.items():
        contents[name] = await up.read()

    tables, errors, truncated = validate_version(contents)
    conn = db.connect()
    try:
        if tables is None:
            vid = db.record_failed_version(conn, label, errors)
            raise HTTPException(
                status_code=422,
                detail={
                    "message": "版本校验失败，未发布；当前正式版保持不变",
                    "version_id": vid,
                    "error_count": len(errors),
                    "truncated": truncated,
                    "errors": errors,
                })
        vid = db.publish_version(conn, label, tables)
        counts = {name: len(rows) for name, rows in tables.items()}
    finally:
        conn.close()
    return {"message": "版本已发布", "version_id": vid, "label": label,
            "status": "published", "counts": counts}


# ---------------- 版本信息 ----------------

@app.get("/api/versions")
def list_versions():
    conn = db.connect()
    try:
        cur_id = db.current_version_id(conn)
        rows = conn.execute(
            "SELECT id, label, status, error_count, created_at, published_at "
            "FROM versions ORDER BY id DESC").fetchall()
    finally:
        conn.close()
    return {"current_version_id": cur_id,
            "versions": [dict(r) for r in rows]}


@app.get("/api/versions/current")
def current_version():
    conn = db.connect()
    try:
        vid = db.current_version_id(conn)
        if vid is None:
            raise HTTPException(404, "尚未发布任何版本")
        row = conn.execute(
            "SELECT id, label, status, created_at, published_at "
            "FROM versions WHERE id = ?", (vid,)).fetchone()
    finally:
        conn.close()
    return dict(row)


@app.get("/api/versions/{version_id}")
def version_detail(version_id: int):
    conn = db.connect()
    try:
        row = conn.execute(
            "SELECT id, label, status, error_count, created_at, published_at "
            "FROM versions WHERE id = ?", (version_id,)).fetchone()
        if row is None:
            raise HTTPException(404, f"版本 {version_id} 不存在")
        errors = conn.execute(
            "SELECT file, row, field, message FROM version_errors "
            "WHERE version_id = ? ORDER BY id", (version_id,)).fetchall()
    finally:
        conn.close()
    result = dict(row)
    result["errors"] = [dict(e) for e in errors]
    return result


# ---------------- 查询（始终读取同一完整正式版） ----------------

def _require_current(conn) -> int:
    vid = db.current_version_id(conn)
    if vid is None:
        raise HTTPException(404, "尚未发布任何版本")
    return vid


def _parse_service_date(raw: str) -> str:
    """校验运营日格式，返回对应的 calendar 星期列名。"""
    try:
        svc_date = date_type.fromisoformat(raw)
    except ValueError:
        raise HTTPException(422, f"date '{raw}' 不是合法的 YYYY-MM-DD 日期")
    return WEEKDAY_COLUMNS[svc_date.weekday()]


def _require_route(conn, vid: int, route_id: str) -> dict:
    row = conn.execute(
        "SELECT route_id, route_short_name, route_long_name, route_type "
        "FROM routes WHERE version_id = ? AND route_id = ?",
        (vid, route_id)).fetchone()
    if row is None:
        raise HTTPException(404, f"线路 '{route_id}' 在当前版本 {vid} 中不存在")
    return dict(row)


def _active_service_ids(conn, vid: int, date: str, weekday_col: str) -> list[str]:
    rows = conn.execute(
        f"SELECT service_id FROM calendar WHERE version_id = ? "
        f"AND start_date <= ? AND end_date >= ? AND {weekday_col} = 1",
        (vid, date, date)).fetchall()
    return [r["service_id"] for r in rows]


def _trips_ordered(conn, vid: int, route_id: str,
                   service_ids: list[str]) -> list:
    """班次按（首站发车时刻, 班次号）排序，只取班次行本身，不取停站明细。

    首站发车时刻经 stop_times 主键索引（version_id, trip_id, stop_sequence）
    直接定位 stop_sequence 最小行的 departure_secs，无需扫描停站明细。
    """
    placeholders = ",".join("?" for _ in service_ids)
    return conn.execute(
        f"SELECT t.trip_id, t.service_id, t.trip_headsign, t.direction_id "
        f"FROM trips t "
        f"WHERE t.version_id = ? AND t.route_id = ? "
        f"AND t.service_id IN ({placeholders}) "
        f"ORDER BY (SELECT st.departure_secs FROM stop_times st "
        f"          WHERE st.version_id = t.version_id "
        f"            AND st.trip_id = t.trip_id "
        f"          ORDER BY st.stop_sequence LIMIT 1), "
        f"         t.trip_id",
        (vid, route_id, *service_ids)).fetchall()


def _trip_with_stop_times(conn, vid: int, trip_row) -> dict:
    """取单个班次的完整停站明细（内存中只驻留一个班次的量）。"""
    stops = conn.execute(
        "SELECT st.stop_sequence, st.stop_id, s.stop_name, "
        "st.arrival_secs, st.departure_secs "
        "FROM stop_times st JOIN stops s "
        "  ON s.version_id = st.version_id AND s.stop_id = st.stop_id "
        "WHERE st.version_id = ? AND st.trip_id = ? "
        "ORDER BY st.stop_sequence",
        (vid, trip_row["trip_id"])).fetchall()
    return {
        "trip_id": trip_row["trip_id"],
        "service_id": trip_row["service_id"],
        "headsign": trip_row["trip_headsign"],
        "direction_id": trip_row["direction_id"],
        "stop_times": [_stop_time_dict(s) for s in stops],
    }


@app.get("/api/routes")
def list_routes():
    conn = db.connect()
    try:
        vid = _require_current(conn)
        rows = conn.execute(
            "SELECT route_id, route_short_name, route_long_name, route_type "
            "FROM routes WHERE version_id = ? ORDER BY route_id", (vid,)).fetchall()
    finally:
        conn.close()
    return {"version_id": vid, "routes": [dict(r) for r in rows]}


@app.get("/api/routes/{route_id}/schedule")
def route_schedule(
    route_id: str,
    date: str = Query(..., description="运营日，格式 YYYY-MM-DD"),
):
    """按线路 + 运营日查询有序停站时刻（整表 JSON，接口保持兼容）。

    班次按（首站发车时刻, 班次号）排序。时刻以 GTFS 形式返回
    （小时可 >= 24，表示跨午夜后仍属该运营日），并附 day_offset 与实际钟点。
    """
    weekday_col = _parse_service_date(date)

    conn = db.connect()
    try:
        # 请求开始时读取一次指针，之后所有查询都固定在这个完整版本上
        vid = _require_current(conn)
        route = _require_route(conn, vid, route_id)
        service_ids = _active_service_ids(conn, vid, date, weekday_col)

        trips_out = []
        if service_ids:
            for t in _trips_ordered(conn, vid, route_id, service_ids):
                trips_out.append(_trip_with_stop_times(conn, vid, t))
    finally:
        conn.close()

    return {
        "version_id": vid,
        "route": route,
        "date": date,
        "weekday": weekday_col,
        "trip_count": len(trips_out),
        "trips": trips_out,
    }


@app.get("/api/routes/{route_id}/schedule/stream")
def route_schedule_stream(
    route_id: str,
    date: str = Query(..., description="运营日，格式 YYYY-MM-DD"),
    batch_size: int = Query(DEFAULT_BATCH_SIZE, ge=1, le=MAX_BATCH_SIZE,
                            description="每批推送的班次数"),
):
    """分批流式查询（NDJSON，每行一个 JSON 帧），与整表接口结果一致。

    帧序列：meta（版本/线路/班次总数）→ 若干 batch（每帧 batch_size 个
    班次的完整停站明细）→ done；流中途异常则改发 error 帧，
    已发出的批次仍然有效。

    - 版本指针在进入流之前读取一次，整个流固定读取该正式版；
      查询途中发布新版不影响本次流。
    - 停站明细按批现取现发，服务端缓冲量不随班次总数增长。
    - 客户端断开（取消）后不再产生下一批。
    """
    weekday_col = _parse_service_date(date)

    conn = db.connect()
    try:
        # 查询开始时读取一次指针，本次流式查询始终使用该正式版
        vid = _require_current(conn)
        route = _require_route(conn, vid, route_id)
        service_ids = _active_service_ids(conn, vid, date, weekday_col)
    except BaseException:
        conn.close()
        raise

    def frames() -> Iterator[str]:
        try:
            trips = (_trips_ordered(conn, vid, route_id, service_ids)
                     if service_ids else [])
            yield _frame({"type": "meta", "version_id": vid, "route": route,
                          "date": date, "weekday": weekday_col,
                          "trip_count": len(trips), "batch_size": batch_size})
            sent = 0
            batch: list[dict] = []
            for t in trips:
                batch.append(_trip_with_stop_times(conn, vid, t))
                if len(batch) >= batch_size:
                    sent += len(batch)
                    yield _frame({"type": "batch", "trips": batch})
                    batch = []
            if batch:
                sent += len(batch)
                yield _frame({"type": "batch", "trips": batch})
            yield _frame({"type": "done", "trip_count": sent})
        except Exception as exc:  # 流中途出错：通知客户端，已发批次保留
            yield _frame({"type": "error",
                          "message": f"时刻表未查完：{type(exc).__name__}: {exc}"})
        finally:
            conn.close()

    return StreamingResponse(
        frames(),
        media_type="application/x-ndjson",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


def _frame(obj: dict) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n"


def _stop_time_dict(row) -> dict:
    arr, dep = row["arrival_secs"], row["departure_secs"]
    return {
        "stop_sequence": row["stop_sequence"],
        "stop_id": row["stop_id"],
        "stop_name": row["stop_name"],
        "arrival": format_gtfs_time(arr),
        "departure": format_gtfs_time(dep),
        "arrival_secs": arr,
        "departure_secs": dep,
        "arrival_day_offset": arr // 86400,
        "departure_day_offset": dep // 86400,
        "arrival_clock": format_gtfs_time(arr % 86400),
        "departure_clock": format_gtfs_time(dep % 86400),
    }


# ---------------- 网页 ----------------

@app.get("/", include_in_schema=False)
def index():
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
