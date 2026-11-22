"""流式（分批 NDJSON）时刻查询测试：分批、排序、版本钉住、取消与兼容。"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from conftest import upload, valid_files

STREAM = "/api/routes/R1/schedule/stream"


def frames_of(resp):
    return [json.loads(line) for line in resp.iter_lines() if line.strip()]


def tie_break_files():
    """三个班次：TA/TB 首站发车同为 08:00，TC 为 09:00。"""
    return valid_files(
        stops="stop_id,stop_name\nS1,甲\nS2,乙\n",
        routes="route_id,route_short_name,route_long_name,route_type\nR1,1路,甲-乙,3\n",
        calendar="service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
                 "WKD,1,1,1,1,1,0,0,20260101,20261231\n",
        trips="route_id,service_id,trip_id,trip_headsign,direction_id\n"
              "R1,WKD,TC,丙,0\nR1,WKD,TB,乙,0\nR1,WKD,TA,甲,0\n",
        stop_times="trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
                   "TB,08:00:00,08:00:00,S1,1\nTB,08:30:00,08:30:00,S2,2\n"
                   "TA,08:00:00,08:00:00,S1,1\nTA,08:20:00,08:20:00,S2,2\n"
                   "TC,09:00:00,09:00:00,S1,1\nTC,09:30:00,09:30:00,S2,2\n")


def test_stream_batches_in_order(client):
    upload(client, valid_files())
    with client.stream("GET", STREAM + "?date=2026-11-23&batch_size=1") as resp:
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("application/x-ndjson")
        frames = frames_of(resp)

    assert [f["type"] for f in frames] == ["meta", "batch", "batch", "done"]
    meta = frames[0]
    assert meta["trip_count"] == 2 and meta["batch_size"] == 1
    assert meta["route"]["route_id"] == "R1"
    assert meta["date"] == "2026-11-23" and meta["weekday"] == "monday"
    # 每批一个班次，按首站发车时刻排序；停站明细完整
    assert [f["trips"][0]["trip_id"] for f in frames[1:3]] == ["T1", "T2"]
    t1 = frames[1]["trips"][0]
    assert [s["stop_id"] for s in t1["stop_times"]] == ["S1", "S2", "S3", "S4"]
    assert frames[3] == {"type": "done", "trip_count": 2}


def test_stream_default_batch_groups_trips(client):
    upload(client, valid_files())
    with client.stream("GET", STREAM + "?date=2026-11-23") as resp:
        frames = frames_of(resp)
    assert [f["type"] for f in frames] == ["meta", "batch", "done"]
    assert [t["trip_id"] for t in frames[1]["trips"]] == ["T1", "T2"]


def test_stream_tie_break_by_trip_id(client):
    upload(client, tie_break_files())
    with client.stream("GET", STREAM + "?date=2026-11-23&batch_size=10") as resp:
        frames = frames_of(resp)
    # 同刻 08:00 的 TA/TB 按班次号排序，09:00 的 TC 最后
    assert [t["trip_id"] for t in frames[1]["trips"]] == ["TA", "TB", "TC"]

    # 原 JSON 接口同样按（首站发车时刻, 班次号）排序
    sch = client.get("/api/routes/R1/schedule?date=2026-11-23").json()
    assert [t["trip_id"] for t in sch["trips"]] == ["TA", "TB", "TC"]


def test_stream_after_midnight(client):
    upload(client, valid_files())
    with client.stream("GET", "/api/routes/R2/schedule/stream?date=2026-11-23") as resp:
        frames = frames_of(resp)
    n1 = frames[1]["trips"][0]
    times = [(s["arrival"], s["arrival_day_offset"], s["arrival_clock"])
             for s in n1["stop_times"]]
    # 跨午夜仍属原运营日：24:20/24:50 → day_offset=1
    assert times[2] == ("24:20:00", 1, "00:20:00")
    assert times[3] == ("24:50:00", 1, "00:50:00")


def test_stream_pins_starting_version(client):
    """查询途中发布新版：本次流始终使用开始时的正式版。"""
    upload(client, valid_files(), label="v1")
    v1 = client.get("/api/versions/current").json()["id"]

    with client.stream("GET", STREAM + "?date=2026-11-23&batch_size=1") as resp:
        it = resp.iter_lines()
        meta = json.loads(next(it))
        assert meta["type"] == "meta" and meta["version_id"] == v1
        first = json.loads(next(it))
        assert first["type"] == "batch" and first["trips"][0]["trip_id"] == "T1"

        # 查询途中照常发布新版：R1 只剩 T1，T2 不复存在
        files2 = valid_files(
            routes="route_id,route_short_name,route_long_name,route_type\n"
                   "R1,1路快线,中央车站-机场,3\n",
            trips="route_id,service_id,trip_id\nR1,WKD,T1\n",
            stop_times="trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
                       "T1,08:00:00,08:00:00,S1,1\n"
                       "T1,09:00:00,09:00:00,S4,2\n")
        assert upload(client, files2, label="v2").status_code == 201
        v2 = client.get("/api/versions/current").json()["id"]
        assert v2 != v1

        rest = [json.loads(line) for line in it if line.strip()]

    # 后续批次仍来自开始时的 v1：T2 照常出现，总数不变
    assert [f["type"] for f in rest] == ["batch", "done"]
    assert rest[0]["trips"][0]["trip_id"] == "T2"
    assert rest[1] == {"type": "done", "trip_count": 2}


def test_stream_client_close_midway(client):
    """客户端中途断开（取消）：服务端不再产生下一批，后续查询不受影响。"""
    upload(client, valid_files())
    with client.stream("GET", STREAM + "?date=2026-11-23&batch_size=1") as resp:
        it = resp.iter_lines()
        meta = json.loads(next(it))
        assert meta["type"] == "meta"
        resp.close()  # 取消：不再读取后续批次

    # 服务端无恙，整表 JSON 接口照常工作
    sch = client.get("/api/routes/R1/schedule?date=2026-11-23").json()
    assert sch["trip_count"] == 2
    # 再次流式查询可完整取完
    with client.stream("GET", STREAM + "?date=2026-11-23") as resp:
        frames = frames_of(resp)
    assert frames[-1] == {"type": "done", "trip_count": 2}


def test_stream_no_service_that_day(client):
    upload(client, valid_files())
    with client.stream("GET", STREAM + "?date=2026-11-21") as resp:  # 周六，R1 无服务
        frames = frames_of(resp)
    assert [f["type"] for f in frames] == ["meta", "done"]
    assert frames[0]["trip_count"] == 0
    assert frames[1] == {"type": "done", "trip_count": 0}


def test_stream_errors(client):
    assert client.get(STREAM + "?date=2026-13-40").status_code == 422  # 非法日期
    assert client.get(STREAM + "?date=2026-11-23").status_code == 404  # 无版本
    upload(client, valid_files())
    assert client.get("/api/routes/RX/schedule/stream?date=2026-11-23").status_code == 404
    # batch_size 越界
    assert client.get(STREAM + "?date=2026-11-23&batch_size=0").status_code == 422


def test_json_schedule_unchanged_shape(client):
    """原 JSON 接口字段结构保持兼容。"""
    upload(client, valid_files())
    sch = client.get("/api/routes/R1/schedule?date=2026-11-23").json()
    assert set(sch) == {"version_id", "route", "date", "weekday",
                        "trip_count", "trips"}
    t1 = sch["trips"][0]
    assert set(t1) == {"trip_id", "service_id", "headsign",
                       "direction_id", "stop_times"}
    s = t1["stop_times"][0]
    assert set(s) == {"stop_sequence", "stop_id", "stop_name", "arrival",
                      "departure", "arrival_secs", "departure_secs",
                      "arrival_day_offset", "departure_day_offset",
                      "arrival_clock", "departure_clock"}
