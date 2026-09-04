# -*- coding: utf-8 -*-
"""
广州天气 & 空气质量 → Turso 同步脚本
======================================
数据源:
  - 天气: open-meteo 归档 API (archive-api, 广州坐标 23.09N/113.25E, 小时级)
  - 空气: open-meteo 空气质量 API (air-quality-api, CAMS 模型, 小时级→日均)

用法:
  python sync_turso.py                  # 增量: 天气最近3天 + 空气最近4天(日均)
  python sync_turso.py --days 7         # 天气最近7天 + 空气最近4天
  python sync_turso.py --backfill       # 天气全量回填(2022-08-01 至今) + 空气最近4天
  python sync_turso.py --backfill-air 2026-07-07 2026-09-01   # 空气指定区间回填(日均)
  python sync_turso.py --skip-weather   # 只同步空气
  python sync_turso.py --skip-air       # 只同步天气

说明:
  - 本脚本只做"写入 Turso"，不读取/修改任何原项目文件。
  - 天气用 INSERT ... ON CONFLICT 增量 upsert，重复运行不会产生重复数据。
  - 空气质量历史(监测站实测)由 import_air_history.py 导入；本脚本只负责用
    模型数据补齐 2026-07 之后缺失段与每日最新日均值，单位/口径与历史一致。
  - TURSO_TOKEN 从环境变量读取，不在代码中硬编码。本地运行请先设置环境变量；
    云端由 GitHub Actions 的 Secrets 注入。
"""

import json
import os
import sys
import argparse
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import libsql

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# ---------------- 配置 ----------------
TURSO_URL = "libsql://guangzhouweather-soraopera.aws-ap-northeast-1.turso.io"
TURSO_TOKEN = os.getenv("TURSO_TOKEN")
if not TURSO_TOKEN:
    raise SystemExit(
        "未设置 TURSO_TOKEN 环境变量。\n"
        "本地运行请先设置: $env:TURSO_TOKEN='...' (Windows) 或 export TURSO_TOKEN=... (Linux/Mac)。\n"
        "云端由 GitHub Actions 工作流通过 secrets.TURSO_TOKEN 自动注入。"
    )

WEATHER_LAT = 23.09
WEATHER_LON = 113.25
WEATHER_VARS = ("temperature_2m,relative_humidity_2m,pressure_msl,precipitation,"
                "weather_code,wind_speed_10m,wind_direction_10m,"
                "wind_speed_100m,wind_direction_100m")
BACKFILL_START = "2022-08-01"

# 空气: open-meteo 空气质量 API
AIR_VARS = "pm2_5,pm10,sulphur_dioxide,nitrogen_dioxide,carbon_monoxide,ozone"
AIR_DAILY_LOOKBACK = 4  # 每日增量同步最近 N 天(日均)，重复运行可自愈近几日

# CAMS 模型相对监测站(广东环境保护公众网)的系统偏差校正系数。
# 计算方式: 重叠期(2025-01-01~2026-07-06) 站均值 / 模型均值，逐污染物。
# 用于把模型浓度校正到与监测站一致的口径，确保拼接处无阶跃。
_AIR_SCALE = {
    "pm25": 0.430,
    "pm10": 0.747,
    "so2":  0.140,
    "no2":  0.506,
    "co":   0.782,
    "o3":   1.117,
}

# ---------------- WAQI 实时监测站（小时级） ----------------
WAQI_TOKEN = os.environ.get("WAQI_TOKEN") or "f79fa00789127ed54c62a0fe82d39dd93d9db08b"
WAQI_STATIONS = [
    ("广州市监测站", 9845),
    ("体育西",      8318),
    ("广雅中学",    9844),
    ("广州市五中",  9846),
    ("海珠沙园",  14368),
    ("花都师范",    9842),
]
# PM10 弃用 WAQI 值，改由 PM2.5 按历史回归重建（154 个月，R²=0.91）
PM10_RECON_A = 1.287
PM10_RECON_B = 10.87

SH_TZ = timezone(timedelta(hours=8))  # 数据统一为 Asia/Shanghai


# ---------------- 建表 ----------------
def create_tables(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS weather_hourly (
            time TEXT PRIMARY KEY,
            temperature_2m REAL,
            relative_humidity_2m REAL,
            pressure_msl REAL,
            precipitation REAL,
            weather_code INTEGER,
            wind_speed_10m REAL,
            wind_direction_10m REAL,
            wind_speed_100m REAL,
            wind_direction_100m REAL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS air_quality (
            time TEXT PRIMARY KEY,
            aqi INTEGER,
            pm25 REAL, pm10 REAL, so2 REAL, no2 REAL, co REAL, o3 REAL,
            humidity REAL, pressure REAL, temperature REAL, wind REAL,
            dew REAL, wind_gust REAL, dominant_pollutant TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS air_hourly (
            time TEXT PRIMARY KEY,
            aqi INTEGER,
            pm25 REAL, pm10 REAL, so2 REAL, no2 REAL, co REAL, o3 REAL,
            dominant_pollutant TEXT,
            station_count INTEGER
        )
    """)
    conn.commit()


# ---------------- 拉取天气 ----------------
def fetch_weather(start_date, end_date):
    url = (f"https://archive-api.open-meteo.com/v1/archive?"
           f"latitude={WEATHER_LAT}&longitude={WEATHER_LON}"
           f"&start_date={start_date}&end_date={end_date}"
           f"&hourly={WEATHER_VARS}&timezone=Asia/Shanghai")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    data = json.load(urllib.request.urlopen(req, timeout=180))
    h = data.get("hourly", {})
    times = h.get("time", [])
    cols = ["temperature_2m", "relative_humidity_2m", "pressure_msl", "precipitation",
            "weather_code", "wind_speed_10m", "wind_direction_10m",
            "wind_speed_100m", "wind_direction_100m"]
    rows = []
    for i, t in enumerate(times):
        row = {"time": t}
        for c in cols:
            row[c] = h[c][i] if h.get(c) is not None else None
        rows.append(row)
    return rows


# ---------------- 中国 AQI 计算（HJ633-2012） ----------------
_AQI_LEVELS = [0, 50, 100, 150, 200, 300, 400, 500]
_CONC_BP = {
    "pm25": [0, 35, 75, 115, 150, 250, 350, 500],          # μg/m³ (24h)
    "pm10": [0, 50, 150, 250, 350, 420, 500, 600],         # μg/m³ (24h)
    "so2":  [0, 50, 150, 475, 800, 1600, 2100, 2620],      # μg/m³ (24h)
    "no2":  [0, 40, 80, 180, 280, 565, 750, 940],          # μg/m³ (24h)
    "co":   [0, 2, 4, 14, 24, 36, 48, 60],                 # mg/m³ (24h)
    "o3":   [0, 100, 160, 215, 265, 800, 1000, 1200],      # μg/m³ (8h 滑动均值，这里用日均近似)
}


def _iaqi_from_conc(poll, conc):
    if conc is None:
        return None
    bp = _CONC_BP[poll]
    if conc <= bp[0]:
        return 0.0
    for i in range(len(_AQI_LEVELS) - 1):
        lo_c, hi_c = bp[i], bp[i + 1]
        if lo_c <= conc <= hi_c:
            lo_i, hi_i = _AQI_LEVELS[i], _AQI_LEVELS[i + 1]
            return lo_i + (conc - lo_c) * (hi_i - lo_i) / (hi_c - lo_c)
    return float(_AQI_LEVELS[-1])


def _china_aqi(pm25, pm10, so2, no2, co, o3):
    vals = [v for v in (
        _iaqi_from_conc("pm25", pm25),
        _iaqi_from_conc("pm10", pm10),
        _iaqi_from_conc("so2", so2),
        _iaqi_from_conc("no2", no2),
        _iaqi_from_conc("co", co),   # co 单位 mg/m³
        _iaqi_from_conc("o3", o3),
    ) if v is not None]
    return round(max(vals)) if vals else None


def _aqi_detail(pm25, pm10, so2, no2, co, o3):
    """返回 (国标AQI, 首要污染物)。"""
    parts = {
        "PM2.5": _iaqi_from_conc("pm25", pm25),
        "PM10":  _iaqi_from_conc("pm10", pm10),
        "SO2":   _iaqi_from_conc("so2", so2),
        "NO2":   _iaqi_from_conc("no2", no2),
        "CO":    _iaqi_from_conc("co", co),
        "O3":    _iaqi_from_conc("o3", o3),
    }
    valid = {k: v for k, v in parts.items() if v is not None}
    if not valid:
        return None, None
    dominant = max(valid, key=valid.get)
    return round(max(valid.values())), dominant


# ---------------- 拉取空气（WAQI 6 站实时 → 小时级） ----------------
def _co_from_openmeteo():
    """WAQI 的 CO 字段单位不可靠，改从 open-meteo 取当前 CO（μg/m³），按口径校正后转 mg/m³。"""
    try:
        url = (f"https://air-quality-api.open-meteo.com/v1/air-quality?"
               f"latitude={WEATHER_LAT}&longitude={WEATHER_LON}"
               f"&current=carbon_monoxide&timezone=Asia%2FShanghai&forecast_days=1")
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        d = json.load(urllib.request.urlopen(req, timeout=30))
        co_ug = (d.get("current") or {}).get("carbon_monoxide")
        if co_ug is None:
            return None
        return co_ug * _AIR_SCALE["co"] / 1000.0  # → mg/m³，与日均口径一致
    except Exception as e:
        print(f"    [CO] open-meteo 拉取失败: {e}")
        return None


def fetch_air_waqi():
    """等权聚合 6 个监测站实时浓度；PM10 由 PM2.5 回归重建，CO 取 open-meteo，返回单条小时记录。"""
    acc = {"pm25": [], "so2": [], "no2": [], "o3": []}
    n_ok = 0
    for name, sid in WAQI_STATIONS:
        url = f"https://api.waqi.info/feed/@{sid}/?token={WAQI_TOKEN}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            d = json.load(urllib.request.urlopen(req, timeout=20))
        except Exception as e:
            print(f"    [{name}] 拉取失败: {e}")
            continue
        if d.get("status") != "ok":
            print(f"    [{name}] status={d.get('status')} {str(d.get('data'))[:80]}")
            continue
        ia = (d.get("data") or {}).get("iaqi", {})

        def gv(k):
            try:
                v = ia.get(k, {}).get("v")
                return float(v) if v not in (None, "") else None
            except (TypeError, ValueError):
                return None

        pm25 = gv("pm25")
        so2 = gv("so2")
        no2 = gv("no2")
        o3 = gv("o3")
        if pm25 is None:
            print(f"    [{name}] 无 PM2.5，跳过")
            continue
        n_ok += 1
        acc["pm25"].append(pm25)
        if so2 is not None:
            acc["so2"].append(so2)
        if no2 is not None:
            acc["no2"].append(no2)
        if o3 is not None:
            acc["o3"].append(o3)

    if n_ok == 0:
        print("[WAQI] 无可用站点数据，跳过小时写入。")
        return None

    def mean(lst):
        return sum(lst) / len(lst) if lst else None

    pm25 = mean(acc["pm25"])
    pm10 = (PM10_RECON_A * pm25 + PM10_RECON_B) if pm25 is not None else None
    so2 = mean(acc["so2"])
    no2 = mean(acc["no2"])
    co = _co_from_openmeteo()
    o3 = mean(acc["o3"])
    aqi, dominant = _aqi_detail(pm25, pm10, so2, no2, co, o3)
    now = datetime.now(SH_TZ).strftime("%Y-%m-%dT%H:00:00")
    print(f"    [WAQI] 站数={n_ok} PM2.5={pm25} PM10重建={pm10} "
          f"SO2={so2} NO2={no2} CO={co} O3={o3} 国标AQI={aqi} 首要={dominant}")
    return {
        "time": now,
        "aqi": aqi,
        "pm25": round(pm25, 1) if pm25 is not None else None,
        "pm10": round(pm10, 1) if pm10 is not None else None,
        "so2": round(so2, 1) if so2 is not None else None,
        "no2": round(no2, 1) if no2 is not None else None,
        "co": round(co, 3) if co is not None else None,
        "o3": round(o3, 1) if o3 is not None else None,
        "dominant_pollutant": dominant,
        "station_count": n_ok,
    }


# ---------------- 拉取空气（open-meteo 小时级 → 日均） ----------------
def fetch_air_openmeteo(start_date, end_date):
    url = (f"https://air-quality-api.open-meteo.com/v1/air-quality?"
           f"latitude={WEATHER_LAT}&longitude={WEATHER_LON}"
           f"&hourly={AIR_VARS}"
           f"&start_date={start_date}&end_date={end_date}"
           f"&timezone=Asia%2FShanghai")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    data = json.load(urllib.request.urlopen(req, timeout=300))
    h = data.get("hourly", {})
    times = h.get("time", [])
    if not times:
        return []

    keys = ["pm2_5", "pm10", "sulphur_dioxide", "nitrogen_dioxide",
            "carbon_monoxide", "ozone"]
    day_sum = defaultdict(lambda: [0.0] * len(keys))
    day_cnt = defaultdict(lambda: [0] * len(keys))
    for i, t in enumerate(times):
        day = t[:10]
        for j, k in enumerate(keys):
            v = h[k][i] if h.get(k) is not None else None
            if v is not None:
                day_sum[day][j] += float(v)
                day_cnt[day][j] += 1

    def mean(day, j):
        return day_sum[day][j] / day_cnt[day][j] if day_cnt[day][j] else None

    rows = []
    for day in sorted(day_sum.keys()):
        def sc(j, poll):
            m = mean(day, j)
            return m * _AIR_SCALE[poll] if m is not None else None
        pm25 = sc(0, "pm25")
        pm10 = sc(1, "pm10")
        so2 = sc(2, "so2")
        no2 = sc(3, "no2")
        co_ug = sc(4, "co")       # 模型 μg/m³，已按系数校正
        o3 = sc(5, "o3")
        if pm25 is None and pm10 is None:
            continue
        # CO 由 μg/m³ 转 mg/m³，与历史 CSV 单位一致
        co = (co_ug / 1000.0) if co_ug is not None else None
        # 物理校验: PM10 恒 >= PM2.5
        if pm10 is not None and pm25 is not None and pm10 < pm25:
            pm10 = pm25
        aqi = _china_aqi(pm25, pm10, so2, no2, co, o3)
        rows.append({
            "time": day + "T00:00:00",
            "aqi": aqi,
            "pm25": round(pm25, 1) if pm25 is not None else None,
            "pm10": round(pm10, 1) if pm10 is not None else None,
            "so2": round(so2, 1) if so2 is not None else None,
            "no2": round(no2, 1) if no2 is not None else None,
            "co": round(co, 3) if co is not None else None,
            "o3": round(o3, 1) if o3 is not None else None,
        })
    return rows


# ---------------- 写入 ----------------
def upsert_weather(conn, rows, batch_size=80):
    cols = ["time", "temperature_2m", "relative_humidity_2m", "pressure_msl",
            "precipitation", "weather_code", "wind_speed_10m", "wind_direction_10m",
            "wind_speed_100m", "wind_direction_100m"]
    col_sql = ", ".join(cols)
    update_sql = ", ".join(f"{c}=excluded.{c}" for c in cols[1:])
    row_ph = "(" + ", ".join(["?"] * len(cols)) + ")"
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        ph = ", ".join(row_ph for _ in batch)
        sql = (f"INSERT INTO weather_hourly ({col_sql}) VALUES {ph} "
               f"ON CONFLICT(time) DO UPDATE SET {update_sql}")
        params = [r[c] for r in batch for c in cols]
        conn.execute(sql, params)
    conn.commit()


def upsert_air_rows(conn, rows):
    if not rows:
        return 0
    for r in rows:
        r.setdefault("humidity", None)
        r.setdefault("pressure", None)
        r.setdefault("temperature", None)
        r.setdefault("wind", None)
        r.setdefault("dew", None)
        r.setdefault("wind_gust", None)
        r.setdefault("dominant_pollutant", None)
    row_ph = "(" + ", ".join(["?"] * 15) + ")"
    col_sql = ("time, aqi, pm25, pm10, so2, no2, co, o3, "
               "humidity, pressure, temperature, wind, dew, wind_gust, dominant_pollutant")
    update_sql = ("aqi=excluded.aqi, pm25=excluded.pm25, pm10=excluded.pm10, "
                  "so2=excluded.so2, no2=excluded.no2, co=excluded.co, o3=excluded.o3, "
                  "humidity=excluded.humidity, pressure=excluded.pressure, "
                  "temperature=excluded.temperature, wind=excluded.wind, "
                  "dew=excluded.dew, wind_gust=excluded.wind_gust, "
                  "dominant_pollutant=excluded.dominant_pollutant")
    for i in range(0, len(rows), 100):
        batch = rows[i:i + 100]
        ph = ", ".join(row_ph for _ in batch)
        sql = (f"INSERT INTO air_quality ({col_sql}) VALUES {ph} "
               f"ON CONFLICT(time) DO UPDATE SET {update_sql}")
        params = []
        for r in batch:
            params += [r["time"], r["aqi"], r["pm25"], r["pm10"], r["so2"],
                       r["no2"], r["co"], r["o3"], r["humidity"], r["pressure"],
                       r["temperature"], r["wind"], r["dew"], r["wind_gust"],
                       r["dominant_pollutant"]]
        conn.execute(sql, params)
    conn.commit()
    return len(rows)


def upsert_air_hourly_rows(conn, rows):
    if not rows:
        return 0
    cols = ["time", "aqi", "pm25", "pm10", "so2", "no2", "co", "o3",
            "dominant_pollutant", "station_count"]
    col_sql = ", ".join(cols)
    update_sql = ", ".join(f"{c}=excluded.{c}" for c in cols[1:])
    row_ph = "(" + ", ".join(["?"] * len(cols)) + ")"
    for i in range(0, len(rows), 100):
        batch = rows[i:i + 100]
        ph = ", ".join(row_ph for _ in batch)
        sql = (f"INSERT INTO air_hourly ({col_sql}) VALUES {ph} "
               f"ON CONFLICT(time) DO UPDATE SET {update_sql}")
        params = [r[c] for r in batch for c in cols]
        conn.execute(sql, params)
    conn.commit()
    return len(rows)


# ---------------- 主流程 ----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--backfill", action="store_true", help="天气从 2022-08-01 全量回填")
    ap.add_argument("--days", type=int, default=3, help="增量同步最近 N 天天气")
    ap.add_argument("--backfill-air", nargs=2, metavar=("START", "END"),
                    help="空气指定区间回填(日均), 如 --backfill-air 2026-07-07 2026-09-01")
    ap.add_argument("--skip-weather", action="store_true")
    ap.add_argument("--skip-air", action="store_true")
    ap.add_argument("--skip-waqi", action="store_true", help="跳过 WAQI 小时级空气")
    args = ap.parse_args()

    conn = libsql.connect(database=TURSO_URL, auth_token=TURSO_TOKEN)
    create_tables(conn)

    SH_TZ = timezone(timedelta(hours=8))  # 数据为 Asia/Shanghai 时区，统一用上海时间
    today = datetime.now(SH_TZ).strftime("%Y-%m-%d")

    if not args.skip_weather:
        if args.backfill:
            start, end = BACKFILL_START, today
            print(f"[天气] 全量回填 {start} ~ {end} ...")
        else:
            start = (datetime.now(SH_TZ) - timedelta(days=args.days)).strftime("%Y-%m-%d")
            end = today
            print(f"[天气] 增量同步 {start} ~ {end} ...")
        rows = fetch_weather(start, end)
        # 只保留已开始的小时，过滤掉 open-meteo 返回的当天未来时段
        now_hour = datetime.now(SH_TZ).strftime("%Y-%m-%dT%H:00")
        rows = [r for r in rows if r["time"] <= now_hour]
        upsert_weather(conn, rows)
        # 清理数据库中已存在的未来小时记录，防止残留
        conn.execute("DELETE FROM weather_hourly WHERE time > ?", [now_hour])
        conn.commit()
        print(f"[天气] 完成，写入/更新 {len(rows)} 条小时记录，已清理未来时段。")

    if not args.skip_air:
        if args.backfill_air:
            start, end = args.backfill_air
            print(f"[空气] 回填日均 {start} ~ {end} ...")
        else:
            start = (datetime.now(SH_TZ) - timedelta(days=AIR_DAILY_LOOKBACK)).strftime("%Y-%m-%d")
            end = today
            print(f"[空气] 增量同步日均 {start} ~ {end} ...")
        rows = fetch_air_openmeteo(start, end)
        n = upsert_air_rows(conn, rows)
        print(f"[空气] 完成，写入/更新 {n} 天日均值。")

    if not args.skip_waqi:
        print("[WAQI] 拉取 6 站实时浓度 ...")
        h_row = fetch_air_waqi()
        if h_row is not None:
            upsert_air_hourly_rows(conn, [h_row])
            print(f"[WAQI] 完成，写入小时记录 {h_row['time']} AQI={h_row['aqi']}。")

    # 汇总
    wc = conn.execute("SELECT COUNT(*) FROM weather_hourly").fetchall()[0][0]
    ac = conn.execute("SELECT COUNT(*) FROM air_quality").fetchall()[0][0]
    ar = conn.execute("SELECT MIN(time), MAX(time) FROM air_quality").fetchall()[0]
    hc = conn.execute("SELECT COUNT(*) FROM air_hourly").fetchall()[0][0]
    hr = conn.execute("SELECT MIN(time), MAX(time) FROM air_hourly").fetchall()[0]
    print(f"\n[汇总] weather_hourly={wc} 行, air_quality={ac} 行 ({ar[0]} ~ {ar[1]}), "
          f"air_hourly={hc} 行 ({hr[0]} ~ {hr[1]})")


if __name__ == "__main__":
    main()