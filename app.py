#!/usr/bin/env python3
"""
OpenRice Route Planner - Flask Backend
- 餐廳數據 API (Excel)
- Google Maps Geocoding API
- 座標缓存
"""

import os
import ssl
import json
import time
import threading
import queue
import concurrent.futures
from pathlib import Path
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS

# macOS SSL fix
ssl._create_default_https_context = ssl._create_unverified_context

# ============ CONFIG ============
API_KEY = os.environ.get("API_KEY", "")
JSON_PATH = Path(__file__).parent / "restaurants.json"
EXCEL_PATH = Path(__file__).parent / "openrice_restaurants.xlsx"
CACHE_PATH = Path(__file__).parent / "geocode_cache.json"
PORT = int(os.environ.get("PORT", 19801))
# ===============================

# ============ BACKGROUND GEOCODING ============
_geo_progress = {"total": 0, "done": 0, "errors": 0, "running": False, "finished": False}
_geo_lock = threading.Lock()
_save_lock = threading.Lock()
_geo_queue = queue.Queue()

app = Flask(__name__, static_folder="static")
CORS(app)  # Enable cross-origin requests for the frontend

# ============ DATA LOADING ============
_restaurants_cache = None

def load_restaurants():
    """載入 JSON 餐廳數據（記憶體緩存）"""
    global _restaurants_cache
    if _restaurants_cache is not None:
        return _restaurants_cache

    if JSON_PATH.exists():
        try:
            with open(JSON_PATH, encoding='utf-8') as f:
                _restaurants_cache = json.load(f)
            print(f"✅ Loaded {len(_restaurants_cache)} restaurants from JSON")
            return _restaurants_cache
        except Exception as e:
            print(f"❌ JSON load error: {e}")
    
    # Fallback to Excel
    if EXCEL_PATH.exists():
        try:
            import openpyxl
            wb = openpyxl.load_workbook(EXCEL_PATH, data_only=True)
            ws = wb.active
            restaurants = []
            headers = None
            for i, row in enumerate(ws.iter_rows(values_only=True)):
                if i == 0:
                    headers = {str(cell): idx for idx, cell in enumerate(row)}
                    continue
                if not row or not row[0]:
                    continue
                def g(key, default=None):
                    idx = headers.get(key, default)
                    return row[idx] if idx is not None and idx < len(row) else None
                restaurants.append({
                    "poiId": str(g("poiId") or "").strip(),
                    "name": str(g("店名") or "").strip(),
                    "district": str(g("地區") or "").strip(),
                    "address": str(g("地址") or "").strip(),
                    "phone": str(g("電話") or "").strip(),
                    "hours": str(g("營業時間") or "").strip(),
                    "url": str(g("url") or "").strip(),
                    "opening_year": str(g("opening_year") or "").strip(),
                    "age": str(g("age") or "").strip(),
                    "age_bucket": str(g("age_bucket") or "").strip(),
                    "has_weekend": bool(g("has_weekend")),
                    "is_late_night": bool(g("is_late_night")),
                    "is_early": bool(g("is_early")),
                    "status": str(g("STATUS") or "").strip(),
                })
            _restaurants_cache = restaurants
            print(f"✅ Loaded {len(restaurants)} restaurants from Excel (fallback)")
            return _restaurants_cache
        except Exception as e:
            print(f"❌ Excel load error: {e}")
    
    _restaurants_cache = []
    print(f"❌ No restaurant data found!")
    return _restaurants_cache


def load_cache():
    if CACHE_PATH.exists():
        try:
            with open(CACHE_PATH, encoding='utf-8') as f:
                return json.load(f)
        except:
            return {}
    return {}


def save_cache(cache):
    """Thread-safe atomic save: copy dict, then write without lock"""
    import tempfile, os
    # Take a snapshot of the cache under lock to avoid dict-changed-during-iterate
    with _save_lock:
        cache_snapshot = dict(cache)
    # Write snapshot (no lock needed, it's a local copy)
    fd, tmp_path = tempfile.mkstemp(dir=CACHE_PATH.parent, suffix='.tmp')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(cache_snapshot, f, ensure_ascii=False)
        os.replace(tmp_path, CACHE_PATH)
    except Exception:
        try: os.unlink(tmp_path)
        except: pass
        raise


# ============ ROUTES ============

@app.route("/")
def index():
    return send_from_directory(".", "index.html")


@app.route("/api/restaurants")
def get_restaurants():
    """獲取餐廳列表，可按地區/宵夜店/狀態篩選"""
    district = request.args.get("district", "").strip()
    late_night = request.args.get("late_night", "").strip()  # "1" = 宵夜店 only
    status = request.args.get("status", "").strip()  # "已認領" / "已進駐" / "未處理"

    restaurants = load_restaurants()
    cache = load_cache()

    result = []
    for r in restaurants:
        poi = r["poiId"]
        if poi in cache:
            r["lat"] = cache[poi]["lat"]
            r["lng"] = cache[poi]["lng"]
            r["geocoded"] = True
        else:
            r["lat"] = None
            r["lng"] = None
            r["geocoded"] = False

        if district and r["district"] != district:
            continue
        if late_night == "1" and not r["is_late_night"]:
            continue
        if status == "已認領" and r["status"] != "已認領":
            continue
        if status == "已進駐" and r["status"] != "已進駐":
            continue
        if status == "未處理" and (not r["status"] or r["status"] == "None"):
            continue
        result.append(r)

    return jsonify(result)


@app.route("/api/debug")
def debug():
    """Debug endpoint - returns status of data loading"""
    import os
    data_dir = Path(__file__).parent
    return jsonify({
        "json_exists": JSON_PATH.exists(),
        "excel_exists": EXCEL_PATH.exists(),
        "json_size": JSON_PATH.stat().st_size if JSON_PATH.exists() else 0,
        "excel_size": EXCEL_PATH.stat().st_size if EXCEL_PATH.exists() else 0,
        "cache_exists": CACHE_PATH.exists(),
        "cache_size": CACHE_PATH.stat().st_size if CACHE_PATH.exists() else 0,
        "cwd": os.getcwd(),
        "data_dir": str(data_dir),
        "files_in_dir": [f.name for f in data_dir.iterdir() if f.is_file()],
        "restaurants_cached": len(_restaurants_cache) if _restaurants_cache is not None else None,
    })


@app.route("/api/districts")
def get_districts():
    restaurants = load_restaurants()
    districts = sorted(set(r["district"] for r in restaurants if r["district"]))
    return jsonify(districts)


@app.route("/api/geocode", methods=["GET"])
def geocode():
    poi_id = request.args.get("poiId", "").strip()
    address = request.args.get("address", "").strip()

    if not address:
        return jsonify({"error": "地址不能為空"}), 400

    cache = load_cache()
    if poi_id and poi_id in cache:
        return jsonify({
            "poiId": poi_id,
            "lat": cache[poi_id]["lat"],
            "lng": cache[poi_id]["lng"],
            "cached": True
        })

    import urllib.parse
    import urllib.request

    url = "https://maps.googleapis.com/maps/api/geocode/json?" + \
          urllib.parse.urlencode({
              "address": address + ", 香港",
              "key": API_KEY,
              "language": "zh-TW",
              "region": "hk"
          })

    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read().decode('utf-8'))

            if data["status"] == "OK" and data["results"]:
                result = data["results"][0]
                location = result["geometry"]["location"]
                lat = location["lat"]
                lng = location["lng"]

                if poi_id:
                    cache[poi_id] = {"lat": lat, "lng": lng}
                    save_cache(cache)

                return jsonify({
                    "poiId": poi_id,
                    "lat": lat,
                    "lng": lng,
                    "formatted": result.get("formatted_address", ""),
                    "cached": False
                })
            else:
                return jsonify({
                    "error": f"Geocoding 失敗: {data.get('status', 'UNKNOWN')}",
                    "poiId": poi_id
                }), 200

    except Exception as e:
        return jsonify({"error": str(e), "poiId": poi_id}), 200


@app.route("/api/geocode_batch", methods=["POST"])
def geocode_batch():
    data = request.get_json()
    items = data.get("items", [])

    results = []
    cache = load_cache()
    updated = False

    for item in items:
        poi_id = item.get("poiId", "")
        address = item.get("address", "")

        if not address:
            results.append({"poiId": poi_id, "error": "無地址"})
            continue

        if poi_id and poi_id in cache:
            results.append({
                "poiId": poi_id,
                "lat": cache[poi_id]["lat"],
                "lng": cache[poi_id]["lng"],
                "cached": True
            })
            continue

        import urllib.parse
        import urllib.request

        url = "https://maps.googleapis.com/maps/api/geocode/json?" + \
              urllib.parse.urlencode({
                  "address": address + ", 香港",
                  "key": API_KEY,
                  "language": "zh-TW"
              })

        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                gdata = json.loads(resp.read().decode('utf-8'))

                if gdata["status"] == "OK" and gdata["results"]:
                    loc = gdata["results"][0]["geometry"]["location"]
                    lat, lng = loc["lat"], loc["lng"]

                    if poi_id:
                        cache[poi_id] = {"lat": lat, "lng": lng}
                        updated = True

                    results.append({
                        "poiId": poi_id,
                        "lat": lat,
                        "lng": lng,
                        "cached": False
                    })
                else:
                    results.append({
                        "poiId": poi_id,
                        "error": gdata.get("status", "FAIL")
                    })
        except Exception as e:
            results.append({"poiId": poi_id, "error": str(e)})

        time.sleep(0.1)

    if updated:
        save_cache(cache)

    return jsonify(results)


@app.route("/api/stats")
def get_stats():
    """返回數據統計"""
    restaurants = load_restaurants()
    late_night = sum(1 for r in restaurants if r["is_late_night"])
    early = sum(1 for r in restaurants if r["is_early"])
    districts = len(set(r["district"] for r in restaurants if r["district"]))
    
    with _geo_lock:
        prog = dict(_geo_progress)
    
    return jsonify({
        "total": len(restaurants),
        "late_night": late_night,
        "early": early,
        "districts": districts,
        "geo_progress": prog,
    })


@app.route("/api/geocode_status")
def geocode_status():
    """返回背景 Geocoding 進度"""
    with _geo_lock:
        return jsonify(dict(_geo_progress))


@app.route("/api/geocode_all", methods=["POST"])
def geocode_all():
    """手動觸發全量背景 Geocoding"""
    with _geo_lock:
        if _geo_progress["running"]:
            return jsonify({"status": "already_running", "progress": _geo_progress})
        # Reset progress
        _geo_progress["running"] = True
        _geo_progress["finished"] = False
        _geo_progress["done"] = 0
        _geo_progress["errors"] = 0
    
    restaurants = load_restaurants()
    cache = load_cache()
    
    # Count how many need geocoding
    needs_geo = [r for r in restaurants if r["poiId"] and r["poiId"] not in cache and r["address"]]
    
    with _geo_lock:
        _geo_progress["total"] = len(needs_geo)
    
    def background_task():
        cache = load_cache()
        updated = False
        
        for i, r in enumerate(needs_geo):
            poi_id = r["poiId"]
            address = r["address"]
            
            if poi_id in cache:
                with _geo_lock:
                    _geo_progress["done"] = i + 1
                continue
            
            import urllib.parse
            import urllib.request
            
            url = "https://maps.googleapis.com/maps/api/geocode/json?" + \
                  urllib.parse.urlencode({
                      "address": address + ", 香港",
                      "key": API_KEY,
                      "language": "zh-TW"
                  })
            
            try:
                with urllib.request.urlopen(url, timeout=10) as resp:
                    gdata = json.loads(resp.read().decode('utf-8'))
                    if gdata["status"] == "OK" and gdata["results"]:
                        loc = gdata["results"][0]["geometry"]["location"]
                        cache[poi_id] = {"lat": loc["lat"], "lng": loc["lng"]}
                        updated = True
                    else:
                        with _geo_lock:
                            _geo_progress["errors"] += 1
            except Exception as e:
                with _geo_lock:
                    _geo_progress["errors"] += 1
            
            with _geo_lock:
                _geo_progress["done"] = i + 1
            
            time.sleep(0.12)  # Rate limit friendly
        
        if updated:
            save_cache(cache)
        
        with _geo_lock:
            _geo_progress["running"] = False
            _geo_progress["finished"] = True
        
        print(f"✅ Background geocoding done! Total: {_geo_progress['total']}, Errors: {_geo_progress['errors']}")
    
    thread = threading.Thread(target=background_task, daemon=True)
    thread.start()
    
    return jsonify({"status": "started", "total": len(needs_geo)})


# ============ BACKGROUND GEOCODING (multi-threaded) ============
def _geocode_one(poi_id, address, cache):
    """Geocode a single address, return (poi_id, lat, lng, error)"""
    import urllib.parse
    import urllib.request
    
    url = "https://maps.googleapis.com/maps/api/geocode/json?" + \
          urllib.parse.urlencode({
              "address": address + ", 香港",
              "key": API_KEY,
              "language": "zh-TW"
          })
    
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            gdata = json.loads(resp.read().decode('utf-8'))
            if gdata["status"] == "OK" and gdata["results"]:
                loc = gdata["results"][0]["geometry"]["location"]
                return (poi_id, loc["lat"], loc["lng"], None)
            return (poi_id, None, None, gdata.get("status", "FAIL"))
    except Exception as e:
        return (poi_id, None, None, str(e))


def _geocode_worker(worker_id, cache, updated_flag):
    """Worker thread: process items from queue"""
    while True:
        try:
            item = _geo_queue.get(timeout=1)
            if item is None:  # Poison pill - shutdown signal
                _geo_queue.task_done()
                break
            
            poi_id, address = item
            
            # Check cache again (another worker might have done it)
            if poi_id in cache:
                with _geo_lock:
                    _geo_progress["done"] += 1
                _geo_queue.task_done()
                continue
            
            poi_id_result, lat, lng, error = _geocode_one(poi_id, address, cache)
            
            if lat is not None:
                cache[poi_id_result] = {"lat": lat, "lng": lng}
                updated_flag["value"] = True
            else:
                with _geo_lock:
                    _geo_progress["errors"] += 1
            
            with _geo_lock:
                _geo_progress["done"] += 1
            
            time.sleep(0.05)  # Brief pause between requests
            _geo_queue.task_done()
            
        except queue.Empty:
            break


def start_background_geocode():
    """Startup: check if we need to resume geocoding"""
    cache = load_cache()
    restaurants = load_restaurants()
    needs_geo = [r for r in restaurants if r["poiId"] and r["poiId"] not in cache and r["address"]]
    
    if not needs_geo:
        print("✅ 所有餐廳已 Geocode 完成！")
        return
    
    print(f"🔍 發現 {len(needs_geo)} 間餐廳需要 Geocode，啟動多線程背景任務...")
    
    with _geo_lock:
        _geo_progress["total"] = len(needs_geo)
        _geo_progress["done"] = 0
        _geo_progress["errors"] = 0
        _geo_progress["running"] = True
        _geo_progress["finished"] = False
    
    # Fill queue
    for r in needs_geo:
        _geo_queue.put((r["poiId"], r["address"]))
    
    updated_flag = {"value": False}
    
    def background_task():
        cache = load_cache()
        updated_flag_local = {"value": False}
        
        # Start worker threads
        num_workers = min(8, len(needs_geo))
        threads = []
        for i in range(num_workers):
            t = threading.Thread(
                target=_geocode_worker,
                args=(i, cache, updated_flag_local),
                daemon=True
            )
            t.start()
            threads.append(t)
        
        # Monitor thread: save cache every 3 seconds
        def monitor():
            last_saved_len = 0
            while True:
                time.sleep(3)
                with _geo_lock:
                    done = _geo_progress["done"]
                    total = _geo_progress["total"]
                    finished = _geo_progress["finished"]
                if finished:
                    break
                # Save cache periodically (workers add to cache dict directly)
                save_cache(cache)
                if done - last_saved_len > 0:
                    print(f"   Progress: {done}/{total} ({done*100//total}%)")
                    last_saved_len = done
        
        monitor_thread = threading.Thread(target=monitor, daemon=True)
        monitor_thread.start()
        
        # Wait for all work to be done
        _geo_queue.join()
        
        # Signal workers to stop
        for _ in range(num_workers):
            _geo_queue.put(None)
        
        for t in threads:
            t.join(timeout=2)
        
        # Final save
        save_cache(cache)
        
        with _geo_lock:
            _geo_progress["running"] = False
            _geo_progress["finished"] = True
        
        print(f"✅ Background geocoding 完成！Total: {_geo_progress['total']}, Errors: {_geo_progress['errors']}")
    
    thread = threading.Thread(target=background_task, daemon=True)
    thread.start()


# ============ START ============
if __name__ == "__main__":
    print(f"🚀 啟動 OpenRice Route Planner: http://localhost:{PORT}")
    start_background_geocode()  # Start BEFORE app.run() so it runs in background
    app.run(host="0.0.0.0", port=PORT, debug=False)

