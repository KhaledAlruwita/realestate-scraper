import json
import os
import re
import signal
import sqlite3
import time
from datetime import datetime, timezone
from typing import Dict, List, Tuple

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError, Error as PWError


# ---------------------------
# Tuning //// Config
# ---------------------------
BASE = "https://www.realestate.com.au/"
DEFAULT_FEED = BASE + "عقارات"
AR_NUMBERS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

OUTPUT_PATH = "output/aqar.sqlite"

max_pages_per_cycle = 200     # default = 200
pages_delay = 0.2             # delay  betweem feeds ,default = 0.2
delay = 0.2                   # delay betweem listings ,default = 0.2

scroll_times = 2              #  default = 2
pause = 0.2                   # pause affter scroll ,default = 0.2


feed_retry_times = 2          
feed_retry_wait = 0.35    
STOP = False
no_gui = True 




def log(level: str, msg: str):
    print(f"[{level}] {msg}", flush=True)


def handle_stop(sig, frame):
    global STOP
    if not STOP:
        STOP = True
        log("INFO", "STOP signal received -> exiting safely...")


signal.signal(signal.SIGINT, handle_stop)
signal.signal(signal.SIGTERM, handle_stop)


def clean_text(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s.translate(AR_NUMBERS)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sleep_interruptible(seconds: float, step: float = 0.1):
    end = time.time() + max(0.0, seconds)
    while time.time() < end:
        if STOP:
            return
        remaining = end - time.time()
        time.sleep(step if remaining > step else remaining)


def feed_page_url(feed: str, page: int) -> str:
    return feed.rstrip("/") if page == 1 else f"{feed.rstrip('/')}/{page}"


def extract_listing_id(url: str) -> str:
    m = re.search(r"-(\d{6,})$", url)
    if m:
        return m.group(1)
    m2 = re.search(r"/listings/(\d+)", url)
    return m2.group(1) if m2 else ""


# ---------------------------
# DB
# ---------------------------
BASE_DDL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS listings (
  listing_id TEXT PRIMARY KEY,
  listing_url TEXT,

  title TEXT,

  -- price
  price_text_full TEXT,
  price_amount REAL,
  price_currency TEXT,
  price_period TEXT,     -- yearly/monthly/weekly/daily/unknown
  payment_terms TEXT,    -- e.g. "دفعة واحدة" or "دفعات"

  region TEXT,
  city TEXT,
  district TEXT,
  street TEXT,
  postal_code TEXT,
  building_no TEXT,
  additional_no TEXT,

  area TEXT,
  rooms TEXT,
  halls TEXT,
  baths TEXT,
  description TEXT,

  features TEXT,

  lat TEXT,
  lon TEXT,
  google_maps_url TEXT,

  ld_json TEXT,
  details_json TEXT,
  scraped_at TEXT,
  last_seen_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_listings_last_seen_at ON listings(last_seen_at);
"""


def db_connect(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.executescript(BASE_DDL)
    ensure_schema_upgrades(conn)
    return conn


def ensure_schema_upgrades(conn: sqlite3.Connection):
    wanted = {
        "price_text_full": "TEXT",
        "price_amount": "REAL",
        "price_currency": "TEXT",
        "price_period": "TEXT",
        "payment_terms": "TEXT",
    }

    cur = conn.execute("PRAGMA table_info(listings)")
    existing = {row[1] for row in cur.fetchall()}

    for col, typ in wanted.items():
        if col not in existing:
            log("WARN", f"DB upgrade: adding column {col}")
            conn.execute(f"ALTER TABLE listings ADD COLUMN {col} {typ}")
            conn.commit()


def db_has_listing(conn: sqlite3.Connection, listing_id: str) -> bool:
    cur = conn.execute("SELECT 1 FROM listings WHERE listing_id = ? LIMIT 1", (listing_id,))
    return cur.fetchone() is not None


def db_touch_seen(conn: sqlite3.Connection, listing_id: str):
    conn.execute("UPDATE listings SET last_seen_at=? WHERE listing_id=?", (now_iso(), listing_id))
    conn.commit()


def db_upsert(conn: sqlite3.Connection, row: Dict[str, object]):
    conn.execute(
        """
        INSERT INTO listings (
          listing_id, listing_url,
          title,
          price_text_full, price_amount, price_currency, price_period, payment_terms,
          region, city, district, street, postal_code, building_no, additional_no,
          area, rooms, halls, baths, description,
          features,
          lat, lon, google_maps_url,
          ld_json, details_json,
          scraped_at, last_seen_at
        ) VALUES (
          :listing_id, :listing_url,
          :title,
          :price_text_full, :price_amount, :price_currency, :price_period, :payment_terms,
          :region, :city, :district, :street, :postal_code, :building_no, :additional_no,
          :area, :rooms, :halls, :baths, :description,
          :features,
          :lat, :lon, :google_maps_url,
          :ld_json, :details_json,
          :scraped_at, :last_seen_at
        )
        ON CONFLICT(listing_id) DO UPDATE SET
          listing_url=excluded.listing_url,
          title=excluded.title,

          price_text_full=excluded.price_text_full,
          price_amount=excluded.price_amount,
          price_currency=excluded.price_currency,
          price_period=excluded.price_period,
          payment_terms=excluded.payment_terms,

          region=excluded.region,
          city=excluded.city,
          district=excluded.district,
          street=excluded.street,
          postal_code=excluded.postal_code,
          building_no=excluded.building_no,
          additional_no=excluded.additional_no,

          area=excluded.area,
          rooms=excluded.rooms,
          halls=excluded.halls,
          baths=excluded.baths,
          description=excluded.description,

          features=excluded.features,

          lat=excluded.lat,
          lon=excluded.lon,
          google_maps_url=excluded.google_maps_url,

          ld_json=excluded.ld_json,
          details_json=excluded.details_json,
          scraped_at=excluded.scraped_at,
          last_seen_at=excluded.last_seen_at
        """,
        row,
    )
    conn.commit()


# ---------------------------
# Parsing: ld+json
# ---------------------------
def parse_ld_json(html: str) -> Dict[str, object]:
    soup = BeautifulSoup(html, "html.parser")
    script = soup.select_one("script[type='application/ld+json']")
    if not script or not script.string:
        return {}

    try:
        data = json.loads(script.string.strip())
    except Exception:
        return {}

    out: Dict[str, object] = {}
    out["ld_json"] = json.dumps(data, ensure_ascii=False)

    out["title"] = clean_text(str(data.get("name", "")))
    out["description"] = clean_text(str(data.get("description", "")))

    offers = data.get("offers") or {}
    if isinstance(offers, dict):
        price = offers.get("price")
        currency = offers.get("priceCurrency")
        out["price_amount"] = float(price) if isinstance(price, (int, float)) else None
        out["price_currency"] = clean_text(str(currency or ""))

    addr = data.get("address") or {}
    if isinstance(addr, dict):
        out["street"] = clean_text(str(addr.get("streetAddress", "")))
        out["district"] = clean_text(str(addr.get("addressLocality", "")))
        out["region"] = clean_text(str(addr.get("addressRegion", "")))
        out["city"] = ""

    feats = data.get("amenityFeature") or []
    feature_names: List[str] = []
    rooms = baths = area = ""
    if isinstance(feats, list):
        for f in feats:
            if not isinstance(f, dict):
                continue
            name = clean_text(str(f.get("name", "")))

            if name and name not in feature_names:
                if name not in ("عدد الغرف", "عدد دورات المياه", "المساحة (متر مربع)"):
                    feature_names.append(name)

            if name == "عدد الغرف" and "value" in f:
                rooms = clean_text(str(f.get("value", "")))
            if name == "عدد دورات المياه" and "value" in f:
                baths = clean_text(str(f.get("value", "")))
            if name == "المساحة (متر مربع)" and "value" in f:
                area = clean_text(str(f.get("value", "")))

    if feature_names:
        out["features"] = "|".join(feature_names)
    if rooms:
        out["rooms"] = rooms
    if baths:
        out["baths"] = baths
    if area:
        out["area"] = f"{area}م²" if not str(area).endswith("م²") else area

    if not out.get("area"):
        fs = data.get("floorSize")
        if isinstance(fs, dict) and fs.get("value") is not None:
            out["area"] = f"{clean_text(str(fs.get('value')))}م²"

    return out


def parse_price_dom(html: str) -> Dict[str, object]:
    soup = BeautifulSoup(html, "html.parser")
    h2 = soup.select_one("h2._price__EH7rC") or soup.select_one("h2[class*='_price__']")
    if not h2:
        return {}

    full = clean_text(h2.get_text(" ", strip=True))
    full_norm = full.replace("﷼", "").replace("SAR", "").strip()

    m_amt = re.search(r"(\d[\d,\.]*)", full_norm)
    price_amount = None
    if m_amt:
        num = m_amt.group(1).replace(",", "")
        try:
            price_amount = float(num)
        except Exception:
            price_amount = None

    period = "unknown"
    if "/سنوي" in full_norm or "سنوي" in full_norm:
        period = "yearly"
    elif "/شهري" in full_norm or "شهري" in full_norm:
        period = "monthly"
    elif "/أسبوعي" in full_norm or "اسبوعي" in full_norm or "أسبوعي" in full_norm:
        period = "weekly"
    elif "/يومي" in full_norm or "يومي" in full_norm:
        period = "daily"

    terms = ""
    m_terms = re.search(r"\(([^)]+)\)", full_norm)
    if m_terms:
        terms = clean_text(m_terms.group(1))

    currency = "SAR" if "SaudiCurrency" in html or "icon-NewSaudiCurrency" in html else ""
    return {
        "price_text_full": full,
        "price_amount": price_amount,
        "price_currency": currency,
        "price_period": period,
        "payment_terms": terms,
    }


def parse_details_table(html: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    details: Dict[str, str] = {}

    items = soup.select("div[class*='_item__']")
    for it in items:
        label_el = it.select_one("span[class*='_label__']")
        if not label_el:
            continue
        label = clean_text(label_el.get_text(" ", strip=True))
        if not label:
            continue

        if label == "المشاهدات":
            num_span = it.select_one("div span")
            val = clean_text(num_span.get_text(" ", strip=True)) if num_span else ""
            if val:
                details[label] = val
            continue

        value = ""
        a = it.select_one("a[href]")
        if a:
            href = (a.get("href") or "").strip()
            a_text = clean_text(a.get_text(" ", strip=True))
            value = href if a_text in {"الرابط", "link", ""} else a_text

        if not value:
            spans = it.find_all("span")
            texts = [clean_text(s.get_text(" ", strip=True)) for s in spans if s != label_el]
            texts = [t for t in texts if t and t != label]
            if texts:
                value = texts[0]

        if value:
            details[label] = value

    return details


def parse_address_tab3(html: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    out = {
        "region": "",
        "city": "",
        "district": "",
        "street": "",
        "postal_code": "",
        "building_no": "",
        "additional_no": "",
    }

    tab = soup.select_one("div._tab3__aAH4l")
    if not tab:
        return out

    for item in tab.select("div._item___4Sv8"):
        label_el = item.select_one("span._label___qjLO")
        spans = item.select("span")
        if not label_el or len(spans) < 2:
            continue
        k = clean_text(label_el.get_text(" ", strip=True))
        v = clean_text(spans[1].get_text(" ", strip=True))

        if k == "المنطقة":
            out["region"] = v
        elif k == "المدينة":
            out["city"] = v
        elif k == "الحي":
            out["district"] = v
        elif k == "الشارع":
            out["street"] = v
        elif k == "الرمز البريدي":
            out["postal_code"] = v
        elif k == "رقم المبنى":
            out["building_no"] = v
        elif k == "الرقم الإضافي":
            out["additional_no"] = v

    return out


def parse_features_dom(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    target_h4 = None
    for h in soup.find_all("h4"):
        if clean_text(h.get_text(" ", strip=True)) == "المميزات":
            target_h4 = h
            break
    if not target_h4:
        return ""
    container = target_h4.find_next("div")
    if not container:
        return ""
    feats = []
    for el in container.select("div._label___qjLO"):
        t = clean_text(el.get_text(" ", strip=True))
        if t:
            feats.append(t)
    seen = set()
    out = []
    for f in feats:
        if f not in seen:
            seen.add(f)
            out.append(f)
    return "|".join(out)


def parse_map_coords(html: str) -> Tuple[str, str, str]:
    lat = lon = ""
    m = re.search(r'"lat"\s*:\s*([0-9.]+)\s*,\s*"lng"\s*:\s*([0-9.]+)', html)
    if m:
        lat, lon = m.group(1), m.group(2)
    google = f"https://maps.google.com/?q={lat},{lon}" if lat and lon else ""
    return google, lat, lon


# ---------------------------
# Feed
# ---------------------------
def scroll_to_load(page, scroll_times: int = 2, pause: float = 0.2):
    for _ in range(scroll_times):
        if STOP:
            return
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        sleep_interruptible(pause)


def extract_urls_from_html(html: str) -> List[str]:
    
    found = re.findall(r'href="([^"]+-\d{6,})"', html)
    out: List[str] = []
    seen = set()
    for u in found:
        if not u:
            continue
        if u.startswith("/"):
            u = BASE.rstrip("/") + u
        u = u.split("#", 1)[0]
        if re.search(r"-\d{6,}$", u) and u not in seen:
            seen.add(u)
            out.append(u)
    return out


def collect_listing_urls(page) -> List[str]:
    # 1) 
    sel = "a:has(div[class*='_listingCard__'])"
    try:
        urls = page.eval_on_selector_all(sel, "els => els.map(e => e.href)")
    except Exception:
        urls = []

    out: List[str] = []
    seen = set()

    if urls:
        for u in urls:
            if not u:
                continue
            u = u.split("#", 1)[0]
            if re.search(r"-\d{6,}$", u) and u not in seen:
                seen.add(u)
                out.append(u)
        if out:
            return out

    # 2) fallback: regex
    try:
        html = page.content()
        out = extract_urls_from_html(html)
        if out:
            return out
    except Exception:
        pass

    return []


# ---------------------------
# Listing scrape
# ---------------------------
def scrape_listing_fast(page, url: str) -> Dict[str, object]:
    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    try:
        page.wait_for_selector("script[type='application/ld+json'], h1, h2", timeout=12000)
    except PWTimeoutError:
        pass

    html = page.content()

    ld = parse_ld_json(html)
    dom_price = parse_price_dom(html)

    details = parse_details_table(html)
    addr3 = parse_address_tab3(html)
    maps_url, lat, lon = parse_map_coords(html)

    features = clean_text(str(ld.get("features", "")))
    if not features:
        features = parse_features_dom(html)

    listing_id = clean_text(details.get("رقم الإعلان", "")) or extract_listing_id(url)

    title = clean_text(str(ld.get("title", "")))
    description = clean_text(str(ld.get("description", "")))

    price_text_full = dom_price.get("price_text_full") or ""
    price_amount = dom_price.get("price_amount")
    price_currency = dom_price.get("price_currency") or (ld.get("price_currency") or "SAR")
    price_period = dom_price.get("price_period") or "unknown"
    payment_terms = dom_price.get("payment_terms") or ""

    if price_amount is None:
        price_amount = ld.get("price_amount")

    if isinstance(price_amount, str):
        try:
            price_amount = float(price_amount.replace(",", ""))
        except Exception:
            price_amount = None

    row: Dict[str, object] = {
        "listing_id": listing_id,
        "listing_url": url,

        "title": title,

        "price_text_full": price_text_full,
        "price_amount": price_amount,
        "price_currency": clean_text(str(price_currency or "")),
        "price_period": clean_text(str(price_period or "unknown")),
        "payment_terms": clean_text(str(payment_terms or "")),

        "region": addr3.get("region", "") or clean_text(str(ld.get("region", ""))),
        "city": addr3.get("city", "") or clean_text(str(ld.get("city", ""))),
        "district": addr3.get("district", "") or clean_text(str(ld.get("district", ""))),
        "street": addr3.get("street", "") or clean_text(str(ld.get("street", ""))),
        "postal_code": addr3.get("postal_code", ""),
        "building_no": addr3.get("building_no", ""),
        "additional_no": addr3.get("additional_no", ""),

        "area": clean_text(str(ld.get("area", ""))),
        "rooms": clean_text(str(ld.get("rooms", ""))),
        "halls": clean_text(str(ld.get("halls", ""))),
        "baths": clean_text(str(ld.get("baths", ""))),

        "description": description,
        "features": features,

        "lat": lat,
        "lon": lon,
        "google_maps_url": maps_url,

        "ld_json": clean_text(str(ld.get("ld_json", ""))),
        "details_json": json.dumps(details, ensure_ascii=False),

        "scraped_at": now_iso(),
        "last_seen_at": now_iso(),
    }

    return row


# ---------------------------
# Main
# ---------------------------
def main():
    conn = db_connect(OUTPUT_PATH)
    log("INFO", f"DB: {OUTPUT_PATH}")

    pw = None
    browser = None
    context = None
    page = None

    try:
        pw = sync_playwright().start()
        browser = pw.chromium.launch(headless=no_gui)
        context = browser.new_context(locale="ar-SA")
        page = context.new_page()

        def route_handler(route):
            r = route.request
            if r.resource_type in ("image", "media", "font", "stylesheet", "other"):
                return route.abort()
            return route.continue_()

        page.route("**/*", route_handler)

        cycle = 0
        while not STOP:
            cycle += 1
            log("INFO", f"===== CYCLE {cycle} START =====")

            for pnum in range(1, max_pages_per_cycle + 1):
                if STOP:
                    break

                feed_url = feed_page_url(DEFAULT_FEED, pnum)
                log("STEP", f"Loading feed page {pnum}: {feed_url}")

                try:
                    page.goto(feed_url, wait_until="domcontentloaded", timeout=60000)
                    # لمسة صغيرة تعالج تأخر React بدون ما تقتل السرعة
                    page.wait_for_timeout(120)
                except PWTimeoutError:
                    log("WARN", "Feed load timeout")
                    continue
                except PWError as e:
                    log("ERROR", f"Feed load error: {e}")
                    continue

                scroll_to_load(page, scroll_times, pause)
                if STOP:
                    break

                # retry لو طلع 0
                urls: List[str] = []
                for _ in range(feed_retry_times + 1):
                    urls = collect_listing_urls(page)
                    if urls:
                        break
                    page.wait_for_timeout(int(feed_retry_wait * 1000))

                log("INFO", f"Found {len(urls)} listing URLs on page {pnum}")

                for u in urls:
                    if STOP:
                        break

                    lid = extract_listing_id(u)
                    if not lid:
                        continue

                    if db_has_listing(conn, lid):
                        db_touch_seen(conn, lid)
                        continue

                    try:
                        row = scrape_listing_fast(page, u)
                        if row.get("listing_id"):
                            db_upsert(conn, row)
                            log("DONE", f"Saved {row['listing_id']}|{datetime.now().isoformat()}")
                    except PWError as e:
                        log("ERROR", f"Listing error {lid}: {e}")
                    except Exception as e:
                        log("ERROR", f"Listing error {lid}: {e}")

                    sleep_interruptible(delay)

                sleep_interruptible(pages_delay)

            log("INFO", f"===== CYCLE {cycle} END =====")
            sleep_interruptible(pages_delay)

    finally:
        log("INFO", "Shutting down...")
        try:
            if page:
                page.close()
        except Exception:
            pass
        try:
            if context:
                context.close()
        except Exception:
            pass
        try:
            if browser:
                browser.close()
        except Exception:
            pass
        try:
            if pw:
                pw.stop()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
        log("INFO", "Stopped.")


if __name__ == "__main__":
    main()
