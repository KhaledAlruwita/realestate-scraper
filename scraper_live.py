#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Aqar live scraper (runs forever until you stop it with CTRL+C)

Captures per listing:
- listing_url, listing_id
- title, price
- region (المنطقة), city (المدينة), district (الحي), street (الشارع),
  postal_code (الرمز البريدي), building_no (رقم المبنى), additional_no (الرقم الإضافي)
  from Tab3: div._tab3__aAH4l  (best source)
- area, rooms, halls, baths (from specs grid)
- description (meta description)
- features (المميزات) pipe-separated
- google_maps_url + lat + lon (coords extracted from inline JS; then builds google maps URL)
- details_json (رقم الإعلان / رخصة الإعلان / تاريخ الإضافة … table)
- scraped_at (UTC ISO)

Windows-friendly:
- BeautifulSoup "html.parser" (no lxml)
- Playwright Chromium
- verbose logging
- debug dumps (screenshot + html) on empty feed / listing errors
"""

import argparse
import csv
import json
import os
import re
import signal
import time
from datetime import datetime, timezone
from typing import Dict, List, Set, Tuple

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError, Error as PWError

BASE = "https://sa.aqar.fm/"
DEFAULT_FEED = BASE + "عقارات"

AR_NUMBERS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
STOP = False


# ----------------------------
# Logging / shutdown handling
# ----------------------------
def log(level: str, msg: str):
    print(f"[{level}] {msg}", flush=True)


def handle_stop(sig, frame):
    global STOP
    if not STOP:
        STOP = True
        log("INFO", "STOP signal received -> exiting safely...")


signal.signal(signal.SIGINT, handle_stop)
signal.signal(signal.SIGTERM, handle_stop)


# ----------------------------
# Helpers
# ----------------------------
def clean_text(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s.translate(AR_NUMBERS)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def ensure_csv(path: str, fieldnames: List[str]):
    if os.path.exists(path) and os.path.getsize(path) > 0:
        log("INFO", f"CSV exists: {path}")
        return
    log("INFO", f"Creating CSV: {path}")
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        csv.DictWriter(f, fieldnames=fieldnames).writeheader()


def append_csv(path: str, fieldnames: List[str], row: Dict[str, str]):
    with open(path, "a", newline="", encoding="utf-8-sig") as f:
        csv.DictWriter(f, fieldnames=fieldnames).writerow({k: row.get(k, "") for k in fieldnames})
    log("OK", f"CSV row written (listing_id={row.get('listing_id')})")


def load_seen(path: str) -> Set[str]:
    if not os.path.exists(path):
        return set()
    with open(path, "r", encoding="utf-8") as f:
        seen = {line.strip() for line in f if line.strip()}
    log("INFO", f"Loaded {len(seen)} seen IDs")
    return seen


def save_seen(path: str, seen: Set[str]):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for x in sorted(seen):
            f.write(x + "\n")
    os.replace(tmp, path)


def extract_listing_id(url: str) -> str:
    # Feed URLs usually end with -<digits>
    m = re.search(r"-(\d{6,})$", url)
    if m:
        return m.group(1)

    # fallback: /listings/<id>
    m2 = re.search(r"/listings/(\d+)", url)
    return m2.group(1) if m2 else ""


def feed_page_url(feed: str, page: int) -> str:
    return feed.rstrip("/") if page == 1 else f"{feed.rstrip('/')}/{page}"


def sleep_interruptible(seconds: float, step: float = 0.1):
    """
    Sleep in small chunks so STOP exits quickly (no recursion).
    """
    end = time.time() + max(0.0, seconds)
    while time.time() < end:
        if STOP:
            return
        remaining = end - time.time()
        time.sleep(step if remaining > step else remaining)
 



def debug_dump(page, tag: str):
    ensure_dir("debug")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    png = os.path.join("debug", f"{tag}_{ts}.png")
    html_path = os.path.join("debug", f"{tag}_{ts}.html")

    try:
        page.screenshot(path=png, full_page=True)
        log("WARN", f"Saved screenshot: {png}")
    except Exception as e:
        log("WARN", f"Screenshot failed: {e}")

    try:
        content = page.content()
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(content)
        log("WARN", f"Saved html dump: {html_path}")
    except Exception as e:
        log("WARN", f"HTML dump failed: {e}")

    try:
        log("WARN", f"Page title: {page.title()}")
    except Exception:
        pass


def scroll_to_load(page, scroll_times: int = 6, pause: float = 0.6):
    for _ in range(scroll_times):
        if STOP:
            return
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        sleep_interruptible(pause)


# ----------------------------
# Parsing
# ----------------------------
def parse_details(html: str) -> Dict[str, str]:
    """
    Captures the "details table" items like:
    رقم الإعلان، رخصة الإعلان، تاريخ الإضافة، آخر تحديث، المشاهدات...
    Fixes المشاهدات to take the numeric span (not "عرض المزيد")
    """
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


def parse_features(html: str) -> List[str]:
    """
    Extract features under <h4>المميزات</h4>
    then the next div container has many div._label___qjLO entries.
    """
    soup = BeautifulSoup(html, "html.parser")

    target_h4 = None
    for h in soup.find_all("h4"):
        if clean_text(h.get_text(" ", strip=True)) == "المميزات":
            target_h4 = h
            break
    if not target_h4:
        return []

    container = target_h4.find_next("div")
    if not container:
        return []

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
    return out


def parse_map_coords(html: str) -> Tuple[str, str, str]:
    """
    Many listing pages don't contain a Google Maps <a>. Coordinates appear in inline JS.
    Extract "lat": X, "lng": Y then build google maps URL.
    """
    lat = lon = ""
    m = re.search(r'"lat"\s*:\s*([0-9.]+)\s*,\s*"lng"\s*:\s*([0-9.]+)', html)
    if m:
        lat, lon = m.group(1), m.group(2)

    google = f"https://maps.google.com/?q={lat},{lon}" if lat and lon else ""
    return google, lat, lon


def parse_address_tab3(html: str) -> Dict[str, str]:
    """
    Parses address block:
    <div class="_tab3__aAH4l">
      <div class="_item___4Sv8"><span class="_label___qjLO">المدينة</span><span>الدمام</span></div>...
    </div>
    """
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


def parse_main_listing_fields(html: str) -> Dict[str, str]:
    """
    Extracts title/price/description/specs (area/rooms/halls/baths).
    Also tries fallback city/district from title if Tab3 is missing.
    """
    soup = BeautifulSoup(html, "html.parser")

    out = {
        "title": "",
        "price": "",
        "city": "",
        "district": "",
        "area": "",
        "rooms": "",
        "halls": "",
        "baths": "",
        "description": "",
    }

    # Title
    h1 = soup.find("h1")
    if h1:
        out["title"] = clean_text(h1.get_text(" ", strip=True))

    # Price
    price_el = soup.select_one("h2[class*='_price__']") or soup.select_one("div[class*='_pricing__']")
    if price_el:
        out["price"] = clean_text(price_el.get_text(" ", strip=True))

    # Description: meta is most stable and complete
    meta_desc = soup.select_one("meta[name='description']")
    if meta_desc and meta_desc.get("content"):
        out["description"] = clean_text(meta_desc.get("content"))

    # Specs grid: div._newSpecCard__hWWBI div._item___4Sv8
    for item in soup.select("div._newSpecCard__hWWBI div._item___4Sv8"):
        label = item.select_one("div._label___qjLO")
        value = item.select_one("div._value__yF2Fx")
        if not label or not value:
            continue

        k = clean_text(label.get_text(" ", strip=True))
        v = clean_text(value.get_text(" ", strip=True))

        if k == "المساحة":
            out["area"] = v
        elif k == "غرف النوم":
            out["rooms"] = v
        elif k == "الصالات":
            out["halls"] = v
        elif k == "دورات المياه":
            out["baths"] = v

    # fallback city/district from title pattern
    if out["title"]:
        m_city = re.search(r"مدينة\s+([^,]+)", out["title"])
        if m_city:
            out["city"] = clean_text(m_city.group(1))
        m_dist = re.search(r"حي\s+([^,]+)", out["title"])
        if m_dist:
            out["district"] = clean_text(m_dist.group(1))

    return out


# ----------------------------
# Feed scraping (listing URLs)
# ----------------------------
def collect_listing_urls(page) -> List[str]:
    """
    Feed anchors wrap div[class*='_listingCard__'] and URLs end with -<id>.
    """
    sel = "a:has(div[class*='_listingCard__'])"
    try:
        urls = page.eval_on_selector_all(sel, "els => els.map(e => e.href)")
    except Exception:
        urls = []

    if not urls:
        try:
            urls = page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)")
        except Exception:
            urls = []

    out = []
    seen = set()
    for u in urls:
        if not u:
            continue
        u = u.split("#", 1)[0]
        if not re.search(r"-\d{6,}$", u):
            continue
        if u not in seen:
            seen.add(u)
            out.append(u)

    log("INFO", f"Found {len(out)} listing URLs")
    if out[:3]:
        log("INFO", "Sample URLs:\n  " + "\n  ".join(out[:3]))
    return out


# ----------------------------
# Listing scrape (visit detail page)
# ----------------------------
def scrape_listing(page, url: str) -> Dict[str, str]:
    log("STEP", f"Opening listing: {url}")
    page.goto(url, wait_until="domcontentloaded", timeout=60000)

    try:
        page.wait_for_selector("h1, div[class*='_newSpecCard__'], div[class*='_body__']", timeout=20000)
    except PWTimeoutError:
        log("WARN", "Key elements not visible yet (continuing)")

    # wait a bit for tabs/map/features to render
    try:
        page.wait_for_selector("div._tab3__aAH4l, h4:has-text('المميزات'), div._mapWrapper__vBG_2", timeout=8000)
    except PWTimeoutError:
        pass

    html = page.content()

    details = parse_details(html)
    main = parse_main_listing_fields(html)
    address = parse_address_tab3(html)
    features_list = parse_features(html)
    maps_url, lat, lon = parse_map_coords(html)

    listing_id = clean_text(details.get("رقم الإعلان", "")) or extract_listing_id(url)

    # prefer tab3 for address if available
    city = address["city"] or main.get("city", "")
    district = address["district"] or main.get("district", "")

    log(
        "OK",
        f"Parsed id={listing_id} | addr_tab3={'yes' if address['city'] or address['district'] else 'no'} "
        f"| features={len(features_list)} | map={'yes' if maps_url else 'no'} | details={len(details)}"
    )

    return {
        "listing_url": url,
        "listing_id": listing_id,
        "title": main.get("title", ""),
        "price": main.get("price", ""),

        "region": address["region"],
        "city": city,
        "district": district,
        "street": address["street"],
        "postal_code": address["postal_code"],
        "building_no": address["building_no"],
        "additional_no": address["additional_no"],

        "area": main.get("area", ""),
        "rooms": main.get("rooms", ""),
        "halls": main.get("halls", ""),
        "baths": main.get("baths", ""),

        "description": main.get("description", ""),
        "features": "|".join(features_list),

        "google_maps_url": maps_url,
        "lat": lat,
        "lon": lon,

        "details_json": json.dumps(details, ensure_ascii=False),
        "scraped_at": now_iso(),
    }


# ----------------------------
# Main loop (forever)
# ----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--feed", default=DEFAULT_FEED)
    ap.add_argument("--out", default="aqar_live.csv")
    ap.add_argument("--seen", default="seen_ids.txt")
    ap.add_argument("--max-pages-per-cycle", type=int, default=20)
    ap.add_argument("--delay", type=float, default=1.5)
    ap.add_argument("--page-delay", type=float, default=1.0)
    ap.add_argument("--headful", action="store_true")
    ap.add_argument("--debug-on-empty", action="store_true", help="Save screenshot/html if feed page has 0 listings")
    ap.add_argument("--debug-on-error", action="store_true", help="Save screenshot/html if listing parse fails")
    args = ap.parse_args()

    fields = [
        "listing_url",
        "listing_id",
        "title",
        "price",
        "region",
        "city",
        "district",
        "street",
        "postal_code",
        "building_no",
        "additional_no",
        "area",
        "rooms",
        "halls",
        "baths",
        "description",
        "features",
        "google_maps_url",
        "lat",
        "lon",
        "details_json",
        "scraped_at",
    ]

    ensure_csv(args.out, fields)
    seen = load_seen(args.seen)

    pw = None
    browser = None
    context = None
    page = None

    try:
        log("INFO", "Starting Playwright...")
        pw = sync_playwright().start()
        browser = pw.chromium.launch(headless=not args.headful)
        context = browser.new_context(locale="ar-SA")
        page = context.new_page()

        cycle = 0
        while not STOP:
            cycle += 1
            log("INFO", f"===== CYCLE {cycle} START =====")

            for pnum in range(1, args.max_pages_per_cycle + 1):
                if STOP:
                    break

                feed_url = feed_page_url(args.feed, pnum)
                log("STEP", f"Loading feed page {pnum}: {feed_url}")

                try:
                    page.goto(feed_url, wait_until="domcontentloaded", timeout=60000)
                except PWTimeoutError:
                    log("WARN", "Feed page load timeout")
                    continue
                except PWError as e:
                    if STOP:
                        break
                    log("ERROR", f"Feed page load error: {e}")
                    continue

                log("STEP", "Scrolling to trigger lazy-load...")
                scroll_to_load(page, scroll_times=6, pause=0.6)

                log("STEP", "Collecting listing URLs...")
                urls = collect_listing_urls(page)

                if not urls:
                    log("WARN", "No listing URLs found on this feed page.")
                    if args.debug_on_empty:
                        debug_dump(page, tag=f"feed_empty_p{pnum}")
                    sleep_interruptible(args.page_delay)
                    continue

                for u in urls:
                    if STOP:
                        break

                    lid = extract_listing_id(u)
                    key = lid or u

                    if key in seen:
                        log("INFO", f"Skipping seen listing: {key}")
                        continue

                    try:
                        row = scrape_listing(page, u)
                        append_csv(args.out, fields, row)
                        seen.add(key)
                        save_seen(args.seen, seen)
                    except PWError as e:
                        if STOP:
                            break
                        log("ERROR", f"Playwright error on listing: {e}")
                        if args.debug_on_error:
                            debug_dump(page, tag=f"listing_error_{lid or 'unknown'}")
                    except Exception as e:
                        log("ERROR", f"Unhandled error on listing: {e}")
                        if args.debug_on_error:
                            debug_dump(page, tag=f"listing_error_{lid or 'unknown'}")

                    log("STEP", f"Sleeping listing delay: {args.delay}s")
                    sleep_interruptible(args.delay)

                log("STEP", f"Sleeping page delay: {args.page_delay}s")
                sleep_interruptible(args.page_delay)

            log("INFO", f"===== CYCLE {cycle} END =====")
            sleep_interruptible(args.page_delay)

    finally:
        log("INFO", "Shutting down...")
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
        log("INFO", f"Stopped. CSV saved at {args.out}")


if __name__ == "__main__":
    main()
