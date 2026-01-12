QUERY = "zebra zebra zebra zebra "
HEADLESS = False
SCROLL_TIMES = 10
SCROLL_PAUSE = 0.8
MAX_URLS = 9999999999999

from playwright.sync_api import sync_playwright


def collect_image_urls(page) -> set[str]:
    urls = set()
    imgs = page.query_selector_all("img")
    for img in imgs:
        src = img.get_attribute("src")
        if not src:
            continue
        src = src.strip()
        if src.startswith("http"):
            urls.add(src)
    return urls


def main():
    search_url = f"https://www.bing.com/images/search?q={QUERY}"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context(locale="ar-SA")
        page = context.new_page()

        page.goto(search_url, wait_until="domcontentloaded")
        page.wait_for_timeout(1500)

        all_urls = set()

        for i in range(SCROLL_TIMES):
            all_urls |= collect_image_urls(page)
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(int(SCROLL_PAUSE * 1000))
            print(f"[{i+1}] urls={len(all_urls)}", flush=True)
            if len(all_urls) >= MAX_URLS:
                break

        for u in list(all_urls)[:MAX_URLS]:
            print(u)

        context.close()
        browser.close()


if __name__ == "__main__":
    main()
