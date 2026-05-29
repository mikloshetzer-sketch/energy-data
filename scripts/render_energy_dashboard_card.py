from pathlib import Path
from playwright.sync_api import sync_playwright

REPO_ROOT = Path(__file__).resolve().parents[1]

HTML_FILE = REPO_ROOT / "docs" / "index.html"
OUTPUT_DIR = REPO_ROOT / "docs" / "assets"
OUTPUT_FILE = OUTPUT_DIR / "energy-dashboard-card.png"


def main():
    if not HTML_FILE.exists():
        raise FileNotFoundError(f"Nem található a dashboard HTML: {HTML_FILE}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    html_url = HTML_FILE.as_uri()

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(
            viewport={
                "width": 1600,
                "height": 1500,
                "device_scale_factor": 2
            }
        )

        page.goto(html_url, wait_until="networkidle", timeout=120000)

        # Várunk, hogy a Chart.js diagramok biztosan kirajzolódjanak
        page.wait_for_timeout(4000)

        card = page.locator("#capture")

        if card.count() == 0:
            raise RuntimeError("Nem található a #capture elem a HTML-ben.")

        card.screenshot(
            path=str(OUTPUT_FILE),
            omit_background=False
        )

        browser.close()

    print(f"Kép elkészült: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
