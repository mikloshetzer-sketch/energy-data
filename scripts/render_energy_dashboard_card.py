from pathlib import Path
import shutil
import time

from playwright.sync_api import sync_playwright

PAGE_URL = "https://mikloshetzer-sketch.github.io/energy-data/"

OUTPUT_DIR = Path("docs/assets")
LATEST_OUTPUT = OUTPUT_DIR / "energy_dashboard_card.png"


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = int(time.time())
    timestamped_output = OUTPUT_DIR / f"energy_dashboard_card_{timestamp}.png"

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-dev-shm-usage", "--no-sandbox"],
        )

        page = browser.new_page(
            viewport={"width": 1800, "height": 2400},
            device_scale_factor=2,
        )

        url = PAGE_URL + f"?cache_bust={timestamp}"
        print(f"Opening: {url}")

        page.goto(url, wait_until="networkidle", timeout=120000)

        print("Waiting for fresh dashboard markers...")

        page.wait_for_selector("text=Inventory Stress", timeout=120000)
        page.wait_for_selector("text=Piaci egyensúly", timeout=120000)
        page.wait_for_selector("text=Market Regime", timeout=120000)

        page.wait_for_timeout(8000)

        print("Taking #capture screenshot...")

        capture = page.locator("#capture")
        capture.screenshot(
            path=str(timestamped_output),
            type="png",
        )

        browser.close()

    shutil.copyfile(timestamped_output, LATEST_OUTPUT)

    print(f"Saved timestamped image: {timestamped_output}")
    print(f"Updated latest image: {LATEST_OUTPUT}")


if __name__ == "__main__":
    main()
