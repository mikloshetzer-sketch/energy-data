from playwright.sync_api import sync_playwright
from pathlib import Path
import time

OUTPUT = Path("docs/assets/energy_dashboard_card.png")

PAGE_URL = "https://mikloshetzer-sketch.github.io/energy-data/"


def main():
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:

        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-dev-shm-usage",
                "--no-sandbox"
            ]
        )

        page = browser.new_page(
            viewport={
                "width": 1800,
                "height": 2600
            },
            device_scale_factor=2
        )

        print("Opening dashboard...")

        page.goto(
            PAGE_URL + f"?ts={int(time.time())}",
            wait_until="networkidle",
            timeout=120000
        )

        print("Waiting for charts...")

        page.wait_for_timeout(8000)

        page.evaluate("""
        window.scrollTo(0, document.body.scrollHeight);
        """)

        page.wait_for_timeout(3000)

        page.evaluate("""
        window.scrollTo(0, 0);
        """)

        page.wait_for_timeout(2000)

        body = page.locator("body")

        body.screenshot(
            path=str(OUTPUT),
            type="png"
        )

        browser.close()

    print(f"Saved: {OUTPUT}")


if __name__ == "__main__":
    main()
