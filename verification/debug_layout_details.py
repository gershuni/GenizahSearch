from playwright.sync_api import sync_playwright
import time
import subprocess
import os
import sys
import requests

def wait_for_server(url, timeout=30):
    start = time.time()
    while time.time() - start < timeout:
        try:
            requests.get(url)
            return True
        except:
            time.sleep(1)
    return False

def run_debug():
    print("Starting server...")
    env = os.environ.copy()
    env['GENIZAH_PORT'] = '8081'

    process = subprocess.Popen(
        [sys.executable, '-m', 'web.main'],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    try:
        if not wait_for_server('http://localhost:8081'):
            print("Server failed to start")
            return

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={'width': 1280, 'height': 800})

            print("Navigating to home...")
            page.goto('http://localhost:8081')
            page.wait_for_load_state('networkidle')

            # Ensure Hebrew
            lang = page.get_attribute('html', 'lang')
            if lang != 'he':
                print("Switching to Hebrew...")
                page.get_by_text("עברית").click()
                page.wait_for_load_state('networkidle')
                time.sleep(1)

            # 1. Inspect Drawer Position
            drawer = page.locator('.q-drawer').first
            drawer_box = drawer.bounding_box()
            print(f"Drawer Box: {drawer_box}")

            # 2. Inspect Main Content Container
            # NiceGUI puts content in a div with id 'c<id>' usually, or we look for .main-content
            content = page.locator('.main-content').first
            content_box = content.bounding_box()
            print(f"Content Box: {content_box}")

            # 3. Check for Overlap
            if drawer_box and content_box:
                overlap = not (drawer_box['x'] + drawer_box['width'] <= content_box['x'])
                print(f"Overlap Detected: {overlap}")
                print(f"Drawer End X: {drawer_box['x'] + drawer_box['width']}")
                print(f"Content Start X: {content_box['x']}")

            # 4. Check Alignment styles
            styles = content.evaluate("""element => {
                const computed = window.getComputedStyle(element);
                return {
                    textAlign: computed.textAlign,
                    direction: computed.direction,
                    paddingLeft: computed.paddingLeft,
                    paddingRight: computed.paddingRight,
                    marginLeft: computed.marginLeft,
                    marginRight: computed.marginRight,
                    display: computed.display,
                    alignItems: computed.alignItems,
                    justifyContent: computed.justifyContent
                }
            }""")
            print(f"Content Styles: {styles}")

            # 5. Check Parent Layout (QPageContainer etc)
            # The .main-content is usually inside a q-page-container
            page_container = page.locator('.q-page-container').first
            if page_container.count() > 0:
                pc_styles = page_container.evaluate("""element => {
                    const computed = window.getComputedStyle(element);
                    return {
                        paddingLeft: computed.paddingLeft,
                        paddingRight: computed.paddingRight,
                        marginLeft: computed.marginLeft
                    }
                }""")
                print(f"Page Container Styles: {pc_styles}")

            # Screenshot
            page.screenshot(path='verification/debug_layout.png')
            print("Captured verification/debug_layout.png")

    finally:
        process.terminate()
        process.wait()

if __name__ == '__main__':
    run_debug()
