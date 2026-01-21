from playwright.sync_api import sync_playwright
import time
import requests
import subprocess
import os
import signal
import sys

def wait_for_server(url, timeout=30):
    start = time.time()
    while time.time() - start < timeout:
        try:
            requests.get(url)
            return True
        except:
            time.sleep(1)
    return False

def run_verification():
    # Start the server
    print("Starting server...")
    env = os.environ.copy()
    env['GENIZAH_PORT'] = '8081'
    # Set language to Hebrew for testing
    # We can't easily set the cookie/storage from here before launch,
    # but the app defaults to English.
    # We'll need to toggle it in the UI.

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
            page = browser.new_page()

            print("Navigating to home...")
            page.goto('http://localhost:8081')
            page.wait_for_load_state('networkidle')

            # Switch to Hebrew if not already
            lang = page.get_attribute('html', 'lang')
            print(f"Initial Lang: {lang}")

            if lang != 'he':
                print("Switching to Hebrew...")
                # Find the language toggle
                # It's in the sidebar footer usually, or we can use the one we just modified
                # We added aria-label or role=button to it.
                # Look for 'עברית' or 'English' text
                toggle = page.get_by_text("עברית")
                if toggle.count() > 0:
                    toggle.click()
                else:
                    # Maybe it says "English" if we are in Hebrew? No, button says target lang.
                    # If we are in English, button says "עברית".
                    pass

                page.wait_for_load_state('networkidle')
                time.sleep(1) # Wait for reload

            lang = page.get_attribute('html', 'lang')
            dir_attr = page.get_attribute('html', 'dir')
            print(f"Current Lang: {lang}, Dir: {dir_attr}")

            if lang != 'he':
                print("FAILED to switch to Hebrew")
                # Continue anyway to see what happens

            # Verify Sidebar Position
            # The drawer should be on the LEFT.
            # In Quasar, the drawer element usually has class 'q-drawer'.
            drawer = page.locator('.q-drawer').first
            box = drawer.bounding_box()
            print(f"Drawer Box: {box}")

            if box['x'] < 50: # Close to 0
                print("PASS: Drawer is on the Left")
            else:
                print(f"FAIL: Drawer X is {box['x']} (Expected near 0)")

            # Verify Header Layout (Menu Button)
            # Menu button should be on the Left.
            # It has icon 'menu'.
            menu_btn = page.locator('button:has(.q-icon:has-text("menu"))').first
            menu_box = menu_btn.bounding_box()
            print(f"Menu Button Box: {menu_box}")

            if menu_box['x'] < 100:
                print("PASS: Menu Button is on the Left")
            else:
                print(f"FAIL: Menu Button X is {menu_box['x']} (Expected near 0)")

            # Verify Auth/Status on Right
            # Status text "Loading..." or "Ready" (Hebrew: "טוען..." / "מוכן")
            # Or the user profile / help button.
            # Help button (question mark)
            help_btn = page.locator('button:has(.q-icon:has-text("help_outline"))').first
            help_box = help_btn.bounding_box()
            viewport_width = page.viewport_size['width']
            print(f"Help Button Box: {help_box}, Viewport: {viewport_width}")

            if help_box['x'] > viewport_width - 200:
                print("PASS: Help Button is on the Right")
            else:
                print(f"FAIL: Help Button X is {help_box['x']} (Expected near Right edge)")

            # Verify Help Page Title
            print("Navigating to Help page...")
            page.goto('http://localhost:8081/help')
            page.wait_for_load_state('networkidle')

            # Check H1
            # Should be "מרכז עזרה"
            h1 = page.locator('h1').first
            text = h1.inner_text()
            print(f"Help Page H1: {text}")

            if "עזרה" in text or "Help" in text:
                print("PASS: Help Page Title found")
            else:
                print("FAIL: Help Page Title mismatch")

            # Capture screenshot
            page.screenshot(path='verification/layout_check.png')
            print("Captured verification/layout_check.png")

            browser.close()

    finally:
        print("Stopping server...")
        process.terminate()
        process.wait()

if __name__ == '__main__':
    run_verification()
