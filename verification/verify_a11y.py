from playwright.sync_api import sync_playwright
import time

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        print('Starting verification...')

        # 1. Test Home Page
        try:
            page.goto('http://localhost:8081', timeout=10000)
            page.wait_for_load_state('networkidle')

            # Check for language attribute
            lang = page.get_attribute('html', 'lang')
            print(f'HTML lang attribute: {lang}')

            # Check for skip link
            skip_link = page.query_selector('.skip-link')
            if skip_link:
                print('Skip link found')
            else:
                print('ERROR: Skip link not found')

            # Check for role=button on cards
            cards = page.query_selector_all('[role="button"]')
            print(f'Found {len(cards)} elements with role=button')

            page.screenshot(path='verification/home_a11y.png')
            print('Captured home_a11y.png')

        except Exception as e:
            print(f'Error on home page: {e}')

        # 2. Test Accessibility Page
        try:
            page.goto('http://localhost:8081/accessibility', timeout=10000)
            page.wait_for_load_state('networkidle')

            # Check for h1
            h1 = page.query_selector('h1')
            if h1:
                print(f'H1 found: {h1.inner_text()}')

            page.screenshot(path='verification/accessibility_page.png')
            print('Captured accessibility_page.png')

        except Exception as e:
            print(f'Error on accessibility page: {e}')

        browser.close()

if __name__ == '__main__':
    run()
