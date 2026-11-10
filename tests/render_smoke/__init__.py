# render_smoke package — NiceGUI User render-smoke tests for /joins-lab.
#
# Task 1 resolution: "manual" (no pytest-asyncio dependency).
# Tests are synchronous functions that call asyncio.run() over a User driver
# constructed on httpx.ASGITransport(core.app), mirroring create_user() in
# nicegui/testing/user_plugin.py.
