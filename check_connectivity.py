import requests
import sys

def check_url(url, description):
    print(f"Checking {description}: {url}...", end=" ")
    try:
        resp = requests.get(url, timeout=10)
        print(f"Status: {resp.status_code}")
        if resp.status_code == 200:
            return True
        else:
            return False
    except Exception as e:
        print(f"Failed: {e}")
        return False

print("--- Connectivity Check ---")

# 1. NLI IIIF
nli_url = "https://iiif.nli.org.il/IIIFv21/DOCID/PNX_MANUSCRIPTS990001859060205171-1/manifest"
check_url(nli_url, "NLI IIIF Manifest (Sample)")

# 2. GitHub Releases
github_url = "https://api.github.com/repos/gershuni/GenizahSearch/releases/latest"
check_url(github_url, "GitHub Releases")

# 3. Google AI
google_url = "https://generativelanguage.googleapis.com"
print(f"Checking Google AI Endpoint: {google_url}...", end=" ")
try:
    resp = requests.get(google_url, timeout=10)
    print(f"Status: {resp.status_code}")
    # 404 is expected for root, proves connectivity
    if resp.status_code in [200, 404]:
        print("Success (Reachability confirmed)")
    else:
        print("Failed (Unexpected status)")
except Exception as e:
    print(f"Failed: {e}")

print("--- Check Complete ---")
