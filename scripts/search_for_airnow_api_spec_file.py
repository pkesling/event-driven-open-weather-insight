import requests
from urllib.parse import urljoin

COMMON_PATHS = [
    "/openapi.json",
    "/openapi.yaml",
    "/swagger.json",
    "/swagger.yaml",
    "/api-docs",
    "/v2/api-docs",
    "/v3/api-docs",
    "/swagger/docs/v1",
    "/swagger/docs/v2",
]

def is_openapi_like(data):
    if not isinstance(data, dict):
        return False
    if "openapi" in data and "paths" in data:
        return True
    if "swagger" in data and "paths" in data:
        return True
    if "info" in data and "paths" in data:
        return True
    return False

def find_spec(base_url, paths=COMMON_PATHS, timeout=5):
    found = []
    for p in paths:
        url = urljoin(base_url.rstrip("/") + "/", p.lstrip("/"))
        try:
            r = requests.get(url, timeout=timeout)
            if r.status_code == 200:
                # Try JSON first
                try:
                    data = r.json()
                    if is_openapi_like(data):
                        found.append(("json", url))
                        continue
                except ValueError:
                    pass

                # If JSON failed, maybe YAML
                # A simple heuristic: look for 'openapi:' or 'swagger:'
                text = r.text
                if "openapi:" in text or "swagger:" in text:
                    found.append(("yaml", url))
        except requests.RequestException:
            continue
    return found

if __name__ == "__main__":
    # base = "https://docs.airnowapi.org"  # or your target endpoint
    base = "https:/www.airnowapi.org"  # or your target endpoint
    results = find_spec(base)
    if results:
        for format_type, url in results:
            print(f"Likely {format_type.upper()} spec at: {url}")
    else:
        print("No OpenAPI/Swagger spec found at common paths.")
