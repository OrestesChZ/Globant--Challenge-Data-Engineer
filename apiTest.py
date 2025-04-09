import requests

API_KEY = "mi_clave_api_unica_12345"
BASE_URL = "http://localhost:8000"

headers = {"X-API-KEY": API_KEY}

endpoints = [
    "/employees-by-job-and-department",
    "/departments-above-average",
]

for endpoint in endpoints:
    url = BASE_URL + endpoint
    print(f"\n🔍 Testing {url}")
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        print("✅ Success")
        print(response.json())
    else:
        print("❌ Failed")
        print(f"Status Code: {response.status_code}")
        print("Response:", response.text)
