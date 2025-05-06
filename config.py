import os

def parse_tag_pairs(env_str):
    print(f"[DEBUG] TAG_PAIRS: {env_str!r}")
    pairs = []

    if not env_str.strip():
        print("[WARN] TAG_PAIRS is empty or missing.")
        return pairs

    for idx, pair in enumerate(env_str.split(","), start=1):
        try:
            k, v = pair.strip().split(":")
            pairs.append((k.strip(), v.strip()))
        except ValueError:
            print(f"[ERROR] Skipping malformed pair #{idx}: {pair!r}")
            continue

    print(f"[INFO] Parsed {len(pairs)} tag pairs:")
    for sp, pv in pairs:
        print(f"  - SetPoint: {sp}, Actual: {pv}")
    
    return pairs

# Load environment variables
TAG_PAIRS = parse_tag_pairs(os.getenv("TAG_PAIRS", ""))

BASE_URL = os.getenv("BASE_URL", "https://webport.it.pitea.se/api")
API_KEY = os.getenv("API_KEY", "")
FIXED_OFFSET = os.getenv("FIXED_OFFSET", "+02:00")

BUFFER_SIZE = int(os.getenv("BUFFER_SIZE", 12))
ANOMALY_STD_MULTIPLIER = float(os.getenv("ANOMALY_STD_MULTIPLIER", 3))
FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL", 300))  # in seconds
