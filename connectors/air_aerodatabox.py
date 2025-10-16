import os
import requests
import pandas as pd

API_KEY = os.getenv("AERODATABOX_API_KEY", "YOUR_API_KEY_HERE")
API_HOST = os.getenv("AERODATABOX_API_HOST", "aerodatabox.p.rapidapi.com")
BASE_URL = f"https://{API_HOST}"
