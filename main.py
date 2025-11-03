import pandas as pd
from sodapy import Socrata
import requests
import json
import os


# With authentication
client = Socrata(
    "mydata.iowa.gov",
    app_token="ezQQ9nWgvKXkhZiQONUhAYlL3",  # Your app token
)

# Fetch data (with higher rate limits!)
results = client.get("kpc8-jmed", limit=1)

results
 

