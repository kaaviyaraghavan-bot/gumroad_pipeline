from flask import Flask, jsonify
import requests
import pandas as pd
import json
from dotenv import load_dotenv
import os

load_dotenv()

app = Flask(__name__)

# -----------------------------
# CONFIG
# -----------------------------
GUMROAD_TOKEN = os.getenv("GUMROAD_TOKEN")
GUMROAD_URL = "https://api.gumroad.com/v2/sales"

AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = "appkPLMUOGU8jpCIA"

RAW_TABLE = "Gumroad_raw_sales"
CLEAN_TABLE = "Gumroad_clean_sales"

HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json"
}

# -----------------------------
# HELPER: GET EXISTING ORDER IDS
# -----------------------------
def get_existing_order_ids(table_name):
    existing_ids = set()
    offset = None

    while True:
        url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{table_name}"
        params = {"offset": offset} if offset else {}

        r = requests.get(url, headers=HEADERS, params=params)
        data = r.json()

        for record in data.get("records", []):
            order_id = record["fields"].get("Order ID")
            if order_id:
                existing_ids.add(str(order_id))

        offset = data.get("offset")
        if not offset:
            break

    return existing_ids

# -----------------------------
# ZAPIER TRIGGER ENDPOINT
# -----------------------------
@app.route("/trigger", methods=["GET","POST"])
def trigger_pipeline():

    # STEP 1: FETCH LATEST SALE FROM GUMROAD
    response = requests.get(
        GUMROAD_URL,
        params={"access_token": GUMROAD_TOKEN, "per_page": 1}
    )

    data = response.json()
    if not data.get("success"):
        return jsonify({"error": "Gumroad API failed"}), 500

    sale = data["sales"][0]
    df = pd.DataFrame([sale])

    df["created_at"] = pd.to_datetime(
        df["created_at"], errors="coerce"
    ).dt.strftime("%Y-%m-%dT%H:%M:%S")

    order_id = str(df.iloc[0]["order_id"])

    # STEP 2: CHECK DUPLICATES
    raw_ids = get_existing_order_ids(RAW_TABLE)
    clean_ids = get_existing_order_ids(CLEAN_TABLE)

    if order_id in raw_ids:
        return jsonify({"status": "duplicate_skipped"})

    # STEP 3: PUSH RAW DATA TO AIRTABLE
    raw_payload = {
        "fields": {
            "Order ID": order_id,
            "Email": df.iloc[0].get("email"),
            "Product Name": df.iloc[0].get("product_name"),
            "Price": df.iloc[0].get("price"),
            "Currency": df.iloc[0].get("currency_symbol"),
            "Country": df.iloc[0].get("country"),
            "State": df.iloc[0].get("state"),
            "Refunded": df.iloc[0].get("refunded"),
            "Purchase Date": df.iloc[0].get("created_at"),
            "Raw JSON": json.dumps(df.iloc[0].to_dict())
        }
    }

    raw_url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{RAW_TABLE}"
    requests.post(raw_url, headers=HEADERS, json=raw_payload)

    # STEP 4: CLEAN DATA
    df["email"] = df["email"].fillna("unknown@email.com")
    df["country"] = df["country"].fillna("Unknown")
    df["state"] = df["state"].fillna("Unknown")
    df["currency"] = df["currency_symbol"].fillna("Unknown")
    df["product_name"] = df["product_name"].fillna("Unknown Product")
    df["price"] = df["price"].fillna(0)

    if order_id in clean_ids:
        return jsonify({"status": "raw_saved_clean_skipped"})

    # STEP 5: PUSH CLEAN DATA
    clean_payload = {
        "fields": {
            "Order ID": order_id,
            "Email": df.iloc[0]["email"],
            "Product Name": df.iloc[0]["product_name"],
            "Price": df.iloc[0]["price"],
            "Currency": df.iloc[0]["currency"],
            "Country": df.iloc[0]["country"],
            "State": df.iloc[0]["state"],
            "Purchase Date": df.iloc[0]["created_at"]
        }
    }

    clean_url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{CLEAN_TABLE}"
    requests.post(clean_url, headers=HEADERS, json=clean_payload)

    return jsonify({"status": "success"})



# -----------------------------
# RUN SERVER
# -----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
