import json

with open("sample_data_1K.json", "r", encoding="utf-8") as f:
    data = json.load(f)

print(f"Number of records: {len(data)}")
