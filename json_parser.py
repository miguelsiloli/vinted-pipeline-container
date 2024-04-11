import pandas as pd
import json

file = pd.read_json("items.json")

thumbnails = [print(photo[0]["thumbnails"][1]["url"]) for photo in file["photos"]]
df = pd.DataFrame({
    "product_id": file["id"], 
    "image": thumbnails
})
print(df["image"])