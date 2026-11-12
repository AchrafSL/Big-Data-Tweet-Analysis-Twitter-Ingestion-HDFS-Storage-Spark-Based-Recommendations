
import pandas as pd
import json
from sentence_transformers import SentenceTransformer

products = pd.read_csv("data/products.csv")

model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

texts = (
    products["name"].fillna("") + " " +
    products["theme"].fillna("") + " " +
    products["category"].fillna("") + " " +
    products["color"].fillna("")
).tolist()

embeddings = model.encode(texts, normalize_embeddings=True)

result = {
    str(pid): emb.tolist()
    for pid, emb in zip(products["productId"], embeddings)
}

with open("data/product_embeddings.json", "w") as f:
    json.dump(result, f)

print("Product embeddings saved!")
