"""
A pretrained Transformer-based sentence-embedding model is used to represent extracted topic labels
as numerical vectors. Cosine similarity measures the semantic proximity between incoming topics
and previously registered canonical topics. Based on a predefined similarity threshold,
semantically similar labels are mapped to a common canonical topic, ensuring consistent 
topic identification across streaming batches.


this is an api that takes topics and give it a unique topic name
topics will be stored so we dont have repeated topics for the same thing

"""

import json  # to write our topics in the register
from contextlib import asynccontextmanager # manage start and stop of api
from pathlib import Path  # simplify register path management
from threading import Lock  # make sure the register is safe even thought we have lots of threads
from uuid import uuid4

import numpy as np
from fastapi import FastAPI, HTTPException
from sentence_transformers import SentenceTransformer


# initialisation
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
SIMILARITY_THRESHOLD = 0.80

REGISTRY_FILE = Path(__file__).with_name("topic_registry.json")

registry_lock = Lock()
model = None
registry = {"topics": []}




def load_registry():
    global registry

    if REGISTRY_FILE.exists():
        with REGISTRY_FILE.open("r", encoding="utf-8") as file:
            registry = json.load(file)
    else:
        registry = {"topics": []}




def save_registry():
    temporary_file = REGISTRY_FILE.with_suffix(".tmp")   # creating temp file before replacing original one (safe measure)

    with temporary_file.open("w", encoding="utf-8") as file:
        json.dump(registry, file, ensure_ascii=False, indent=2)

    temporary_file.replace(REGISTRY_FILE)







# life cycle of the app
@asynccontextmanager
async def lifespan(app):
    global model

    print("Loading Sentence-Transformers model...")
    model = SentenceTransformer(MODEL_NAME)   # load the model once in the global variable

    load_registry()  # load the register in the global variable

    print(f"Model loaded. Registered topics: {len(registry['topics'])}")

    yield

    print("Topic normalization service stopped.")





app = FastAPI(title="Topic Normalization Service", lifespan=lifespan)





@app.post("/normalize")
def normalize_topics(payload: dict):
    topics = payload.get("topics", [])

    if not isinstance(topics, list):
        raise HTTPException(status_code=400, detail="'topics' must be a list")


    for item in topics:
        if not isinstance(item, dict):
            raise HTTPException(status_code=400, detail="Each topic must be an object")

        if "id" not in item or "topic" not in item:
            raise HTTPException(status_code=400, detail="Each topic must contain id and topic")

        if not isinstance(item["topic"], str) or not item["topic"].strip():
            raise HTTPException(status_code=400, detail="Topic labels cannot be empty")




    labels = [item["topic"].strip() for item in topics]

    embeddings = model.encode(labels, normalize_embeddings=True, convert_to_numpy=True)


    results = []

    for item, label, embedding in zip(topics, labels, embeddings):
        best_topic = None
        best_similarity = -1.0



        # search in our topic book if the current topic match with already saved topics
        for known_topic in registry["topics"]:
            known_embedding = np.asarray(known_topic["embedding"], dtype=np.float32)

            similarity = float(np.dot(embedding, known_embedding))

            if similarity > best_similarity:
                best_similarity = similarity
                best_topic = known_topic

        # take the best match

        # and filter it if it's > threshold -> match , else add it to the topic book
        if best_topic is not None and best_similarity >= SIMILARITY_THRESHOLD:
            canonical_topic = best_topic
            similarity = best_similarity
            action = "matched"
        else:
            canonical_topic = {
                "id": f"T{uuid4().hex[:12].upper()}",
                "label": label,
                "embedding": embedding.tolist()
            }

            registry["topics"].append(canonical_topic)
            similarity = None
            action = "created"




        results.append({
            "id": item["id"],
            "original_topic": label,
            "canonical_topic": canonical_topic["label"],
            "topic_id": canonical_topic["id"],
            "action": action,
            "similarity": round(similarity, 4) if similarity is not None else None
        })



    save_registry()
    return {"results": results}


