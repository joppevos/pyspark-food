from fastapi import FastAPI
from pymongo import MongoClient

import sys
from pathlib import Path

# adding configs to sys path
sys.path.insert(0, str(Path.cwd()))
from config import config

db = MongoClient()[config.DATABASE]
app = FastAPI()


@app.get("/spend/{customerid}")
async def spend(customerid: str):
    # retrieve from the 'spend' collection from the database
    r = db[config.COLLECTION].find_one({"customerId": customerid})
    if not r:
        return f"Unable to find customerId: {customerid}"
    if '_id' in r:
        # remove the document_id from the response object
        r.pop('_id')
        return r
