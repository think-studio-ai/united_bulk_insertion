import logging

from elasticsearch import Elasticsearch, helpers
from fastapi import FastAPI
import os

from bulk import bulk_file
from bulk_model import BulkModel
from constants import (beneficial_required_columns,
                       volunteer_required_columns,
                       coordinators_required_columns)


# Establish connection to Elastic Cloud
es = Elasticsearch(
    cloud_id=os.getenv("cloud_id"),
    http_auth=(os.getenv("username"), os.getenv("password")),
    timeout=60,
    max_retries=10,
)

app = FastAPI()

@app.post("bulk")
async def bulk_insert(bulk_request: BulkModel):
    try:
        # Exclude beneficial
        if bulk_request.type == "beneficial":
            bulks = bulk_file(file_path=bulk_request.path, required_columns=beneficial_required_columns)

        elif bulk_request.type == "volunteer":
            bulks = bulk_file(file_path=bulk_request.path, required_columns=volunteer_required_columns)

        elif bulk_request.type == "coordinators":
            bulks = bulk_file(file_path=bulk_request.path, required_columns=coordinators_required_columns)

        else:
            logging.error("Invalid type provided")
            raise ValueError("Invalid type provided")

        for bulk in bulks:
            response = helpers.bulk(es, bulk)
            success, failed = response
            logging.info(f"Successfully inserted {success} documents.")
            es.indices.refresh(index="visualization_new_united")
        return {"Bulks inserted successfully"}
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return {"An error occurred"}
