import logging

from bulk_beneficial import bulk_beneficial
from elasticsearch import Elasticsearch, helpers
import os


# Establish connection to Elastic Cloud
es = Elasticsearch(
    cloud_id=os.getenv("cloud_id"),
    http_auth=(os.getenv("username"), os.getenv("password")),
    timeout=60,
    max_retries=10,
)

file_path = '/media/zifo/Test/project/data_analysis/data/beneficial_csv/beneficial-bank-elshifa-elmasry.csv'

try:
    # Exclude beneficial
    bulks = bulk_beneficial(file_path=file_path)

    for bulk in bulks:
        response = helpers.bulk(es, bulk)
        success, failed = response
        logging.info(f"Successfully inserted {success} documents.")
        es.indices.refresh(index="visualization_new_united")
except Exception as e:
    logging.error(f"Failed to bulk insert data: {e}")
    raise e
