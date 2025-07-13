import logging
import azure.functions as func
from .blob_handler import archive_blob
from .utils import get_archive_dest

app = func.FunctionApp()
INGEST_CONTAINER = "production"

@app.event_grid_trigger(arg_name="azeventgrid")
def MoveToArchive(azeventgrid: func.EventGridEvent):
    logging.info("Event Grid triggered.")
    event_data = azeventgrid.get_json()
    source_url = event_data.get("url")

    if not source_url:
        logging.error("No URL in event data")
        return

    blob_name = source_url.split(f"/{INGEST_CONTAINER}/")[-1] if f"/{INGEST_CONTAINER}/" in source_url else source_url.split("/")[-1]
    logging.info(f"Processing blob: {blob_name}")

    result = archive_blob(blob_name, INGEST_CONTAINER, get_archive_dest)
    logging.info(result)