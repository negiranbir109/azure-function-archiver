import azure.functions as func
import logging
import os
from azure.storage.blob import BlobServiceClient, StandardBlobTier
from datetime import datetime
import time

app = func.FunctionApp()

def get_archive_dest(filename):
    """
    Given a filename, return (container, subfolder) for archive storage account.
    """
    fname = filename.lower()
    # ---- SAP DSP ----
    if fname.startswith("audit_log_kpmgukdsp"):
        return "sap-dsp", "audit"
    elif fname.startswith("event_log_kpmgukdsp"):
        return "sap-dsp", "event"
    elif fname.startswith("app_log_kpmgukdsp"):
        return "sap-dsp", "spaces-db"
    # ---- SAP CIS ----
    if fname.startswith("audit_log_kpmgukcis"):
        return "sap-cis", "audit"
    elif fname.startswith("event_log_kpmgukcis"):
        return "sap-cis", "event"
    # ---- SAP IAG ----
    if fname.startswith("audit_log_kpmgukiag"):
        return "sap-iag", "audit"
    elif fname.startswith("event_log_kpmgukiag"):
        return "sap-iag", "event"
    # ---- SAP MRM ----
    if fname.startswith("audit_log_kpmgukmrm"):
        return "sap-mrm", "audit"
    elif fname.startswith("event_log_kpmgukmrm"):
        return "sap-mrm", "event"
    # ---- SAP BTP-ABAP ----
    if fname.startswith("audit_log_kpmguks4"):
        return "sap-btp-abap", "audit"
    elif fname.startswith("event_log_kpmguks4"):
        return "sap-btp-abap", "event"
    # Fallback
    return "other", "misc"

@app.event_grid_trigger(arg_name="azeventgrid")
def MoveToArchive(azeventgrid: func.EventGridEvent):
    try:
        logging.info('Triggered')
        event_data = azeventgrid.get_json()
        logging.info(f"Event Data: {event_data}")

        source_url = event_data.get('url')
        if not source_url:
            logging.error("No URL found in event data")
            return

        # Set your ingestion container name
        INGEST_CONTAINER = "production"
        if f"/{INGEST_CONTAINER}/" in source_url:
            blob_name = source_url.split(f"/{INGEST_CONTAINER}/")[-1]
        else:
            blob_name = source_url.split("/")[-1]
        logging.info(f"Blob Name: {blob_name}")

        # Determine archive container and subfolder
        archive_container, archive_subfolder = get_archive_dest(blob_name)
        logging.info(f"Archive Container: {archive_container}, Subfolder: {archive_subfolder}")

        # Add timestamp for uniqueness
        if '.' in blob_name:
            base, ext = blob_name.rsplit('.', 1)
            ext = '.' + ext
        else:
            base = blob_name
            ext = ''
        timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
        archive_blob_name = f"{archive_subfolder}/{base}_{timestamp}{ext}"

        # Initialize storage clients
        src_conn_str = os.environ['IngestStorageConnection']
        dst_conn_str = os.environ['ArchiveStorageConnection']
        src_service = BlobServiceClient.from_connection_string(src_conn_str)
        dst_service = BlobServiceClient.from_connection_string(dst_conn_str)
        src_blob = src_service.get_blob_client(container=INGEST_CONTAINER, blob=blob_name)
        dst_blob = dst_service.get_blob_client(container=archive_container, blob=archive_blob_name)

        # Start the copy operation
        logging.info(f"Starting copy: {blob_name} -> {archive_container}/{archive_blob_name}")
        dst_blob.start_copy_from_url(src_blob.url)

        # Wait for copy to finish (max 30 seconds)
        for _ in range(30):
            props = dst_blob.get_blob_properties()
            if props.copy.status != 'pending':
                break
            time.sleep(1)

        # Set archive tier and delete source
        if props.copy.status == 'success':
            dst_blob.set_standard_blob_tier(StandardBlobTier.Archive)
            src_blob.delete_blob()
            logging.info(f"Copied and archived: {archive_container}/{archive_blob_name}")
        else:
            logging.error(f"Copy did not complete successfully for {blob_name}. Status: {props.copy.status}")

    except Exception as e:
        logging.error(f"Function failed: {e}")