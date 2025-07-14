import logging, os, time
from datetime import datetime
import azure.functions as func
from azure.storage.blob import BlobServiceClient, StandardBlobTier

from shared.utils import load_env_config, load_archive_map, resolve_archive_dest

def main(event: func.EventGridEvent):
    try:
        config_path = os.environ["ConfigPath"]
        load_env_config(config_path)
        archive_map = load_archive_map()

        logging.info(f"[{os.environ.get('EnvironmentName', 'unknown')}] Function triggered")
        event_data = event.get_json()

        source_url = event_data.get("url")
        if not source_url:
            logging.error("No URL in event")
            return

        source_container = os.environ["IngestContainerName"]
        blob_name = source_url.split(f"/{source_container}/")[-1] if f"/{source_container}/" in source_url else source_url.split("/")[-1]

        archive_container, archive_subfolder = resolve_archive_dest(blob_name, archive_map)
        base, ext = (blob_name.rsplit('.', 1) + [''])[:2]
        timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
        archive_blob_name = f"{archive_subfolder}/{base}_{timestamp}.{ext}" if ext else f"{archive_subfolder}/{base}_{timestamp}"

        src_client = BlobServiceClient.from_connection_string(os.environ["IngestStorageConnection"])
        dst_client = BlobServiceClient.from_connection_string(os.environ["ArchiveStorageConnection"])
        src_blob = src_client.get_blob_client(container=source_container, blob=blob_name)
        dst_blob = dst_client.get_blob_client(container=archive_container, blob=archive_blob_name)

        dst_blob.start_copy_from_url(src_blob.url)

        for _ in range(30):
            if dst_blob.get_blob_properties().copy.status != "pending":
                break
            time.sleep(1)

        if dst_blob.get_blob_properties().copy.status == "success":
            dst_blob.set_standard_blob_tier(StandardBlobTier.Archive)
            src_blob.delete_blob()
            logging.info(f"✅ Archived: {archive_blob_name}")
        else:
            logging.error(f"❌ Copy failed: {dst_blob.get_blob_properties().copy.status}")

    except Exception as e:
        logging.error(f"Function error: {e}")