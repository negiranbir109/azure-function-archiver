import logging
import os
import time
from datetime import datetime
import azure.functions as func
from azure.storage.blob import BlobServiceClient, StandardBlobTier
from shared.utils import load_env_config, load_archive_map, resolve_archive_dest

def main(event: func.EventGridEvent):
    try:
        # 🔧 Load config from file specified in App Settings
        config_path = os.environ["ConfigPath"]
        load_env_config(config_path)
        archive_map = load_archive_map()

        # 🧾 Event Grid info
        event_data = event.get_json()
        source_url = event_data.get("url")
        if not source_url:
            logging.error("No URL found in Event Grid trigger")
            return

        # 📦 Extract blob name from URL
        source_container = os.environ["IngestContainerName"]
        blob_name = source_url.split(f"/{source_container}/")[-1] if f"/{source_container}/" in source_url else source_url.split("/")[-1]

        # 🧠 Resolve destination routing
        archive_container, archive_subfolder = resolve_archive_dest(blob_name, archive_map)

        # 🕓 Timestamped archive blob name
        base, ext = (blob_name.rsplit(".", 1) + [""])[:2]
        timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        archive_blob_name = f"{archive_subfolder}/{base}_{timestamp}.{ext}" if ext else f"{archive_subfolder}/{base}_{timestamp}"

        # 🔐 SAS-enabled copy source
        src_conn_str = os.environ["IngestStorageConnection"]
        dst_conn_str = os.environ["ArchiveStorageConnection"]
        src_sas = src_conn_str.split("SharedAccessSignature=")[-1]
        sas_url = f"{source_url}?{src_sas}"

        # 📤 Copy blob
        dst_client = BlobServiceClient.from_connection_string(dst_conn_str)
        dst_blob = dst_client.get_blob_client(container=archive_container, blob=archive_blob_name)
        dst_blob.start_copy_from_url(sas_url)

        # ⏳ Poll for copy status
        for _ in range(30):
            props = dst_blob.get_blob_properties()
            if props.copy.status != "pending":
                break
            time.sleep(1)

        # 🎯 Archive tier + delete source
        if props.copy.status == "success":
            dst_blob.set_standard_blob_tier(StandardBlobTier.Archive)

            src_client = BlobServiceClient.from_connection_string(src_conn_str)
            src_blob = src_client.get_blob_client(container=source_container, blob=blob_name)
            src_blob.delete_blob()

            logging.info(f"✅ Archived and cleaned: {archive_container}/{archive_blob_name}")
        else:
            logging.error(f"❌ Copy failed for: {blob_name} — Status: {props.copy.status}")

    except Exception as e:
        logging.error(f"🔥 Function error: {e}")