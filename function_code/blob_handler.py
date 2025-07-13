import os, time
from datetime import datetime
from azure.storage.blob import BlobServiceClient, StandardBlobTier

def archive_blob(blob_name, ingest_container, get_dest_fn):
    container, subfolder = get_dest_fn(blob_name)
    
    timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
    if '.' in blob_name:
        base, ext = blob_name.rsplit('.', 1)
        ext = '.' + ext
    else:
        base, ext = blob_name, ''
    archive_blob_name = f"{subfolder}/{base}_{timestamp}{ext}"

    src_client = BlobServiceClient.from_connection_string(os.environ["IngestStorageConnection"])
    dst_client = BlobServiceClient.from_connection_string(os.environ["ArchiveStorageConnection"])
    src_blob = src_client.get_blob_client(container=ingest_container, blob=blob_name)
    dst_blob = dst_client.get_blob_client(container=container, blob=archive_blob_name)

    dst_blob.start_copy_from_url(src_blob.url)

    for _ in range(30):
        props = dst_blob.get_blob_properties()
        if props.copy.status != "pending":
            break
        time.sleep(1)

    if props.copy.status == "success":
        dst_blob.set_standard_blob_tier(StandardBlobTier.Archive)
        src_blob.delete_blob()
        return f"Archived to {container}/{archive_blob_name}"
    return f"Copy failed for {blob_name}: {props.copy.status}"
