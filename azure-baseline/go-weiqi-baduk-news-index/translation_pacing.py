"""Pace translation across threads locally and across Azure worker instances."""
import json
import os
import threading
import time

from azure.core.exceptions import HttpResponseError, ResourceExistsError
from azure.storage.blob import BlobServiceClient

_mutex = threading.Lock()
_next_local = 0.0

def duration(characters):
    rate = max(1000, int(os.environ.get('NEWS_TRANSLATION_CHARS_PER_MINUTE', '18000')))
    return max(0.2, characters / rate * 60)

def reserve_slot(next_slot, current_time, characters):
    start = max(next_slot, current_time)
    return start, start + duration(characters)

def pace_translation(characters):
    if os.environ.get('NEWS_TRANSLATION_SHARED_PACING') == 'true':
        return pace_shared(characters)
    global _next_local
    with _mutex:
        start, _next_local = reserve_slot(_next_local, time.monotonic(), characters)
    time.sleep(max(0, start - time.monotonic()))

def pace_shared(characters):
    container = BlobServiceClient.from_connection_string(os.environ['AzureWebJobsStorage']).get_container_client('go-news-index')
    blob = container.get_blob_client('translation-pacing.json')
    try:
        blob.upload_blob('{"next":0}', overwrite=False)
    except ResourceExistsError:
        pass
    except HttpResponseError as error:
        # Another translator can hold the existing pacing blob's lease. Azure
        # may report this before reporting that create-if-absent already exists.
        # Proceed to normal lease acquisition; never reset its reserved slots.
        if getattr(error, 'error_code', None) != 'LeaseIdMissing':
            raise
    for attempt in range(120):
        try:
            lease = blob.acquire_lease(lease_duration=15)
        except HttpResponseError as error:
            if error.status_code != 409:
                raise
            time.sleep(.5)
            continue
        try:
            state = json.loads(blob.download_blob(lease=lease).readall())
            start, next_slot = reserve_slot(float(state.get('next', 0)), time.time(), characters)
            blob.upload_blob(json.dumps({'next': next_slot}), overwrite=True, lease=lease)
        finally:
            lease.release()
        time.sleep(max(0, start - time.time()))
        return
    raise RuntimeError('Translation pacing is busy; retry on the next collection')
