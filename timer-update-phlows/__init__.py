import datetime
import logging

import azure.functions as func

from .config import gis, centroid_layer_id

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    centroid_itm = gis.content.search(centroid_layer_id)    

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
