import logging
import json
import azure.functions as func
from arcgis.features import Feature

from src.reach_tools import *
from src import config

def main(msg: func.QueueMessage) -> None:

   # we need a connection to arcgis, so start there
    gis = GIS(username=config.arcgis_username, password=config.arcgis_password)
    logging.info(f'Connected to GIS at {gis.url}.')

    # since the input message content is a single feature, extract the reach id from the feature
    feature_json = json.loads(msg.content)
    reach_id = feature_json['attributes']['reach_id']
    logging.info(f'Preparing to update reach id {reach_id}.')

    # create a reach object to work with
    reach = Reach.get_from_aw(reach_id)
    logging.info(f'Retrieved reach id {reach_id} from AW.')

    # do the hard work, trace it
    reach.snap_putin_and_takeout_and_trace(gis=gis)
    logging.info(f'Successfully traced {reach_id}.')

    # create layers to be updated
    lyr_centroid = ReachFeatureLayer(config.url_reach_centroid, gis)
    lyr_line = ReachFeatureLayer(config.url_reach_line, gis)
    lyr_points = ReachPointFeatureLayer(config.url_reach_points, gis)

    # update ArcGIS Online
    reach.publish_updates(lyr_line, lyr_centroid, lyr_points)
    logging.info(f'Successfully updated {reach_id} on ArcGIS Online.')
 