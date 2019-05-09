from arcgis.gis import GIS
from azure.storage.queue import QueueService
import json
import logging

import sys
sys.path.append('../src')
sys.path.append('../')

from reach_tools import ReachFeatureLayer
import config

# create the connection to the GIS to get access
gis = GIS(username=config.arcgis_username, password=config.arcgis_password)
logging.info(f'Connected to GIS at {gis.url}')

# create a layer instance to query and get all existing features
lyr_centroid = ReachFeatureLayer(config.url_reach_centroid, gis)
logging.info(f'Successfully created {lyr_centroid.properties.name} layer.')

# creat a connection to the Azure Queue
queue = QueueService(connection_string=config.azure_queue_conn_str)
logging.info(f'Successfully connected to the {config.azure_queue_update_name} Queue')

# query the feature service to get all existing features - notably the reach ids
reach_id_fs = lyr_centroid.query(out_fields='reach_id', return_geometry=False)
logging.info(f'Retrieved {len(reach_id_fs.features)} from the {lyr_centroid.properties.name} layer.')

# iterate the features and load up the queue with each respective feature
for feature in reach_id_fs.features:
    
    # convert the feature json to a string
    feature_str = json.dumps(feature.as_dict)
    
    # push the feature to the queue
    queue.put_message(config.azure_queue_update_name, feature_str)
             
logging.info(f'Successfully loaded {len(reach_id_fs.features)} into the {config.azure_queue_update_name} Queue.')
