from collections import namedtuple

WebGIS = namedtuple('WebGIS', ['url', 'username', 'password'])
gis = WebGIS('https://knu2xs.maps.arcgis.com', 'knu2xs', 'K3mosabe')

centroid_layer_id = '63e974b6ee1049aa91c4f024e82e76d4'