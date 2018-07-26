import unittest
from arcgis.geometry import Geometry
import pandas as pd

from reach_tools import *


class AccessPutin(unittest.TestCase):

    canyon_reach_id = 3066
    putin_point = Geometry({'x': -121.633094, 'y': 45.79532367, 'spatialReference': {'wkid': 4326}})
    point_type = 'access'
    subtype = 'putin'
    name = 'Hayes Creek'
    side_of_river = 'left'
    collection_method = 'digitized'
    collection_date = '02 Nov 1998'

    test_series = pd.Series({
        "SHAPE": Geometry({'x': -121.633094, 'y': 45.79532367, 'spatialReference': {'wkid': 4326}}),
        "reach_id": str(canyon_reach_id),
        "point_type": point_type,
        "subtype": subtype,
        "name": name,
        "side_of_river": side_of_river,
        "nhdplus_measure": None,
        "nhdplus_reach_id": None,
        "collection_method": collection_method,
        "collection_date": collection_date,
        "notes": None,
        "description": None,
        "type": point_type
    })

    test_feature_dict = {
        "geometry": {
            "x": -121.633094,
            'y': 45.79532367,
            "spatialReference": {"wkid": 4326}
        },
        "attributes": {
            "reach_id": "3066",
            "point_type": "access",
            "subtype": "putin",
            "name": "Hayes Creek",
            "side_of_river": "left",
            "nhdplus_measure": None,
            "nhdplus_reach_id": None,
            "collection_method": "digitized",
            "collection_date": "02 Nov 1998",
            "notes": None,
            "description": None
        }
    }

    test_snap_geom_dict = {'x': -121.63309439504, 'y': 45.7953235252763, 'spatialReference': {'wkid': 4326}}

    def test_instantiate_access(self):
        access = ReachPoint(
            reach_id=self.canyon_reach_id,
            geometry=self.putin_point,
            point_type=self.point_type,
            subtype=self.subtype,
            name=self.name,
            side_of_river=self.side_of_river,
            collection_method=self.collection_method,
            update_date=self.collection_date
        )
        self.assertEqual(type(access), ReachPoint)

    def test_as_feature(self):
        access = ReachPoint(
            reach_id=self.canyon_reach_id,
            geometry=self.putin_point,
            point_type=self.point_type,
            subtype=self.subtype,
            name=self.name,
            side_of_river=self.side_of_river,
            collection_method=self.collection_method,
            update_date=self.collection_date
        )
        self.assertDictEqual(access.as_feature.as_dict, self.test_feature_dict)

    def test_snap_geom_to_nhdplus(self):
        access = ReachPoint(
            reach_id=self.canyon_reach_id,
            geometry=self.putin_point,
            point_type=self.point_type,
            subtype=self.subtype,
            name=self.name,
            side_of_river=self.side_of_river,
            collection_method=self.collection_method,
            update_date=self.collection_date
        )
        access.snap_to_nhdplus()
        self.assertDictEqual(access.as_feature.as_dict['geometry'], self.test_snap_geom_dict)


if __name__ == '__main__':
    unittest.main()
