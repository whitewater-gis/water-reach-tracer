import unittest
from arcgis.geometry import Geometry
import pandas as pd

from reach_tools import *


class ReachLDub(unittest.TestCase):
    reach_id = 2156
    putin_x = -121.634402
    putin_y = 45.794848
    takeout_x = -121.645582
    takeout_y = 45.718817

    def test_class_init(self):
        ldub = Reach(self.reach_id)
        self.assertEqual(str(self.reach_id), ldub.reach_id)

    def test_download_raw_json_from_aw(self):
        ldub = Reach(self.reach_id)
        raw_json = ldub._download_raw_json_from_aw()
        self.assertTrue('CContainerViewJSON_view' in raw_json)

    def test_parse_difficulty_string(self):
        difficulty = 'IV-V(V+)'
        ldub = Reach(self.reach_id)
        ldub._parse_difficulty_string(difficulty)
        if ldub.difficulty_minimum != 'IV':
            status = False
        elif ldub.difficulty_maximum != 'V':
            status = False
        elif ldub.difficulty_outlier != 'V+':
            status = False
        else:
            status = True
        self.assertTrue(status)

    def test_get_from_aw(self):
        ldub = Reach.get_from_aw(self.reach_id)
        self.assertTrue(ldub.river_name == 'Little White Salmon')

    def test_get_accesses_by_type(self):
        ldub = Reach.get_from_aw(self.reach_id)
        putin = ldub._get_accesses_by_type('putin')[0]
        self.assertTupleEqual((self.putin_x, self.putin_y), (putin.geometry.x, putin.geometry.y))

    def test_putin(self):
        ldub = Reach.get_from_aw(self.reach_id)
        putin = ldub.putin
        self.assertTupleEqual((self.putin_x, self.putin_y), (putin.geometry.x, putin.geometry.y))

    def test_takeout(self):
        ldub = Reach.get_from_aw(self.reach_id)
        takeout = ldub.takeout
        self.assertTupleEqual((self.takeout_x, self.takeout_y), (takeout.geometry.x, takeout.geometry.y))



class AccessPutin(unittest.TestCase):
    canyon_reach_id = 3066
    putin_point = Geometry({'x': -121.633094, 'y': 45.79532367, 'spatialReference': {'wkid': 4269}})
    point_type = 'access'
    subtype = 'putin'
    name = 'Hayes Creek'
    side_of_river = 'left'
    collection_method = 'digitized'
    collection_date = '02 Nov 1998'

    test_series = pd.Series({
        "SHAPE": Geometry({'x': -121.633094, 'y': 45.79532367, 'spatialReference': {'wkid': 4269}}),
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
            "spatialReference": {"wkid": 4269}
        },
        "attributes": {
            'reach_id': '3066',
            'point_type': 'access',
            'subtype': 'putin',
            'name': 'Hayes Creek',
            'side_of_river': 'left',
            'nhdplus_measure': None,
            'nhdplus_reach_id': None,
            'collection_method': 'digitized',
            'update_date': '02 Nov 1998',
            'notes': None,
            'description': None
        }
    }

    test_snap_geom_dict = {'x': -121.63309439504, 'y': 45.7953235252763, 'spatialReference': {'wkid': 4269}}

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
        access.pop('uid')  # since this will be different every time, just remove it
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
