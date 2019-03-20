import unittest
from arcgis.geometry import Geometry
import pandas as pd
from arcgis.gis import GIS, Item
import os

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

from src.reach_tools import *
import src.hydrology as hydrology

gis = GIS(username=os.getenv('ARCGIS_USERNAME'), password=os.getenv('ARCGIS_PASSWORD'))
url_reach_line = os.getenv('URL_REACH_LINE')
url_reach_centroid = os.getenv('URL_REACH_CENTROID')
url_reach_points = os.getenv('URL_REACH_POINT')

lyr_reach_line = ReachFeatureLayer(url_reach_line, gis)
lyr_reach_centroid = ReachFeatureLayer(url_reach_centroid, gis)
lyr_reach_points = ReachPointFeatureLayer(url_reach_points, gis)

class ReachLDub(unittest.TestCase):
    reach_id = 2156
    putin_x = -121.634402
    putin_y = 45.794848
    takeout_x = -121.645582
    takeout_y = 45.718817
    name = 'Little White Salmon'

    def test_class_init(self):
        reach = Reach(self.reach_id)
        self.assertEqual(str(self.reach_id), reach.reach_id)

    def test_download_raw_json_from_aw(self):
        reach = Reach(self.reach_id)
        raw_json = reach._download_raw_json_from_aw()
        self.assertTrue('CContainerViewJSON_view' in raw_json)

    def test_parse_difficulty_string(self):
        difficulty = 'IV-V(V+)'
        reach = Reach(self.reach_id)
        reach._parse_difficulty_string(difficulty)
        if reach.difficulty_minimum != 'IV':
            status = False
        elif reach.difficulty_maximum != 'V':
            status = False
        elif reach.difficulty_outlier != 'V+':
            status = False
        else:
            status = True
        self.assertTrue(status)

    def test_get_from_aw(self):
        reach = Reach.get_from_aw(self.reach_id)
        self.assertTrue(reach.river_name == 'Little White Salmon')

    def test_get_accesses_by_type(self):
        reach = Reach.get_from_aw(self.reach_id)
        putin = reach._get_accesses_by_type('putin')[0]
        self.assertTupleEqual((self.putin_x, self.putin_y), (putin.geometry.x, putin.geometry.y))

    def test_putin(self):
        reach = Reach.get_from_aw(self.reach_id)
        putin = reach.putin
        self.assertTupleEqual((self.putin_x, self.putin_y), (putin.geometry.x, putin.geometry.y))

    def test_takeout(self):
        reach = Reach.get_from_aw(self.reach_id)
        takeout = reach.takeout
        self.assertTupleEqual((self.takeout_x, self.takeout_y), (takeout.geometry.x, takeout.geometry.y))

    def test_trace_result(self):
        reach = Reach.get_from_aw(self.reach_id)
        reach.snap_putin_and_takeout_and_trace()
        self.assertIsInstance(reach.geometry, Polyline)


class ReachCanyon(unittest.TestCase):
    reach_id = 3066
    putin_x = -122.31600189209
    putin_y = 45.939998626709
    takeout_x = -122.373001098633
    takeout_y = 45.9604988098145

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
        self.assertTrue(ldub.river_name == 'Canyon Creek (Lewis River trib.)')

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

    def test_trace_result(self):
        reach = Reach.get_from_aw(self.reach_id)
        reach.snap_putin_and_takeout_and_trace()
        self.assertIsInstance(reach.geometry, Polyline)


class ReachAnon(unittest.TestCase):
    reach_id = 5523

    def test_publish(self):
        reach = Reach.get_from_aw(self.reach_id)
        reach.snap_putin_and_takeout_and_trace()
        result = reach.publish(gis, lyr_reach_line, lyr_reach_centroid, lyr_reach_points)
        self.assertTrue(result)


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
        "_geometry": Geometry({'x': -121.633094, 'y': 45.79532367, 'spatialReference': {'wkid': 4326}}),
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
            'description': None,
            'difficulty': None
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
        delattr(access, 'uid')  # since this will be different every time, just remove it
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


class ReachOutOfUSA(unittest.TestCase):

    """
    Starts in BC and ends in Alaska, should ideally write to centroid feature service with
    error flagged and described in notes. Longer term, should fall back to ArcGIS Online
    Service to trace using elevation service.
    """
    reach_id = 3

    def test_trace_result(self):
        reach = Reach.get_from_aw(self.reach_id)
        reach.snap_putin_and_takeout_and_trace()
        self.assertEqual(type(reach.centroid), Point)

from arcgis.features import Feature

class HydrologyUnitTest(unittest.TestCase):

    reach_id = 1

    def test_trace_number_one(self):

        # reach = Reach.get_from_arcgis(self.reach_id, lyr_reach_points, lyr_reach_centroid, lyr_reach_line)
        reach = Reach.get_from_aw(self.reach_id)

        feat = reach.as_centroid_feature

        self.assertTrue(len(reach))

if __name__ == '__main__':
    unittest.main()
