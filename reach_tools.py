"""
author:     Joel McCune (joel.mccune+gis@gmail.com)
dob:        03 Dec 2014
purpose:    Provide the utilities to process and work with whitewater reach data.
    Copyright 2014 Joel McCune
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
"""
import requests
from arcgis.geometry import Geometry, Point, Polyline
import itertools
import datetime
from arcgis.features import SpatialDataFrame as SDF
from arcgis.features import FeatureLayer, Feature
from arcgis.gis import GIS, Item
import pandas as pd
import re
from html2text import html2text
from uuid import uuid4
import shapely.ops
import shapely.geometry

# kind of nasty hack module for stuff not in the Python API
from geometry_monkeypatch import *


class TraceException(Exception):
    """
    Specific type of exception to be thrown in this module.
    """
    pass


class WATERS(object):

    @staticmethod
    def _get_point_indexing(x, y, search_distance=5, return_geometry=False):
        """
        Get the raw response JSON for snapping points to hydrolines from the WATERS Point Indexing Service
            (https://www.epa.gov/waterdata/point-indexing-service)
        :param x: X coordinate (longitude) in decimal degrees (WGS84)
        :param y: Y coordinate (latitude) in decimal degrees (WGS84)
        :param search_distance: Distance radius to search for a hydroline to snap to in kilometers (default 5km)
        :param return_geometry: Whether or not to return the geometry of the matching hydroline (default False)
        :return: Raw response JSON as a dictionary
        """
        url = "https://ofmpub.epa.gov/waters10/PointIndexing.Service"

        query_string = {
            "pGeometry": "POINT({} {})".format(x, y),
            "pGeometryMod": "WKT,SRSNAME=urn:ogc:def:crs:OGC::CRS84",
            "pPointIndexingMethod": "DISTANCE",
            "pPointIndexingMaxDist": search_distance,
            "pOutputPathFlag": True,
            "pReturnFlowlineGeomFlag": return_geometry,
            "optOutCS": "SRSNAME=urn:ogc:def:crs:OGC::CRS84",
            "optOutPrettyPrint": 0,
            "f": "json"
        }

        response = requests.get(
            url=url,
            params=query_string
        )

        return response.json()

    def get_epa_snap_point(self, x, y):
        """
        Get the snapped point defined both with a geometry, and as an ID and measure needed for tracing.
        :param x: X coordinate (longitude) in decimal degrees (WGS84)
        :param y: Y coordinate (latitude) in decimal degrees (WGS84)
        :return: Dictionary with three keys; geometry, measure, and id. Geometry is an ArcGIS Python API Point Geometry
        object. Measure and ID are values required as input parameters when using tracing WATER services.
        """
        response_json = self._get_point_indexing(x, y)

        coordinates = response_json['output']['end_point']['coordinates']

        return {
            "geometry": Geometry(x=coordinates[0], y=coordinates[1], spatialReference={"wkid": 4326}),
            "measure": response_json["output"]["ary_flowlines"][0]["fmeasure"],
            "id": response_json["output"]["ary_flowlines"][0]["comid"]
        }

    @staticmethod
    def _get_epa_downstream_navigation_response(putin_epa_reach_id, putin_epa_measure):
        """
        Make a call to the WATERS Navigation Service and trace downstream using the putin snapped using
            the EPA service with keys for geometry, measure and feature id.
        :param putin_epa_reach_id: Required - Integer or String
            Reach id of EPA NHD Plus reach to start from.
        :param putin_epa_measure: Required - Integer
            Measure along specified reach to start from.
        :return: Raw response object from REST call.
        """

        # url for the REST call
        url = "http://ofmpub.epa.gov/waters10/Navigation.Service"

        # input parameters as documented at https://www.epa.gov/waterdata/navigation-service
        queryString = {
            "pNavigationType": "DM",
            "pStartComID": putin_epa_reach_id,
            "pStartMeasure": putin_epa_measure,
            "pMaxDistanceKm": 5000,
            "pReturnFlowlineAttr": True,
            "f": "json"
        }

        # since requests don't always work, enable repeated tries up to 10
        attempts = 0
        status_code = 0

        while attempts < 10 and status_code != 200:

            # make the actual response to the REST endpoint
            resp = requests.get(url, queryString)

            # increment the attempts and pull out the status code
            attempts = attempts + 1
            status_code = resp.status_code

            # if the status code is anything other than 200, provide a message of status
            if status_code != 200:
                print('Attempt {:02d} failed with status code {}'.format(attempts, status_code))

        return resp

    @staticmethod
    def _epa_navigation_response_to_esri_polyline(navigation_response):
        """
        From the raw response returned from the trace create a single ArcGIS Python API Line Geometry object.
        :param navigation_response: Raw trace response received from the REST endpoint.
        :return: Single continuous ArcGIS Python API Line Geometry object.
        """
        resp_json = navigation_response.json()

        # if any flowlines were found, combine all the coordinate pairs into a single continuous line
        if resp_json['output']['ntNavResultsStandard']:

            # extract the dict descriptions of the geometries and convert to Shapely geometries
            flowline_list = [shapely.geometry.shape(flowline['shape'])
                             for flowline in resp_json['output']['ntNavResultsStandard']]

            # use Shapely to combine all the lines into a single line
            flowline = shapely.ops.linemerge(flowline_list)

            # convert the LineString to a Polyline, and return the result
            return Geometry.from_shapely(flowline)

        # if no geometry is found, puke
        else:
            raise TraceException('the tracing operation did not find any hydrolines')

    def get_downstream_trace_polyline(self, putin_epa_reach_id, putin_epa_measure):
        """
        Make a call to the WATERS Upstream/Downstream Search Service and trace downstream using the putin snapped using
            the EPA service with keys for geometry, measure and feature id.
        :param putin_epa_reach_id: Required - Integer or String
            Reach id of EPA NHD Plus reach to start from.
        :param putin_epa_measure: Required - Integer
            Measure along specified reach to start from.
        :return: Single continuous ArcGIS Python API Line Geometry object.
        """
        resp = self._get_epa_downstream_navigation_response(putin_epa_reach_id, putin_epa_measure)
        return self._epa_navigation_response_to_esri_polyline(resp)


class ReachAccessesSDF(SDF):

    @classmethod
    def from_csv(cls, path, header=0, sep=',', index_col=0,
                 parse_dates=True, encoding=None, tupleize_cols=None,
                 infer_datetime_format=False, geometry_column='SHAPE'):
        """
        Read a CSV file and create a ReachAccessesSDF object instance. This table must
            contain a field named "reach_id" and another containing the geometry for
            the accesses.
        :param path: string file path or file handle / StringIO
        :param header: int, default 0
            Row to use as header (skip prior rows)
        :param sep: string, default ','
            Field delimiter
        :param index_col: int or sequence, default 0
            Column to use for index. If a sequence is given, a MultiIndex
            is used. Different default from read_table
        :param parse_dates: boolean, default True
            Parse dates. Different default from read_table
        :param encoding:
        :param tupleize_cols: boolean, default False
            write multi_index columns as a list of tuples (if True)
            or new (expanded format) if False)
        :param infer_datetime_format: boolean, default False
            If True and `parse_dates` is True for a column, try to infer the
            datetime format based on the first datetime string. If the format
            can be inferred, there often will be a large parsing speed-up.
        :param geometry_column: string, default 'SHAPE'
            Column containing the geometry.
        :return: ReachesSDF
        """
        from pandas.io.parsers import read_table

        df = read_table(path, header=header, sep=sep, parse_dates=parse_dates, index_col=index_col, encoding=encoding,
                        tupleize_cols=tupleize_cols, infer_datetime_format=infer_datetime_format)

        reach_id_field = 'reach_id'

        # check for necessary fields
        if reach_id_field not in df.columns:
            raise Exception('input table must contain the {} field'.format(reach_id_field))
        if geometry_column not in df.columns:
            raise Exception('input table does not contain a geometry column named {}'.format(geometry_column))

        # ensure the reach_id field is a string
        if df[reach_id_field].dtype != object:
            df[reach_id_field] = df[reach_id_field].astype(str)

        # return a copy of this class with the data populated
        return cls(
            data=df[[col for col in df.columns if col != geometry_column]],
            geometry=df[geometry_column].apply(lambda value: Geometry(eval(value)))
        )

    def _get_reach_accesses(self, reach_id, access_type):
        """
        When provided with a SpatialDataFrame with a reach_id field, extract a SpatialDataFrame for the correct type of
            access - either putin, takeout, or intermediate.
        :param reach_id: Reach ID
        :param access_type: either exactly "putin", "takeout", or "intermediate"
        :return: Series with all information for access type specified.
        """
        # just in case the reach_id is provided as an integer
        reach_id = str(reach_id)

        # a little error catching
        if access_type != 'putin' or access_type != 'takeout' or access_type != 'intermediate':
            raise TraceException('access_type must be either putin, takeout, or intermediate')
        if type(reach_id) != str:
            raise TraceException('reach_id must be a string representation of an integer')

        # get the putin for the reach_id as a SpatialDataFrame
        return self[(self[reach_id] == reach_id) & (self[type] == access_type)]

    def _get_putin_takeout(self, reach_id, access_type):
        """
        Provide error catching wrapper to retrieve just
        :param reach_id: Reach ID
        :param access_type: either exactly "putin" or "takeout"
        :return: Series with all information for access type specified.
        """
        if access_type != 'putin' or access_type != 'takeout':
            raise TraceException('access_type must be either putin, or takeout')

        filtered_sdf = self._get_reach_accesses(reach_id, access_type)

        # if only one access exists, return it - otherwise start breaking stuff
        if len(filtered_sdf.index) == 1:
            return filtered_sdf.iloc[0]
        elif len(filtered_sdf.index) > 1:
            raise TraceException('more than one {} exists for reach_id {}'.format(access_type, reach_id))
        else:
            raise TraceException('no {} exists for reach_id {}'.format(access_type, reach_id))

    def get_putin(self, reach_id):
        """
        Get just the putin as a Series from a SpatialDataFrame.
        :param reach_id: Reach ID
        :return: Series with all information for the putin.
        """
        return self._get_putin_takeout(reach_id, 'putin')

    def get_takeout(self, reach_id):
        """
        Get just the putin as a Series from a SpatialDataFrame.
        :param reach_id: Reach ID
        :return: Series with all information for the takeout.
        """
        return self._get_putin_takeout(reach_id, 'takeout')


class ReachSDF(SDF):

    @classmethod
    def from_csv(cls, path, header=0, sep=',', index_col=0,
                 parse_dates=True, encoding=None, tupleize_cols=None,
                 infer_datetime_format=False, geometry_column='SHAPE'):
        """
        Read a CSV file and create a ReachAccessesSDF object instance. This table must
            contain a field named "reach_id" and another containing the geometry for
            the accesses.
        :param path: string file path or file handle / StringIO
        :param header: int, default 0
            Row to use as header (skip prior rows)
        :param sep: string, default ','
            Field delimiter
        :param index_col: int or sequence, default 0
            Column to use for index. If a sequence is given, a MultiIndex
            is used. Different default from read_table
        :param parse_dates: boolean, default True
            Parse dates. Different default from read_table
        :param encoding:
        :param tupleize_cols: boolean, default False
            write multi_index columns as a list of tuples (if True)
            or new (expanded format) if False)
        :param infer_datetime_format: boolean, default False
            If True and `parse_dates` is True for a column, try to infer the
            datetime format based on the first datetime string. If the format
            can be inferred, there often will be a large parsing speed-up.
        :param geometry_column: string, default 'SHAPE'
            Column containing the geometry.
        :return: ReachesSDF
        """
        from pandas.io.parsers import read_table

        df = read_table(path, header=header, sep=sep, parse_dates=parse_dates, index_col=index_col, encoding=encoding,
                        tupleize_cols=tupleize_cols, infer_datetime_format=infer_datetime_format)

        reach_id_field = 'reach_id'

        # check for necessary fields
        if reach_id_field not in df.columns:
            raise Exception('input table must contain the {} field'.format(reach_id_field))
        if geometry_column not in df.columns:
            raise Exception('input table does not contain a geometry column named {}'.format(geometry_column))

        # ensure the reach_id field is a string
        if df[reach_id_field].dtype != object:
            df[reach_id_field] = df[reach_id_field].astype(str)

        # return a copy of this class with the data populated
        return cls(
            data=df[[col for col in df.columns if col != geometry_column]],
            geometry=df[geometry_column].apply(lambda value: Geometry(eval(value)))
        )

    @classmethod
    def from_layer_item(cls, gis, item_id):
        """
        Create a ReachSDF from a feature layer.
        :param gis: Valid Web GIS.
        :param item_id: Layer Item identifier.
        :return: ReachSDF created from either a point or line feature layer.
        """
        return Item(gis, item_id).layers[0].query().df


class _ReachIdFeatureLayer(FeatureLayer):

    @classmethod
    def from_item_id(cls, gis, item_id):
        url = Item(gis, item_id).layers[0].url
        return cls(url, gis)

    @classmethod
    def from_url(cls, gis, url):
        return cls(url, gis)

    def query_by_reach_id(self, reach_id):
        return self.query("reach_id = '{}'".format(reach_id)).df


class ReachPointFeatureLayer(_ReachIdFeatureLayer):

    def _add_reach_point(self, reach_point):
        # TODO: Implement _add_reach_point for ReachPointFeatureLayer
        return None

    def add_access(self, access):
        # TODO: Implement add_access for ReachPointFeatureLayer
        return None

    def add_putin(self, access):
        # TODO: Implement add_putin for ReachPointFeatureLayer
        return None

    def add_takeout(self, access):
        # TODO: Implement add_takeout for ReachPointFeatureLayer
        return None

    def add_intermediate(self, access):
        # TODO: Implement add_intermediate for ReachPointFeatureLayer
        return None

    def _update_putin_takeout(self, access):
        # TODO: Implement _update_putin_takeout for ReachPointFeatureLayer
        return None

    def update_putin(self, access):
        # TODO: Implement update_putin for ReachPointFeatureLayer
        return None

    def update_takeout(self, access):
        # TODO: Implement update_takeout for ReachPointFeatureLayer
        return None

    def get_accesses_sdf(self, reach_id):
        # TODO: return array of Access objects
        return ReachAccessesSDF(self.query_by_reach_id(reach_id))

    def get_putin_sdf(self, reach_id):
        return self.query("type = 'putin' AND reach_id = '{}'".format(reach_id)).df

    def get_takeout_sdf(self, reach_id):
        return self.query("type = 'takeout' AND reach_id = '{}'".format(reach_id)).df

    def _create_reach_point_from_series(self, reach_point):

        # create an access object instance with the required parameters
        access = ReachPoint(reach_point['reach_id'], reach_point['SHAPE'], reach_point['type'])

        # for the remainder of the fields from the service, populate if matching key in access object
        for key in [val for val in reach_point.keys() if val not in ['reach_id', 'SHAPE', 'type']]:
            if key in access.keys():
                access[key] = reach_point[key]

        return access

    def get_putin(self, reach_id):

        # get a pandas series from the feature service representing the putin access
        sdf = self.get_putin_sdf(reach_id)
        putin_series = sdf.iloc[0]
        return self._create_reach_point_from_series(putin_series)

    def get_takeout(self, reach_id):

        # get a pandas series from the feature service representing the putin access
        sdf = self.get_takeout_sdf(reach_id)
        takeout_series = sdf.iloc[0]
        return self._create_reach_point_from_series(takeout_series)


class ReachFeatureLayer(_ReachIdFeatureLayer):

    def query_by_river_name(self, river_name_search):
        field_name = 'name_river'
        where_list = ["{} LIKE '%{}%'".format(field_name, name_part) for name_part in river_name_search.split()]
        where_clause = ' AND '.join(where_list)
        return self.query(where_clause).df

    def query_by_section_name(self, section_name_search):
        field_name = 'name_section'
        where_list = ["{} LIKE '%{}%'".format(field_name, name_part) for name_part in section_name_search.split()]
        where_clause = ' AND '.join(where_list)
        return self.query(where_clause).df

    def add_reach(self, reach):
        # TODO: implement add reach method to ReachFeatureLayer
        return None

    def update_reach(self, reach):
        # TODO: implement update reach method to ReachFeatureLayer
        return None


class Reach(pd.Series):

    def __init__(self, reach_id):

        super().__init__()

        self['reach_id'] = str(reach_id)
        self['reach_name'] = ''
        self['reach_name_alternate'] = ''
        self['river_name'] = ''
        self['river_name_alternate'] = ''
        self['error'] = None  # boolean
        self['notes'] = ''
        self['difficulty'] = ''
        self['difficulty_minimum'] = ''
        self['difficulty_maximum'] = ''
        self['difficulty_outlier'] = ''
        self['abstract'] = ''
        self['description'] = ''
        self['update_aw'] = None  # datetime
        self['update_arcgis'] = None  # datetime
        self['validated'] = None  # boolean
        self['validated_by'] = ''
        self['SHAPE'] = None
        self['reach_points'] = []

    @property
    def centroid(self):
        """
        Get a point geometry centroid for the hydroline.
        :return: Point Geometry
            Centroid representing the reach location as a point.
        """
        if type(self.geometry) is Polyline:
            return self.hydroline.centroid
        else:
            return None

    def _download_raw_json_from_aw(self):
        url = 'https://www.americanwhitewater.org/content/River/detail/id/{}/.json'.format(self.reach_id)

        attempts = 0
        status_code = 0

        while attempts < 10 and status_code != 200:
            response = requests.get(url)
            if response.status_code == 200:
                return response.json()
            else:
                attempts = attempts + 1
        raise Exception('cannot download data for reach_id {}'.format(self['reach_id']))

    def _parse_difficulty_string(self, difficulty_combined):
        match = re.match(
            '^([I|IV|V|VI|5\.\d]{1,3}(?=-))?-?([I|IV|V|VI|5\.\d]{1,3}[+|-]?)\(?([I|IV|V|VI|5\.\d]{0,3}[+|-]?)',
            difficulty_combined
        )
        self.difficulty_minimum = self._get_if_length(match.group(1))
        self.difficulty_maximum = self._get_if_length(match.group(2))
        self.difficulty_outlier = self._get_if_length(match.group(3))

    @staticmethod
    def _get_if_length(match_string):
        if match_string and len(match_string):
            return match_string
        else:
            return None

    def _validate_aw_json(self, json_block, key):

        # check to ensure a value exists
        if key not in json_block.keys():
            return None

        else:

            # clean up the text garbage...because there is a lot of it
            value = self._cleanup_string(json_block[key])

            # now, ensure something is still there...not kidding, this frequently is the case...it is all gone
            if not value:
                return None
            elif not len(value):
                return None

            else:
                # now check to ensure there is actually some text in the block, not just blank characters
                if not re.match(r'^( |\r|\n|\t)+$', value) and value != 'N/A':

                    # if everything is good, return a value
                    return value
                else:
                    return None

    @staticmethod
    def _cleanup_string(input_string):

        # ensure something to work with
        if not input_string:
            return input_string

        # convert to markdown first, so any reasonable formatting is retained
        cleanup = html2text(input_string)

        # since people love to hit the space key multiple times in stupid places, get rid of multiple space, but leave
        # newlines in there since they actually do contribute to formatting
        cleanup = re.sub(r'\s{2,}', ' ', cleanup)

        # apparently some people think it is a good idea to hit return more than twice...account for this foolishness
        cleanup = re.sub(r'\n{3,}', '\n\n', cleanup)

        # get rid of any trailing newlines at end of entire text block
        cleanup = re.sub(r'\n+$', '', cleanup)

        # get rid of any leading or trailing spaces
        cleanup = cleanup.strip()

        # finally call it good
        return cleanup

    def _parse_json(self, raw_json):

        # pluck out the stuff we are interested in
        self._reach_json = raw_json['CContainerViewJSON_view']['CRiverMainGadgetJSON_main']

        # pull a bunch of attributes through validation and save as properties
        reach_info = self._reach_json['info']
        self.river_name = self._validate_aw_json(reach_info, 'river')
        self.reach_name = self._validate_aw_json(reach_info, 'section')
        self.reach_alternate_name = self._validate_aw_json(reach_info, 'altname')
        self.huc = self._validate_aw_json(reach_info, 'huc')
        self.description = self._validate_aw_json(reach_info, 'description')
        self.abstract = self._validate_aw_json(reach_info, 'abstract')
        self.length = float(self._validate_aw_json(reach_info, 'length'))

        # save the update datetime as a true datetime object
        self.update_aw = datetime.datetime.strptime(reach_info['edited'], '%Y-%m-%d %H:%M:%S')

        # process difficulty
        self.difficulty = self._validate_aw_json(reach_info, 'class')
        self._parse_difficulty_string(str(self.difficulty))

        # ensure putin coordinates are present, and if so, add the put-in point to the points list
        if reach_info['plon'] is not None and reach_info['plat'] is not None:
            self.reach_points.append(
                ReachPoint(
                    reach_id=self.reach_id,
                    geometry=Point(
                        x=float(reach_info['plon']),
                        y=float(reach_info['plat']),
                        spatialReference={'wkid': 4326}
                    ),
                    point_type='access',
                    subtype='putin'
                )
            )

        # ensure take-out coordinates are present, and if so, add take-out point to points list
        if reach_info['tlon'] is not None and reach_info['tlat'] is not None:
            self.reach_points.append(
                ReachPoint(
                    reach_id=self.reach_id,
                    point_type='access',
                    subtype='takeout',
                    geometry=Point(
                        x=float(reach_info['tlon']),
                        y=float(reach_info['tlat']),
                        spatialReference={'wkid': 4326}
                    )
                )
            )

    @classmethod
    def get_from_aw(cls, reach_id):

        # create instance of reach
        reach = cls(reach_id)

        # download raw JSON from American Whitewater
        raw_json = reach._download_raw_json_from_aw()

        # parse data out of the AW JSON
        reach._parse_json(raw_json)

        # return the result
        return reach

    @classmethod
    def get_from_arcgis(cls, reach_layer, reach_id):

        if type(reach_layer) != ReachFeatureLayer:
            raise Exception('reach_layer must be a ReachFeatureLayer')

        response = reach_layer.query_by_reach_id(reach_id)
        # TODO: finish implementing get_from_arcgis method

    def _get_accesses_by_type(self, access_type):

        # check to ensure the correct access type is being specified
        if access_type != 'putin' and access_type != 'takeout' and access_type != 'intermediate':
            raise Exception('access type must be either "putin", "takeout" or "intermediate"')

        # return list of all accesses of specified type
        return [pt for pt in self.reach_points if pt.subtype == access_type and pt.point_type == 'access']

    def _set_putin_takeout(self, access, access_type):
        """
        Set the putin or takeout using a ReachPoint object.
        :param access: ReachPoint - Required
            ReachPoint geometry delineating the location of the geometry to be modified.
        :param access_type: String - Required
            Either "putin" or "takeout".
        :return:
        """
        # enforce correct object type
        if type(access) != ReachPoint:
            raise Exception('{} access must be an instance of ReachPoint object type'.format(access_type))

        # check to ensure the correct access type is being specified
        if access_type != 'putin' and access_type != 'takeout':
            raise Exception('access type must be either "putin" or "takeout"')

        # update the list to NOT include the point we are adding
        self.access_list = [pt for pt in self.reach_points if pt.subtype != access_type]

        # ensure the new point being added is the right type
        access.point_type = 'access'
        access.subtype = access_type

        # add it to the reach point list
        self.reach_points.append(access)

    @property
    def putin(self):
        access_list = self._get_accesses_by_type('putin')
        if len(access_list) > 0:
            return access_list[0]
        else:
            return None

    def set_putin(self, access):
        self._set_putin_takeout(access, 'putin')

    @property
    def takeout(self):
        access_list = self._get_accesses_by_type('takeout')
        if len(access_list) > 0:
            return access_list[0]
        else:
            return None

    def set_takeout(self, access):
        self._set_putin_takeout(access, 'takeout')

    @property
    def intermediate_accesses(self):
        access_list = self._get_accesses_by_type('intermediate')
        if len(access_list) > 0:
            return access_list
        else:
            return None

    def add_intermediate_access(self, access):
        # TODO: update add_intermediate_access to support the subclassed Series paradigm
        access.set_type('intermediate')
        self.access_list.append(access)

    @property
    def as_feature(self):
        """
        Get the reach as an ArcGIS Python API Feature object.
        :return: ArcGIS Python API Feature object representing the reach.
        """
        return Feature(
            geometry=self['SHAPE'],
            attributes=self[[val for val in self.keys() if val != 'SHAPE']].to_dict()
        )


class ReachPoint(pd.Series):
    """
    Subclass of Pandas Series representing an access.
    """

    def __init__(self, reach_id, geometry, point_type, uid=None, subtype=None, name=None, side_of_river=None,
                 collection_method=None, update_date=None, notes=None, description=None):

        super().__init__()

        self['reach_id'] = str(reach_id)
        self['point_type'] = point_type
        self['subtype'] = subtype
        self['name'] = name
        self['side_of_river'] = side_of_river
        self['nhdplus_measure'] = None
        self['nhdplus_reach_id'] = None
        self['collection_method'] = collection_method
        self['update_date'] = update_date
        self['notes'] = notes
        self['description'] = description

        self.set_geometry(geometry)
        self.set_side_of_river(side_of_river)  # left or right

        if uid is None:
            self['uid'] = uuid4().hex
        else:
            self['uid'] = uid

    @property
    def type_id(self):
        id_list = ['null' if val is None else val for val in [self.reach_id, self.point_type, self.subtype]]
        return '_'.join(id_list)

    @property
    def geometry(self):
        """
        Geometry for the access, a point.
        :return: Point Geometry object
            Point where access is located.
        """
        return self['SHAPE']

    def set_geometry(self, geometry):
        """
        Set the geometry for the access.
        :param geometry: Point Geometry Object
        :return: Boolean True if successful
        """
        if type(geometry) != Point:
            raise Exception('access geometry must be a valid ArcGIS Point Geometry object')
        else:
            self['SHAPE'] = geometry
            return True

    def set_side_of_river(self, side_of_river):
        """
        Set the side of the river the access is located on.
        :param side_of_river:
        :return:
        """
        if side_of_river is not None and side_of_river != 'left' and side_of_river != 'right':
            raise Exception('side of river must be either "left" or "right"')
        else:
            self['side_of_river'] = side_of_river

    def snap_to_nhdplus(self):
        """
        Snap the access geometry to the nearest NHD Plus hydroline, and get the measure and NHD Plus Reach ID
            needed to perform traces against the EPA WATERS Upstream/Downstream service.
        :return: Boolean True when complete
        """
        if self.geometry:
            waters = WATERS()
            epa_point = waters.get_epa_snap_point(self.geometry.x, self.geometry.y)
            self.set_geometry(epa_point['geometry'])
            self['nhdplus_measure'] = epa_point['measure']
            self['nhdplus_reach_id'] = epa_point['id']
        return True

    @property
    def as_feature(self):
        """
        Get the access as an ArcGIS Python API Feature object.
        :return: ArcGIS Python API Feature object representing the access.
        """
        return Feature(
            geometry=self['SHAPE'],
            attributes=self[[val for val in self.keys() if val != 'SHAPE']].to_dict()
        )
