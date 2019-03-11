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
import datetime
from arcgis.features import FeatureLayer, Feature, GeoAccessor, GeoSeriesAccessor
from arcgis.geometry import Geometry, Point, Polyline
from arcgis.gis import GIS, Item
import pandas as pd
import numpy as np
import re
from uuid import uuid4
import shapely.ops
import shapely.geometry

# overcoming challenges of python 3.x relative imports
try:
    from html2text import html2text
    from geometry_monkeypatch import *
except:
    from .html2text import html2text
    from .geometry_monkeypatch import *


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
        # hit the EPA's Point Indexing service to true up the point
        response_json = self._get_point_indexing(x, y)

        # if the point is not in the area covered by NHD (likely in Canada)
        if response_json['output'] is None:
            return False

        else:

            # extract out the coordinates
            coordinates = response_json['output']['end_point']['coordinates']

            # construct a Point geometry along with sending back the ComID and Measure needed for tracing
            return {
                "geometry": Geometry({'x': coordinates[0], 'y':coordinates[1], 'spatialReference':{"wkid": 4326}}),
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
        query_string = {
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
            resp = requests.get(url, query_string)

            # increment the attempts and pull out the status code
            attempts = attempts + 1
            status_code = resp.status_code

            # if the status code is anything other than 200, provide a message of status
            if status_code != 200:
                print('Attempt {:02d} failed with status code {}'.format(attempts, status_code))

        return resp

    @staticmethod
    def _get_epa_updown_ptp_response(putin_epa_reach_id, putin_epa_measure, takeout_epa_reach_id,
                                     takeout_epa_measure):
        """
        Make a call to the WATERS Navigation Service and trace downstream using the putin snapped using
            the EPA service with keys for geometry, measure and feature id.
        :param putin_epa_reach_id: Required - Integer or String
            Reach id of EPA NHD Plus reach to start from.
        :param putin_epa_measure: Required - Integer
            Measure along specified reach to start from.
        :param takeout_epa_reach_id: Required - Integer or String
            Reach id of EPA NHD Plus reach to end at.
        :param takeout_epa_measure: Required - Integer
            Measure along specified reach to end at.
        :return: Raw response object from REST call.
        """

        # url for the REST call
        url = "http://ofmpub.epa.gov/waters10/Navigation.Service"

        # input parameters as documented at https://www.epa.gov/waterdata/upstreamdownstream-search-service
        url = "http://ofmpub.epa.gov/waters10/UpstreamDownStream.Service"
        query_string = {
            "pNavigationType": "PP",
            "pStartComID": putin_epa_reach_id,
            "pStartMeasure": putin_epa_measure,
            "pStopComID": takeout_epa_reach_id,
            "pStopMeasure": takeout_epa_measure,
            "pFlowlinelist": True,
            "f": "json"
        }

        # since requests don't always work, enable repeated tries up to 10
        attempts = 0
        status_code = 0

        while attempts < 10 and status_code != 200:

            # make the actual response to the REST endpoint
            resp = requests.get(url, query_string)

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
            arcgis_geometry = Polyline({'paths': [[c for c in flowline.coords]], 'spatialReference': {'wkid': 4326}})

            return arcgis_geometry

        # if no geometry is found, puke
        else:
            raise TraceException('the tracing operation did not find any hydrolines')

    @staticmethod
    def _epa_updown_response_to_esri_polyline(updown_response):
        """
        From the raw response returned from the trace create a single ArcGIS Python API Line Geometry object.
        :param updown_response: Raw trace response received from the REST endpoint.
        :return: Single continuous ArcGIS Python API Line Geometry object.
        """
        resp_json = updown_response.json()

        # if any flowlines were found, combine all the coordinate pairs into a single continuous line
        if resp_json['output']['flowlines_traversed']:

            # extract the dict descriptions of the geometries and convert to Shapely geometries
            flowline_list = [shapely.geometry.shape(flowline['shape'])
                             for flowline in resp_json['output']['flowlines_traversed']]

            # use Shapely to combine all the lines into a single line
            flowline = shapely.ops.linemerge(flowline_list)

            # convert the LineString to a Polyline, and return the result
            arcgis_geometry = Polyline({'paths': [[c for c in flowline.coords]], 'spatialReference': {'wkid': 4326}})

            return arcgis_geometry

        # if no geometry is found, puke
        else:
            raise TraceException('the tracing operation did not find any hydrolines')

    def get_downstream_navigation_polyline(self, putin_epa_reach_id, putin_epa_measure):
        """
        Make a call to the WATERS Navigation Search Service and trace downstream using the putin snapped using
            the EPA service with keys for geometry, measure and feature id.
        :param putin_epa_reach_id: Required - Integer or String
            Reach id of EPA NHD Plus reach to start from.
        :param putin_epa_measure: Required - Integer
            Measure along specified reach to start from.
        :return: Single continuous ArcGIS Python API Line Geometry object.
        """
        resp = self._get_epa_downstream_navigation_response(putin_epa_reach_id, putin_epa_measure)
        return self._epa_navigation_response_to_esri_polyline(resp)

    def get_updown_ptp_polyline(self, putin_epa_reach_id, putin_epa_measure, takeout_epa_reach_id, takeout_epa_measure):
        """
        Make a call to the WATERS Upstream/Downstream Search Service and trace downstream using the putin and takeout
            snapped using the EPA service with keys for measure and feature id.
        :param putin_epa_reach_id: Required - Integer or String
            Reach id of EPA NHD Plus reach to start from.
        :param putin_epa_measure: Required - Integer
            Measure along specified reach to start from.
        :param takeout_epa_reach_id: Required - Integer or String
            Reach id of EPA NHD Plus reach to end at.
        :param takeout_epa_measure: Required - Integer
            Measure along specified reach to end at.
        :return: Single continuous ArcGIS Python API Line Geometry object.
        """
        resp = self._get_epa_updown_ptp_response(putin_epa_reach_id, putin_epa_measure, takeout_epa_reach_id,
                                                 takeout_epa_measure)
        return self._epa_updown_response_to_esri_polyline(resp)


class Reach(object):

    def __init__(self, reach_id):

        self.reach_id = str(reach_id)
        self.reach_name = ''
        self.reach_name_alternate = ''
        self.river_name = ''
        self.river_name_alternate = ''
        self.error = None  # boolean
        self.notes = ''
        self.difficulty = ''
        self.difficulty_minimum = ''
        self.difficulty_maximum = ''
        self.difficulty_outlier = ''
        self.abstract = ''
        self.description = ''
        self.update_aw = None  # datetime
        self.update_arcgis = None  # datetime
        self.validated = None  # boolean
        self.validated_by = ''
        self._geometry = None
        self._reach_points = []
        self.agency = None

    @property
    def reach_points_as_features(self):
        """
        Get all the reach points as a list of features.
        :return: List of ArcGIS Python API Feature objects.
        """
        return [pt.as_feature for pt in self._reach_points]

    @property
    def reach_points_as_dataframe(self):
        """
        Get the reach points as an Esri Spatially Enabled Pandas DataFrame.
        :return:
        """
        df_pt = pd.DataFrame([pt.as_dictionary for pt in self._reach_points])
        df_pt.spatial.set_geometry('SHAPE')
        return df_pt

    @property
    def centroid(self):
        """
        Get a point geometry centroid for the hydroline.

        :return: Point Geometry
            Centroid representing the reach location as a point.
        """
        # if the hydroline is defined, use the centroid of the hydroline
        if type(self.geometry) is Polyline:
            return Geometry({
                'x': np.mean([self.putin.geometry.x, self.takeout.geometry.x]),
                'y': np.mean([self.putin.geometry.y, self.takeout.geometry.y]),
                'spatialReference': self.putin.geometry.spatial_reference
            })

        # if both accesses are defined, use the mean of the accesses
        elif type(self.putin) is ReachPoint and type(self.takeout) is ReachPoint:

            # create a point geometry using the average coordinates
            return Geometry({
                'x': np.mean([self.putin.geometry.x, self.takeout.geometry.x]),
                'y': np.mean([self.putin.geometry.y, self.takeout.geometry.y]),
                'spatialReference': self.putin.geometry.spatial_reference
            })

        else:
            return None

    @property
    def extent(self):
        """
        Provide the extent of the reach as (xmin, ymin, xmax, ymax)
        :return: Set (xmin, ymin, xmax, ymax)
        """
        if self._geometry:
            return self._geometry.extent
        else:
            return (
                min(self.putin.geometry.x, self.takeout.geometry.x), 
                min(self.putin.geometry.y, self.takeout.geometry.y)
            )

    def _download_raw_json_from_aw(self):
        url = 'https://www.americanwhitewater.org/content/River/detail/id/{}/.json'.format(self.reach_id)

        attempts = 0
        status_code = 0

        while attempts < 10 and status_code != 200:
            resp = requests.get(url)
            if resp.status_code == 200 and len(resp.content):
                return resp.json()
            elif resp.status_code == 200 and not len(resp.content):
                return False
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

        # ensure there is a value for the key
        elif json_block[key] is None:
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
                if not (re.match(r'^([ \r\n\t])+$', value) or not (value != 'N/A')):

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

        # correct any leftover standalone links
        cleanup = cleanup.replace('<', '[').replace('>', ']')

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
        self.agency = self._validate_aw_json(reach_info, 'agency')
        length = self._validate_aw_json(reach_info, 'length')
        if length:
            self.length = float(length)

        # save the update datetime as a true datetime object
        self.update_aw = datetime.datetime.strptime(reach_info['edited'], '%Y-%m-%d %H:%M:%S')

        # process difficulty
        self.difficulty = self._validate_aw_json(reach_info, 'class')
        self._parse_difficulty_string(str(self.difficulty))

        # ensure putin coordinates are present, and if so, add the put-in point to the points list
        if reach_info['plon'] is not None and reach_info['plat'] is not None:
            self._reach_points.append(
                ReachPoint(
                    reach_id=self.reach_id,
                    geometry=Point({
                        'x': float(reach_info['plon']),
                        'y': float(reach_info['plat']),
                        'spatialReference': {'wkid': 4326}
                    }),
                    point_type='access',
                    subtype='putin'
                )
            )

        # ensure take-out coordinates are present, and if so, add take-out point to points list
        if reach_info['tlon'] is not None and reach_info['tlat'] is not None:
            self._reach_points.append(
                ReachPoint(
                    reach_id=self.reach_id,
                    point_type='access',
                    subtype='takeout',
                    geometry=Point({
                        'x': float(reach_info['tlon']),
                        'y': float(reach_info['tlat']),
                        'spatialReference': {'wkid': 4326}
                    })
                )
            )

    @classmethod
    def get_from_aw(cls, reach_id):

        # create instance of reach
        reach = cls(reach_id)

        # download raw JSON from American Whitewater
        raw_json = reach._download_raw_json_from_aw()

        # if a reach does not exist at url, simply a blank response, return false
        if not raw_json:
            return False

        # parse data out of the AW JSON
        reach._parse_json(raw_json)

        # return the result
        return reach

    @classmethod
    def get_from_arcgis(cls, reach_layer, reach_id):

        if type(reach_layer) != ReachFeatureLayer:
            raise Exception('reach_layer must be a ReachFeatureLayer')

        # create instance of reach
        reach = cls(reach_id)

        # get a dataframe, and since there is only one of each reach, get the first
        reach_s = reach_layer.query_by_reach_id(reach_id).sdf.iloc[0]
        # TODO: finish implementing get_from_arcgis method

    def _get_accesses_by_type(self, access_type):

        # check to ensure the correct access type is being specified
        if access_type != 'putin' and access_type != 'takeout' and access_type != 'intermediate':
            raise Exception('access type must be either "putin", "takeout" or "intermediate"')

        # return list of all accesses of specified type
        return [pt for pt in self._reach_points if pt.subtype == access_type and pt.point_type == 'access']

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
        self.access_list = [pt for pt in self._reach_points if pt.subtype != access_type]

        # ensure the new point being added is the right type
        access.point_type = 'access'
        access.subtype = access_type

        # add it to the reach point list
        self._reach_points.append(access)

    @property
    def putin(self):
        access_df = self._get_accesses_by_type('putin')
        if len(access_df) > 0:
            return access_df[0]
        else:
            return None

    def set_putin(self, access):
        self._set_putin_takeout(access, 'putin')

    @property
    def takeout(self):
        access_lst = self._get_accesses_by_type('takeout')
        if len(access_lst) > 0:
            return access_lst[0]
        else:
            return None

    def set_takeout(self, access):
        self._set_putin_takeout(access, 'takeout')

    @property
    def intermediate_accesses(self):
        access_df = self._get_accesses_by_type('intermediate')
        if len(access_df) > 0:
            return access_df
        else:
            return None

    def add_intermediate_access(self, access):
        access.set_type('intermediate')
        self.access_list.append(access)

    def update_putin_takeout_and_trace(self, webmap=False):
        """
        Update the putin and takeout coordinates, and trace the hydroline
        using the EPA's WATERS services.
        :param map: Boolean - Optional
            Return a web map widget if successful - useful for visualizing single reach.
        :return:
        """
        # ensure a putin and takeout actually were found
        if self.putin is None or self.takeout is None:
            self.error = True
            self.notes = 'Reach does not appear to have both a put-in and take-out location defined.'
            return False

        # get the snapped and corrected reach locations for the put-in
        self.putin.snap_to_nhdplus()

        # ensure a putin was actually found
        if self.putin.nhdplus_measure is None or self.putin.nhdplus_reach_id is None:
            self.error = True
            self.notes = 'Putin could not be located using EPA\'s WATERS service'
            return False

        # try to trace a few times, but if it doesn't work, error and report
        attempts = 0
        max_attempts = 5

        try:

            # use the EPA navigate service to trace downstream
            waters = WATERS()
            trace_polyline = waters.get_downstream_navigation_polyline(self.putin.nhdplus_reach_id,
                                                                       self.putin.nhdplus_measure)

            # project the takeout geometry to the same spatial reference as the trace polyline
            takeout_geom = self.takeout.geometry.match_spatial_reference(self.takeout.geometry)

            # snap the takeout geometry to the hydroline
            takeout_geom = takeout_geom.snap_to_line(trace_polyline)

            # update the takeout to the snapped point
            self.takeout.set_geometry(takeout_geom)

            # now dial in the coordinates using the EPA service - getting the rest of the attributes
            self.takeout.snap_to_nhdplus()

            # ensure a takeout was actually found
            if self.takeout.nhdplus_measure is None or self.takeout.nhdplus_reach_id is None:
                self.error = True
                self.notes = 'Takeout could not be located using EPS\'s WATERS service'
                return False

            # get the geometry between the putin and takeout
            self._geometry = waters.get_updown_ptp_polyline(self.putin.nhdplus_reach_id,
                                                            self.putin.nhdplus_measure,
                                                            self.takeout.nhdplus_reach_id,
                                                            self.takeout.nhdplus_measure)

        except:

            # increment the attempt counter
            attempts += 1

            # if tried too many times
            if attempts == max_attempts:
                self.error = True
                self.notes = 'The reach could not be traced using the EPA\'s WATERS service.'
                return False

        # if map result desired, return it
        if webmap:
            return self.plot_map()
        else:
            return True

    @property
    def geometry(self):
        """
        Return the reach polyline geometry.
        :return: Polyline Geometry
        """
        return self._geometry

    @property
    def as_feature(self):
        """
        Get the reach as an ArcGIS Python API Feature object.
        :return: ArcGIS Python API Feature object representing the reach.
        """
        return Feature(
            geometry=self._geometry,
            attributes={key: vars(self)[key] for key in vars(self).keys()
                        if key != '_geometry' and not key.startswith('_')}
        )

    @property
    def as_centroid_feature(self):
        """
        Get a feature with the centroid geometry.
        :return: Feature with point geometry for the reach centroid.
        """
        return Feature(
            geometry=self.centroid,
            attributes={key: vars(self)[key] for key in vars(self).keys()
                        if key != '_geometry' and not key.startswith('_')}
        )

    def publish(self, gis, reach_line_layer, reach_centroid_layer, reach_point_layer):
        """
        Publish the reach to three feature layers; the reach line layer, the reach centroid layer,
        and the reach points layer.
        :param gis: GIS object providing the credentials.
        :param reach_line_layer: ReachLayer with line geometry to publish to.
        :param reach_centroid_layer: ReachLayer with point geometry for the centroid to publish to.
        :param reach_point_layer: ReachPointLayer
        :return: True if successful
        """

        if type(gis) != GIS:
            raise Exception('gis must be a valid GIS object.')
        if type(reach_line_layer) != ReachFeatureLayer or type(reach_centroid_layer) != ReachFeatureLayer:
            raise Exception('Reach line and point layers must be a valid ReachFeatureLayer instances.')

        # add the reach line if it was successfully traced
        if not self.error:
            resp_line = reach_line_layer.add_reach(self)

        # regardless, add the centroid and points
        resp_centroid = reach_centroid_layer.add_reach(self)
        resp_point = reach_point_layer.add_reach(self)

        # TODO: Add error checking responses ensuring all uploads worked

        return True

    def plot_map(self, gis=GIS()):
        """
        Display reach and accesses on web map widget.
        :param gis: ArcGIS Python API GIS object instance.
        :return: map widget
        """
        webmap = gis.map()
        webmap.basemap = 'topo-vector'
        webmap.extent = {
            'xmin': self.geometry.extent[0],
            'ymin': self.geometry.extent[1],
            'xmax': self.geometry.extent[2],
            'ymax': self.geometry.extent[3],
            'spatialReference': {'wkid': 4326}
        }
        webmap.draw(
            shape=self.geometry,
            symbol={
                "type": "esriSLS",
                "style": "esriSLSSolid",
                "color": [0, 0, 255, 255],
                "width": 1.5
            }
        )
        webmap.draw(
            shape=self.putin.geometry,
            symbol={
                "xoffset": 12,
                "yoffset": 12,
                "type": "esriPMS",
                "url": "http://static.arcgis.com/images/Symbols/Basic/GreenFlag.png",
                "contentType": "image/png",
                "width": 24,
                "height": 24
            }
        )
        webmap.draw(
            shape=self.takeout.geometry,
            symbol={
                "xoffset": 12,
                "yoffset": 12,
                "type": "esriPMS",
                "url": "http://static.arcgis.com/images/Symbols/Basic/RedFlag.png",
                "contentType": "image/png",
                "width": 24,
                "height": 24
            }
        )
        webmap.draw(
            shape=self.as_centroid_feature.geometry,
            symbol={
                "type": "esriPMS",
                "url": "http://static.arcgis.com/images/Symbols/Basic/CircleX.png",
                "contentType": "image/png",
                "width": 24,
                "height": 24
            }
        )
        return webmap


class ReachPoint(object):
    """
    Discrete object facilitating working with reach points.
    """

    def __init__(self, reach_id, geometry, point_type, uid=None, subtype=None, name=None, side_of_river=None,
                 collection_method=None, update_date=None, notes=None, description=None, difficulty=None):

        self.reach_id = str(reach_id)
        self.point_type = point_type
        self.subtype = subtype
        self.name = name
        self.side_of_river = side_of_river
        self.nhdplus_measure = None
        self.nhdplus_reach_id = None
        self.collection_method = collection_method
        self.update_date = update_date
        self.notes = notes
        self.description = description
        self.difficulty = difficulty
        self._geometry = None

        self.set_geometry(geometry)
        self.set_side_of_river(side_of_river)  # left or right

        if uid is None:
            self.uid = uuid4().hex
        else:
            self.uid = uid

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
        return self._geometry

    def set_geometry(self, geometry):
        """
        Set the geometry for the access.
        :param geometry: Point Geometry Object
        :return: Boolean True if successful
        """
        if geometry.type != 'Point':
            raise Exception('access geometry must be a valid ArcGIS Point Geometry object')
        else:
            self._geometry = geometry
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
            self.side_of_river = side_of_river

    def snap_to_nhdplus(self):
        """
        Snap the access geometry to the nearest NHD Plus hydroline, and get the measure and NHD Plus Reach ID
            needed to perform traces against the EPA WATERS Upstream/Downstream service.
        :return: Boolean True when complete
        """
        if self.geometry:
            waters = WATERS()
            epa_point = waters.get_epa_snap_point(self.geometry.x, self.geometry.y)

            # if the EPA WATERSs' service was able to locate a point
            if epa_point:

                # set properties accordingly
                self.set_geometry(epa_point['geometry'])
                self.nhdplus_measure = epa_point['measure']
                self.nhdplus_reach_id = epa_point['id']
                return True

            # if a point was not located, return false
            else:
                return False

    @property
    def as_feature(self):
        """
        Get the access as an ArcGIS Python API Feature object.
        :return: ArcGIS Python API Feature object representing the access.
        """
        return Feature(
            geometry=self._geometry,
            attributes={key: vars(self)[key] for key in vars(self).keys()
                        if key != '_geometry' and not key.startswith('_')}
        )

    @property
    def as_dictionary(self):
        """
        Get the point as a dictionary of values making it easier to build DataFrames.
        :return: Dictionary of all properties, with a little modification for geometries.
        """
        dict_point = {key: vars(self)[key] for key in vars(self).keys() if not key.startswith('_')}
        dict_point['SHAPE'] = self.geometry
        return dict_point


class _ReachIdFeatureLayer(FeatureLayer):

    @classmethod
    def from_item_id(cls, gis, item_id):
        url = Item(gis, item_id).layers[0].url
        return cls(url, gis)

    @classmethod
    def from_url(cls, gis, url):
        return cls(url, gis)

    def query_by_reach_id(self, reach_id):
        return self.query("reach_id = '{}'".format(reach_id))

    def flush(self):
        """
        Delete all data!
        :return: Response
        """
        # get a list of all OID's
        oid_list = self.query(return_ids_only=True)['objectIds']

        # if there are features
        if len(oid_list):

            # convert the list to a comma separated string
            oid_deletes = ','.join([str(v) for v in oid_list])

            # delete all the features using the OID string
            return self.edit_features(deletes=oid_deletes)


class ReachPointFeatureLayer(_ReachIdFeatureLayer):

    def add_reach(self, reach):
        """
        Push new reach points to the reach point feature service in bulk.
        :param reach: Reach - Required
            Reach object being pushed to feature service.
        :return: Dictionary response from edit features method.
        """
        # check for correct object type
        if type(reach) != Reach:
            raise Exception('Reach to add must be a Reach object instance.')

        return self.edit_features(adds=reach.reach_points_as_features)

    def _add_reach_point(self, reach_point):
        # add a new reach point to ArcGIS Online
        resp = self.update(adds=[reach_point.as_feature])

        # TODO: handle the response
        return None

    def _update_putin_takeout(self, access):
        # TODO: Implement _update_putin_takeout for ReachPointFeatureLayer
        # query to get the putin by reach_id and access type
        access_fs = self.query(f"reach_id = '{access.reach_id}' AND type = '{access.point_type}'")

        # update the feature set with the updated access properties
        # push the update and return boolean success or failure
        return None

    def update_putin(self, access):
        # TODO: Implement update_putin for ReachPointFeatureLayer
        return None

    def update_takeout(self, access):
        # TODO: Implement update_takeout for ReachPointFeatureLayer
        return None

    def get_putin_sdf(self, reach_id):
        return self.query("type = 'putin' AND reach_id = '{}'".format(reach_id)).sdf

    def get_takeout_sdf(self, reach_id):
        return self.query("type = 'takeout' AND reach_id = '{}'".format(reach_id)).sdf

    def _create_reach_point_from_series(self, reach_point):

        # create an access object instance with the required parameters
        access = ReachPoint(reach_point['reach_id'], reach_point['_geometry'], reach_point['type'])

        # for the remainder of the fields from the service, populate if matching key in access object
        for key in [val for val in reach_point.keys() if val not in ['reach_id', '_geometry', 'type']]:
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
        """
        Push reach to feature service.
        :param reach: Reach - Required
            Reach object being pushed to feature service.
        :return: Dictionary response from edit features method.
        """

        # check for correct object type
        if type(reach) != Reach:
            raise Exception('Reach to add must be a Reach object instance.')

        # check the geometry type of the target feature service - point or line
        if self.properties.geometryType == 'esriGeometryPoint':
            resp = self.edit_features(adds=[reach.as_centroid_feature])

        elif self.properties.geometryType == 'esriGeometryPolyline':
            resp = self.edit_features(adds=[reach.as_feature])

        else:
            raise Exception('The feature service geometry type must be either point or polyline.')

        return resp

    def update_reach(self, reach):
        # TODO: implement update reach method to ReachFeatureLayer
        # get uid of records matching reach_id and note feature count
        # add features
        # if response indicates adds successful, delete original records based on uid
        return None
