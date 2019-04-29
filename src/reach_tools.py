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
import arcgis
from arcgis.features import FeatureLayer, Feature, GeoAccessor, GeoSeriesAccessor
from arcgis.geometry import Geometry, Point, Polyline, Polygon
from arcgis.gis import GIS
from arcgis.env import active_gis
from scipy.interpolate import splprep, splev
from copy import deepcopy
from arcgis.gis import GIS, Item
import pandas as pd
import numpy as np
import re
from uuid import uuid4
import shapely.ops
import shapely.geometry
import inspect
from html.parser import HTMLParser
import json

# overcoming challenges of python 3.x relative imports
try:
    from html2text import html2text
    from geometry_monkeypatch import *
    import hydrology  # until my PR gets accepted
except:
    from src.html2text import html2text
    from src.geometry_monkeypatch import *
    import src.hydrology as hydrology  # until my PR gets accepted


# helper for cleaning up HTML strings
# From - https://stackoverflow.com/questions/753052/strip-html-from-strings-in-python
class _MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.strict = False
        self.convert_charrefs = True
        self.fed = []

    def handle_data(self, d):
        self.fed.append(d)

    def get_data(self):
        return ''.join(self.fed)


def _strip_tags(html):
    s = _MLStripper()
    s.feed(html)
    return s.get_data()


# smoothing function for geometry
def _smooth_geometry(geom, densify_max_segment_length=0.009, gis=None):

    if not isinstance(geom, Polygon) and not isinstance(geom, Polyline):
        raise Exception('Smoothing can only be performed on Esri Polygon or Polyline geometry types.')

    # get a GIS instance to have a geometry service to resolve to
    if not gis and active_gis:
        gis = active_gis
    elif not gis and not active_gis:
        raise Exception('an active GIS or explicitly defined GIS is required to smooth geometry')

    def _make_geometry_request(in_geom, url_extension, params):

        # create the url for making the
        url = f'{gis.properties.helperServices.geometry.url}/{url_extension}'

        # make a copy to not modify the original
        geom = deepcopy(in_geom)

        # get the key for the geometry coordinates
        geom_key = list(geom.keys())[0]

        params['geometries'] = {
            'geometryType': 'esriGeometryPolyline',
            'geometries': [
                {geom_key: geom[geom_key]}
            ]
        }

        # convert all dict or list params not at the top level of the dictionary to strings
        payload = {k: json.dumps(v) if isinstance(v, (dict, list)) else v for k, v in params.items()}

        attempts = 0
        status = None

        while attempts < 5 and status != 200:

            try:

                # make the post request
                resp = requests.post(url, payload)

                # extract out the result from the request and patch into the original geometry object
                geom[geom_key] = resp.json()['geometries'][0][geom_key]

                status = resp.status_code

            except:

                attempts = attempts + 1

        # return the modified geometry object
        return geom

    def densify(in_geom):

        # construct the request parameter dictionary less the geometries
        params = {
            'f': 'json',
            'sr': {'wkid': 4326},
            'maxSegmentLength': densify_max_segment_length
        }

        # return the densified geometry object
        return _make_geometry_request(in_geom, 'densify', params)

    def simplify(in_geom):

        # construct the request parameter dictionary less the geometries
        params = {
            'f': 'json',
            'sr': {'wkid': 4326}
        }

        return _make_geometry_request(in_geom, 'simplify', params)

    def smooth_coord_lst(coord_lst):
        x_lst, y_lst = zip(*coord_lst)

        smoothing = 0.0005
        spline_order = 2
        knot_estimate = -1
        tck, fp, ier, msg = splprep([x_lst, y_lst], s=smoothing, k=spline_order, nest=knot_estimate, full_output=1)

        zoom = 5
        n_len = len(x_lst) * zoom
        x_ip, y_ip = splev(np.linspace(0, 1, n_len), tck[0])

        return [[x_ip[i], y_ip[i]] for i in range(0, len(x_ip))]

    # densify the geometry to help with too much deflection when smoothing
    new_geom = densify(geom)

    # get the dictionary key containing the geometry coordinate pairs
    geom_key = list(new_geom.keys())[0]

    # use the key to get all the coordinate pairs
    new_geom[geom_key] = [smooth_coord_lst(coords) for coords in new_geom[geom_key]]

    # simplify the geometry to remove unnecessary vertices
    new_geom = simplify(new_geom)

    # return smoothed geometry
    return new_geom


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
                "geometry": Geometry({'x': coordinates[0], 'y': coordinates[1], 'spatialReference': {"wkid": 4326}}),
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
        self.gauge_observation = None
        self.gauge_id = None
        self.gauge_units = None
        self.gauge_metric = None
        self.gauge_r0 = None
        self.gauge_r1 = None
        self.gauge_r2 = None
        self.gauge_r3 = None
        self.gauge_r4 = None
        self.gauge_r5 = None
        self.gauge_r6 = None
        self.gauge_r7 = None
        self.gauge_r8 = None
        self.gauge_r9 = None
        self.tracing_method = None
        self.trace_source = None

    def __str__(self):
        return f'{self.river_name} - {self.reach_name} - {self.difficulty}'

    def __repr__(self):
        return f'{self.__class__.__name__ } ({self.river_name} - {self.reach_name} - {self.difficulty})'

    @property
    def putin_x(self):
        return self.putin.geometry.x

    @property
    def putin_y(self):
        return self.putin.geometry.y

    @property
    def takeout_x(self):
        return self.takeout.geometry.x

    @property
    def takeout_y(self):
        return self.takeout.geometry.y

    @property
    def difficulty_filter(self):
        lookup_dict = {
            'I':    1.1,
            'I+':   1.2,
            'II-':  2.0,
            'II':   2.1,
            'II+':  2.2,
            'III-': 3.0,
            'III':  3.1,
            'III+': 3.2,
            'IV-':  4.0,
            'IV':   4.1,
            'IV+':  4.2,
            'V-':   5.0,
            'V':    5.1,
            'V+':   5.3
        }
        return lookup_dict[self.difficulty_maximum]

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
        if isinstance(self.geometry, Polyline):
            return Geometry({
                'x': np.mean([self.putin.geometry.x, self.takeout.geometry.x]),
                'y': np.mean([self.putin.geometry.y, self.takeout.geometry.y]),
                'spatialReference': self.putin.geometry.spatial_reference
            })

        # if both accesses are defined, use the mean of the accesses
        elif isinstance(self.putin, ReachPoint) and isinstance(self.takeout, ReachPoint):

            # create a point geometry using the average coordinates
            return Geometry({
                'x': np.mean([self.putin.geometry.x, self.takeout.geometry.x]),
                'y': np.mean([self.putin.geometry.y, self.takeout.geometry.y]),
                'spatialReference': self.putin.geometry.spatial_reference
            })

        # if only the putin is defined, use that
        elif isinstance(self.putin, ReachPoint):
            return self.putin.geometry

        # and if on the takeout is defined, likely the person digitizing was taking too many hits from the bong
        elif isinstance(self.takeout, ReachPoint):
            return self.takeout.geometry

        else:
            return None

    @property
    def extent(self):
        """
        Provide the extent of the reach as (xmin, ymin, xmax, ymax)
        :return: Set (xmin, ymin, xmax, ymax)
        """
        return (
            min(self.putin.geometry.x, self.takeout.geometry.x),
            min(self.putin.geometry.y, self.takeout.geometry.y),
            max(self.putin.geometry.x, self.takeout.geometry.x),
            max(self.putin.geometry.y, self.takeout.geometry.y),
        )

    @property
    def reach_search(self):
        if len(self.river_name) and len(self.reach_name):
            return f'{self.river_name} {self.reach_name}'
        elif len(self.river_name) and not len(self.reach_name):
            return self.river_name
        elif len(self.reach_name) and not len(self.river_name):
            return self.reach_name
        else:
            return ''

    @property
    def has_a_point(self):
        if self.putin is None and self.takeout is None:
            return False

        elif self.putin.geometry.type == 'Point' or self.putin.geometry == 'Point':
            return True

        else:
            return False

    @property
    def gauge_min(self):
        gauge_min_lst = [self.gauge_r0, self.gauge_r1, self.gauge_r2, self.gauge_r3, self.gauge_r4, self.gauge_r5]
        gauge_min_lst = [val for val in gauge_min_lst if val is not None]
        if len(gauge_min_lst):
            return min(gauge_min_lst)
        else:
            return None

    @property
    def gauge_max(self):
        gauge_max_lst = [self.gauge_r4, self.gauge_r5, self.gauge_r6, self.gauge_r7, self.gauge_r8, self.gauge_r9]
        gauge_max_lst = [val for val in gauge_max_lst if val is not None]
        if len(gauge_max_lst):
            return max(gauge_max_lst)
        else:
            return None

    @property
    def gauge_runnable(self):
        if (self.gauge_min and self.gauge_max and self.gauge_observation) and \
                (self.gauge_min < self.gauge_observation < self.gauge_max):
            return True
        else:
            return False

    @property
    def gauge_stage(self):
        metric_keys = ['gauge_r0', 'gauge_r1', 'gauge_r2', 'gauge_r3', 'gauge_r4', 'gauge_r5', 'gauge_r6', 'gauge_r7',
                       'gauge_r8', 'gauge_r9']

        def get_metrics(metric_keys):
            metrics = [getattr(self, key) for key in metric_keys]
            metrics = list(set(val for val in metrics if val is not None))
            metrics.sort()
            return metrics

        metrics = get_metrics(metric_keys)
        if not len(metrics):
            return None

        low_metrics = get_metrics(metric_keys[:6])
        high_metrics = get_metrics(metric_keys[5:])

        if not self.gauge_observation:
            return 'no gauge reading'

        if self.gauge_observation < metrics[0]:
            return 'too low'
        if self.gauge_observation > metrics[-1]:
            return 'too high'

        if len(metrics) == 2 or (len(metrics) == 1 and len(high_metrics) > 0):
            return 'runnable'

        if len(metrics) == 3:
            if metrics[0] < self.gauge_observation < metrics[1]:
                return 'lower runnable'
            if metrics[1] < self.gauge_observation < metrics[2]:
                return 'higher runnable'

        if len(metrics) == 4:
            if metrics[0] < self.gauge_observation < metrics[1]:
                return 'low'
            if metrics[1] < self.gauge_observation < metrics[2]:
                return 'medium'
            if metrics[2] < self.gauge_observation < metrics[3]:
                return 'high'

        if len(metrics) == 5 and len(low_metrics) > len(high_metrics):
            if metrics[0] < self.gauge_observation < metrics[1]:
                return 'very low'
            if metrics[1] < self.gauge_observation < metrics[2]:
                return 'medium low'
            if metrics[2] < self.gauge_observation < metrics[3]:
                return 'medium'
            if metrics[3] < self.gauge_observation < metrics[4]:
                return 'high'

        if len(metrics) == 5 and len(low_metrics) < len(high_metrics):
            if metrics[0] < self.gauge_observation < metrics[1]:
                return 'low'
            if metrics[1] < self.gauge_observation < metrics[2]:
                return 'medium'
            if metrics[2] < self.gauge_observation < metrics[3]:
                return 'medium high'
            if metrics[3] < self.gauge_observation < metrics[4]:
                return 'very high'

        if len(metrics) == 6:
            if metrics[0] < self.gauge_observation < metrics[1]:
                return 'low'
            if metrics[1] < self.gauge_observation < metrics[2]:
                return 'medium low'
            if metrics[2] < self.gauge_observation < metrics[3]:
                return 'medium'
            if metrics[3] < self.gauge_observation < metrics[4]:
                return 'medium high'
            if metrics[4] < self.gauge_observation < metrics[5]:
                return 'high'

        if len(metrics) == 7 and len(low_metrics) > len(high_metrics):
            if metrics[0] < self.gauge_observation < metrics[1]:
                return 'very low'
            if metrics[1] < self.gauge_observation < metrics[2]:
                return 'low'
            if metrics[2] < self.gauge_observation < metrics[3]:
                return 'medium low'
            if metrics[3] < self.gauge_observation < metrics[4]:
                return 'medium'
            if metrics[4] < self.gauge_observation < metrics[5]:
                return 'medium high'
            if metrics[5] < self.gauge_observation < metrics[6]:
                return 'high'

        if len(metrics) == 7 and len(low_metrics) < len(high_metrics):
            if metrics[0] < self.gauge_observation < metrics[1]:
                return 'low'
            if metrics[1] < self.gauge_observation < metrics[2]:
                return 'medium low'
            if metrics[2] < self.gauge_observation < metrics[3]:
                return 'medium'
            if metrics[3] < self.gauge_observation < metrics[4]:
                return 'medium high'
            if metrics[4] < self.gauge_observation < metrics[5]:
                return 'high'
            if metrics[5] < self.gauge_observation < metrics[6]:
                return 'very high'

        if len(metrics) == 8:
            if metrics[0] < self.gauge_observation < metrics[1]:
                return 'very low'
            if metrics[1] < self.gauge_observation < metrics[2]:
                return 'low'
            if metrics[2] < self.gauge_observation < metrics[3]:
                return 'medium low'
            if metrics[3] < self.gauge_observation < metrics[4]:
                return 'medium'
            if metrics[4] < self.gauge_observation < metrics[5]:
                return 'medium high'
            if metrics[5] < self.gauge_observation < metrics[6]:
                return 'high'
            if metrics[6] < self.gauge_observation < metrics[7]:
                return 'very high'

        if len(metrics) == 9 and len(low_metrics) > len(high_metrics):
            if metrics[0] < self.gauge_observation < metrics[1]:
                return 'extremely low'
            if metrics[1] < self.gauge_observation < metrics[2]:
                return 'very low'
            if metrics[2] < self.gauge_observation < metrics[3]:
                return 'low'
            if metrics[3] < self.gauge_observation < metrics[4]:
                return 'medium low'
            if metrics[4] < self.gauge_observation < metrics[5]:
                return 'medium'
            if metrics[5] < self.gauge_observation < metrics[6]:
                return 'medium high'
            if metrics[6] < self.gauge_observation < metrics[7]:
                return 'high'
            if metrics[7] < self.gauge_observation < metrics[8]:
                return 'very high'

        if len(metrics) == 9 and len(low_metrics) > len(high_metrics):
            if metrics[0] < self.gauge_observation < metrics[1]:
                return 'very low'
            if metrics[1] < self.gauge_observation < metrics[2]:
                return 'low'
            if metrics[2] < self.gauge_observation < metrics[3]:
                return 'medium low'
            if metrics[3] < self.gauge_observation < metrics[4]:
                return 'medium'
            if metrics[4] < self.gauge_observation < metrics[5]:
                return 'medium high'
            if metrics[5] < self.gauge_observation < metrics[6]:
                return 'high'
            if metrics[6] < self.gauge_observation < metrics[7]:
                return 'very high'
            if metrics[7] < self.gauge_observation < metrics[8]:
                return 'extremely high'

        if len(metrics) == 10:
            if metrics[0] < self.gauge_observation < metrics[1]:
                return 'extremely low'
            if metrics[1] < self.gauge_observation < metrics[2]:
                return 'very low'
            if metrics[2] < self.gauge_observation < metrics[3]:
                return 'low'
            if metrics[3] < self.gauge_observation < metrics[4]:
                return 'medium low'
            if metrics[4] < self.gauge_observation < metrics[5]:
                return 'medium'
            if metrics[5] < self.gauge_observation < metrics[6]:
                return 'medium high'
            if metrics[6] < self.gauge_observation < metrics[7]:
                return 'high'
            if metrics[7] < self.gauge_observation < metrics[8]:
                return 'very high'
            if metrics[8] < self.gauge_observation < metrics[9]:
                return 'extremely high'

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
            elif resp.status_code == 500:
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
        cleanup = re.sub('(.)\n(.)', '\g<1> \g<2>', cleanup)

        # get rid of any trailing newlines at end of entire text block
        cleanup = re.sub(r'\n+$', '', cleanup)

        # correct any leftover standalone links
        cleanup = cleanup.replace('<', '[').replace('>', ']')

        # get rid of any leading or trailing spaces
        cleanup = cleanup.strip()

        # finally call it good
        return cleanup

    def _parse_json(self, raw_json):

        def remove_backslashes(input_str):
            if isinstance(input_str, str) and len(input_str):
                return input_str.replace('\\', '')
            else:
                return input_str

        # pluck out the stuff we are interested in
        self._reach_json = raw_json['CContainerViewJSON_view']['CRiverMainGadgetJSON_main']

        # pull a bunch of attributes through validation and save as properties
        reach_info = self._reach_json['info']
        self.river_name = self._validate_aw_json(reach_info, 'river')

        self.reach_name = remove_backslashes(self._validate_aw_json(reach_info, 'section'))
        self.reach_alternate_name = remove_backslashes(self._validate_aw_json(reach_info, 'altname'))

        self.huc = self._validate_aw_json(reach_info, 'huc')
        self.description = self._validate_aw_json(reach_info, 'description')
        self.abstract = self._validate_aw_json(reach_info, 'abstract')
        self.agency = self._validate_aw_json(reach_info, 'agency')
        length = self._validate_aw_json(reach_info, 'length')
        if length:
            self.length = float(length)

        # helper to extract gauge information
        def get_gauge_metric(gauge_info, metric):
            if metric in gauge_info.keys() and gauge_info[metric] is not None:
                return float(gauge_info[metric])

        # get the gauge information
        if len(self._reach_json['gauges']):
            gauge_info = self._reach_json['gauges'][0]
            self.gauge_observation = get_gauge_metric(gauge_info, 'gauge_reading')
            self.gauge_id = gauge_info['gauge_id']
            self.gauge_units = gauge_info['metric_unit']
            self.gauge_metric = gauge_info['gauge_metric']

            for rng in self._reach_json['guagesummary']['ranges']:
                if rng['range_min'] and rng['gauge_min']:
                    setattr(self, f"gauge_{rng['range_min'].lower()}", float(rng['gauge_min']))
                if rng['range_max'] and rng['gauge_max']:
                    setattr(self, f"gauge_{rng['range_max'].lower()}", float(rng['gauge_max']))

        # save the update datetime as a true datetime object
        if reach_info['edited']:
            self.update_aw = datetime.datetime.strptime(reach_info['edited'], '%Y-%m-%d %H:%M:%S')

        # process difficulty
        if len(reach_info['class']) and reach_info['class'].lower() != 'none':
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

        # if there is not an abstract, create one from the description
        if (not self.abstract or len(self.abstract) == 0) and (self.description and len(self.description) > 0):

            # reomve all line returns, html tags, trim to 500 characters, and trim to last space to ensure full word
            self.abstract = self._cleanup_string(_strip_tags(reach_info['description']))
            self.abstract = self.abstract.replace('\\', '').replace('/n', '')[:500]
            self.abstract = self.abstract[:self.abstract.rfind(' ')]
            self.abstract = self.abstract + '...'

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
    def get_from_arcgis(cls, reach_id, reach_point_layer, reach_centroid_layer, reach_line_layer):

        # create instance of reach
        reach = cls(reach_id)

        # get a data frame for the centroid, since this is used to store the most reach information
        df_centroid = reach_centroid_layer.query_by_reach_id(reach_id).sdf

        # populate all relevant properties of the reach using the downloaded reach centroid
        for column in df_centroid.columns:
            if hasattr(reach, column):
                setattr(reach, column, df_centroid.iloc[0][column])

        # if reach points provided...is optional
        if reach_point_layer:

            # get the reach points as a spatially enabled dataframe
            df_points = reach_point_layer.query_by_reach_id(reach_id).sdf

            # iterate rows to create reach points in the parent reach object
            for _, row in df_points.iterrows():

                # get a dictionary of values, and swap out geometry for SHAPE
                row_dict = row.to_dict()
                row_dict['geometry'] = row_dict['SHAPE']

                # get a list of ReachPoint input args
                reach_point_args = inspect.getfullargspec(ReachPoint).args

                # create a list of input arguments from the columns in the row
                input_args = []
                for arg in reach_point_args[1:]:
                    if arg in row_dict.keys():
                        input_args.append(row_dict[arg])
                    else:
                        input_args.append(None)

                # use the input args to create a new reach point
                reach_point = ReachPoint(*input_args)

                # add the reach point to the reach points list
                reach._reach_points.append(reach_point)

        # try to get the line geometry, and use this for the reach geometry
        fs_line = reach_line_layer.query_by_reach_id(reach_id)
        if len(fs_line.features) > 0:
            for this_feature in fs_line.features:
                if this_feature.geometry is not None:
                    reach._geometry = Geometry(this_feature.geometry)
                    break

        # return the reach object
        return reach

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
        self._reach_points = [pt for pt in self._reach_points if pt.subtype != access_type]

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

    def snap_putin_and_takeout_and_trace(self, webmap=False, gis=None):
        """
        Update the putin and takeout coordinates, and trace the hydroline
        using the EPA's WATERS services.
        :param webmap: Boolean - Optional
            Return a web map widget if successful - useful for visualizing single reach.
        :param gis: Active GIS for performing hydrology analysis.
        :return:
        """
        # ensure a putin and takeout actually were found
        if self.putin is None or self.takeout is None:
            self.error = True
            self.notes = 'Reach does not appear to have both a put-in and take-out location defined.'
            trace_status = False

        # if there is something to work with, keep going
        else:

            # get the snapped and corrected reach locations for the put-in
            self.putin.snap_to_nhdplus()

            # if a put-in was not located using the WATERS service, flag
            if self.putin.nhdplus_measure is None or self.putin.nhdplus_reach_id is None:
                nhd_status = False

            # if the put-in was located using WATERS, flag as successful
            else:
                nhd_status = True

            # initialize trace_status to False first
            trace_status = False

            if nhd_status:

                # try to trace a few times using WATERS, but if it doesn't work, bingo to Esri Hydrology
                attempts = 0
                max_attempts = 5

                while attempts < max_attempts:

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
                            trace_status = False

                        else:
                            trace_status = True
                            self.tracing_method = 'EPA WATERS NHD Plus v2'
                            break

                    except:

                        # increment the attempt counter
                        attempts += 1

            # if the put-in has not yet been located using the WATERS service
            if not trace_status:

                # do a little voodoo to get a feature set containing just the put-in
                pts_df = self.reach_points_as_dataframe
                putin_fs = pts_df[
                    (pts_df['point_type'] == 'access') & (pts_df['subtype'] == 'putin')
                    ].spatial.to_featureset()

                # use the feature set to get a response from the watershed function using Esri's Hydrology service
                wtrshd_resp = hydrology.watershed(
                    input_points=putin_fs,
                    point_id_field='reach_id',
                    snap_distance=100,
                    snap_distance_units='Meters',
                    gis=gis
                )

                # update the putin if a point was found using the watershed function
                if len(wtrshd_resp._fields) and len(wtrshd_resp.snapped_points.features):
                    putin = self.putin
                    putin_geometry = wtrshd_resp.snapped_points.features[0].geometry
                    putin_geometry['spatialReference'] = wtrshd_resp.snapped_points.spatial_reference
                    putin.set_geometry(Geometry(putin_geometry))
                    self.set_putin(putin)

                # if a putin was not found, quit swimming in the ocean
                else:
                    self.error = True
                    self.notes = 'Put-in could not be located with neither WATERS nor Esri Hydrology services.'

                # trace using Esri Hydrology services
                attempts = 10
                fail_count = 0

                # set variable for tracking the trace response
                trace_resp = None

                # try to get a trace response
                while fail_count < attempts:
                    try:
                        trace_resp = hydrology.trace_downstream(putin_fs, point_id_field='reach_id', gis=gis)
                        break
                    except:
                        fail_count = fail_count + 1

                # if the trace was successful
                if trace_resp and not self.error:

                    # extract out the trace geometry
                    trace_geom = trace_resp.features[0].geometry
                    trace_geom['spatialReference'] = trace_resp.spatial_reference
                    trace_geom = Geometry(trace_geom)

                    # save the resolution for the smoothing later
                    trace_data_resolution = float(trace_resp.features[0].attributes['DataResolution'])

                    # snap the takeout to the traced line
                    takeout_geom = self.takeout.geometry.snap_to_line(trace_geom)
                    self.takeout.set_geometry(takeout_geom)

                    # trim the reach line to the takeout
                    line_geom = trace_geom.trim_at_point(self.takeout.geometry)

                    # ensure there are more than two vertices for smoothing
                    if line_geom.coordinates().size > 6:

                        # smooth the geometry since the hydrology tracing can appear a little jagged
                        self._geometry = _smooth_geometry(line_geom,
                                                          densify_max_segment_length=trace_data_resolution * 2,
                                                          gis=gis)

                    else:
                        self._geometry = line_geom

                    trace_status = True
                    self.tracing_method = "ArcGIS Online Hydrology Services"

            # if neither of those worked, flag the error
            if not trace_status:
                self.error = True
                self.notes = "The reach could not be trace with neither the EPA's WATERS service nor the Esri " \
                             "Hydrology services."

        # if map result desired, return it
        if webmap:
            return self.plot_map()
        else:
            return trace_status

    @property
    def geometry(self):
        """
        Return the reach polyline geometry.
        :return: Polyline Geometry
        """
        return self._geometry

    def _get_feature_attributes(self):
        """helper function for exporting features"""
        srs = pd.Series(dir(self))
        srs = srs[
            (~srs.str.startswith('_'))
            & (~srs.str.contains('as_'))
            & (srs != 'putin')
            & (srs != 'takeout')
            & (srs != 'intermediate_accesses')
            & (srs != 'geometry')
            & (srs != 'has_a_point')
            ]
        srs = srs[srs.apply(lambda p: not hasattr(getattr(self, p), '__call__'))]
        return {key: getattr(self, key) for key in srs}

    @property
    def as_feature(self):
        """
        Get the reach as an ArcGIS Python API Feature object.
        :return: ArcGIS Python API Feature object representing the reach.
        """
        if self.geometry:
            feat = Feature(geometry=self.geometry, attributes=self._get_feature_attributes())
        else:
            feat = Feature(attributes=self._get_feature_attributes())
        return feat

    @property
    def as_centroid_feature(self):
        """
        Get a feature with the centroid geometry.
        :return: Feature with point geometry for the reach centroid.
        """
        return Feature(geometry=self.centroid, attributes=self._get_feature_attributes())

    def publish(self, reach_line_layer, reach_centroid_layer, reach_point_layer):
        """
        Publish the reach to three feature layers; the reach line layer, the reach centroid layer,
        and the reach points layer.
        :param gis: GIS object providing the credentials.
        :param reach_line_layer: ReachLayer with line geometry to publish to.
        :param reach_centroid_layer: ReachLayer with point geometry for the centroid to publish to.
        :param reach_point_layer: ReachPointLayer
        :return: Boolean True if successful and False if not
        """
        if not self.putin and not self.takeout:
            return False

        # add the reach line if it was successfully traced
        if not self.error:
            resp_line = reach_line_layer.add_reach(self)
            add_line = len(resp_line['addResults'])

        # regardless, add the centroid and points
        resp_centroid = reach_centroid_layer.add_reach(self)
        add_centroid = len(resp_centroid['addResults'])

        resp_point = reach_point_layer.add_reach(self)
        add_point = len(resp_point['addResults'])

        # check results for adds and return correct response
        if not self.error and add_line and add_centroid and add_point:
            return True
        elif add_centroid and add_point:
            return True
        else:
            return False

    def publish_updates(self, reach_line_layer, reach_centroid_layer, reach_point_layer):
        """
        Based on the current status of the reach, push updates to the online Feature Services.
        :param reach_line_layer: ReachLayer with line geometry to publish to.
        :param reach_centroid_layer: ReachLayer with point geometry for the centroid to publish to.
        :param reach_point_layer: ReachPointLayer
        :return: Boolean True if successful and False if not
        """
        if not self.putin and not self.takeout:
            return False

        resp_line = reach_line_layer.update_reach(self)
        update_line = len(resp_line['updateResults'])

        resp_centroid = reach_centroid_layer.update_reach(self)
        update_centroid = len(resp_centroid['updateResults'])

        resp_putin = reach_point_layer.update_putin(self.putin)
        update_putin = len(resp_putin['updateResults'])

        resp_takeout = reach_point_layer.update_takeout(self.takeout)
        update_takeout = len(resp_takeout['updateResults'])

        # check results for adds and return correct response
        if update_line and update_centroid and update_putin and update_takeout:
            return True
        elif update_centroid and update_putin and update_takeout:
            return True
        else:
            return False

    def plot_map(self, gis=None):
        """
        Display reach and accesses on web map widget.
        :param gis: ArcGIS Python API GIS object instance.
        :return: map widget
        """
        if gis is None and arcgis.env.active_gis is not None:
            gis = arcgis.env.active_gis
        elif gis is None:
            gis = GIS()

        webmap = gis.map()
        webmap.basemap = 'topo-vector'
        webmap.extent = {
            'xmin': self.extent[0],
            'ymin': self.extent[1],
            'xmax': self.extent[2],
            'ymax': self.extent[3],
            'spatialReference': {'wkid': 4326}
        }
        if self.geometry:
            webmap.draw(
                shape=self.geometry,
                symbol={
                    "type": "esriSLS",
                    "style": "esriSLSSolid",
                    "color": [0, 0, 255, 255],
                    "width": 1.5
                }
            )
        if self.putin.geometry:
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
        if self.takeout.geometry:
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
        if self.as_centroid_feature.geometry:
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

        mpbx_otdrs = 'mapbox_outdoors'
        if mpbx_otdrs in webmap.gallery_basemaps:
            webmap.basemap = mpbx_otdrs

        def _fix_extent(wbmp):
            wbmp.extent = wbmp.extent - 1

        webmap.on_draw_end(_fix_extent, True)

        return webmap


class ReachPoint(object):
    """
    Discrete object facilitating working with reach points.
    """

    def __init__(self, reach_id, geometry, point_type, uid=None, subtype=None, name=None, side_of_river=None,
                 collection_method=None, update_date=None, notes=None, description=None, difficulty=None, **kwargs):

        self.reach_id = str(reach_id)
        self.point_type = point_type
        self.subtype = subtype
        self.name = name
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

    def __repr__(self):
        return f'{self.__class__.__name__ } ({self.reach_id} - {self.point_type} - {self.subtype})'

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

    def query_by_reach_id(self, reach_id, spatial_reference={'wkid': 4326}):
        return self.query(f"reach_id = '{reach_id}'", out_sr=spatial_reference)

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

    def update_putin_or_takeout(self, access):
        access_resp = self.query(
            f"reach_id = '{access.reach_id}' AND point_type = 'access' AND subtype = '{access.subtype}'",
            return_ids_only=True)['objectIds']
        if len(access_resp):
            oid_access = access_resp[0]
            access_feature = access.as_feature
            access_feature.attributes['OBJECTID'] = oid_access
            return self.edit_features(updates=[access_feature])
        else:
            return self.edit_features(adds=[access.as_feature])

    def update_putin(self, access):
        if not access.subtype == 'putin':
            raise Exception('A put-in access point must be provided to update the put-in.')
        return self.update_putin_or_takeout(access)

    def update_takeout(self, access):
        if not access.subtype == 'takeout':
            raise Exception('A take-out access point must be provided to update the take-out.')
        return self.update_putin_or_takeout(access)

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
            point_feature = reach.as_centroid_feature
            resp = self.edit_features(adds=[point_feature])

        elif self.properties.geometryType == 'esriGeometryPolyline':
            line_feature = reach.as_feature
            resp = self.edit_features(adds=[line_feature])

        else:
            raise Exception('The feature service geometry type must be either point or polyline.')

        return resp

    def update_reach(self, reach):

        # get oid of records matching reach_id
        oid_lst = self.query(f"reach_id = '{reach.reach_id}'", return_ids_only=True)['objectIds']

        # if a feature already exists - hopefully the case, get the oid, add it to the feature, and push it
        if len(oid_lst) > 0:

            # check the geometry type of the target feature service - point or line
            if self.properties.geometryType == 'esriGeometryPoint':
                update_feat = reach.as_centroid_feature

            elif self.properties.geometryType == 'esriGeometryPolyline':
                update_feat = reach.as_feature

            update_feat.attributes['OBJECTID'] = oid_lst[0]
            resp = self.edit_features(updates=[update_feat])

        # if the feature does not exist, add it
        else:
            resp = self.add_reach(reach)

        return resp
