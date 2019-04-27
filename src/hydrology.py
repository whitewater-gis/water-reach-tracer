# coding: utf-8
"""
These functions help you use hydrology analysis.
"""

import logging as _logging
import arcgis
from arcgis.geoprocessing._support import _execute_gp_tool
from arcgis.features import FeatureSet
from arcgis.features.geo._accessor import _is_geoenabled
from pandas import DataFrame

_log = _logging.getLogger(__name__)


def _evaluate_spatial_input(input_points):
    """
    Helper function to determine if the input is either a FeatureSet or Spatially Enabled DataFrame, and
    output to FeatureSet for subsequent processing.
    :param input_points: FeatureSet or Spatially Enabled DataFrame
    :return: FeatureSet
    """
    if isinstance(input_points, FeatureSet):
        return input_points

    elif isinstance(input_points, DataFrame) and _is_geoenabled(input_points):
        return input_points.spatial.to_featureset()

    elif isinstance(input_points, DataFrame) and not _is_geoenabled(input_points):
        raise Exception(
            'input_points is a DataFrame, but does not appear to be spatially enabled. Using the <df>.spatial.set_'
            'geometry(col, sr=None) may help. (https://esri.github.io/arcgis-python-api/apidoc/html/arcgis.features.'
            'toc.html#arcgis.features.GeoAccessor.set_geometry)')

    else:
        raise Exception('input_points must be either a FeatureSet or Spatially Enabled DataFrame instead of {}'.format(
            type(input_points)))


def trace_downstream(input_points, point_id_field=None, source_database='Finest', generalize=False,
                     gis=None):
    """
    The Trace Downstream method delineates the downstream path from a specified location.
    Esri-curated elevation data is used to create an output polyline delineating the flow path
    downstream from the specified input location. This method accesses a service using multiple
    source databases which are available for different geographic areas and at different
    spatial scales.

    ==================     ====================================================================
    **Argument**           **Description**
    ------------------     --------------------------------------------------------------------
    input_points           Required Feature Set or Spatially Enabled DataFrame
                           Points delineating the starting location to calculate the downstream
                           location from.
    ------------------     --------------------------------------------------------------------
    point_id_field         Optional String
                           Field used to identify the feature from the source data. This is
                           useful for relating the results back to the original source data.
    ------------------     --------------------------------------------------------------------
    source_database        Optional String - Default "Finest"
                           Keyword indicating the source data that will be used in the
                           analysis. This keyword is an approximation of the spatial resolution
                           of the digital elevation model used to build the foundation
                           hydrologic  database. Since many elevation sources are distributed
                           with units of  arc seconds, this keyword is an approximation in
                           meters for easier understanding.
                           - Finest : Finest resolution available at each location from all
                             possible data sources.
                           - 10m : The hydrologic source was built from 1/3 arc second -
                             approximately 10 meter resolution, elevation data.
                           - 30m : The hydrologic source was built from 1 arc second -
                             approximately 30 meter resolution, elevation data.
                           - 90m : The hydrologic source was built from 3 arc second -
                             approximately 90 meter resolution, elevation data.
    ------------------     --------------------------------------------------------------------
    generalize             Optional Boolean - Default False
                           Determines if the output downstream trace lines will be smoothed
                           into simpler lines.
    ------------------     --------------------------------------------------------------------
    gis                    Optional GIS Object instance
                           If not provided as input, a GIS object instance logged into an
                           active portal with elevation helper services defined must already
                           be created in the active Python session. A GIS object instance can
                           also be optionally explicitly passed in through this parameter.
    ==================     ====================================================================

    :return:
       FeatureSet
    """
    kwargs = locals()

    param_db = {
        "input_points": (FeatureSet, "InputPoints"),
        "point_id_field": (str, 'PointIDField'),
        "source_database": (str, 'SourceDatabase'),
        "generalize": (str, 'Generalize'),
        "output_trace_line": (FeatureSet, "Output Trace Line"),
    }

    return_values = [
        {"name": "output_trace_line", "display_name": "Output Trace Line", "type": FeatureSet},
    ]

    # use helper function to evaluate the input points and convert them, if necessary, to a FeatureSet
    input_fs = _evaluate_spatial_input(input_points)

    if input_fs.geometry_type != 'esriGeometryPoint':
        raise Exception(
            'input_points FeatureSet must be point esriGeometryPoint, not {}'.format(input_fs.geometry_type))

    input_fields = input_fs.fields
    if point_id_field and point_id_field not in [f['name'] for f in input_fields] and len(input_fields):
        input_fields_str = ','.join(input_fields)
        raise Exception(
            'The provided point_id_field {} does not appear to be in the input_points FeatureSet fields - {}'.format(
                point_id_field, input_fields_str))

    if source_database not in ['Finest', '10m', '30m', '90m']:
        raise Exception(
            'source_database must be either "Finest", "10m", "30m", or "90m". {} does not appear to be one of '
            'these.'.format(source_database))

    if gis is None and arcgis.env.active_gis is None:
        raise Exception(
            'GIS must be defined either by directly passing in a GIS object created using credentials, or one must '
            'already be created in the active Python session.')
    elif gis is None:
        gis = arcgis.env.active_gis

    url = gis.properties.helperServices.hydrology.url

    return _execute_gp_tool(gis, "TraceDownstream", kwargs, param_db, return_values, True, url)


def watershed(input_points, point_id_field=None, snap_distance=10, snap_distance_units='Meters',
              source_database='Finest', generalize=False, gis=None):
    """
    The Watershed task is used to identify catchment areas based on a particular location you
    provide and ArcGIS Online Elevation data.

    ==================     ====================================================================
    **Argument**           **Description**
    ------------------     --------------------------------------------------------------------
    input_points           Required Feature Set or Spatially Enabled DataFrame
                           Points delineating the starting location to calculate the downstream
                           location from.
    ------------------     --------------------------------------------------------------------
    point_id_field         Optional String
                           Field used to identify the feature from the source data. This is
                           useful for relating the results back to the original source data.
    ------------------     --------------------------------------------------------------------
    snap_distance          Optional Integer - Default 10
    ------------------     --------------------------------------------------------------------
    snap_distance_units    Optional String - Default Meters
                           Meters | Kilometers | Feet | Yards | Miles
    ------------------     --------------------------------------------------------------------
    source_database        Optional String - Default "Finest"
                           Keyword indicating the source data that will be used in the analysis.
                           This keyword is an approximation of the spatial resolution of the
                           digital elevation model used to build the foundation hydrologic
                           database. Since many elevation sources are distributed with units of
                           arc seconds, this keyword is an approximation in meters for easier
                           understanding.
                           - Finest : Finest resolution available at each location from all
                             possible data sources.
                           - 10m : The hydrologic source was built from 1/3 arc second -
                             approximately 10 meter resolution, elevation data.
                           - 30m : The hydrologic source was built from 1 arc second -
                             approximately 30 meter resolution, elevation data.
                           - 90m : The hydrologic source was built from 3 arc second -
                             approximately 90 meter resolution, elevation data.
    ------------------     --------------------------------------------------------------------
    generalize             Optional Boolean - Default False
                           Determines if the output downstream trace lines will be smoothed
                           into simpler lines.
    ------------------     --------------------------------------------------------------------
    gis                    Optional GIS Object instance
                           If not provided as input, a GIS object instance logged into an
                           active portal with elevation helper services defined must already
                           be created in the active Python session. A GIS object instance can
                           also be optionally explicitly passed in through this parameter.
    ==================     ====================================================================
    :return:
        Result object comprised of two FeatureSets - one for watershed_area, and another for
        snapped_points
    """

    kwargs = locals()

    param_db = {
        "input_points": (FeatureSet, "InputPoints"),
        "point_id_field": (str, 'PointIDField'),
        "snap_distance": (int, 'SnapDistance'),
        "snap_distance_units": (int, 'SnapDistanceUnits'),
        "source_database": (str, 'SourceDatabase'),
        "generalize": (str, 'Generalize'),
        "watershed_area": (FeatureSet, "Watershed Area"),
        "snapped_points": (FeatureSet, "SnappedPoints")
    }

    return_values = [
        {"name": "watershed_area", "display_name": "Watershed Area", "type": FeatureSet},
        {"name": "snapped_points", "display_name": "Snapped Points", "type": FeatureSet}
    ]

    # use helper function to evaluate the input points and convert them, if necessary, to a FeatureSet
    input_fs = _evaluate_spatial_input(input_points)

    if input_fs.geometry_type != 'esriGeometryPoint':
        raise Exception(
            'input_points FeatureSet must be point esriGeometryPoint, not {}.'.format(input_fs.geometry_type))

    input_fields = input_fs.fields
    if point_id_field and point_id_field not in [f['name'] for f in input_fields] and len(input_fields):
        input_fields_str = ','.join(input_fields)
        raise Exception(
            'The provided point_id_field {} does not appear to be in the input_points FeatureSet fields - {}'.format(
                point_id_field, input_fields_str))

    if gis is None and arcgis.env.active_gis is None:
        raise Exception(
            'GIS must be defined either by directly passing in a GIS object created using credentials, or one must '
            'already be created in the active Python session.')
    elif gis is None:
        gis = arcgis.env.active_gis

    url = gis.properties.helperServices.hydrology.url

    return _execute_gp_tool(gis, "Watershed", kwargs, param_db, return_values, True, url)
