from arcgis.geometry import Geometry, Point, Polyline
from arcgis.geometry._types import HASARCPY, HASSHAPELY
from shapely import ops


@classmethod
def from_shapely(cls, shapely_geometry, spatial_reference=None):
    """
    Creates a Python API Geometry object from a Shapely geometry object.
    ===============     ====================================================
    **Argument**        **Description**
    ---------------     ----------------------------------------------------
    shapely_geometry    Required Shapely Geometry.
    ---------------     ----------------------------------------------------
    spatial_reference   Optional SpatialReference for the output geometry.
    ===============     ====================================================
    :returns: Python API Geometry object.
    """
    if HASSHAPELY:
        geometry = cls(shapely_geometry.__geo_interface__)
        if spatial_reference:
            geometry.spatial_reference = spatial_reference
        return geometry
    else:
        raise Exception('Shapely is required to execute from_shapely.')


def snap_to_line(self, polyline_geometry):
    """
    Returns a new point snapped to the closest location along the input line geometry.

    ===============     ====================================================================
    **Argument**        **Description**
    ---------------     --------------------------------------------------------------------
    polyline_geometry   Required arcgis.geometry.Polyline geometry the Point will be snapped
                        to.
    ===============     ====================================================================

    :return: arcgis.geometry.Point coincident with the nearest location along the input
             arcgis.geometry.Polyline object

    """
    if not isinstance(self, Point):
        raise Exception('Snap to line can only be performed on a Point geometry object.')
    if not isinstance(polyline_geometry, Polyline):
        raise Exception('Snapping target must be a single Polyline geometry object.')
    if self.spatial_reference is None:
        raise Warning('The spatial reference for the point to be snapped to a line is not defined.')
    if polyline_geometry.spatial_reference is None:
        raise Warning('The spatial reference of the line being snapped to is not defined.')
    if not self.spatial_reference != polyline_geometry.spatial_reference:
        raise Exception('The spatial reference for the point and the line are not the same.')

    if HASARCPY:
        polyline_geometry = polyline_geometry.as_arcpy
        return Point(self.as_arcpy.snapToLine(in_point=polyline_geometry))

    elif HASSHAPELY:
        polyline_geometry = polyline_geometry.as_shapely
        point_geometry = self.as_shapely
        snap_point = polyline_geometry.interpolate(polyline_geometry.project(point_geometry))
        snap_point = Geometry.from_shapely(snap_point, self.spatial_reference)
        return snap_point

    else:
        raise Exception('Either arcpy or Shapely is required to perform snap_to_line')


def split_at_point(self, point_geometry):
    """
    Returns two polyline geometry objects as a list split at the intersection of the line.

    ===============     ====================================================================
    **Argument**        **Description**
    ---------------     --------------------------------------------------------------------
    point_geometry      Required arcgis.geometry.Point geometry defining the location the line
                        will be split at.
    ===============     ====================================================================

    :return: Two item list of arcgis.geometry.Polyline objects on either side of the input
             point location.
    """
    if not isinstance(self, Polyline):
        raise Exception('Split at point can only be performed on a Polyline geometry object.')
    if not isinstance(point_geometry, Point):
        raise Exception('Split at point requires a Point geometry object to define the split location.')
    if self.spatial_reference is None:
        raise Warning('The spatial reference for the line to be split is not defined.')
    if point_geometry.spatial_reference is None:
        raise Warning('The spatial reference of the point defining the split location is not defined.')
    if not self.spatial_reference != point_geometry.spatial_reference:
        raise Exception('The spatial reference for the line and point are not the same.')

    #     if HASARCPY:
    #         raise Exception('Not yet implemented')

    if HASSHAPELY:
        linestring_geometry = self.as_shapely
        point_geometry = point_geometry.as_shapely
        split_result = ops.split(linestring_geometry, point_geometry)
        polyline_list = [Geometry.from_shapely(line_string, self.spatial_reference)
                         for line_string in split_result]
        return polyline_list

    else:
        raise Exception('Shapely is required to perform split_at_point')


def trim_at_point(self, point_geometry):
    return self.split_at_point(point_geometry)[0]


Geometry.from_shapely = from_shapely
Geometry.snap_to_line = snap_to_line
Geometry.split_at_point = split_at_point
Geometry.trim_at_point = trim_at_point
