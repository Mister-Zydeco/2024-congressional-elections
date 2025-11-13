#!/usr/bin/env python
# coding: utf-8

import json

import shapefile as shpf
from pyproj import Transformer
from shapely.geometry import mapping, shape

import HRElectViz.ushelper as ush

type GeoJSONfcb = shpf.GeoJSONFeatureCollectionWithBBox


def roundpt(v: list[float], places=5):
    return [round(v[0], places), round(v[1], places)]


def transform_coords(coords, transformer):
    if isinstance(coords[0], (float, int)):  # Point
        return roundpt(transformer.transform(coords[0], coords[1]))
    elif isinstance(coords[0][0], (float, int)):  # LineString or Polygon ring
        return [roundpt(transformer.transform(x, y)) for x, y in coords]
    else:  # MultiLineString or MultiPolygon
        return [
            transform_coords(sub_coords, transformer) for sub_coords in coords
        ]


def transform_geometry(geom: GeoJSONfcb, src_epsg: str, dest_epsg: str) -> None:
    xform = Transformer.from_crs(src_epsg, dest_epsg, always_xy=True)
    for feature in geom['features']:
        if 'geometry' in feature and 'coordinates' in feature['geometry']:
            feature['geometry']['coordinates'] = transform_coords(
                feature['geometry']['coordinates'], xform
            )


def filter_by_state_f(
    data: GeoJSONfcb, state_abbrs: list[str], exclude: bool
) -> None:
    mapping = ush.fips_to_abbr

    def condition(feat) -> bool:
        cond: bool = mapping[feat['properties']['STATEFP']] in state_abbrs
        return cond if not exclude else cond

    data['features'] = [
        feature for feature in data['features'] if condition(feature)
    ]


def parse_geoid(geoid: str) -> tuple[str, int]:
    district_no: int = 0
    match geoid[-2:]:
        case 'ZZ':
            pass
        case '98' | '00':
            district_no = 1
        case _:
            district_no = int(geoid[-2:])
    state = ush.fips_to_abbr[geoid[:2]]
    return state, district_no


class DistrictsGeoData:
    def __init__(self, shp_path: str, src_epsg: str):
        self.geojson_data = shpf.Reader(shp_path).__geo_interface__
        self.epsg = src_epsg

    def as_str(self) -> str:
        return json.dumps(self.geojson_data, indent=4)

    def to_file(self, path: str) -> None:
        with open(path, 'w') as outfile:
            json.dump(self.geojson_data, outfile, indent=4)

    def xform_geometry(self, dest_epsg: str) -> None:
        transform_geometry(self.geojson_data, self.epsg, dest_epsg)
        self.epsg = dest_epsg

    def filter_by_state(self, state_abbrs: list[str], exclude=False) -> None:
        filter_by_state_f(self.geojson_data, state_abbrs, exclude)

    def get_props(self, props: list[str]) -> dict[str, list]:
        props_full = [prop for prop in props if prop != 'GEOID']
        props_full.append('GEOID')
        records: list[list] = [
            [feature['properties'][prop] for prop in props_full]
            for feature in self.geojson_data['features']
        ]
        col_dict: dict[str, list] = {
            prop: [rec[propno] for rec in records]
            for propno, prop in enumerate(props_full)
        }
        return col_dict

    def simplify(self, tolerance):
        simplified_features = []
        for feature in self.geojson_data['features']:
            geometry = shape(feature['geometry'])
            simplified_geometry = geometry.simplify(
                tolerance, preserve_topology=True
            )  # preserve_topology is crucial for polygon simplification

            simplified_feature = {
                'type': 'Feature',
                'geometry': mapping(simplified_geometry),
                'properties': feature['properties'],
            }
            simplified_features.append(simplified_feature)

        self.geojson_data['features'] = simplified_features
