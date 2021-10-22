from typing import List
from typing import Union

import pandas as pd
import pystac
import shapely
from google.cloud import bigquery
from google.oauth2 import service_account
from pystac.extensions.eo import AssetEOExtension
from pystac.extensions.eo import EOExtension
from pystac.extensions.projection import ProjectionExtension
from satextractor.models.constellation_info import BAND_INFO
from satextractor.models.constellation_info import LANDSAT_PROPERTIES
from satextractor.models.constellation_info import MEDIA_TYPES
from satextractor.utils import get_utm_epsg


def gcp_region_to_item_collection(
    credentials: str,
    region: Union[shapely.geometry.Polygon, shapely.geometry.MultiPolygon],
    start_date: str,
    end_date: str,
    constellations: List[str],
) -> pystac.ItemCollection:
    """Create stac ItemCollection for a given Sentinel 2
       Google Storage Region between dates.

    Args:
        credentials (str): The bigquery client credentials json path
        region (Union[shapely.geometry.Polygon, shapely.geometry.MultiPolygon]): the region
        start_date (str): sensing start date
        end_date (str): sensing end date

    Returns:
        pystac.ItemCollection: a item collection for the given region and dates
    """
    credentials = service_account.Credentials.from_service_account_file(credentials)

    # Construct a BigQuery client object.
    client = bigquery.Client(credentials=credentials)

    dfs = []

    for constellation in constellations:

        if constellation == "sentinel-2":
            df = get_sentinel_2_assets_df(client, region, start_date, end_date)
        else:
            df = get_landsat_assets_df(
                client,
                region,
                start_date,
                end_date,
                constellation,
            )

        df["constellation"] = constellation
        dfs.append(df)

    df = pd.concat(dfs)

    return create_stac_item_collection_from_df(df)


def get_landsat_assets_df(
    client: bigquery.Client,
    shp: Union[shapely.geometry.Polygon, shapely.geometry.MultiPolygon],
    start_date: str,
    end_date: str,
    constellation: str,
) -> pd.DataFrame:
    """Perform a bigquery to obtain landsat assets as a dataframe.

    Args:
        client (bigquery.Client): The bigquery client with correct auth
        region (Union[shapely.geometry.Polygon, shapely.geometry.MultiPolygon]): the region
        start_date (str): sensing start date
        end_date (str): sensing end date
        constellation (str): which constellation to retreive in ['landsat-5','landsat-7','landsat-8']

    Returns:
        [type]: a dataframe with the query results
    """

    if shp.type == "Polygon":
        shp = [shp]

    dfs = []

    for subshp in shp:
        (
            region_west_lon,
            region_south_lat,
            region_east_lon,
            region_north_lat,
        ) = subshp.bounds  # this won't work for multipolygons. need to manage this.

        query = f"""
        SELECT * FROM
        `bigquery-public-data.cloud_storage_geo_index.landsat_index`
        WHERE DATE(sensing_time) >= "{start_date}" and DATE(sensing_time) <= "{end_date}"
        AND spacecraft_id = "{constellation.upper().replace('-','_')}"
        AND data_type = "{LANDSAT_PROPERTIES[constellation]['DATA_TYPE']}"
        AND sensor_id = "{LANDSAT_PROPERTIES[constellation]['SENSOR_ID']}"
        AND west_lon <= {region_east_lon}
        AND east_lon >= {region_west_lon}
        AND north_lat >= {region_south_lat}
        AND south_lat <= {region_north_lat}
        """
        query_job = client.query(query)  # Make an API request.

        dfs.append(query_job.to_dataframe())

    df = pd.concat(dfs)

    # de-dup
    df = df.groupby("product_id").nth(0).reset_index()

    return df


def get_sentinel_2_assets_df(
    client: bigquery.Client,
    shp: Union[shapely.geometry.Polygon, shapely.geometry.MultiPolygon],
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """Perform a bigquery to obtain sentinel 2 assets as a dataframe.

    Be aware that west/east_lon and south/nort_lat in bigquery are the
    availaible pixels bounds and not the actual Granule bounds.

    Args:
        client (bigquery.Client): The bigquery client with correct auth
        region (Union[shapely.geometry.Polygon, shapely.geometry.MultiPolygon]): the region
        start_date (str): sensing start date
        end_date (str): sensing end date

    Returns:
        [type]: a dataframe with the query results
    """

    if shp.type == "Polygon":
        shp = [shp]

    dfs = []

    for subshp in shp:
        (
            region_west_lon,
            region_south_lat,
            region_east_lon,
            region_north_lat,
        ) = subshp.bounds  # this won't work for multipolygons. need to manage this.

        query = f"""
        SELECT * FROM
        `bigquery-public-data.cloud_storage_geo_index.sentinel_2_index`
        WHERE DATE(sensing_time) >= "{start_date}" and DATE(sensing_time) <= "{end_date}"
        AND west_lon <= {region_east_lon}
        AND east_lon >= {region_west_lon}
        AND north_lat >= {region_south_lat}
        AND south_lat <= {region_north_lat}
        AND NOT REGEXP_CONTAINS(granule_id,"S2A_OPER")
        """
        query_job = client.query(query)  # Make an API request.

        dfs.append(query_job.to_dataframe())

    df = pd.concat(dfs)

    # de-dup
    df = df.groupby("product_id").nth(0).reset_index()

    return df


def create_stac_item_collection_from_df(df: pd.DataFrame) -> pystac.ItemCollection:
    """Given a df containing the results of a bigquery sentinel 2 job
    creates a stac item collection with all the assets

    Args:
        df (pd.DataFrame): The dataframe resulting from a bigquery job

    Returns:
        pystac.ItemCollection: a item collection for the given region and dates
    """
    items = pystac.ItemCollection([create_stac_item(row) for _, row in df.iterrows()])
    return items


def get_landsat_asset_images_url(row: pd.Series, band: str) -> str:
    """Given a bigquery df row and a band return the image url

    Args:
        row (pd.Series): a row from the bigquery job df
        band (str): the band names

    Returns:
        [str]: the url of the band tif file
    """
    return f"{row.base_url}/{row.base_url.split('/')[-1]}_{band}.TIF"


def get_s2_asset_images_url(row: pd.Series, band: str) -> str:
    """Given a bigquery df row and a band return the image url

    Args:
        row (pd.Series): a row from the bigquery job df
        band (str): the band names

    Returns:
        [str]: the url of the jp2 file
    """
    datatake_sensing_time = row.product_id.split("_")[2]
    base_url = f"{row.base_url}/GRANULE/{row.granule_id}/IMG_DATA/T{row.mgrs_tile}_{datatake_sensing_time}"
    return f"{base_url}_{band}.jp2"


def create_stac_item(row: pd.Series) -> pystac.Item:
    """Creates a stac Item from a given bigquery job df row

    Args:
        row (pd.Series): a row from the bigquery job df

    Returns:
        pystac.Item: The resulting pystac Item
    """
    coordinates = [
        [
            [row.west_lon, row.south_lat],
            [row.east_lon, row.south_lat],
            [row.east_lon, row.north_lat],
            [row.west_lon, row.north_lat],
            [row.west_lon, row.south_lat],
        ],
    ]
    geometry = {"type": "Polygon", "coordinates": coordinates}
    bbox = [row.west_lon, row.south_lat, row.east_lon, row.north_lat]

    if row.constellation == "sentinel-2":
        _id = row.granule_id
    else:
        _id = row.scene_id

    item = pystac.Item(
        id=_id,
        geometry=geometry,
        bbox=bbox,
        datetime=row.sensing_time,
        properties={},
    )

    # Set commo gsd to 10m, bands in different resolution will be explicit
    item.common_metadata.gsd = 10.0
    item.common_metadata.constellation = row.constellation

    # Enable eo
    EOExtension.add_to(item)
    eo_ext = EOExtension.ext(item)
    eo_ext.cloud_cover = row.cloud_cover

    # Enable proj
    ProjectionExtension.add_to(item)
    proj_ext = ProjectionExtension.ext(item)
    proj_ext.epsg = get_utm_epsg(
        row.north_lat,
        row.west_lon,
    )  # might need to make sure this comes from somewhere else.

    # Add bands
    for band_id, band_info in BAND_INFO[row.constellation].items():

        if row.constellation == "sentinel-2":
            band_url = get_s2_asset_images_url(row, band_id)
        else:
            band_url = get_landsat_asset_images_url(row, band_id)

        asset = pystac.Asset(
            href=band_url,
            media_type=MEDIA_TYPES[row.constellation],
            roles=["data"],
            extra_fields={"gsd": band_info["gsd"]},
        )
        eo_asset = AssetEOExtension.ext(asset)
        eo_asset.bands = [band_info["band"]]

        item.add_asset(band_id, asset)

    return item
