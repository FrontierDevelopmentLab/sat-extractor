from pystac import MediaType
from pystac.extensions.eo import Band

MEDIA_TYPES = {
    "sentinel-2": MediaType.JPEG2000,
    "landsat-5": MediaType.GEOTIFF,
    "landsat-7": MediaType.GEOTIFF,
    "landsat-8": MediaType.GEOTIFF,
}

LANDSAT_PROPERTIES = {
    "landsat-5": {"DATA_TYPE": "L1TP", "SENSOR_ID": "TM"},
    "landsat-7": {"DATA_TYPE": "L1TP", "SENSOR_ID": "ETM"},
    "landsat-8": {"DATA_TYPE": "L1TP", "SENSOR_ID": "OLI_TIRS"},
}


LANDSAT8_BAND_INFO = {
    "B1": {
        "band": Band.create(
            name="B1",
            common_name="coastal",
            center_wavelength=0.48,
            full_width_half_max=0.02,
        ),
        "gsd": 30.0,
    },
    "B2": {
        "band": Band.create(
            name="B2",
            common_name="blue",
            center_wavelength=0.44,
            full_width_half_max=0.06,
        ),
        "gsd": 30.0,
    },
    "B3": {
        "band": Band.create(
            name="B3",
            common_name="green",
            center_wavelength=0.56,
            full_width_half_max=0.06,
        ),
        "gsd": 30.0,
    },
    "B4": {
        "band": Band.create(
            name="B4",
            common_name="red",
            center_wavelength=0.65,
            full_width_half_max=0.04,
        ),
        "gsd": 30.0,
    },
    "B5": {
        "band": Band.create(
            name="B5",
            common_name="nir",
            center_wavelength=0.86,
            full_width_half_max=0.03,
        ),
        "gsd": 30.0,
    },
    "B6": {
        "band": Band.create(
            name="B6",
            common_name="swir1",
            center_wavelength=1.6,
            full_width_half_max=0.08,
        ),
        "gsd": 30.0,
    },
    "B7": {
        "band": Band.create(
            name="B7",
            common_name="swir2",
            center_wavelength=2.2,
            full_width_half_max=0.2,
        ),
        "gsd": 30.0,
    },
    "B8": {
        "band": Band.create(
            name="B8",
            common_name="pan",
            center_wavelength=0.59,
            full_width_half_max=0.18,
        ),
        "gsd": 15.0,
    },
    "B9": {
        "band": Band.create(
            name="B9",
            common_name="cirrus",
            center_wavelength=1.37,
            full_width_half_max=0.02,
        ),
        "gsd": 30.0,
    },
    "B10": {
        "band": Band.create(
            name="B10",
            common_name="tirs1",
            center_wavelength=10.9,
            full_width_half_max=0.8,
        ),
        "gsd": 100.0,
    },
    "B11": {
        "band": Band.create(
            name="B11",
            common_name="tirs2",
            center_wavelength=12.0,
            full_width_half_max=1.0,
        ),
        "gsd": 100.0,
    },
}

LANDSAT7_BAND_INFO = {
    "B1": {
        "band": Band.create(
            name="B1",
            common_name="blue",
            center_wavelength=0.485,
            full_width_half_max=0.035,
        ),
        "gsd": 30.0,
    },
    "B2": {
        "band": Band.create(
            name="B2",
            common_name="green",
            center_wavelength=0.56,
            full_width_half_max=0.04,
        ),
        "gsd": 30.0,
    },
    "B3": {
        "band": Band.create(
            name="B3",
            common_name="red",
            center_wavelength=0.66,
            full_width_half_max=0.03,
        ),
        "gsd": 30.0,
    },
    "B4": {
        "band": Band.create(
            name="B4",
            common_name="nir",
            center_wavelength=0.835,
            full_width_half_max=0.065,
        ),
        "gsd": 30.0,
    },
    "B5": {
        "band": Band.create(
            name="B5",
            common_name="swir1",
            center_wavelength=1.65,
            full_width_half_max=0.10,
        ),
        "gsd": 30.0,
    },
    "B6_VCID_1": {
        "band": Band.create(
            name="B6_VCID_1",
            common_name="low-gain thermal infrared 1",
            center_wavelength=11.45,
            full_width_half_max=1.05,
        ),
        "gsd": 60.0,
    },
    "B6_VCID_2": {
        "band": Band.create(
            name="B6_VCID_2",
            common_name="high-gain thermal infrared 2",
            center_wavelength=11.45,
            full_width_half_max=1.05,
        ),
        "gsd": 60.0,
    },
    "B7": {
        "band": Band.create(
            name="B7",
            common_name="swir2",
            center_wavelength=2.215,
            full_width_half_max=0.135,
        ),
        "gsd": 30.0,
    },
    "B8": {
        "band": Band.create(
            name="B8",
            common_name="pan",
            center_wavelength=0.71,
            full_width_half_max=0.24,
        ),
        "gsd": 15.0,
    },
}

LANDSAT5_BAND_INFO = {
    "B1": {
        "band": Band.create(
            name="B1",
            common_name="blue",
            center_wavelength=0.485,
            full_width_half_max=0.035,
        ),
        "gsd": 30.0,
    },
    "B2": {
        "band": Band.create(
            name="B2",
            common_name="green",
            center_wavelength=0.56,
            full_width_half_max=0.04,
        ),
        "gsd": 30.0,
    },
    "B3": {
        "band": Band.create(
            name="B3",
            common_name="red",
            center_wavelength=0.66,
            full_width_half_max=0.03,
        ),
        "gsd": 30.0,
    },
    "B4": {
        "band": Band.create(
            name="B4",
            common_name="nir",
            center_wavelength=0.835,
            full_width_half_max=0.065,
        ),
        "gsd": 30.0,
    },
    "B5": {
        "band": Band.create(
            name="B5",
            common_name="swir1",
            center_wavelength=1.65,
            full_width_half_max=0.10,
        ),
        "gsd": 30.0,
    },
    "B6": {
        "band": Band.create(
            name="B6",
            common_name="thermal infrared 1",
            center_wavelength=11.45,
            full_width_half_max=1.05,
        ),
        "gsd": 60.0,
    },
    "B7": {
        "band": Band.create(
            name="B7",
            common_name="swir2",
            center_wavelength=2.215,
            full_width_half_max=0.135,
        ),
        "gsd": 30.0,
    },
}


SENTINEL2_BAND_INFO = {
    "B01": {
        "band": Band.create(
            name="B01",
            common_name="coastal",
            center_wavelength=0.443,
        ),
        "gsd": 60.0,
    },
    "B02": {
        "band": Band.create(
            name="B02",
            common_name="blue",
            center_wavelength=0.490,
        ),
        "gsd": 10.0,
    },
    "B03": {
        "band": Band.create(
            name="B03",
            common_name="green",
            center_wavelength=0.560,
        ),
        "gsd": 10.0,
    },
    "B04": {
        "band": Band.create(
            name="B04",
            common_name="red",
            center_wavelength=0.665,
        ),
        "gsd": 10.0,
    },
    "B05": {
        "band": Band.create(
            name="B05",
            common_name="rededge1",
            center_wavelength=0.705,
        ),
        "gsd": 20.0,
    },
    "B06": {
        "band": Band.create(
            name="B06",
            common_name="rededge2",
            center_wavelength=0.740,
        ),
        "gsd": 20.0,
    },
    "B07": {
        "band": Band.create(
            name="B07",
            common_name="rededge3",
            center_wavelength=0.783,
        ),
        "gsd": 20.0,
    },
    "B08": {
        "band": Band.create(
            name="B08",
            common_name="nir",
            center_wavelength=0.842,
        ),
        "gsd": 10.0,
    },
    "B8A": {
        "band": Band.create(
            name="B8A",
            common_name="nir08",
            center_wavelength=0.865,
        ),
        "gsd": 20.0,
    },
    "B09": {
        "band": Band.create(
            name="B09",
            common_name="nir09",
            center_wavelength=0.945,
        ),
        "gsd": 60.0,
    },
    "B10": {
        "band": Band.create(
            name="B10",
            common_name="cirrus",
            center_wavelength=1.375,
        ),
        "gsd": 60.0,
    },
    "B11": {
        "band": Band.create(
            name="B11",
            common_name="swir1",
            center_wavelength=1.610,
        ),
        "gsd": 20.0,
    },
    "B12": {
        "band": Band.create(
            name="B12",
            common_name="swir2",
            center_wavelength=2.190,
        ),
        "gsd": 20.0,
    },
}

BAND_INFO = {
    "sentinel-2": SENTINEL2_BAND_INFO,
    "landsat-5": LANDSAT5_BAND_INFO,
    "landsat-7": LANDSAT7_BAND_INFO,
    "landsat-8": LANDSAT8_BAND_INFO,
}
