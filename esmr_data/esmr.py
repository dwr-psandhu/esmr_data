"""
Electronic Self Monitoring Report
"""

from dataclasses import dataclass, field
from typing import Any
import pandas as pd
import os
import warnings


def warn_single_unique(df, column):
    vals = df[column].unique().astype(str)
    if len(vals) == 0:
        warnings.warn(f"{column} does not have any unique values!")
        return "nan"  # FIXME: what else to do?
    elif len(vals) > 1:
        warnings.warn(f"{column} does not have a single unique value!: {vals}")
    else:
        return vals[0]


@dataclass
class Variable:
    name: str
    calculated_method: str
    units: str
    parameter: Any
    df: pd.DataFrame = field(default=None, repr=False)
    result: pd.DataFrame = field(repr=False, default=None)
    analytical_method_code: str = None
    qualifier: str = None
    mdl: pd.DataFrame = field(repr=False, default=None)
    ml: pd.DataFrame = field(repr=False, default=None)
    rl: pd.DataFrame = field(repr=False, default=None)

    def __post_init__(self):
        self.df = (
            self.parameter.df.astype({"calculated_method": "str"})
            .replace({"calculated_method": "nan"}, "")
            .query(
                f'(units == "{self.units}") '
                + f'& (calculated_method == "{self.calculated_method}")'
            )
        )
        self.df = self.df.set_index("sampling_datetime").sort_index()
        self.result = self.df.result
        self.qualifier = (
            "="  # FIXME:I don't understand DQ and DNQ with values in result column!
        )
        self.analytical_method_code = warn_single_unique(
            self.df, "analytical_method_code"
        )
        # if self.qualifier == 'DQ' or self.qualifier == 'DNQ':
        self.mdl = self.df[["mdl"]]
        self.ml = self.df[["ml"]]
        self.rl = self.df[["rl"]]
        # else:
        qtext = self.qualifier if self.qualifier != "=" else ""
        ctext = (
            "[" + self.calculated_method + "]" if self.calculated_method != "" else ""
        )
        # FIXME: not sure how to handle multiple units specified. Selecting first one in the unique list.
        self.result = self.df[["result"]].rename(
            {"result": f"{self.name} {ctext} ({self.units}) {qtext}"}, axis=1
        )


@dataclass
class Parameter:
    """
    Parameter is the value being measured. It can have one or more variables if measured in multiple units or calculated_method(s)
    """

    name: str
    location: Any
    df: pd.DataFrame = field(default=None, repr=False)

    def __post_init__(self):
        self.df = self.location.df.query(f'(parameter == "{self.name}")')

    def get_variables(self):
        cols = ["calculated_method", "units"]
        dfp = (
            self.df[cols]
            .astype(str)
            .replace("nan", "")
            .groupby(cols)
            .count()
            .reset_index()
        )
        return [Variable(self.name, *x, self) for x in dfp.values]


@dataclass
class Location:
    name: str
    facility: Any
    df: pd.DataFrame = field(default=None, repr=False)
    place_id: int = 0
    place_type: str = None
    desc: str = None

    def __post_init__(self):
        self.df = self.facility.df.query(f'location == "{self.name}"')
        self.place_type = warn_single_unique(self.df, "location_place_type")
        # FIXME: location_place_id can be different from the same location name!!!
        # self.place_id = assert_single_unique(self.df,'location_place_id')
        # self.desc = assert_single_unique(self.df,'location_desc')
        self.place_id = self.df["location_place_id"].unique().astype(str)
        self.desc = self.df["location_desc"].unique().astype(str)

    def get_parameter_names(self):
        return self.df.parameter.unique().astype(str)

    def get_parameter(self, parameter):
        return Parameter(parameter, self)


@dataclass
class Facility:
    name: str
    df: pd.DataFrame = field(repr=False)
    region: str = None
    place_id: int = 0

    def __post_init__(self):
        self.dfo = self.df
        self.df = self.df.query(f'facility_name == "{self.name}"')
        self.region = self.df["region"].unique()[0]
        self.place_id = self.df["facility_place_id"].unique()[0]

    def get_location_names(self):
        return self.df["location"].unique().astype(str)

    def get_location(self, location):
        return Location(location, self)

    def get_locations_of_type(self, location_place_type="Effluent Monitoring"):
        locations = (
            self.get_location_names()
        )  # TODO: could be more efficient with direct dataframe ops
        locs = []
        for l in locations:
            loc = self.get_location(l)
            if loc.place_type == location_place_type:
                locs.append(loc)
        return locs


@dataclass
class ESMR:
    df: pd.DataFrame

    def get_region_names(self):
        return self.df["region"].unique().astype(str)

    def get_facility_names(self, region=None):
        if region:
            dfx = self.df.query(f'region == "{region}"')
        else:
            dfx = self.df
        return dfx["facility_name"].unique().astype(str)

    def get_facility(self, name):
        return Facility(name, self.df)


def categorical_types():
    categorical_types = {
        "region": "category",
        "location": "category",
        "location_place_type": "category",
        "parameter": "category",
        "analytical_method_code": "category",
        "analytical_method": "category",
        "calculated_method": "category",
        "qualifier": "category",
        "units": "category",
        "review_priority_indicator": "category",
        "qa_codes": "category",
        "receiving_water_body": "category",
    }
    return categorical_types


def read_data_csv(csv_file):
    """
    read data from csv and convert to appropriate data type. Also cache in .pkl (pickle) format for faster loads
    """
    fname, _ = os.path.splitext(csv_file)
    pkl_file = fname + ".pkl"
    if os.path.exists(pkl_file):
        df = pd.read_pickle(pkl_file)
    else:
        df = pd.read_csv(csv_file, dtype=categorical_types())
        # TEMP FIX:
        # df.loc[13330034, "sampling_date"] = "2016-11-30"
        df["sampling_datetime"] = pd.to_datetime(
            df["sampling_date"] + " " + df["sampling_time"], errors="coerce"
        )
        df = df.drop(["sampling_date", "sampling_time"], axis=1)
        df["analysis_datetime"] = pd.to_datetime(
            df["analysis_date"] + " " + df["analysis_time"], errors="coerce"
        )
        df = df.drop(["analysis_date", "analysis_time"], axis=1)
        df.set_index("analysis_datetime", inplace=True)
        df.sort_index(inplace=True)
        df.to_pickle(pkl_file)
    return df


def build_facility_location_lat_lon(df):
    cols = ["latitude", "longitude", "facility_name", "location", "location_desc"]
    location_lat_lon = df.astype({"location": str}).groupby(cols).count().reset_index()
    # fixup obvious errors
    # +ve longitudes not in California, reverse the sign
    lon_0plus = location_lat_lon.eval("longitude > 0")
    location_lat_lon.loc[lon_0plus, "longitude"] = -location_lat_lon.loc[
        lon_0plus, "longitude"
    ]
    # -ve latitudes not in California, reverse the sign
    lat_0minus = location_lat_lon.eval("latitude < 0")
    location_lat_lon.loc[lat_0minus, "latitude"] = -location_lat_lon.loc[
        lat_0minus, "latitude"
    ]
    # specific fix for couple of locations based on location_desc field
    location_lat_lon.loc[location_lat_lon.eval("latitude < 1"), "latitude"] = (
        38 + 0 / 60.0 + 29.73 / 3600.0
    )  # correction from location_desc of Lincoln Center GWT System
    location_lat_lon.loc[location_lat_lon.eval("longitude > -40"), "longitude"] = -(
        121 + 3 / 60.0 + 10.6 / 3600.0
    )  # another one-off with longitude from description field
    return location_lat_lon


def get_facilities_with_no_latlon_info(df, facility_location_lat_lon):
    cols = ["facility_name"]
    df_facilities = df[cols].astype(str).groupby(cols).count().reset_index()
    df_facilites_merged = df_facilities.merge(facility_location_lat_lon, how="outer")
    return df_facilites_merged[
        (df_facilites_merged["latitude"].isnull())
        | (df_facilites_merged["longitude"].isnull())
    ]
