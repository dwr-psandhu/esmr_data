# %%
# # ESMR data from open data
# https://data.ca.gov/dataset/water-quality-effluent-electronic-self-monitoring-report-esmr-data
import hvplot.pandas
import numpy as np
import pandas as pd
import os
import warnings
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import zipfile, io
import logging

# %%

url = "https://data.ca.gov/dataset/water-quality-effluent-electronic-self-monitoring-report-esmr-data"
urlparse(url)
response = requests.get(url)
if response.status_code == 200:
    soup = BeautifulSoup(response.content, "html.parser")
    link = soup.find(lambda tag: tag.name == "a" and tag.text.find("Zipped CSV") > 0)
    parsed_url = urlparse(response.url)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    if link:
        csv_url = link["href"]
        logging.info(f"Found Zipped CSV link: {base_url+csv_url}")

        # Follow the link to the zipped CSV page
        csv_response = requests.get(base_url + csv_url)
        if csv_response.status_code == 200:
            csv_soup = BeautifulSoup(csv_response.content, "html.parser")
            url_text = csv_soup.find(
                lambda tag: tag.name == "a" and tag.text.find(".zip") > 0
            ).text
            if url_text:
                final_url = url_text
            else:
                logging.info("URL for zip not found")
        else:
            logging.info(f"Failed to retrieve the CSV URL: {csv_response.status_code}")
    else:
        logging.info("Zipped CSV link not found")
else:
    logging.info(f"Failed to retrieve the URL: {response.status_code}")
# %%

# Download the zipped file
fname = urlparse(final_url).path.split("/")[-1]
# %%
with requests.get(final_url, stream=True, verify=False) as r:
    r.raise_for_status()  # Check for any errors in the response
    with open(fname, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):  # Iterate over chunks
            f.write(chunk)
logging.info(f"File downloaded successfully: {fname}")
# %%
with zipfile.ZipFile(fname, "r") as z:
    z.extractall(".")  # Extract the contents of the zip file
logging.info("File downloaded and extracted successfully")

# %%
esmr_file = fname.replace(".zip", ".csv")
# esmr_file = "../tests/data/esmr-analytical-export_years-2006-2024_2024-03-13.csv"
# esmr_file = '../tests/data/esmr-analytical-export_year-2024_2024-12-02.csv'


# %%
from esmr_data import esmr

df = esmr.read_data_csv(esmr_file)
data = esmr.ESMR(df)
logging.info("Number of WWTP facilities : ", len(data.get_facility_names()))

# %%
facility_location_lat_lon = esmr.build_facility_location_lat_lon(df)

# %%
facility_names = [
    "EchoWater Resource Recovery Facility",
    "Mountain House WWTP",
    "Tracy WWTP",
    "City of Manteca WW Quality Control Facility",
    "Stockton Regional WW Control Facility",
    "White Slough Water Pollution Control Facility",
    "Ironhouse WWTF",
    "Sac City Combined WW Collection/TRT Sys",
    "Brentwood WWTP",
]
parameters = ["Flow", "Temperature", "Electrical Conductivity @ 25 Deg. C"]
plots = {"Flow": [], "Temperature": [], "Electrical_Conductivity_@_25_Deg._C": []}
dfmap = {}
for facility_name in facility_names:
    location_place_type = "Effluent Monitoring"
    for parameter in parameters:
        dff = df[
            (df.facility_name == facility_name)
            & (df.location_place_type == "Effluent Monitoring")
            & (df.parameter == parameter)
        ]
        fname = facility_name.replace(" ", "_")
        fname = fname.replace("/", "_")
        pname = parameter.replace(" ", "_")
        dfmap[f"{fname}_{pname}"] = dff


# %%
def get_columns_unique_vals(df):
    col_vals = {}
    for col in df.columns:
        if col not in [
            "result",
            "sampling_datetime",
            "analysis_datetime",
            "report_name",
            "smr_document_id",
        ]:
            col_vals[col] = df[col].unique().tolist()
    return col_vals


# %%
def write_out_data(dfk, key, metadata):
    with open(f"{key}.csv", "w", newline="") as f:
        for ckey, cval in metadata.items():
            f.write(f"# {ckey}: {cval}\n")
        dfk.to_csv(f)


# %%
def extract_result(df, filter_condition, resample_condition="D"):
    dfk = df[filter_condition]
    if key.endswith("Flow"):
        dfr = dfk[["result"]].resample(resample_condition).sum()
    else:
        dfr = dfk[["result"]].resample(resample_condition).mean()
    return dfr


# %%
# Define the filter conditions
filter_conditions = {
    "EchoWater_Resource_Recovery_Facility_Flow": ("analytical_method", "notna"),
    "EchoWater_Resource_Recovery_Facility_Temperature": (
        "calculated_method",
        "Daily Average (Mean)",
    ),
    "EchoWater_Resource_Recovery_Facility_Electrical_Conductivity_@_25_Deg._C": (
        "analytical_method_code",
        "E120.1",
    ),
    "Mountain_House_WWTP_Flow": ("analytical_method_code", "DU"),
    "Mountain_House_WWTP_Temperature": ("calculated_method", "Daily Average (Mean)"),
    "Mountain_House_WWTP_Electrical_Conductivity_@_25_Deg._C": (
        "analytical_method_code",
        "notna",
    ),
    "Tracy_WWTP_Flow": ("analytical_method_code", "notna"),
    "Tracy_WWTP_Temperature": ("calculated_method", "Daily Average (Mean)"),
    "Tracy_WWTP_Electrical_Conductivity_@_25_Deg._C": (
        "analytical_method_code",
        "notna",
    ),
    "City_of_Manteca_WW_Quality_Control_Facility_Flow": (
        "analytical_method_code",
        "notna",
    ),
    "City_of_Manteca_WW_Quality_Control_Facility_Temperature": (
        "calculated_method",
        "Daily Average (Mean)",
    ),
    "City_of_Manteca_WW_Quality_Control_Facility_Electrical_Conductivity_@_25_Deg._C": (
        "analytical_method_code",
        "notna",
    ),
    "Stockton_Regional_WW_Control_Facility_Flow": ("analytical_method_code", "notna"),
    "Stockton_Regional_WW_Control_Facility_Temperature": (
        "calculated_method",
        "Daily Average (Mean)",
    ),
    "Stockton_Regional_WW_Control_Facility_Electrical_Conductivity_@_25_Deg._C": (
        "analytical_method_code",
        "notna",
    ),
    "Brentwood_WWTP_Flow": ("analytical_method_code", "notna"),
    "Brentwood_WWTP_Temperature": ("analytical_method_code", "notna"),
    "Brentwood_WWTP_Electrical_Conductivity_@_25_Deg._C": (
        "analytical_method_code",
        "notna",
    ),
    "White_Slough_Water_Pollution_Control_Facility_Flow": (
        "analytical_method_code",
        "notna",
    ),
    "White_Slough_Water_Pollution_Control_Facility_Temperature": (
        "calculated_method",
        "24-hour Average",
    ),
    "White_Slough_Water_Pollution_Control_Facility_Electrical_Conductivity_@_25_Deg._C": (
        "analytical_method_code",
        "notna",
    ),
}
# %%
# Plot data for each key
plotmap = {}
for key, (column, condition) in filter_conditions.items():
    dfk = dfmap[key]
    if condition == "notna":
        filter_condition = dfk[column].notna()
    else:
        filter_condition = dfk[column] == condition
    plot_type = "step" if "Electrical_Conductivity" in key else "line"
    dfr = extract_result(dfk, filter_condition)
    metadata = get_columns_unique_vals(dfk)
    if plot_type == "step":
        plotmap[key] = dfr.hvplot.step()
    else:
        plotmap[key] = dfr.hvplot()
    write_out_data(dfr, key, metadata)
# %%
for k in plotmap.keys():
    hvplot.save(plotmap[k].opts(title=k), f"{k}.png")

# %%
