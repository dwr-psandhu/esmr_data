import numpy as np
import pandas as pd
import os
import warnings
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import zipfile
import io
import logging
import yaml
import argparse
import esmr

# add name of this class to the logger instead of root logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


def download_and_unzip(url, extract_to="."):
    # Ensure the extract_to directory exists
    if not os.path.exists(extract_to):
        os.makedirs(extract_to)

    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        link = soup.find(lambda tag: tag.name == "a" and "Zipped CSV" in tag.text)
        parsed_url = urlparse(response.url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        if link:
            csv_url = link["href"]
            logger.info(f"Found Zipped CSV link: {base_url+csv_url}")

            # Follow the link to the zipped CSV page
            csv_response = requests.get(base_url + csv_url)
            if csv_response.status_code == 200:
                csv_soup = BeautifulSoup(csv_response.content, "html.parser")
                url_text = csv_soup.find(
                    lambda tag: tag.name == "a" and ".zip" in tag.text
                ).text
                if url_text:
                    final_url = url_text
                    fname = os.path.join(
                        extract_to, urlparse(final_url).path.split("/")[-1]
                    )

                    # Download the zipped file
                    with requests.get(final_url, stream=True, verify=True) as r:
                        r.raise_for_status()
                        with open(fname, "wb") as f:
                            for chunk in r.iter_content(chunk_size=8192):
                                f.write(chunk)
                    logger.info(f"File downloaded successfully: {fname}")

                    # Unzip the file
                    with zipfile.ZipFile(fname, "r") as z:
                        z.extractall(extract_to)
                    logger.info("File downloaded and extracted successfully")
                    return fname.replace(".zip", ".csv")
                else:
                    logger.info("URL for zip not found")
            else:
                logger.info(
                    f"Failed to retrieve the CSV URL: {csv_response.status_code}"
                )
        else:
            logger.info("Zipped CSV link not found")
    else:
        logger.info(f"Failed to retrieve the URL: {response.status_code}")
    return None


def process_csv(esmr_file, filter_conditions, extract_to="."):

    df = esmr.read_data_csv(esmr_file)
    data = esmr.ESMR(df)
    logger.info(f"Number of WWTP facilities : {len(data.get_facility_names())}")

    # Extract facility names from filter_conditions
    facility_names = set()
    for key in filter_conditions.keys():
        # facility name is the name after "_" of Flow,
        if key.endswith("_Flow"):
            facility_name = "_".join(key.split("_")[:-1])
            facility_name = facility_name.replace("_", " ")
        facility_names.add(facility_name)
    facility_names = list(facility_names)

    parameters = ["Flow", "Temperature", "Electrical Conductivity @ 25 Deg. C"]
    dfmap = {}
    for facility_name in facility_names:
        logger.info(f"Processing facility: {facility_name}")
        for parameter in parameters:
            dff = df[
                (df.facility_name == facility_name)
                & (df.location_place_type == "Effluent Monitoring")
                & (df.parameter == parameter)
            ]
            fname = facility_name.replace(" ", "_").replace("/", "_")
            pname = parameter.replace(" ", "_")
            dfmap[f"{fname}_{pname}"] = dff

    plotmap = {}
    for key, (column, condition) in filter_conditions.items():
        dfk = dfmap[key]
        if condition == "notna":
            filter_condition = dfk[column].notna()
        else:
            filter_condition = dfk[column] == condition
        dfr = extract_result(dfk, filter_condition, key)
        metadata = get_columns_unique_vals(dfk)
        plotmap[key] = (dfr, metadata)
        fname = os.path.join(extract_to, f"{key}.csv")
        write_out_data(dfr, metadata, fname)

    return plotmap


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


def write_out_data(dfk, metadata, fname):
    with open(fname, "w", newline="") as f:
        for ckey, cval in metadata.items():
            cval = str(cval).encode("ascii", "replace").decode("ascii")
            f.write(f"# {ckey}: {cval}\n")
        dfk.to_csv(f)


def extract_result(df, filter_condition, key, resample_condition="D"):
    dfk = df[filter_condition]
    if key.endswith("Flow"):
        dfr = dfk[["result"]].resample(resample_condition).sum()
    else:
        dfr = dfk[["result"]].resample(resample_condition).mean()
    return dfr


def plot_data(plotmap):
    import hvplot.pandas  # Import hvplot here to avoid dependency in the main script

    for key, (dfr, metadata) in plotmap.items():
        plot_type = "step" if "Electrical_Conductivity" in key else "line"
        if plot_type == "step":
            plot = dfr.hvplot.step()
        else:
            plot = dfr.hvplot()
        hvplot.save(plot.opts(title=key), f"{key}.png")


def main():
    parser = argparse.ArgumentParser(description="Process ESMR data")
    parser.add_argument(
        "--url", type=str, required=True, help="URL to download the zipped CSV file"
    )
    parser.add_argument(
        "--config", type=str, required=True, help="Path to the YAML configuration file"
    )
    parser.add_argument(
        "--extract_to",
        type=str,
        default=".",
        help="Directory to extract the zipped file",
    )
    # add option to skip download and unzip step
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip downloading and unzipping the file",
    )
    # add option to plot the data
    parser.add_argument(
        "--plot",
        action="store_true",
        help="Plot the data",
    )
    args = parser.parse_args()

    with open(args.config, "r") as f:
        filter_conditions = yaml.safe_load(f)
    if not args.skip_download:
        esmr_file = download_and_unzip(args.url, args.extract_to)
    else:
        # find local file starging with esmr ending with .csv
        esmr_file = None
        # Find the latest ESMR file in the directory
        esmr_files = [
            os.path.join(args.extract_to, file)
            for file in os.listdir(args.extract_to)
            if file.startswith("esmr") and file.endswith(".csv")
        ]
        if esmr_files:
            esmr_file = max(esmr_files, key=os.path.getctime)
    if esmr_file:
        logger.info(f"Processing ESMR file: {esmr_file}")
        plotmap = process_csv(esmr_file, filter_conditions, args.extract_to)
        if args.plot:
            plot_data(plotmap)
    else:
        logger.error("No ESMR file found")


if __name__ == "__main__":
    main()
