#!/usr/bin/env python3
"""
 @author Johannes Aalto
 SPDX-License-Identifier: Apache-2.0
"""

import os, sys

import pandas as pd
import requests

from typing import Iterable, Dict, List
from influxdb_client import InfluxDBClient
from datetime import datetime

try:
    from dotenv import load_dotenv

    load_dotenv(dotenv_path=".env")
except ImportError:
    pass

#disable warnings
import urllib3
urllib3.disable_warnings()

import warnings
from influxdb_client.client.warnings import MissingPivotFunction
warnings.simplefilter("ignore", MissingPivotFunction)


#######################################################
#####  CONFIG                                     #####
#######################################################

ignored_columns = ["host", "topic"]  # Define the columns you want to ignore
start_date = '2023-02-20'
end_date = '2024-03-03'


# Generate daily time frames. For other timeframes change freq='D'
date_ranges = pd.date_range(start=start_date, end=end_date, freq='D')
time_frames = [(start, end) for start, end in zip(date_ranges, date_ranges[1:])]

# Add a final chunk to now if necessary
if date_ranges[-1] < pd.to_datetime(end_date):
    time_frames.append((date_ranges[-1], pd.to_datetime(end_date)))



def get_tag_cols(dataframe_keys: Iterable) -> Iterable:
    """Filter out dataframe keys that are not tags"""
    return (
        k
        for k in dataframe_keys
        if not k.startswith("_") and k not in ["result", "table"]
    )

def get_influxdb_lines(df: pd.DataFrame) -> str:
    """
    Convert the Pandas Dataframe into InfluxDB line protocol.

    The dataframe should be similar to results received from query_api.query_data_frame()

    Not quite sure if this supports all kinds if InfluxDB schemas.
    It might be that influxdb_client package could be used as an alternative to this,
    but I'm not sure about the authorizations and such.

    Protocol description: https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/
    """
    # dont use this ignore this by setting -influxSkipMeasurement
    #line = df["_measurement"]
    line = "thisISignored"

    for col_name in get_tag_cols(df):
        if col_name not in ignored_columns:  # Only process columns not in the ignored list
            line += ("," + col_name + "=") + df[col_name].astype(str)

    line += (
        " "
        + df["_field"]
        + "="
        + df["_value"].astype(str)
        + " "
        + df["_time"].astype(int).astype(str)
    )
    return "\n".join(line)


def main(args: Dict[str, str]):
    print("args: " + str(args.keys()))
    bucket = args.pop("bucket")
    url = args.pop("vm_addr")

    for k, v in args.items():
        if v is not None:
            os.environ[k] = v
        print(f"Using {k}={os.getenv(k)}")

    client = InfluxDBClient.from_env_properties()

    query_api = client.query_api()  # use synchronous to see errors

    # Get all unique series by reading first entry of every table.
    # With latest InfluxDB we could possibly use "schema.measurements()" but this doesn't exist in 2.0
    first_in_series = f"""
    from(bucket: "{bucket}")
    |> range(start: 0, stop: now())
    |> first()"""
    timeseries: List[pd.DataFrame] = query_api.query_data_frame(first_in_series)

    # get all unique measurement-field pairs and then fetch and export them one-by-one.
    # With really large databases the results should be possibly split further
    # Something like query_data_frame_stream() might be then useful.
    measurements_and_fields = [
#        gr[0] for df in timeseries for gr in df.groupby(["_measurement", "_field"])
        # fix from JasperE84 https://github.com/jonppe/influx_to_victoriametrics/issues/1
        gr[0] for df in timeseries for gr in df.groupby(["_measurement", "_field"])
    ]
    print(f"Found {len(measurements_and_fields)} unique time series")
    for meas, field in measurements_and_fields:
      for start, end in time_frames:
        start_str = start.strftime('%Y-%m-%d')
        end_str = end.strftime('%Y-%m-%d')
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Exporting {meas}_{field}")
        whole_series = f"""
        from(bucket: "{bucket}")
        |> range(start: {start_str}, stop: {end_str})
        |> filter(fn: (r) => r["_measurement"] == "{meas}")
        |> filter(fn: (r) => r["_field"] == "{field}")
        """
        df = query_api.query_data_frame(whole_series)
        
        if df is not None and not df.empty:
            line = get_influxdb_lines(df)
            # "db" is added as an extra tag for the value.
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Processed chunk: {start_str} to {end_str} for {meas}_{field}")
            requests.post(f"{url}/write?db={bucket}", data=line)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Script for exporting InfluxDB data into victoria metrics instance. \n"
        " InfluxDB settings can be defined on command line or as environment variables"
        " (or in .env file if python-dotenv is installed)."
        " InfluxDB related args described in \n"
        "https://github.com/influxdata/influxdb-client-python#via-environment-properties"
    )
    parser.add_argument(
        "bucket",
        type=str,
        help="InfluxDB source bucket",
    )
    parser.add_argument(
        "--INFLUXDB_V2_ORG",
        "-o",
        type=str,
        help="InfluxDB organization",
    )
    parser.add_argument(
        "--INFLUXDB_V2_URL",
        "-u",
        type=str,
        help="InfluxDB Server URL, e.g., http://localhost:8086",
    )
    parser.add_argument(
        "--INFLUXDB_V2_TOKEN",
        "-t",
        type=str,
        help="InfluxDB access token.",
    )
    parser.add_argument(
        "--INFLUXDB_V2_SSL_CA_CERT",
        "-S",
        type=str,
        help="Server SSL Cert",
    )
    parser.add_argument(
        "--INFLUXDB_V2_TIMEOUT",
        "-T",
        type=str,
        help="InfluxDB timeout",
    )
    parser.add_argument(
        "--INFLUXDB_V2_VERIFY_SSL",
        "-V",
        type=str,
        help="Verify SSL CERT.",
    )

    parser.add_argument(
        "--vm-addr",
        "-a",
        type=str,
        help="VictoriaMetrics server",
    )
    main(vars(parser.parse_args()))
