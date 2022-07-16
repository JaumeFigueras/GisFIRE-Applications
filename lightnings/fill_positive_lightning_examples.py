#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import dateutil.parser
import json
import argparse
import csv
from requests.auth import HTTPBasicAuth
from typing import Any
from typing import Dict
from typing import List
from typing import Union

import pytz
import requests
import math


def download_lightning(identifier: int, host: str, username: str, token: str) -> Union[Dict[str, Any], None]:
    auth: HTTPBasicAuth = HTTPBasicAuth(username, token)
    url: str = "{}/meteocat/lightning/{}".format(host, identifier)
    response: requests.Response = requests.get(url, auth=auth)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        return None


def get_land_cover(identifier: int, host: str, username: str, token: str) -> Union[Dict[str, Any], None]:
    auth: HTTPBasicAuth = HTTPBasicAuth(username, token)
    url: str = "{}/meteocat/lightning/land_cover/{}".format(host, identifier)
    response: requests.Response = requests.get(url, auth=auth)
    print(response.text)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        return None


if __name__ == "__main__":  # pragma: no cover
    # Config the program arguments
    # noinspection DuplicatedCode
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input-file', help='Positive examples lightnings file')
    parser.add_argument('-o', '--output-file', help='Data of the lightnings file')
    parser.add_argument('-H', '--host', help='Host name were the database cluster is located')
    parser.add_argument('-u', '--username', help='Database username')
    parser.add_argument('-t', '--token', help='Database password')
    # noinspection DuplicatedCode
    args = parser.parse_args()

    csv_lightnings: List[List[str]] = list()
    with open(args.input_file) as csvfile:
        reader = csv.reader(csvfile)
        next(reader, None)  # Skip header
        for row in reader:
            csv_lightnings.append(row)

    csv_output = list()
    header = ['ID', 'PEAK_CURRENT', 'CHI_SQUARED', 'NUMBER_OF_SENSORS', 'HIT_GROUND', 'LAND_COVER']
    csv_output.append(header)
    for lightning in csv_lightnings:
        identifier = int(lightning[0])
        data = download_lightning(identifier, args.host, args.username, args.token)
        if data is None:
            print('Lightning not found!', identifier)
            break
        peak_current = float(data['peak_current'])
        chi_squared = float(data['chi_squared'])
        number_of_sensors = int(data['number_of_sensors'])
        hit_ground = bool(data['hit_ground'])
        land = get_land_cover(identifier, args.host, args.username, args.token)
        if data is None:
            print('Land cover not found!', identifier)
            break
        land_cover = int(land['land_cover_type'])

        new_row = [identifier, peak_current, chi_squared, number_of_sensors, hit_ground, land_cover]
        csv_output.append(new_row)
        print(csv_output)

    with open(args.output_file, 'w') as file:
        writer = csv.writer(file)
        writer.writerows(csv_output)

