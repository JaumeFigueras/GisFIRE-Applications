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
    url: str = "{}/meteocat/lightning/{}?srid=25831".format(host, identifier)
    response: requests.Response = requests.get(url, auth=auth)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        return None


def download_discharges(identifier: int, host: str, username: str, token: str) -> Union[Dict[str, Any], None]:
    auth: HTTPBasicAuth = HTTPBasicAuth(username, token)
    url: str = "{}/meteocat/lightning/discharge_count/{}".format(host, identifier)
    response: requests.Response = requests.get(url, auth=auth)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        print(response.text)
        return None


def get_land_cover(identifier: int, host: str, username: str, token: str) -> Union[Dict[str, Any], None]:
    auth: HTTPBasicAuth = HTTPBasicAuth(username, token)
    url: str = "{}/meteocat/lightning/land_cover/{}".format(host, identifier)
    response: requests.Response = requests.get(url, auth=auth)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        return None


def get_nearest_weather_stations(date: datetime.date, x: float, y: float, host: str, username: str, token: str) -> Union[Dict[str, Any], None]:
    auth: HTTPBasicAuth = HTTPBasicAuth(username, token)
    url: str = "{}/meteocat/station/nearest?date={}&x={}&y={}&srid=25831".format(host,
                                                                                 date.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                                                                                 str(x), str(y))
    response: requests.Response = requests.get(url, auth=auth)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        print(response.text)
        return None


def get_humidity(date: datetime.date, average_of_previous_days: int, station_code: str, host: str, username: str, token: str) -> Union[Dict[str, Any], None]:
    auth: HTTPBasicAuth = HTTPBasicAuth(username, token)
    url: str = "{}/meteocat/data/measure/{}/HR?date={}".format(host, station_code,
                                                               date.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
    if average_of_previous_days != 0:
        url: str = "{}/meteocat/data/measure/{}/HR?date={}&operation=average,{}".\
            format(host, station_code, date.strftime("%Y-%m-%dT%H:%M:%S.%fZ"), str(average_of_previous_days))
    response: requests.Response = requests.get(url, auth=auth)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        print(response.text)
        return None


def get_temperature(date: datetime.date, average_of_previous_days: int, station_code: str, host: str, username: str, token: str) -> Union[Dict[str, Any], None]:
    auth: HTTPBasicAuth = HTTPBasicAuth(username, token)
    url: str = "{}/meteocat/data/measure/{}/T?date={}".format(host, station_code,
                                                               date.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
    if average_of_previous_days != 0:
        url: str = "{}/meteocat/data/measure/{}/HR?date={}&operation=average,{}".\
            format(host, station_code, date.strftime("%Y-%m-%dT%H:%M:%S.%fZ"), str(average_of_previous_days))
    response: requests.Response = requests.get(url, auth=auth)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        print(response.text)
        return None

def get_rain(date: datetime.date, average_of_previous_days: int, station_code: str, host: str, username: str, token: str) -> Union[Dict[str, Any], None]:
    auth: HTTPBasicAuth = HTTPBasicAuth(username, token)
    url: str = "{}/meteocat/data/measure/{}/T?date={}".format(host, station_code,
                                                               date.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
    if average_of_previous_days != 0:
        url: str = "{}/meteocat/data/measure/{}/PPT?date={}&operation=average,{}".\
            format(host, station_code, date.strftime("%Y-%m-%dT%H:%M:%S.%fZ"), str(average_of_previous_days))
    response: requests.Response = requests.get(url, auth=auth)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        print(response.text)
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
    header = ['ID', 'PEAK_CURRENT', 'CHI_SQUARED', 'NUMBER_OF_SENSORS', 'HIT_GROUND', 'DISCHARGES', 'LAND_COVER', 'REL_HUMIDITY',
              'AVG_REL_HUMIDITY_1_DAY', 'AVG_REL_HUMIDITY_3_DAY', 'AVG_REL_HUMIDITY_5_DAY', 'AVG_REL_HUMIDITY_10_DAY',
              'AVG_REL_HUMIDITY_15_DAY', 'TEMPERATURE', 'AVG_TEMPERATURE_1_DAY', 'AVG_TEMPERATURE_3_DAY',
              'AVG_TEMPERATURE_5_DAY', 'AVG_TEMPERATURE_10_DAY', 'AVG_TEMPERATURE_15_DAY']
    csv_output.append(header)
    for lightning in csv_lightnings[:10]:
        identifier = int(lightning[0])
        data = download_lightning(identifier, args.host, args.username, args.token)
        if data is None:
            print('Lightning not found!', identifier)
            break
        print(data['date'])
        peak_current = float(data['peak_current'])
        chi_squared = float(data['chi_squared'])
        number_of_sensors = int(data['number_of_sensors'])
        hit_ground = bool(data['hit_ground'])
        count = download_discharges(identifier, args.host, args.username, args.token)
        if count is None:
            print('Error in discharges count!', identifier)
            break
        discharges = int(count['count'])
        land = get_land_cover(identifier, args.host, args.username, args.token)
        if data is None:
            print('Land cover not found!', identifier)
            break
        land_cover = int(land['land_cover_type'])
        weather_station = get_nearest_weather_stations(dateutil.parser.isoparse(data['date']),
                                                       float(data['coordinates_x']), float(data['coordinates_y']),
                                                       args.host, args.username, args.token)
        weather_station_code = weather_station['code']
        print(weather_station_code)
        days = [0, 1, 3, 5, 10, 15]
        measures = list()
        for day in days:
            measure = get_humidity(dateutil.parser.isoparse(data['date']), day, weather_station_code, args.host, args.username, args.token)
            if measure is not None:
                measures.append(float(measure['value']))
            else:
                measures.append(None)
        # Get Temperature
        for day in days:
            measure = get_temperature(dateutil.parser.isoparse(data['date']), day, weather_station_code, args.host, args.username, args.token)
            if measure is not None:
                measures.append(float(measure['value']))
            else:
                measures.append(None)

        new_row = [identifier, peak_current, chi_squared, number_of_sensors, hit_ground, discharges, land_cover]
        for measure in measures:
            new_row.append(measure)
        csv_output.append(new_row)

    with open(args.output_file, 'w') as file:
        writer = csv.writer(file)
        writer.writerows(csv_output)

