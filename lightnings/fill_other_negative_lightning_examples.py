#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import dateutil.parser
import json
import argparse
import csv
from numpy.random import RandomState

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


def download_lightnings(requested_day: datetime.datetime, host: str, username: str, token: str) -> Union[List[Dict[str, Any]], None]:
    year: int = requested_day.year
    month: int = requested_day.month
    day: int = requested_day.day
    auth: HTTPBasicAuth = HTTPBasicAuth(username, token)
    url: str = "{}/meteocat/lightning/{}/{}/{}?srid=25831".format(host, year, month, day)
    response: requests.Response = requests.get(url, auth=auth)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        return None


def download_discharges(identifier: int, host: str, username: str, token: str) -> Union[Dict[str, Any], None]:
    auth: HTTPBasicAuth = HTTPBasicAuth(username, token)
    url: str = "{}/meteocat/lightning/discharge_count/{}?srid=25831".format(host, identifier)
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
        #print(response.text)
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
        #print(response.text)
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
        #print(response.text)
        return None

def get_rain(date: datetime.date, average_of_previous_days: int, station_code: str, host: str, username: str, token: str) -> Union[Dict[str, Any], None]:
    auth: HTTPBasicAuth = HTTPBasicAuth(username, token)
    url: str = "{}/meteocat/data/measure/{}/T?date={}".format(host, station_code,
                                                               date.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
    if average_of_previous_days != 0:
        url: str = "{}/meteocat/data/measure/{}/PPT?date={}&operation=sum,{}".\
            format(host, station_code, date.strftime("%Y-%m-%dT%H:%M:%S.%fZ"), str(average_of_previous_days))
    response: requests.Response = requests.get(url, auth=auth)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        #print(response.text)
        return None


def get_solar_irradiance(date: datetime.date, average_of_previous_days: int, station_code: str, host: str, username: str, token: str) -> Union[Dict[str, Any], None]:
    auth: HTTPBasicAuth = HTTPBasicAuth(username, token)
    url: str = "{}/meteocat/data/measure/{}/RS?date={}".format(host, station_code,
                                                               date.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
    if average_of_previous_days != 0:
        url: str = "{}/meteocat/data/measure/{}/RS?date={}&operation=sum,{}".\
            format(host, station_code, date.strftime("%Y-%m-%dT%H:%M:%S.%fZ"), str(average_of_previous_days))
    response: requests.Response = requests.get(url, auth=auth)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        #print(response.text)
        return None


def get_wind(date: datetime.date, average_of_previous_days: int, station_code: str, host: str, username: str, token: str) -> Union[Dict[str, Any], None]:
    auth: HTTPBasicAuth = HTTPBasicAuth(username, token)
    url: str = "{}/meteocat/data/measure/{}/VV10?date={}".format(host, station_code,
                                                               date.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
    if average_of_previous_days != 0:
        url: str = "{}/meteocat/data/measure/{}/VV10?date={}&operation=sum,{}".\
            format(host, station_code, date.strftime("%Y-%m-%dT%H:%M:%S.%fZ"), str(average_of_previous_days))
    response: requests.Response = requests.get(url, auth=auth)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        #print(response.text)
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
    header = ['ID', 'DATE', 'PEAK_CURRENT', 'CHI_SQUARED', 'NUMBER_OF_SENSORS', 'HIT_GROUND', 'DISCHARGES', 'LAND_COVER',
              'REL_HUMIDITY', 'AVG_REL_HUMIDITY_1_DAY', 'AVG_REL_HUMIDITY_3_DAY', 'AVG_REL_HUMIDITY_5_DAY',
              'AVG_REL_HUMIDITY_10_DAY', 'AVG_REL_HUMIDITY_15_DAY', 'TEMPERATURE', 'AVG_TEMPERATURE_1_DAY',
              'AVG_TEMPERATURE_3_DAY', 'AVG_TEMPERATURE_5_DAY', 'AVG_TEMPERATURE_10_DAY', 'AVG_TEMPERATURE_15_DAY',
              'RAIN', 'SUM_RAIN_1_DAY', 'SUM_RAIN_3_DAY', 'SUM_RAIN_5_DAY', 'SUM_RAIN_10_DAY', 'SUM_RAIN_15_DAY',
              'SOLAR_IRRADIANCE', 'SUM_SOLAR_IRRADIANCE_1_DAY', 'SUM_SOLAR_IRRADIANCE_3_DAY',
              'SUM_SOLAR_IRRADIANCE_5_DAY', 'SUM_SOLAR_IRRADIANCE_10_DAY', 'SUM_SOLAR_IRRADIANCE_15_DAY', 'WIND',
              'SUM_WIND_1_DAY', 'SUM_WIND_3_DAY', 'SUM_WIND_5_DAY', 'SUM_WIND_10_DAY', 'SUM_WIND_15_DAY',
              ]
    csv_output.append(header)
    actual_date = datetime.date(2014, 1, 1)
    end_date = datetime.date(2020, 1, 1)
    possible_dates = list()
    valid_dates = list()
    invalid_dates = list()
    while actual_date < end_date:
        possible_dates.append(actual_date)
        actual_date += datetime.timedelta(days=1)
    for lightning in csv_lightnings:
        date = dateutil.parser.isoparse(lightning[3])
        invalid_dates.append(datetime.date(date.year, date.month, date.day))
    valid_dates = [x for x in possible_dates if x not in invalid_dates]
    rs = RandomState(1234567890)
    rs.shuffle(valid_dates)
    picked_days = 0
    for date in valid_dates:
        # Get the same day lightnings
        negative_lightnings: List[Dict[str, Any]] = download_lightnings(datetime.datetime(date.year, date.month, date.day, 0, 0, 0), args.host, args.username, args.token)
        if negative_lightnings is None:
            print('Lightnings not found!', date)
            continue
        negative_lightnings = [i for i in negative_lightnings if bool(i['hit_ground'])]
        if len(negative_lightnings) < 10:
            print('Lightnings not found!', date)
            continue
        print('Found lightnings in date:', date)
        # Remove non ground and lightnings that caused ignition
        # Use random to select elements, but keep consistency between executions
        rs = RandomState(1234567890)
        rs.shuffle(negative_lightnings)
        negative_dataset = list()
        for negative_lightning in negative_lightnings:
            land_cover = None
            land = get_land_cover(int(negative_lightning['id']), args.host, args.username, args.token)
            if land is None:
                print('Land cover not found!', negative_lightning['id'])
                break
            else:
                if 0 < int(land['land_cover_type']) < 300:
                    land_cover = int(land['land_cover_type'])
                else:
                    continue
            count = download_discharges(int(negative_lightning['id']), args.host, args.username, args.token)
            if count is None:
                print('Error in discharges count!', int(negative_lightning['id']))
                break
            discharges = int(count['count'])
            peak_current = float(negative_lightning['peak_current'])
            chi_squared = float(negative_lightning['chi_squared'])
            number_of_sensors = int(negative_lightning['number_of_sensors'])
            hit_ground = bool(negative_lightning['hit_ground'])
            weather_station = get_nearest_weather_stations(dateutil.parser.isoparse(negative_lightning['date']),
                                                           float(negative_lightning['coordinates_x']), float(negative_lightning['coordinates_y']),
                                                           args.host, args.username, args.token)
            weather_station_code = weather_station['code']
            print(weather_station_code)
            days = [0, 1, 3, 5, 10, 15]
            measures = list()
            # Get Humidity
            for day in days:
                measure = get_humidity(dateutil.parser.isoparse(negative_lightning['date']), day, weather_station_code, args.host, args.username, args.token)
                if measure is not None:
                    measures.append(float(measure['value']))
                else:
                    measures.append(None)
            if measures[-1] is None:
                continue
            # Get Temperature
            for day in days:
                measure = get_temperature(dateutil.parser.isoparse(negative_lightning['date']), day, weather_station_code, args.host, args.username, args.token)
                if measure is not None:
                    measures.append(float(measure['value']))
                else:
                    measures.append(None)
            if measures[-1] is None:
                continue
            # Get Rain
            for day in days:
                measure = get_rain(dateutil.parser.isoparse(negative_lightning['date']), day, weather_station_code, args.host, args.username, args.token)
                if measure is not None:
                    measures.append(float(measure['value']))
                else:
                    measures.append(None)
            if measures[-1] is None:
                continue
            # Get Solar irradiance
            for day in days:
                measure = get_solar_irradiance(dateutil.parser.isoparse(negative_lightning['date']), day, weather_station_code, args.host, args.username, args.token)
                if measure is not None:
                    measures.append(float(measure['value']))
                else:
                    measures.append(None)
            # Get Wind
            for day in days:
                measure = get_wind(dateutil.parser.isoparse(negative_lightning['date']), day, weather_station_code, args.host, args.username, args.token)
                if measure is not None:
                    measures.append(float(measure['value']))
                else:
                    measures.append(None)

            new_row = [int(negative_lightning['id']), negative_lightning['date'], peak_current, chi_squared, number_of_sensors, hit_ground, discharges, land_cover]
            for measure in measures:
                new_row.append(measure)
            negative_dataset.append(new_row)
            if len(negative_dataset) == 10:
                break

        csv_output += negative_dataset

        with open(args.output_file, 'w') as file:
            writer = csv.writer(file)
            writer.writerows(csv_output)

        if len(csv_output) >= len(csv_lightnings) * 10:
            break

