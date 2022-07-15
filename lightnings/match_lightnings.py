#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import dateutil.parser
import json
import argparse
import csv
from base64 import b64encode
from typing import Any
from typing import Dict
from typing import List
from typing import Union

import pytz
import requests
import math


def download_lightnings(requested_day: datetime.datetime) -> Union[List[Dict[str, Any]], None]:
    year: int = requested_day.year
    month: int = requested_day.month
    day: int = requested_day.day
    headers: Dict[str, str] = {'Authorization': 'Basic {}'.format(
        b64encode(b"").decode("utf-8"))}
    url: str = "https://gisfire.petprojects.tech/api/v1/meteocat/lightning/{}/{}/{}?srid=25831".format(year, month, day)
    response: requests.Response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        return None


if __name__ == "__main__":  # pragma: no cover
    # Config the program arguments
    # noinspection DuplicatedCode
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input-file', help='Firefigter file')
    parser.add_argument('-o', '--output-file', help='Matched lightnings file')
    parser.add_argument('-H', '--host', help='Host name were the database cluster is located')
    parser.add_argument('-u', '--username', help='Database username')
    parser.add_argument('-t', '--token', help='Database password')
    # noinspection DuplicatedCode
    args = parser.parse_args()

    # Loaf firefigters records
    carpetaTreball = r'C:\Users\Toni Guasch\PycharmProjects\llamps\Dades\AnalisiTreball'
    csv_lightnings: List[List[str]] = list()
    with open(args.input_file) as csvfile:
        reader = csv.reader(csvfile)
        next(reader, None)  # Skip header
        for row in reader:
            csv_lightnings.append(row)
    filtered_lightnings: List[List[Any]] = list()
    for lightning in csv_lightnings:
        str_date = lightning[1] + ' ' + lightning[2]
        try:
            lightning_date = datetime.datetime.strptime(str_date, "%d/%m/%Y %H:%M:%S")
            lightning_date = lightning_date.replace(tzinfo=pytz.timezone('Europe/Paris'))
            x: float = float(lightning[3])
            y: float = float(lightning[4])
        except ValueError as _:
            continue
        new_row = [lightning_date, x, y]
        filtered_lightnings.append(new_row)
    filtered_lightnings.sort(key=lambda l: l[0])

    days_to_search: List[datetime.datetime] = list()
    matched_lightnings = list()
    matched_lightnings.append(['id', 'meteocat_id', 'date-UTC', 'x', 'y', 'weight', 'date-UTC-ff', 'x-ff', 'y-ff'])
    for lightning in filtered_lightnings:
        days_to_search = [lightning[0] - datetime.timedelta(days=day) for day in range(6)]
        lightnings: List[Dict[str, Any]] = list()
        for day in days_to_search:
            lightnings += download_lightnings(datetime.datetime(day.year, day.month, day.day, 0, 0, 0))
        print(lightning[0], len(lightnings))
        if lightnings is not None and len(lightnings) > 0:
            # print(len(lightnings))
            distance_max = 10000000000
            lightning_max = None
            for possible in lightnings:
                possible_date = dateutil.parser.isoparse(possible['date'])
                possible_x = possible['coordinates_x']
                possible_y = possible['coordinates_y']

                diff_time: datetime.timedelta = lightning[0] - possible_date

                if diff_time.total_seconds() > 0:
                    distance = math.sqrt((lightning[1] - possible_x)**2 + (lightning[2] - possible_y)**2 + (diff_time.total_seconds() / (60*60))**2)
                    if distance < distance_max:
                        distance_max = distance
                        lightning_max = possible.copy()
            date_in_utc: datetime.datetime = lightning[0].astimezone(pytz.utc)
            new_row = [lightning_max['id'], lightning_max['meteocat_id'], lightning_max['date'],
                       lightning_max['coordinates_x'], lightning_max['coordinates_y'], distance_max,
                       date_in_utc.strftime("%Y-%m-%dT%H:%M:%S.%fZ"), lightning[1], lightning[2]]
            matched_lightnings.append(new_row)
    with open(args.output_file, 'w') as file:
        writer = csv.writer(file)
        writer.writerows(matched_lightnings)

