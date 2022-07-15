from dagster import asset
import csv
import requests


@asset
def asset1():
    pass


@asset
def asset2():
    pass


@asset(group_name="mygroup")
def asset3():
    pass


@asset
def cereals():
    response = requests.get("https://docs.dagster.io/assets/cereal.csv")
    lines = response.text.split("\n")
    cereal_rows = [row for row in csv.DictReader(lines)]

    return cereal_rows
