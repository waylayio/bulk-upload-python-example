import sys
import pandas
from configparser import RawConfigParser
import requests

if len(sys.argv) < 3:
    print("Usage: `python ./main.py <csv-path> <configuration-path>`")
    exit()

input_path = sys.argv[1]
resource_name = sys.argv[2]
config_path = sys.argv[3]

config = RawConfigParser()
config.read(config_path)


def get_auth(config: RawConfigParser):
    """ Get the authentication out of our config file """
    return config.get("domain", "client-key"), config.get("domain", "client-secret")


def send_messages(data: pandas.DataFrame, config: RawConfigParser, chunk_size: int = 1000):
    """ Bulk send messages to the broker """

    auth = get_auth(config)
    while len(data):
        chunk = data[:chunk_size]
        data = data[chunk_size:]
        messages = chunk.to_dict("records")

        resp = requests.post("%s/messages?store=true&forward=false" % (config.get("domain", "data-path")), json=messages, auth=auth)

        if not resp.ok:
            raise ConnectionError("Uploading messages failed.", resp.json())


def create_resource(resource_name: str, config: RawConfigParser):
    """ Create a resource """

    auth = get_auth(config)
    resp = requests.post(config.get("domain", "resource-path") + "/api/resources/", auth=auth, json={
        "id": resource_name,
        "name": resource_name,
    })

    if not resp.ok:
        raise ConnectionError("Creating resource failed.", resp.json())


def run(input_path: str, resource_name: str, config: RawConfigParser):
    """ Create a resource and pre-process the messages """

    create_resource(resource_name, config)

    data = pandas.read_csv(input_path, sep=";")
    data["resource"] = resource_name
    send_messages(data, config)


run(input_path, resource_name, config)
