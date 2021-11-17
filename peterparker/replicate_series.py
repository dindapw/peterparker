import json

import boto3

from common import config, save, archive_file


def do_replicate_series(data):
    if "created_by" in data.keys() and not data["created_by"]:
        save(data["created_by"], "creator", ["id"])
        data["created_by"] = [i["id"] for i in data["created_by"]]

    if "genres" in data.keys() and not data["genres"]:
        save(data["genres"], "genre", ["id"])
        data["genres"] = [i["id"] for i in data["genres"]]

    if "networks" in data.keys() and not data["networks"]:
        save(data["networks"], "network", ["id"])
        data["networks"] = [i["id"] for i in data["networks"]]

    if "production_companies" in data.keys() \
            and not data["production_companies"]:
        save(data["production_companies"], "production_company", ["id"])
        data["production_companies"] = [i["id"] for i in
                                        data["production_companies"]]

    save(data, "series", ["id"])


def run():
    resource = boto3.resource(
        's3',
        aws_access_key_id=config["boto"]["aws_access_key_id"],
        aws_secret_access_key=config["boto"]["aws_secret_key"],
        region_name=config["boto"]["region"]
    )
    objects = resource.Bucket(config["boto"]["bucket"]) \
        .objects.filter(Prefix="series")

    for obj in objects:
        print(obj.key)
        data = json.load(obj.get()['Body'])

        try:
            do_replicate_series(data)
            archive_file(resource, obj)
        except Exception as ex:
            print(ex)
            archive_file(resource, obj, as_error=True)


if __name__ == '__main__':
    run()
