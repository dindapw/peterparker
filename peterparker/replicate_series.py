import json

from peterparker.common import config, save, archive_file, s3


def do_replicate_series(data):
    if "created_by" in data.keys() and data["created_by"]:
        save(data["created_by"], "creator", ["id"])
        data["created_by"] = [i["id"] for i in data["created_by"]]

    if "genres" in data.keys() and data["genres"]:
        save(data["genres"], "genre", ["id"])
        data["genres"] = [i["id"] for i in data["genres"]]

    if "networks" in data.keys() and data["networks"]:
        save(data["networks"], "network", ["id"])
        data["networks"] = [i["id"] for i in data["networks"]]

    if "production_companies" in data.keys() and data["production_companies"]:
        save(data["production_companies"], "production_company", ["id"])
        data["production_companies"] = [i["id"] for i in
                                        data["production_companies"]]

    save(data, "series", ["id"])


def run():
    objects = s3.Bucket(config["boto"]["bucket"]) \
        .objects.filter(Prefix="series")

    for obj in objects:
        print(obj.key)
        data = json.load(obj.get()['Body'])

        try:
            do_replicate_series(data)
            archive_file(s3, obj)
        except Exception as ex:
            print(ex)
            archive_file(s3, obj, as_error=True)


if __name__ == '__main__':
    run()
