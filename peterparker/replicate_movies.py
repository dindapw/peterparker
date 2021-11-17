import json

import boto3

from common import config, save, archive_file


def do_replicate_movies(data):
    if "genres" in data.keys():
        save(data["genres"], "genre", ["id"])
        data["genres"] = [i["id"] for i in data["genres"]]
    if "production_companies" in data.keys():
        save(data["production_companies"], "production_company", ["id"])
        data["production_companies"] = [i["id"] for i in
                                        data["production_companies"]]
    if "production_countries" in data.keys():
        temp = [{"code": each["iso_3166_1"], "name": each["name"]}
                for each in data["production_countries"]]
        save(temp, "country", ["code"])
        data["production_countries"] = [i["iso_3166_1"] for i in
                                        data["production_countries"]]
    if "spoken_languages" in data.keys():
        temp = [{"code": each["iso_639_1"], "name": each["name"]}
                for each in data["spoken_languages"]]
        save(temp, "language", ["code"])
        data["spoken_languages"] = [i["iso_639_1"]
                                    for i in data["spoken_languages"]]
    if data["release_date"] == "":
        data["release_date"] = None
    save(data, "movie", ["id"])


def run():
    resource = boto3.resource(
        's3',
        aws_access_key_id=config["boto"]["aws_access_key_id"],
        aws_secret_access_key=config["boto"]["aws_secret_key"],
        region_name=config["boto"]["region"]
    )
    objects = resource.Bucket(config["boto"]["bucket"]) \
        .objects.filter(Prefix="movies")

    for obj in objects:
        print(obj.key)
        data = json.load(obj.get()['Body'])

        try:
            do_replicate_movies(data)
            archive_file(resource, obj)
        except Exception as ex:
            print(ex)
            archive_file(resource, obj, as_error=True)


if __name__ == '__main__':
    run()
