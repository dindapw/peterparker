import configparser
from datetime import datetime

import dataset

config = configparser.ConfigParser()
config.read("/home/ec2-user/config.ini")

db = dataset.connect(config["postgres"]["url"])


def archive_file(resource, obj, as_error=False):
    filename = obj.key.split("/")[1]
    if as_error:
        dest_file = f"archive/{filename}"
    else:
        dest_file = f"error/{filename}"
    resource.Object(config["boto"]["bucket"], dest_file) \
        .copy_from(CopySource=f'{config["boto"]["bucket"]}/{obj.key}')
    resource.Object(config["boto"]["bucket"], obj.key).delete()


def save(data, table, pk):
    if not data:
        return

    if type(data) == dict:
        data["date_effective"] = datetime.now()
        db[table].upsert(data, pk)
        return

    temp = []
    for each in data:
        each["date_effective"] = datetime.now()
        temp.append(each)
    db[table].upsert_many(temp, pk)
