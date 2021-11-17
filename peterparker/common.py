import configparser
from datetime import datetime

import boto3
import dataset

config = configparser.ConfigParser()
config.read("/home/ec2-user/config.ini")

db = dataset.connect(config["postgres"]["url"])

s3 = boto3.resource(
    's3',
    aws_access_key_id=config["boto"]["aws_access_key_id"],
    aws_secret_access_key=config["boto"]["aws_secret_key"],
    region_name=config["boto"]["region"]
)


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
