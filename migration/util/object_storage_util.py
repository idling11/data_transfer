import json
import os


def get_object_storage_config(storage_conf_path):
    with open(storage_conf_path, 'r') as f:
        storage_config = json.load(f)
    config = {}
    if storage_config['storage.type'] == 'oss':
        config['type'] = 'oss'
        config['bucket'] = storage_config['cz.fs.oss.bucket']
        config['id'] = storage_config['cz.fs.oss.access.id']
        config['key'] = storage_config['cz.fs.oss.access.key']
        config['endpoint'] = storage_config['cz.fs.oss.endpoint']
        config['region'] = storage_config['cz.fs.oss.sts.region'] if 'cz.fs.oss.sts.region' in storage_config else storage_config['cz.fs.oss.endpoint'].split('.')[0]
    elif storage_config['storage.type'] == 'cos':
        config['type'] = 'cos'
        config['bucket'] = storage_config['cz.fs.cos.bucket']
        config['id'] = storage_config['cz.fs.cos.access.key']
        config['key'] = storage_config['cz.fs.cos.secret.key']
        config['endpoint'] = storage_config['cz.fs.cos.endpoint']
        config['region'] = storage_config['cz.fs.cos.region']
    else:
        raise Exception(f"Unsupported storage type:{storage_config['storage.type']}")

    return config
