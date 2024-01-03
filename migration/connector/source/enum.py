class Column:
    def __init__(self, name, type, is_null, default_value=None):
        self.name = name
        self.type = type
        self.is_null = is_null
        self.default_value = default_value


class ClusterInfo:
    def __init__(self, bucket_num, cluster_keys):
        self.bucket_num = bucket_num
        self.cluster_keys = cluster_keys
