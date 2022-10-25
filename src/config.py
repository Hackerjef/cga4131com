from functools import reduce

import yaml


class Config:
    def __init__(self):
        with open('config.yaml', 'r') as f:
            self.config = yaml.load(f.read(), Loader=yaml.FullLoader)
            f.close()

    def Getcfgvalue(self, keys, default=None):
        return reduce(lambda d, key: d.get(key, default) if isinstance(d, dict) else default, keys.split("."), self.config)