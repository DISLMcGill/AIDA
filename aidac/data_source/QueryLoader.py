import yaml
import os
from pathlib import Path

TEMPLATE_PATH = 'template.yaml'
cur = Path(__file__, '..').resolve()
__location__ = cur.joinpath(TEMPLATE_PATH)


class QueryLoader:
    def __init__(self, ds_type):
        with open(__location__, 'r') as f:
            self.template = yaml.safe_load(f)
        self.ds_type = ds_type

    def load_query(self, qry_type):
        return self.template[qry_type][self.ds_type]
