from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, top_count: int=None):
        self.spark = None
        self.update(top_count)

    def update(self, top_count: int=15):
        self.top_count = self.get_int_value(top_count)
        pass
