from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, top_count: int=None, todays_date: str=None):
        self.spark = None
        self.update(top_count, todays_date)

    def update(self, top_count: int=10, todays_date: str="current_date()"):
        self.top_count = self.get_int_value(top_count)
        self.todays_date = todays_date
        pass
