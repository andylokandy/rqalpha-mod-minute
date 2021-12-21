from rqalpha.interface import AbstractMod
from data_source import MinuteDataSource


class MinuteMod(AbstractMod):
    def start_up(self, env, mod_config):
        env.set_data_source(MinuteDataSource(env.config.base.data_bundle_path))

    def tear_down(self, success, exception=None):
        pass
