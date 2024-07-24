import logging


class MySparkLog(object):
    def __init__(self, name="Sparkmagic", base_dir="/logs", username=None, project_id=None):
        self._logger = logging.getLogger(name)
        self.__format = ""