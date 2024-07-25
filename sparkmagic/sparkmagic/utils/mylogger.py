import logging
from logging.handlers import RotatingFileHandler


class MySparkLog(object):
    def __init__(self, name="Sparkmagic", base_dir="/logs", username=None, project_id=None):
        self._logger = logging.getLogger(name)
        self.__format = "[%(asctime)s]|%(name)s|%(funcName)s:%(lineno)s|%s(levelname)s|%(message)s"
        self._logger.setLevel(logging.INFO)
        self._log_path = "{0}/{1}_{2}_{3}.log".format(base_dir, "Sparkmagic", username, project_id)
        self.set_file_logger()

    def set_file_logger(self):
        formatter = logging.Formatter(self.__format)
        fh = RotatingFileHandler(self._log_path, "a",enccodting="utf-8", maxBytes=1024 * 1024 * 30, backupCount=5)
        fh.setLevel(logging.INFO)
        fh.setFormatter(formatter)
        self._logger.addHandler(fh)

    def get_logger(self):
         return self._logger