from logging import (
    Logger,
    getLogger,
)
from typing import Optional


class LoggerMixin:
    _logger: Logger
    @property
    def logger(self):
        if not (hasattr(self, '_logger') and self._logger):
            self._logger = getLogger(self.__class__.__name__)

        return self._logger
