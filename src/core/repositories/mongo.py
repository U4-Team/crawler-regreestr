from pymongo import MongoClient

from core.mixins import LoggerMixin
from core.settings import MONGODB_URI, MONGODB_DB


class BaseMongoRepository(LoggerMixin):
    _client: MongoClient
    _db: 'HzDb'

    def __init__(
        self,
        mongodb_uri: str = MONGODB_URI
    ):
        self._client = MongoClient(MONGODB_URI)
        self.db = self._client[MONGODB_DB]