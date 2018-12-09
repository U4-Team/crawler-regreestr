from core.mixins import LoggerMixin
from crawler_regreestr.repositories import MongoRepository


class MongoPipeline(LoggerMixin):
    mongo_repo: MongoRepository
    def __init__(self):
        self.mongo_repo = MongoRepository()

    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    def process_item(self, item, spider):
        self.mongo_repo.store_company(item)
        self.logger.info('Stored: (%s) %s', item['INN'], item['COMPANY_SHORT_NAME'] if 'COMPANY_SHORT_NAME' in item else '---')

