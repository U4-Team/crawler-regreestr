from typing import List,Dict

from core.repositories import BaseMongoRepository


class MongoRepository(BaseMongoRepository):
    def store_company(self, company: Dict):
        if 'INN' not in company:
            self.logger.warning('Skip: %s', company)
            return
        self._client.crawlerRegreestr.companies.replace_one(
            {'INN': company['INN']},
            company,
            True
        )