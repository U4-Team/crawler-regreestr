from typing import List,Dict

from core.repositories import BaseMongoRepository


class MongoRepository(BaseMongoRepository):
    def store_company(self, company: Dict):
        self._client.crawlerRegreestr.companies.replace_one(
            {'INN': company['INN']},
            company,
            True
        )