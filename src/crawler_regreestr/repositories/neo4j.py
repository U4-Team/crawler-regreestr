from core.repositories import BaseNeo4jRepository


class Neo4jRepository(BaseNeo4jRepository):
    def get_short_data_of_all_companies(self):
        if not self._session:
            raise RuntimeError("You have to call the method only using a context")
        
        query = 'MATCH (n:Company:CrawlerDataSet) RETURN n.COMPANY_SHORT_NAME, n.INN'
        for record in self._session.read_transaction(lambda tx: tx.run(query)):
            yield record