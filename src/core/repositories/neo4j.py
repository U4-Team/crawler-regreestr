from typing import List, Dict, Optional

from neo4j import GraphDatabase, StatementResult
from core.mixins import LoggerMixin
from core.settings import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, NEO4J_REQUIRED_LABEL


class BaseNeo4jRepository(LoggerMixin):
    driver: GraphDatabase
    _session: 'HzSession' = None
    _required_label: Optional[str]

    def __init__(
            self,
            uri: str = NEO4J_URI,
            user: str = NEO4J_USER,
            password: str = NEO4J_PASSWORD,
            required_label: Optional[str] = NEO4J_REQUIRED_LABEL
    ):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self._required_label = required_label

    def __enter__(self):
        self._session = self.driver.session()
        return self

    def __exit__(self, *args):
        self._session.close()

    def create_node(self, labels: List[str], properties: Dict) -> StatementResult:
        if not self._session:
            raise RuntimeError("You have to call the method only using a context")
        
        labels_string = ':'.join(labels + ([self._required_label] if self._required_label else []))
        
        return self._session.write_transaction(
            lambda tx: tx.run(
                f'CREATE (n:{labels_string} $properties)',
                properties=properties
            )
        )

    def merge_node(self, merge_by: str, labels: List[str], properties: Dict) -> StatementResult:
        if not self._session:
            raise RuntimeError("You have to call the method only using a context")
        
        labels_string = ':'.join(labels + ([self._required_label] if self._required_label else []))
        
        return self._session.write_transaction(
            lambda tx: tx.run(
                'MERGE (n:'+labels_string+' {'+merge_by+': $properties.'+merge_by+'}) '
                'ON CREATE SET n=$properties',
                properties=properties
            )
        )

    def merge_directed_edge(
            self,
            left_node_by_key: str,
            left_node_by: Dict,
            edge_label: str,
            right_node_by_key: str,
            right_node_by: Dict
    ) -> StatementResult:
        if not self._session:
            raise RuntimeError("You have to call the method only using a context")
        
        left_labels = f':{self._required_label}' if self._required_label else ''
        right_labels = f':{self._required_label}' if self._required_label else ''

        return self._session.write_transaction(
            lambda tx: tx.run('''
                MATCH (n'''+left_labels+''' {'''+left_node_by_key+''': $left_node_by.'''+left_node_by_key+'''}),
                      (m'''+right_labels+''' {'''+right_node_by_key+''': $right_node_by.'''+right_node_by_key+'''})
                MERGE
                    (n)-[:'''+edge_label+''']->(m)
            ''',
            left_node_by=left_node_by,
            right_node_by=right_node_by
            )
        )

