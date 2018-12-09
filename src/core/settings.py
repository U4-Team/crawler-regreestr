import os

NEO4J_URI = os.getenv('NEO4J_URI', 'bolt://68.183.76.109:7687')
NEO4J_USER = os.getenv('NEO4J_USER', 'neo4j')
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD', 'itstimeforneo4j')
NEO4J_REQUIRED_LABEL = os.getenv('NEO4J_REQUIRED_LABEL')

MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://mongodb:itstimeformongodb@68.183.76.109:27017/')
MONGODB_DB = os.getenv('MONGODB_DB', 'crawlerDataset')