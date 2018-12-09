import logging
from urllib import parse

import click
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from crawler_regreestr.crawler import RegreestrCrawler

logger = logging.getLogger(__name__)

@click.group()
def cli() -> None:  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(process)-5s] %(name)-24s %(levelname)s %(message)s',
        datefmt='%d.%m.%Y[%H:%M:%S]',
    )
    logging.getLogger('scrapy').setLevel(logging.DEBUG)
    logging.getLogger('scrapy').propagate = False



@cli.command()
def run() -> None:
    logger.info('=== START ===')
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36',
        'DOWNLOAD_DELAY': 0.1,
        'DEPTH_LIMIT': 0,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'ITEM_PIPELINES': {
            'crawler_regreestr.pipelines.MongoPipeline': 300
        },
    })
    process.crawl(RegreestrCrawler)
    process.start()
    logger.info('done')
