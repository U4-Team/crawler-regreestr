import scrapy
from furl import furl
import phonenumbers
from urllib import parse
import re
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule, CrawlSpider
from datetime import datetime


class RegreestrCrawler(CrawlSpider):
    name = "reg_reestr_crawler"
    allowed_domains = ['moskva.regreestr.com']
    start_urls = ['http://moskva.regreestr.com']
    rules = (
       # Extract links matching 'item.php' and parse them with the spider's method parse_item
        Rule(
            LinkExtractor(
                allow=(r'/page[0-9]+'),
                canonicalize=True
            ),
            follow=True
        ),
        Rule(
            LinkExtractor(
                # allow=('http://moskva.regreestr.com/10830235'),
                allow=(r'/[0-9]+'),
                canonicalize=True
            ),
            callback='parse_company_page',
            follow=True
        ),
    )
    
    def parse_company_page(self, response):
        print(response.url)
        data = {}

        # prepare selectors
        container = response.css('div.container > div.row > div')[0]
        main = container.css('article > div#main')
        firm_info_blocks = main.css('div.firm-info')
        
        # parse data
        self.parse_short_name(response, data)
        for block in main.css('div.row > div > table'):
            self.parse_block(block, data)
        
        yield data

    def parse_short_name(self, container, data):
        name = container.css('h1::text').extract_first()
        if name:
            data['COMPANY_SHORT_NAME'] = name.strip()

    def parse_block(self, block, data):
        criteria_1 = block.css('tr:nth-child(1) td:nth-child(1)::text').extract_first()
        criteria_2 = block.css('tr:nth-child(1) td h2::text').extract_first()
        if criteria_1 and criteria_1.strip() == 'Статус юридического лица:':
            self.parse_main_block(block, data)
        elif criteria_2:
            if criteria_2.strip() == 'Реквизиты':
                self.parse_requisite_block(block, data)
            elif criteria_2.strip() == 'Коды ОКВЭД':
                self.parse_okved_block(block, data)
            elif criteria_2.strip() == 'Учредители':
                self.parse_founders_block(block, data)
            elif criteria_2.strip() == 'История изменений в ЕГРЮЛ':
                pass
            elif criteria_2.strip() == 'Прочая информация':
                self.parse_etc_block(block, data)
            elif criteria_2.strip() == 'Ликвидация':
                self.parse_liquidation_block(block, data)
    
    def parse_main_block(self, block, data):
        for row in block.css('tr'):
            field, value = row.css('td')
            field = field.css('::text').extract_first().strip()
            if field == 'Статус юридического лица:':
                value = value.css('::text').extract_first()
                value = True if value and value.strip() == 'Действующее' else False
                data['ACTIVE'] = value
            elif field == 'Полное наименование:':
                value = value.css('::text').extract_first()
                if value:
                    data['COMPANY_FULL_NAME'] = value.strip()
            elif field == 'ИНН:':
                value = value.css('::text').extract_first()
                if value:
                    data['INN'] = value.strip()
            elif field == 'ОГРН:':
                value = value.css('::text').extract_first()
                if value:
                    data['OGRN'] = value.strip()
            elif field == 'Юридический адрес:':
                pass
            elif field == 'Руководитель:':
                value = value.css('::text').extract_first().strip().split(':')
                if value and len(value) > 1:
                    data['CEO'] = value[1].strip()
            else:
                pass

    def parse_requisite_block(self, block, data):
        for row in block.css('tr'):
            try:
                field, value = row.css('td')
            except ValueError:
                continue
            else:
                field = field.css('::text').extract_first().strip()
                if field == 'КПП':
                    value = value.css('::text').extract_first()
                    if value:
                        data['KPP'] = value.strip()
                elif field == 'Организационно-правовая форма (ОПФ)':
                    value = value.css('::text').extract_first()
                    if value:
                        data['OPF'] = value.strip()
                elif field == 'Регион':
                    value = value.css('::text').extract_first()
                    if value and value != 'город Москва':
                        return # we parse only Moscow
                elif field == 'Юридический адрес':
                    value = value.css('::text').extract_first()
                    if value:
                        data['LEGAL_ADDRESS'] = value.strip()
                elif field == 'Дата регистрации':
                    value = value.css('::text').extract_first()
                    if value:
                        data['REGISRTRATION_DATE'] = datetime.strptime(value.strip(), '%d.%m.%Y')
                elif field == 'Дата регистрации':
                    value = value.css('::text').extract_first()
                    if value:
                        data['REGISRTRATION_DATE'] = datetime.strptime(value.strip(), '%d.%m.%Y')
                else:
                    pass

    def parse_okved_block(self, block, data):
        index = 0
        for row in block.css('tr'):
            try:
                field, value = row.css('td')
            except ValueError:
                continue
            else:
                field = field.css('::text').extract_first().strip()
                value = value.css('::text').extract_first().strip()
                if index == 0:
                    data['OKVED_MAIN'] = (field, value)
                else:
                    if 'OKVEDs' not in data:
                        data['OKVEDs'] = []
                    data['OKVEDs'].append((field, value))
                index += 1

    def parse_founders_block(self, block, data):
        data['FOUNDERs'] = []
        for person in block.css('tr strong'):
            person = person.css('::text').extract_first().strip()
            data['FOUNDERs'].append(person)
    
    def parse_etc_block(self, block, data):
        for row in block.css('tr'):
            try:
                field, value = row.css('td')
            except ValueError:
                continue
            else:
                field = field.css('::text').extract_first().strip()
                if field == 'Уставный капитал':
                    value = value.css('::text').extract_first()
                    if value:
                        value = float(value.strip().replace(' ', '').replace(',', '.'))
                        data['AUTH_CAPICAL'] = value
                else:
                    pass

    def parse_liquidation_block(self, block, data):
        data['LIQUIDATION'] = {}
        for row in block.css('tr'):
            try:
                field, value = row.css('td')
            except ValueError:
                continue
            else:
                field = field.css('::text').extract_first().strip()
                if field == 'Дата ликвидации компании':
                    value = value.css('::text').extract_first()
                    if value:
                        data['LIQUIDATION']['DATE'] = datetime.strptime(value.strip(), '%d.%m.%Y')
                elif field == 'Способ прекращения юридического лица':
                    value = value.css('::text').extract_first()
                    if value:
                        data['LIQUIDATION']['TYPE'] = value.strip().split(' - ')
                elif field == 'Регистрирующий ликвидацию налоговый орган':
                    value = value.css('::text').extract_first()
                    if value:
                        data['LIQUIDATION']['AUTHORITY'] = value.strip()
                else:
                    pass
