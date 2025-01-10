import csv
import os
import time

from datetime import datetime
from scrapy import Selector
from selenium import webdriver

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from scrapy.spiders import CrawlSpider, Request, Spider

from ...utils import (
    exception_handler,
    get_adverts_for_takendown,
    get_missing_seller_url,
    insert_region_and_country,
    scraper_url_list,
    unique_list_of_product,
    update_last_seen,
    update_sellers,
    write_to_excel,
    get_today_date
)


class PoshmarkParser(Spider):
    name = "poshmark-parser"

    @exception_handler
    def parse(self, response, **kwargs):
        """Parse individual product details"""

        parse_product = {
            "seller": "poshmark",
            "company_id": kwargs.get("company_id"),
            "keyword": kwargs.get("keyword"),

            "region": "NA",
            "country": "United States", 
            "domain": "poshmark.com",

            "currency": "USD",
            "shipping_address": "",
            "created_at": get_today_date(),

            "url": response.url,
            "title": self.get_title(response),
            "description": self.get_description(response),
            "price": self.get_price(response),
            "pic": self.get_image(response)
        }

        PoshmarkCrawler.products.append(parse_product)

    def get_product_title(self, response):
        product_title = response.css(".listing__title h1::text").get()
        return product_title.strip() if product_title else ""

    def get_product_description(self, response):
        product_description = response.css(".listing__description ::text").get()
        return product_description.strip() if product_description else ""

    def get_product_price(self, response):
        product_price = response.css(".listing__ipad-centered p::text").get()
        return product_price.strip() if product_price else ""

    def get_product_image(self, response):
        product_image = response.css(".carousel__inner img::attr(src)").get()
        return product_image or ""


class PoshmarkCrawler(CrawlSpider):
    """Crawler for Poshmark website"""

    name = "PoshmarkUSSpider"
    allowed_domains = ["poshmark.com"]
    base_url = "https://poshmark.com"

    parser = PoshmarkParser()

    products = []
    taken_down_list = []
    missing_seller_list = []

    custom_settings = {
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 4.0,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 4.0,
        "CONCURRENT_REQUESTS": 2,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "DOWNLOAD_DELAY": 1,
        "HTTPERROR_ALLOWED_CODES": [code for code in range(100, 1000) if code != 200],
        "REDIRECT_ENABLED": True,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.driver = self.get_driver()
        self.driver.implicitly_wait(180)
        insert_region_and_country("NA", "United States")

    @staticmethod
    def get_driver():
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox") 
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--incognito")

        return webdriver.Chrome(options=options)

    def start_requests(self):
        """Initialize scraping requests"""

        poshmark_search_url = "https://poshmark.com/search?query={0}&type=listings&src=dir"
        search_url_variations = scraper_url_list(poshmark_search_url)
        
        for obj in search_url_variations:
            regions = obj.get("regions", "").split(", ") if obj.get("regions") else []
            countries = obj.get("countries", "").split(", ") if obj.get("countries") else []

            should_crawl = (
                not regions and not countries or 
                "NA" in regions or 
                "United States" in countries
            )

            if not should_crawl:
                continue
            
            yield Request(
                url=obj.get("url"),
                callback=self.crawl_poshmark_products,
                cb_kwargs={
                    "company_id": obj.get("company_id"),
                    "keyword": obj.get("keyword"),
                },
                dont_filter=True
            )

        yield from self._handle_missing_seller_urls()
        yield from self._handle_taken_down_adverts()

    def crawl_poshmark_products(self, response, **kwargs):
        self.driver.get(response.url)
        
        poshmark_products_css = ".grid-page .item__details"
        poshmark_products_locator = (By.CSS_SELECTOR, poshmark_products_css)

        # Wait for the product elements to load
        webdriver_wait = WebDriverWait(self.driver, 10)
        if not webdriver_wait.until(EC.presence_of_element_located(poshmark_products_locator)):
            self.logger.error("Could not find product elements on page")
            self.driver.quit()
            return

        self._scroll_page()
        
        # Get the product elements
        poshmark_products = Selector(text=self.driver.page_source).css(poshmark_products_css)
        if not poshmark_products:
            self.logger.error("No products found after scrolling")
            self.driver.quit()
            return

        # Iterate over the products and yield requests for each product
        for product in poshmark_products:
            product_url = product.css("a::attr(href)").get()
            if not product_url:
                continue
                
            if not product_url.startswith("http"):
                product_url = self.base_url + product_url
                
            yield Request(
                url=product_url,
                callback=self.parser.parse,
                cb_kwargs={
                    "company_id": kwargs.get("company_id"),
                    "keyword": kwargs.get("keyword"),
                },
                dont_filter=True
            )

        self.driver.quit()

    def _scroll_page(self):
        """Handle infinite scrolling"""

        last_height = self.driver.execute_script("return document.body.scrollHeight")            
        products_count = 0
        scroll_attempts = 0
        max_scroll_attempts = 30

        while scroll_attempts < max_scroll_attempts:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

            new_height = self.driver.execute_script("return document.body.scrollHeight")
            products = self.driver.find_elements(By.CSS_SELECTOR, ".grid-page .item__details")
            
            if (new_height == last_height or 
                len(products) == products_count or 
                len(products) > 500):
                break
                
            last_height = new_height
            products_count = len(products)
            scroll_attempts += 1

    def _handle_missing_seller_urls(self):
        """Process URLs with missing seller info"""

        missing_seller_urls = get_missing_seller_url(domain="poshmark.com")
        for url in missing_seller_urls:
            yield Request(
                url=url,
                callback=self.parser.parse,
                meta={"missing_seller": True}
            )

    def _handle_taken_down_adverts(self):
        """Process taken down advertisements"""

        taken_down_adverts = get_adverts_for_takendown(domain="poshmark.com")
        for advert in taken_down_adverts:
            advert_id, product, url = advert
            yield Request(
                url=url,
                callback=self.parser.parse,
                meta={"advert_id": advert_id, "product": product}
            )

    def closed(self, reason):
        """Handle spider shutdown and data export"""
        output_dir = 'outputs'
        os.makedirs(output_dir, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_filename = os.path.join(output_dir, f'poshmark_products_{timestamp}.csv')

        unique_products = unique_list_of_product(self)
        if unique_products:
            with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=unique_products[0].keys())
                writer.writeheader()
                writer.writerows(unique_products)

        write_to_excel(
            spider_name=self.name,
            products=unique_products,
            domain="poshmark.com",
            region="NA",
            country="United States",
            seller="poshmark",
        )

        if self.taken_down_list:
            update_last_seen(self.taken_down_list)

        if self.missing_seller_list:
            update_sellers(self.missing_seller_list)
