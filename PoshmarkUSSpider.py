import csv
import os
import scrapy
import time

from datetime import datetime
from scrapy import Spider
from selenium import webdriver

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from ...utils import (
    get_adverts_for_takendown,
    get_missing_seller_url,
    handle_errors,
    insert_region_and_country,
    scraper_url_list,
    unique_list_of_product,
    update_last_seen,
    update_sellers,
    write_to_excel,
)


class PoshmarkUSSpider(Spider):
    """Spider for scraping Poshmark US website."""

    name = "PoshmarkUSSpider"
    allowed_domains = ["poshmark.com"]
    base_url = "https://poshmark.com"
    domain = "poshmark.com"
    region = "NA"
    country = "United States" 
    seller = "poshmark"

    # Initialize tracking lists
    products = []
    taken_down_list = []
    missing_seller_list = []

    # Register region and country
    insert_region_and_country(region, country)

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
        """Initialize spider with webdriver."""

        super().__init__(*args, **kwargs)
        self.driver = self.get_driver()
        self.driver.implicitly_wait(180)

    @staticmethod
    def get_driver():
        """Configure and return Chrome webdriver."""

        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--incognito")

        return webdriver.Chrome(options=options)

    def start_requests(self):
        """Initialize scraping requests."""

        search_urls = scraper_url_list("https://poshmark.com/search?query={0}&type=listings&src=dir")
        for url_data in search_urls:
            regions = url_data.get("regions", "").split(", ") if url_data.get("regions") else []
            countries = url_data.get("countries", "").split(", ") if url_data.get("countries") else []
            
            scrape_all = not regions and not countries
            region_match = self.region in regions
            country_match = self.country in countries

            if scrape_all or region_match or country_match:
                yield scrapy.Request(
                    url=url_data["url"],
                    callback=self.parse,
                    cb_kwargs={
                        "company_id": url_data["company_id"],
                        "keyword": url_data["keyword"],
                    },
                    dont_filter=True,
                )

        self._handle_missing_seller_urls()
        self._handle_taken_down_adverts()
    
    def _handle_missing_seller_urls(self):
        """Process URLs with missing seller info."""

        missing_seller_urls = get_missing_seller_url(domain=self.domain)
        for url in missing_seller_urls:

            yield scrapy.Request(
                url=url,
                callback=self.parse_missing_seller,
                meta={"missing_seller": True},
            )

    def _handle_taken_down_adverts(self):
        """Process taken down advertisements."""

        taken_down_adverts = get_adverts_for_takendown(domain=self.domain)
        for advert in taken_down_adverts:
            advert_id, product, url = advert

            yield scrapy.Request(
                url=url,
                callback=self.parse_product_detail,
                meta={"advert_id": advert_id, "product": product},
            )
    
    def parse(self, response, **kwargs):
        """Parse search results with infinite scroll handling."""

        self.driver.get(response.url)
        
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".grid-page .item__details"))
            )

            self._scroll_page()

            response_data = scrapy.Selector(text=self.driver.page_source)
            for product in response_data.css(".grid-page .item__details"):
                listing = {
                    "seller": self.seller,
                    "region": self.region,
                    "country": self.country,
                    "domain": self.domain,
                    "currency": "USD",
                    "keyword": kwargs.get("keyword"),
                    "company_id": kwargs.get("company_id"),
                    "created_at": datetime.now().strftime("%d-%m-%Y"),
                    "shipping_address": ""
                }
                
                product_url = product.css("a::attr(href)").get()
                if product_url:
                    if not product_url.startswith("http"):
                        product_url = response.urljoin(product_url)

                    listing["url"] = product_url

                    yield scrapy.Request(
                        url=listing["url"],
                        callback=self.parse_product_detail,
                        cb_kwargs=kwargs,
                        meta={"listing": listing},
                        dont_filter=True
                    )

        except Exception as e:
            self.logger.error(f"Error during parsing: {str(e)}")
            
        finally:
            self.driver.quit()

    def _scroll_page(self):
        """Handle infinite scrolling of the page."""

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

    @handle_errors 
    def parse_product_detail(self, response, **kwargs):
        """Parse individual product details."""

        product_title = response.css(".listing__title h1::text").get()
        product_description = response.css(".listing__description ::text").get()
        product_price = response.css(".listing__ipad-centered p::text").get()
        product_image = response.css(".carousel__inner img::attr(src)").get()

        listing = response.meta["listing"]
        listing.update({
            "title": product_title.strip() if product_title else "",
            "description": product_description.strip() if product_description else "",
            "price": product_price.strip() if product_price else "",
            "pic": product_image if product_image else ""
        })

        self.products.append(listing)

    def closed(self, reason):
        """Handle spider shutdown and data export."""

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
            domain=self.domain,
            region=self.region,
            country=self.country,
            seller=self.seller,
        )

        if self.taken_down_list:
            update_last_seen(self.taken_down_list)

        if self.missing_seller_list:
            update_sellers(self.missing_seller_list)

