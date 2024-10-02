import os
import urllib.parse
import asyncio
import logging
from datetime import datetime
from lxml import etree
from io import StringIO
from slugify import slugify
from scrapingant_client import ScrapingAntClient
import os
# Import Azure Blob Storage libraries
from azure.storage.blob.aio import BlobServiceClient, ContainerClient, BlobClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

logging.info('Imported libraries in scraper')
# ScrapingAnt API details set in azure settings 
SCRAPINGANT_API_KEY = os.getenv("SCRAPINGANT_API_KEY")

# Root URL
ROOT_URL = "https://www.sportsdirect.com/football/all-football"

# Logging setup without local file dependency
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )
    logging.info('SD Runtime Log: \n')

# ScrapingAnt request function with retry
async def scrapingant_request(url, client, max_retries=3):
    for attempt in range(max_retries):
        try:
            # Use the async request
            result = await client.general_request_async(url, browser=True)
            return result.content
        except Exception as e:
            logging.error(f"ScrapingAnt error on attempt {attempt + 1} for {url}: {str(e)}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
    return None

# Parse HTML content using lxml
def parse_html(content):
    parser = etree.HTMLParser()
    tree = etree.parse(StringIO(content), parser)
    return tree

# Get page URL function (unchanged)
def get_page_url(page):
    return f"{ROOT_URL}#dcp={page}&dppp=59&OrderBy=rank"

# Get page URLs function
async def get_page_urls(client):
    html_content = await scrapingant_request(ROOT_URL, client)
    if not html_content:
        logging.error("Failed to retrieve content for root URL")
        return {}

    tree = parse_html(html_content)

    # XPath to find the pagination div
    pagination = tree.xpath('//div[@id="divPagination"]')
    if not pagination:
        logging.error("Pagination div not found")
        return {}

    # Extract all page links with class 'swipeNumberClick'
    page_links = pagination[0].xpath('.//a[contains(@class, "swipeNumberClick")]/text()')
    if not page_links:
        logging.error("No pagination links found")
        return {}

    try:
        max_page = int(page_links[-1].strip())
    except ValueError:
        logging.error("Failed to parse the maximum page number")
        return {}

    pages = range(1, max_page + 1)
    return {page: get_page_url(page) for page in pages}

# Get page products function
async def get_page_products(page_url, client):
    html_content = await scrapingant_request(page_url, client)
    if not html_content:
        logging.error(f"Failed to retrieve content for page URL: {page_url}")
        return {}

    tree = parse_html(html_content)

    # XPath to find the products container
    products_container = tree.xpath('//div[@id="productlistcontainer"]')
    if not products_container:
        logging.error(f"Products container not found for page URL: {page_url}")
        return {}

    # Extract all product items; assuming 'li-name' is a custom attribute
    product_items = products_container[0].xpath('.//li[@li-name]')
    if not product_items:
        logging.error(f"No product items found for page URL: {page_url}")
        return {}

    products = {}
    for item in product_items:
        href = item.xpath('.//a/@href')
        if not href:
            continue
        full_url = urllib.parse.urljoin('https://www.sportsdirect.com', href[0])

        product_id = item.get('li-productid', '').strip()
        brand = item.get('li-brand', '').strip()
        name = item.get('li-name', '').strip()

        if product_id and brand and name:
            product_name = f"{product_id}-{brand} - {name}"
            products[full_url] = product_name
        else:
            logging.warning(f"Missing data for product on page URL: {page_url}")

    return products

# Identify all products function
async def identify_all_products(client):
    page_urls = await get_page_urls(client)
    if not page_urls:
        logging.error("No page URLs found")
        return {}

    all_products = {}
    for page, url in page_urls.items():
        # if page == 2:
        #     break
        logging.info(f"Processing page {page}: {url}")
        products = await get_page_products(url, client)
        if products:
            all_products.update(products)
            logging.info(f"Found {len(products)} products on page {page}")
        else:
            logging.warning(f"No products found on page {page}")

    return all_products

# Write product to blob storage function
async def write_product_to_blob(container_client, product, url, client):
    product_base_name = slugify(product)
    extension = '.html'
    current_date = datetime.now().strftime('%Y-%m-%d')
    blob_name = "sportsdirect/" + current_date + "/" + product_base_name + extension

    count = 1
    while True:
        blob_client = container_client.get_blob_client(blob=blob_name)
        try:
            await blob_client.get_blob_properties()
            # Blob exists, need to create a new name
            blob_name = f"{product_base_name}-var-{count}{extension}"
            count +=1
        except ResourceNotFoundError:
            # Blob does not exist
            break

    html_content = await scrapingant_request(url, client)
    if html_content:
        try:
            await blob_client.upload_blob(data=html_content, overwrite=False)
            return blob_name
        except Exception as e:
            logging.error(f"Failed to upload blob {blob_name}: {str(e)}")
            return None
    else:
        logging.error(f"ERROR: Failed at scraping {url}")
        return None

# Main function to run the scraper
async def main():
    setup_logging()

    client = ScrapingAntClient(token=SCRAPINGANT_API_KEY)

    # Set up BlobServiceClient
    STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not STORAGE_CONNECTION_STRING:
        logging.error("Azure Storage connection string is not set. Please set AZURE_STORAGE_CONNECTION_STRING environment variable.")
        return

    blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
    container_name = "scraping"  # Replace with your actual container name
    container_client = blob_service_client.get_container_client(container_name)

    try:
        await container_client.create_container()
    except ResourceExistsError:
        pass

    logging.info('Identifying all products...')
    all_products = await identify_all_products(client)
    logging.info(f'All products identified! Total: {len(all_products)}')

    if not all_products:
        logging.error("No products found. Exiting.")
        return

    logging.info('Writing product HTMLs to blob storage')
    tasks = [
        write_product_to_blob(container_client, product, url, client)
        for url, product in all_products.items()
    ]
    results = await asyncio.gather(*tasks)

    successful_scrapes = sum(1 for result in results if result is not None)
    logging.info(f'Written! Successfully scraped {successful_scrapes} out of {len(all_products)} products.')

