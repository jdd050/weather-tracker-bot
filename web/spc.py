import requests
import os
import re
import logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from abc import ABC, abstractmethod

# Configure Logging for library use
logger = logging.getLogger(__name__)

class SPCBaseScraper(ABC):
    """
    Abstract Base Class for SPC Scrapers.
    Provides shared utility methods for networking and file management.
    """
    def __init__(self, output_dir):
        self.base_url = "https://www.spc.noaa.gov"
        self.output_dir = output_dir
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
        })

    def _get_soup(self, url):
        """Fetches a URL and returns a BeautifulSoup object."""
        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None

    def _save_resource(self, url, folder_path, filename):
        """Downloads and saves a binary file (image/zip/etc)."""
        try:
            res = self.session.get(url, timeout=10)
            if res.status_code == 200:
                full_path = os.path.join(folder_path, filename)
                with open(full_path, "wb") as f:
                    f.write(res.content)
                return True
            return False
        except Exception as e:
            logger.error(f"Error downloading {filename} from {url}: {e}")
            return False

    @abstractmethod
    def run(self):
        """Each module must implement its own run logic."""
        pass

class OutlookModule(SPCBaseScraper):
    """
    Module for Convective Outlooks (Day 1, 2, 3, and 4-8).
    """
    def __init__(self, root_output_dir="./spc_data"):
        super().__init__(output_dir=os.path.join(root_output_dir, "outlooks"))
        self.entry_point = f"{self.base_url}/products/outlook/"

    def run(self):
        logger.info("Starting Outlook Module...")
        soup = self._get_soup(self.entry_point)
        if not soup: return

        links = soup.find_all('a', href=re.compile(r'day\dotlk\.html|day4-8'))
        unique_urls = {urljoin(self.entry_point, link['href']) for link in links}

        for url in unique_urls:
            self._process_outlook_page(url)

    def _process_outlook_page(self, url):
        folder_name = url.split('/')[-1].replace('.html', '').upper()
        if "DAY4-8" in url: folder_name = "DAY4-8"
        
        page_path = os.path.join(self.output_dir, folder_name)
        os.makedirs(page_path, exist_ok=True)
        
        soup = self._get_soup(url)
        if not soup: return

        # 1. Extract Discussion
        pre_tag = soup.find('pre')
        if pre_tag:
            with open(os.path.join(page_path, "discussion.txt"), "w") as f:
                f.write(pre_tag.get_text(strip=True))

        # 2. Extract Images
        elements = soup.find_all(attrs={re.compile(r'onclick|onClick'): re.compile(r"show_tab")})
        for el in elements:
            attr_val = el.get('onclick') or el.get('onClick')
            match = re.search(r"show_tab\('([^']+)'\)", attr_val)
            if match:
                nam = match.group(1)
                if "DAY4-8" in folder_name:
                    img_filename = f"day{nam}prob.gif"
                else:
                    prefix = folder_name.split('OTLK')[0].lower()
                    img_filename = f"{prefix}{nam}.png"
                
                img_url = urljoin(url, img_filename)
                success = self._save_resource(img_url, page_path, img_filename)
                if not success and img_url.endswith('.gif'):
                    alt_url = img_url.replace('.gif', '.png')
                    self._save_resource(alt_url, page_path, img_filename.replace('.gif', '.png'))

class SPCScraperManager:
    """
    Orchestrator to manage and run multiple scraping modules.
    """
    def __init__(self):
        self.modules = []
        # Setup basic logging if not already configured by the caller
        if not logging.root.handlers:
            logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    def add_module(self, module):
        self.modules.append(module)

    def start_all(self):
        for module in self.modules:
            module.run()