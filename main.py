import json
import os
from dataclasses import dataclass, field

import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential


@dataclass
class Article:
    url: str
    post_id: str
    title: str
    keywords: list
    thumbnail: str
    publication_date: str
    last_updated_date: str
    author: str
    content: str
    video_duration: str
    word_count: int
    description: str = ""
    lang: str = ""
    classes: list = field(default_factory=list)
    lite_url: str = ""

class SitemapParser:
    def __init__(self, sitemap_index_url):
        self.sitemap_index_url = sitemap_index_url

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def get_monthly_sitemaps(self):
        try:
            print(f"Fetching sitemap index from {self.sitemap_index_url}")
            response = requests.get(self.sitemap_index_url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'lxml')
            sitemaps = [loc.text for loc in soup.find_all('loc')]
            print(f"Found {len(sitemaps)} monthly sitemaps.")
            return sitemaps
        except requests.RequestException as e:
            print(f"Failed to fetch sitemap index: {e}")
            return []

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def get_article_urls(self, monthly_sitemap_url):
        try:
            print(f"Fetching article URLs from {monthly_sitemap_url}")
            response = requests.get(monthly_sitemap_url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'lxml')
            article_urls = [loc.text for loc in soup.find_all('loc')]
            print(f"Found {len(article_urls)} articles in the sitemap.")
            return article_urls
        except requests.RequestException as e:
            print(f"Failed to fetch article URLs from {monthly_sitemap_url}: {e}")
            return []

class ArticleScraper:
    def __init__(self, article_url):
        self.article_url = article_url

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def scrape(self):
        try:
            print(f"Scraping article from {self.article_url}")
            response = requests.get(self.article_url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "lxml")

            script = soup.find("script", {"id": "tawsiyat-metadata", "type": "text/tawsiyat"})
            metadata = json.loads(script.string) if script else {}

            full_text = ""
            for p in soup.find_all("p"):
                full_text += p.text + "\n"

            # Calculate word count
            len(full_text.split())

            article = Article(
                url=self.article_url,
                post_id=metadata.get('postid', ''),
                title=metadata.get('title', ''),
                keywords=metadata.get('keywords', '').split(" "),
                thumbnail=metadata.get('thumbnail', ''),
                publication_date=metadata.get('published_time', ''),
                last_updated_date=metadata.get('last_updated', ''),
                author=metadata.get('author', ''),
                content=full_text,
                video_duration=metadata.get('video_duration', ''),
                word_count=int(metadata.get('word_count', 0)),
                description=metadata.get('description', ''),
                lang=metadata.get('lang', ''),
                classes=metadata.get('classes', []),
                lite_url=metadata.get('lite_url', '')
            )
            return article
        except Exception as e:
            print(f"Failed to scrape article {self.article_url}: {e}")
            return None

class FileUtility:
    @staticmethod
    def save_to_json(articles, year, month):
        directory = r"C:\Users\Owner\PycharmProjects\pythonProject2"
        if not os.path.exists(directory):
            os.makedirs(directory)
        filepath = os.path.join(directory, f"articles_{year}_{month}.json")
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump([article.__dict__ for article in articles if article], f, ensure_ascii=False, indent=4)
            print(f"Successfully saved {len(articles)} articles to {filepath}")
        except Exception as e:
            print(f"Failed to save articles to {filepath}: {e}")

def main():
    sitemap_parser = SitemapParser('https://www.almayadeen.net/sitemaps/all.xml')
    monthly_sitemaps = sitemap_parser.get_monthly_sitemaps()

    articles = []
    article_count = 0
    max_articles = 10000

    for sitemap_url in monthly_sitemaps:
        if article_count >= max_articles:
            break
        article_urls = sitemap_parser.get_article_urls(sitemap_url)

        for url in article_urls:
            if article_count >= max_articles:
                break
            scraper = ArticleScraper(url)
            article = scraper.scrape()
            if article:
                articles.append(article)
                article_count += 1

                # Track progress
                print(f"Progress: {article_count}/{max_articles} articles scraped.")

        # Save articles for the current month after all articles in the sitemap are scraped
        if articles:
            year, month = sitemap_url.split('-')[-2], sitemap_url.split('-')[-1].replace('.xml', '')
            FileUtility.save_to_json(articles, year, month)
            articles = []  # Clear the list after saving

    # Final progress check
    print(f"Scraping complete. Total articles scraped: {article_count}")

if __name__ == "__main__":
    main()
