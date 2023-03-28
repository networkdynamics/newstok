import argparse
from datetime import datetime
import os

from seldonite.sources import news
from seldonite import collect, run, sources
from seldonite.helpers import utils

def main():
    this_dir_path = os.path.dirname(os.path.abspath(__file__))
    data_dir_path = os.path.join(this_dir_path, "..", "data")

    aws_access_key = os.environ['AWS_ACCESS_KEY']
    aws_secret_key = os.environ['AWS_SECRET_KEY']

    keywords = ['ukraine', 'russia', 'putin', 'zelenskyy', 'kyiv', 'moscow', 'kiev', 'crimea', \
        'donetsk', "Donetsk People's Republic", 'luhansk', "Luhansk People's Republic", 'donbas', \
        "Minsk II", 'NATO', 'shoigu', 'reznikov', 'gerasimov', 'prigozhin', 'kharkiv', 'odesa', \
        'odessa', 'chechen', 'russophobia', 'azov', 'peskov', 'kremlin', 'denazify', 'denazification', \
        'dvornikov', 'mariupol', 'kherson', 'HIMARS', 'no-fly zone'] 

    sites = ["nytimes.com", "washingtonpost.com", "apnews.com", "bbc.com", "reuters.com", "aljazeera.com", \
            "cnn.com", "foxnews.com", "breitbart.com", "msnbc.com", "wsj.com", "huffpost.com", "vox.com", \
            "nbcnews.com", "usatoday.com", "npr.org"]

    start_date = datetime(2022, 1, 1)
    end_date = datetime.today()

    crawls = utils.get_cc_crawls_since(start_date)
    blacklist = ['*/sports/*', '*rus.reuters*', '*fr.reuters*', '*br.reuters*', '*de.reuters*', '*es.reuters*', \
                 '*lta.reuters*', '*ara.reuters*', '*it.reuters*', '*ar.reuters*', '*blogs.reuters*', '*graphics.reuters*', \
                 '*jp.mobile.reuters*', '*live.special.reuters*', '*plus.reuters*', '*af.reuters*', \
                 '*in.reuters*', '*ru.reuters*', '*jp.reuters*', '*live.jp.reuters*', '*in.mobile.reuters*', \
                 '*br.mobile.reuters*', '*mx.reuters*', '*live.reuters*', '*cn.reuters*', '*agency.reuters*', \
                 '*widerimage.reuters*']

    crawls.reverse()
    for crawl in crawls:
        print(f"Starting pull for crawl: {crawl}")

        cc_source = sources.news.CommonCrawl(aws_access_key, aws_secret_key)
        cc_source.set_crawls(crawl)
        collector = collect.Collector(cc_source)
        collector.on_sites(sites) \
                 .by_keywords(keywords) \
                 .exclude_in_url(blacklist) \
                 .in_language(lang='en') \
                 .in_date_range(start_date, end_date) \
                 .distinct()

        runner = run.Runner(collector, driver_cores=22, driver_memory='256g')
        df = runner.to_pandas()

        news_csv_path = os.path.join(data_dir_path, f"{crawl}_news.csv")
        df.to_csv(news_csv_path)

        print(f"Finished pull for crawl: {crawl}")

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--dev-key")
    parser.add_argument("--engine-id")
    args = parser.parse_args()

    main()