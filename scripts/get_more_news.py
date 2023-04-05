import argparse
import datetime
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

    sites = ["apnews.com", "nytimes.com", "washingtonpost.com", "bbc.com", "reuters.com", "aljazeera.com", \
            "cnn.com", "foxnews.com", "breitbart.com", "msnbc.com", "wsj.com", "huffpost.com", "vox.com", \
            "nbcnews.com", "usatoday.com", "npr.org"]

    start_date = datetime.date(2022, 1, 1)
    end_date = datetime.date.today()

    crawls = utils.get_cc_crawls_since(start_date)
    all_blacklist = {
        'nytimes.com': ['*/recipes/*', '*/video/*'],
        'vox.com': ['*/users/*', '*/authors/*'],
        'reuters.com': ['*rus.reuters*', '*fr.reuters*', '*br.reuters*', '*de.reuters*', '*es.reuters*', \
                 '*lta.reuters*', '*ara.reuters*', '*it.reuters*', '*ar.reuters*', '*blogs.reuters*', '*graphics.reuters*', \
                 '*jp.mobile.reuters*', '*live.special.reuters*', '*plus.reuters*', '*af.reuters*', \
                 '*in.reuters*', '*ru.reuters*', '*jp.reuters*', '*live.jp.reuters*', '*in.mobile.reuters*', \
                 '*br.mobile.reuters*', '*mx.reuters*', '*live.reuters*', '*cn.reuters*', '*agency.reuters*', \
                 '*widerimage.reuters*']
    }

    crawls.reverse()
    for crawl in crawls:
        for site in sites:
            print(f"Starting pull for crawl: {crawl}, and site: {site}")

            blacklist = all_blacklist.get(site, [])

            cc_source = sources.news.CommonCrawl(aws_access_key, aws_secret_key)
            cc_source.set_crawls(crawl)
            collector = collect.Collector(cc_source)
            collector.on_sites([site]) \
                    .by_keywords(keywords) \
                    .exclude_in_url(blacklist) \
                    .in_language(lang='en') \
                    .in_date_range(start_date, end_date) \
                    .distinct()
                    # .limit_num_articles(100000) \

            runner = run.Runner(collector, driver_cores=24, driver_memory='256g', python_executable='/home/ndg/users/bsteel2/.conda/envs/seldonite/bin/python')
            df = runner.to_pandas()

            news_csv_path = os.path.join(data_dir_path, f"{crawl}_{site.replace('.', '')}_news.csv")
            df.to_csv(news_csv_path)

            print(f"Finished pull for crawl: {crawl}, and site: {site}")

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--dev-key")
    parser.add_argument("--engine-id")
    args = parser.parse_args()

    main()