import argparse
from datetime import datetime
import itertools
import logging
import os
import urllib

import pandas as pd

from seldonite.sources import news
from seldonite import collect, run

def main():
    this_dir_path = os.path.dirname(os.path.abspath(__file__))
    data_dir_path = os.path.join(this_dir_path, "..", "data", "google_news")

    # keywords = ['ukraine', 'russia', 'putin', 'zelenskyy', 'kyiv', 'moscow', 'kiev', 'crimea', \
    #     'donetsk', "Donetsk People's Republic", 'luhansk', "Luhansk People's Republic", 'donbas', \
    #     "Minsk II", 'NATO', 'shoigu', 'reznikov', 'gerasimov', 'prigozhin', 'kharkiv', 'odesa', \
    #     'odessa', 'chechen', 'russophobia', 'azov', 'peskov', 'kremlin', 'denazify', 'denazification', \
    #     'dvornikov', 'mariupol', 'kherson', 'HIMARS', 'no-fly zone']
    keywords = ['nato', 'war']

    sites = ["apnews.com", "nytimes.com", "washingtonpost.com", "bbc.com", "reuters.com", "aljazeera.com", \
            "cnn.com", "foxnews.com", "breitbart.com", "msnbc.com", "wsj.com", "huffpost.com", "vox.com", \
            "nbcnews.com", "usatoday.com", "npr.org"]

    start_date = datetime(2021, 1, 1)
    end_date = datetime.today()

    google_source = news.Google(dev_key=args.dev_key, engine_id=args.engine_id)

    for keyword, site in itertools.product(keywords, sites):
        keyword_slug = keyword.lower().replace(' ', '').replace("'", '').replace('-', '')
        site_slug = site.replace('.', '_')
        news_csv_path = os.path.join(data_dir_path, f"{site_slug}_{keyword_slug}_google_news.csv")
        if os.path.exists(news_csv_path):
            continue

        collector = collect.Collector(google_source) \
                        .on_sites([site]) \
                        .by_keywords([keyword]) \
                        .in_date_range(start_date, end_date)
        runner = run.Runner(collector, python_executable='/home/ndg/users/bsteel2/.conda/envs/seldonite/bin/python')
        
        try:
            df = runner.to_pandas()
        except ValueError:
            # TODO don't keep on doing this search if we get nothing
            logging.exception(f"Failed to collect news for {keyword} on {site}")
            # make a csv anyway to stop this being collected again
            df = pd.DataFrame()
            df.to_csv(news_csv_path)
            continue
        except urllib.error.HTTPError:
            logging.exception(f"Failed to collect news for {keyword} on {site}")
            break

        df.to_csv(news_csv_path)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--dev-key")
    parser.add_argument("--engine-id")
    args = parser.parse_args()

    main()