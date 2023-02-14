import argparse
from datetime import datetime
import os

from seldonite.sources import news
from seldonite import collect, run

def main():
    sites = ["nytimes.com", "washingtonpost.com", "apnews.com", "theglobeandmail.com", "bbc.com", "cbc.ca"]
    keywords = ["ukraine", "putin", "zelensky", "russia"]
    start_date = datetime(2021, 1, 1)
    end_date = datetime.today()

    google_source = news.Google(dev_key=args.dev_key, engine_id=args.engine_id)

    collector = collect.Collector(google_source) \
                    .on_sites(sites) \
                    .by_keywords(keywords) \
                    .in_date_range(start_date, end_date)
    runner = run.Runner(collector)
    df = runner.to_pandas()

    this_dir_path = os.path.dirname(os.path.abspath(__file__))
    data_dir_path = os.path.join(this_dir_path, "..", "data")
    news_csv_path = os.path.join(data_dir_path, "news.csv")
    df.to_csv(news_csv_path)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--dev-key")
    parser.add_argument("--engine-id")
    args = parser.parse_args()

    main()