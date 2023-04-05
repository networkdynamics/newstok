import os

import pandas as pd

def main():
    this_dir_path = os.path.dirname(os.path.abspath(__file__))
    data_dir_path = os.path.join(this_dir_path, "..", "data")

    dfs = []
    for filename in os.listdir(data_dir_path):
        if not filename.endswith('_news.csv'):
            continue

        news_csv_path = os.path.join(data_dir_path, filename)
        df = pd.read_csv(news_csv_path)
        dfs.append(df)

    df = pd.concat(dfs)
    df = df.drop_duplicates('url')
    df = df[['title', 'text', 'url', 'publish_date']]
    df.to_csv(os.path.join(data_dir_path, 'all_news.csv'))

if __name__ == '__main__':
    main()