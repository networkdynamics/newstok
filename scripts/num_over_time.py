import datetime
import os

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd

from pytok import utils

def main():
    this_dir_path = os.path.dirname(os.path.abspath(__file__))
    data_dir_path = os.path.join(this_dir_path, "..", "data")
    news_csv_path = os.path.join(data_dir_path, "all_google_news.csv")
    related_videos_path = os.path.join(data_dir_path, "related_videos.csv")
    apr_related_videos_path = os.path.join(data_dir_path, "related_apr_videos.csv")

    news_df = pd.read_csv(news_csv_path)
    def try_parse_date(date):
        try:
            return datetime.datetime.strptime(date, '%Y-%m-%d')
        except:
            return None
    news_df['publish_date'] = news_df['publish_date'].apply(try_parse_date)
    news_df = news_df[news_df['publish_date'] > datetime.datetime(2022, 1, 1)]

    related_videos_df = utils.try_load_video_df_from_file(related_videos_path)
    apr_related_videos_df = utils.try_load_video_df_from_file(apr_related_videos_path)
    related_videos_df = pd.concat([related_videos_df, apr_related_videos_df])
    related_videos_df = related_videos_df.drop_duplicates(subset=['video_id'])

    related_videos_df = related_videos_df[related_videos_df['createtime'] > datetime.datetime(2022, 1, 1)]

    comment_path = os.path.join(this_dir_path, '..', '..', 'polar-seeds', 'data', 'cache', 'related_comments.parquet.gzip')
    comments_df = utils.try_load_comment_df_from_file(comment_path)

    fig, ax = plt.subplots(figsize=(8, 3))

    news_count_df = news_df.resample('SMS', on='publish_date').count()
    news_ln = ax.plot(news_count_df.index, news_count_df['title'], label='News', color='blue')

    video_count_df = related_videos_df.resample('SMS', on='createtime').count()
    video_ln = ax.plot(video_count_df.index, video_count_df['desc'], label='TikTok Videos', color='green')

    twin_ax = ax.twinx()
    comment_count_df = comments_df.resample('SMS', on='createtime').count()
    comment_ln = twin_ax.plot(comment_count_df.index, comment_count_df['text'], label='TikTok Comments', color='orange')

    ax.set_xlabel('Date', fontsize=14)
    ax.set_ylabel('No. Videos/News', fontsize=14)
    twin_ax.set_ylabel('No. Comments', fontsize=14)

    # reduce number of data labels
    ax.xaxis.set_major_locator(matplotlib.dates.YearLocator())
    ax.xaxis.set_minor_locator(matplotlib.dates.MonthLocator((0,3,6,9)))

    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("\n%Y"))
    ax.xaxis.set_minor_formatter(matplotlib.dates.DateFormatter("%b"))
    fig.autofmt_xdate()

    lns = news_ln + video_ln + comment_ln
    labs = [l.get_label() for l in lns]
    ax.legend(lns, labs, loc=0, prop={'size': 12})

    fig.tight_layout()
    fig.savefig(os.path.join(data_dir_path, 'outputs', 'num_over_time.png'))

if __name__ == '__main__':
    main()