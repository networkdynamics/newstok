import datetime
import os

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
    news_df['publish_date'] = pd.to_datetime(news_df['publish_date'])
    news_df = news_df[news_df['publish_date'] > datetime.datetime(2022, 1, 1)]

    related_videos_df = utils.get_video_df(related_videos_path)
    apr_related_videos_df = utils.get_video_df(apr_related_videos_path)
    related_videos_df = pd.concat([related_videos_df, apr_related_videos_df])
    related_videos_df = related_videos_df.drop_duplicates(subset=['video_id'])

    related_videos_df = related_videos_df[related_videos_df['createtime'] > datetime.datetime(2022, 1, 1)]

    comment_path = os.path.join(this_dir_path, '..', '..', 'polar-seeds', 'data', 'cache', 'related_comments.csv')
    comments_df = utils.get_comment_df(comment_path)

    fig, ax = plt.subplots(figsize=(8, 3))

    news_count_df = news_df.resample('SMS', on='publish_date').count()
    news_ln = ax.plot(news_count_df.index, news_count_df['title'], label='News')

    video_count_df = related_videos_df.resample('SMS', on='createtime').count()
    video_ln = ax.plot(video_count_df.index, video_count_df['desc'], label='Videos')

    twin_ax = ax.twinx()
    twin_ax._get_lines.prop_cycler = ax._get_lines.prop_cycler
    comment_count_df = comments_df.resample('SMS', on='createtime').count()
    comment_ln = twin_ax.plot(comment_count_df.index, comment_count_df['text'], label='Comments')

    ax.set_xlabel('Date', fontsize=14)
    ax.set_ylabel('Num Videos/News', fontsize=14)
    twin_ax.set_ylabel('Num Comments', fontsize=14)

    # reduce number of data labels
    ax.xaxis.set_major_locator(plt.MaxNLocator(8))

    lns = news_ln + video_ln + comment_ln
    labs = [l.get_label() for l in lns]
    ax.legend(lns, labs, loc=0, prop={'size': 12})

    fig.tight_layout()
    fig.savefig(os.path.join(data_dir_path, 'outputs', 'num_over_time.png'))

if __name__ == '__main__':
    main()