import os

import matplotlib.pyplot as plt

from comparison_timelines import get_keyword_dfs

def main():
    this_dir_path = os.path.dirname(os.path.abspath(__file__))
    data_dir_path = os.path.join(this_dir_path, "..", "data")

    news_df, related_videos_df, comments_df, keywords = get_keyword_dfs()

    stats = {}

    batch_size = 4
    for keyword in keywords:
        fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(8, 2.5))

        news_data = news_df[['publish_date', 'text', f'contains_{keyword}']].resample('SMS', on='publish_date').agg({'text': 'count', f'contains_{keyword}': 'sum'})
        news_data['percentage'] = 100 * news_data[f'contains_{keyword}'] / news_data['text']
        ln_news = ax.plot(news_data.index, news_data['percentage'], label='News')

        related_video_data = related_videos_df[['createtime', 'desc', f'contains_{keyword}']].resample('SMS', on='createtime').agg({'desc': 'count', f'contains_{keyword}': 'sum'})
        related_video_data['percentage'] = 100 * related_video_data[f'contains_{keyword}'] / related_video_data['desc']
        ln_video = ax.plot(related_video_data.index, related_video_data['percentage'], label='TikTok Videos')

        t_axis = ax.twinx()
        t_axis._get_lines.prop_cycler = ax._get_lines.prop_cycler
        comments_data = comments_df[['createtime', 'text', f'contains_{keyword}']].resample('SMS', on='createtime').agg({'text': 'count', f'contains_{keyword}': 'sum'})
        comments_data['percentage'] = 100 * comments_data[f'contains_{keyword}'] / comments_data['text']
        ln_comment = t_axis.plot(comments_data.index, comments_data['percentage'], label='TikTok Comments')

        lns = ln_news + ln_video + ln_comment
        labs = [l.get_label() for l in lns]
        ax.legend(lns, labs, loc=0, prop={'size': 10})
        # ax.set_title(f'Timeline of mentions of {keyword} in News vs TikTok')
        ax.set_xlabel('Date', fontsize=14)
        ax.set_ylabel('Percentage of news/videos', fontsize=10)
        t_axis.set_ylabel('Percentage of comments', fontsize=10)

        fig.tight_layout()
        fig.savefig(os.path.join(data_dir_path, 'outputs', f'{keyword}_timeline.png'))

if __name__ == '__main__':
    main()