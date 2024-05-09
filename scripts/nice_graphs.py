import os

import matplotlib
import matplotlib.pyplot as plt

from comparison_timelines import get_keyword_dfs

def merge_timelines(all_df, news_df, video_df, comment_df, keyword):
    renamed_news_df = news_df.rename(columns={'text': 'num_news', f'contains_{keyword}': f'num_news_contains_{keyword}', 'percentage': f'percentage_news_contains_{keyword}'})
    renamed_video_df = video_df.rename(columns={'desc': 'num_video', f'contains_{keyword}': f'num_video_contains_{keyword}', 'percentage': f'percentage_video_contains_{keyword}'})
    renamed_comment_df = comment_df.rename(columns={'text': 'num_comment', f'contains_{keyword}': f'num_comment_contains_{keyword}', 'percentage': f'percentage_comment_contains_{keyword}'})

    if all_df is None:
        all_df = renamed_news_df
    else:
        news_cols = renamed_news_df.columns
        if 'num_news' in all_df.columns:
            news_cols = [col for col in news_cols if col != 'num_news']
        all_df = all_df.merge(renamed_news_df[news_cols], how='outer', left_index=True, right_index=True)
        
    video_cols = renamed_video_df.columns
    if 'num_video' in all_df.columns:
        video_cols = [col for col in video_cols if col != 'num_video']
    all_df = all_df.merge(renamed_video_df[video_cols], how='outer', left_index=True, right_index=True)

    comment_cols = renamed_comment_df.columns
    if 'num_comment' in all_df.columns:
        comment_cols = [col for col in comment_cols if col != 'num_comment']
    all_df = all_df.merge(renamed_comment_df[comment_cols], how='outer', left_index=True, right_index=True)

    return all_df

def main():
    this_dir_path = os.path.dirname(os.path.abspath(__file__))
    data_dir_path = os.path.join(this_dir_path, "..", "data")

    news_df, related_videos_df, comments_df, keywords = get_keyword_dfs()

    stats = {}

    all_df = None

    batch_size = 4
    for keyword in keywords:
        fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(8, 2.5))

        news_data = news_df[['publish_date', 'text', f'contains_{keyword}']].resample('SMS', on='publish_date').agg({'text': 'count', f'contains_{keyword}': 'sum'})
        news_data['percentage'] = 100 * news_data[f'contains_{keyword}'] / news_data['text']
        ln_news = ax.plot(news_data.index, news_data['percentage'], label='News', color='blue')

        related_video_data = related_videos_df[['createtime', 'desc', f'contains_{keyword}']].resample('SMS', on='createtime').agg({'desc': 'count', f'contains_{keyword}': 'sum'})
        related_video_data['percentage'] = 100 * related_video_data[f'contains_{keyword}'] / related_video_data['desc']
        ln_video = ax.plot(related_video_data.index, related_video_data['percentage'], label='TikTok Videos', color='green')

        t_axis = ax.twinx()
        # t_axis._get_lines.prop_cycler = ax._get_lines.prop_cycler
        comments_data = comments_df[['createtime', 'text', f'contains_{keyword}']].resample('SMS', on='createtime').agg({'text': 'count', f'contains_{keyword}': 'sum'})
        comments_data['percentage'] = 100 * comments_data[f'contains_{keyword}'] / comments_data['text']
        ln_comment = t_axis.plot(comments_data.index, comments_data['percentage'], label='TikTok Comments', color='orange')

        all_df = merge_timelines(all_df, news_data, related_video_data, comments_data, keyword)

        lns = ln_news + ln_video + ln_comment
        labs = [l.get_label() for l in lns]
        ax.legend(lns, labs, loc=0, prop={'size': 10})
        # ax.set_title(f'Timeline of mentions of {keyword} in News vs TikTok')
        ax.set_xlabel('Date', fontsize=14)
        ax.set_ylabel('Share of news/videos (%)', fontsize=10)
        t_axis.set_ylabel('Share of comments (%)', fontsize=10)

        ax.xaxis.set_major_locator(matplotlib.dates.YearLocator())
        ax.xaxis.set_minor_locator(matplotlib.dates.MonthLocator((0,3,6,9)))

        ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("\n%Y"))
        ax.xaxis.set_minor_formatter(matplotlib.dates.DateFormatter("%b"))
        fig.autofmt_xdate()

        fig.tight_layout()
        fig.savefig(os.path.join(data_dir_path, 'outputs', f'{keyword}_timeline.png'))

    all_df.to_csv(os.path.join(data_dir_path, 'outputs', 'all_keywords_timeline.csv'))

if __name__ == '__main__':
    main()