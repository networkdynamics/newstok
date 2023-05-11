import json
import os

import comparison_timelines

def add_stats(stats, keyword, series, medium):

    stats[keyword][f'{medium}_mean'] = round(series.mean(), 2)
    stats[keyword][f'{medium}_std'] = round(series.std(), 2)
    stats[keyword][f'{medium}_max'] = round(series.max(), 2)
    stats[keyword][f'{medium}_min'] = round(series.min(), 2)

    return stats

def main():
    news_df, related_videos_df, comments_df, keywords = comparison_timelines.get_keyword_dfs()

    # get summary stats for each keyword
    stats = {}

    for keyword in keywords:
        stats[keyword] = {}

        news_data = news_df[['publish_date', 'text', f'contains_{keyword}']].resample('SMS', on='publish_date').agg({'text': 'count', f'contains_{keyword}': 'sum'})
        news_data['percentage'] = 100 * news_data[f'contains_{keyword}'] / news_data['text']
        stats = add_stats(stats, keyword, news_data['percentage'], 'news')

        related_video_data = related_videos_df[['createtime', 'desc', f'contains_{keyword}']].resample('SMS', on='createtime').agg({'desc': 'count', f'contains_{keyword}': 'sum'})
        related_video_data['percentage'] = 100 * related_video_data[f'contains_{keyword}'] / related_video_data['desc']
        stats = add_stats(stats, keyword, related_video_data['percentage'], 'videos')

        comments_data = comments_df[['createtime', 'text', f'contains_{keyword}']].resample('SMS', on='createtime').agg({'text': 'count', f'contains_{keyword}': 'sum'})
        comments_data['percentage'] = 100 * comments_data[f'contains_{keyword}'] / comments_data['text']
        stats = add_stats(stats, keyword, comments_data['percentage'], 'comments')

    this_dir_path = os.path.dirname(os.path.abspath(__file__))
    data_dir_path = os.path.join(this_dir_path, '..', 'data')

    with open(os.path.join(data_dir_path, 'keyword_stats.json'), 'w') as f:
        json.dump(stats, f, indent=4)

if __name__ == '__main__':
    main()