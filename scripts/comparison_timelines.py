import os
import datetime
import functools
import json
import multiprocessing

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import tqdm

from pytok import utils

def year_month_to_month_range(year, month):
    if month == 12:
        return datetime.datetime(year, month, 1), datetime.datetime(year + 1, 1, 1)
    else:
        return datetime.datetime(year, month, 1), datetime.datetime(year, month + 1, 1)

def convert_month_lines(line):
    if len(line) == 0:
        return []

    months = line.split(', ')
    date_months = []
    for month in months:
        if len(month) == 2 and month[0] == '0':
            date_months.append(year_month_to_month_range(2023, int(month[1])))
        else:
            date_months.append(year_month_to_month_range(2022, int(month)))

    return date_months

def add_stats(stats, keyword, series, medium):

    stats[keyword][f'{medium}_mean'] = round(series.mean(), 2)
    stats[keyword][f'{medium}_std'] = round(series.std(), 2)
    stats[keyword][f'{medium}_max'] = round(series.max(), 2)
    stats[keyword][f'{medium}_min'] = round(series.min(), 2)

    return stats

def contains_keyword(df, columns, keyword, spellings):
    if isinstance(spellings, str):
        spellings = [spellings]
    if isinstance(columns, str):
        columns = [columns]
    df[f'contains_{keyword}'] = functools.reduce(np.logical_or, [df[column].str.contains(spelling, case=False) for spelling in spellings for column in columns])
    return df

def get_examples(news_df, related_videos_df, comments_df, keywords, data_dir_path):
    with open(os.path.join(data_dir_path, 'month_samples.txt'), 'r') as f:
        text_lines = f.readlines()

    text_lines = [line.strip() for line in text_lines]

    mediums = [col.lower() for col in text_lines[1:4]]
    keywords = [k.lower() for k in text_lines[4::4]]
    comment_months = [convert_month_lines(line) for line in text_lines[5::4]]
    video_months = [convert_month_lines(line) for line in text_lines[6::4]]
    news_months = [convert_month_lines(line) for line in text_lines[7::4]]

    months = {keyword: {'comments': comment_months[i], 'videos': video_months[i], 'news': news_months[i]}
        for i, keyword in enumerate(keywords)}

    examples = {}

    for keyword in keywords:
        if keyword not in months:
                continue

        examples[keyword] = {}
        for medium in mediums:

            examples[keyword][medium] = {}
            if medium == 'news':
                df = news_df
                date_col = 'publish_date'
                def sort_func(d):
                    return d.sample(frac=1)
                sample_size = 15
                text_col = 'title'
                return_cols = ['title']
            elif medium == 'videos':
                df = related_videos_df
                date_col = 'createtime'
                def sort_func(d):
                    return d.sort_values('play_count', ascending=False)
                sample_size = 20
                text_col = 'desc'
                return_cols = ['desc', 'play_count', 'url']
            elif medium == 'comments':
                df = comments_df
                date_col = 'createtime'
                def sort_func(d):
                    return d.sort_values('digg_count', ascending=False)
                sample_size = 30
                text_col = 'text'
                return_cols = ['text', 'digg_count']

            df = df[df[f'contains_{keyword}'] == True]

            for month_span in months[keyword][medium]:
                start_date, end_date = month_span
                month_df = df[(df[date_col] >= start_date) & (df[date_col] < end_date)]
                month_df = sort_func(month_df)
                month_df = month_df.head(sample_size)
                if medium == 'videos':
                    month_df['url'] = month_df[['author_name', 'video_id']].apply(lambda x: f'https://www.tiktok.com/@{x[0]}/video/{x[1]}', axis=1)
                examples[keyword][medium][month_span[0].strftime('%m-%y')] = month_df[return_cols].to_dict('records')

    with open(os.path.join(data_dir_path, 'keyword_timeline_examples.json'), 'w') as f:
        json.dump(examples, f, ensure_ascii=False, indent=4)


def get_keyword_dfs():
    keywords = ['ukraine', 'russia', 'putin', 'zelenskyy', 'kyiv', 'moscow', 'crimea', \
        'donetsk', 'luhansk', 'donbas', \
        "Minsk II", 'NATO', 'shoigu', 'reznikov', 'gerasimov', 'prigozhin', 'kharkiv', 'odesa', \
        'chechen', 'russophobia', 'azov', 'peskov', 'kremlin', 'denazification', \
        'dvornikov', 'mariupol', 'kherson', 'HIMARS', 'no-fly zone']
    
    alt_spellings = {
        'kyiv': ['kyiv', 'kiev'],
        'zelenskyy': ['zelenskyy', 'zelensky', 'zelenskiy'],
        'denazification': ['denazification', 'denazify'],
        'odesa': ['odesa', 'odessa'],
    }

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

    for keyword in tqdm.tqdm(keywords):
        # check if keyword in title or text
        news_df = contains_keyword(news_df, ['title', 'text'], keyword, alt_spellings.get(keyword, keyword))
        related_videos_df = contains_keyword(related_videos_df, 'desc', keyword, alt_spellings.get(keyword, keyword))
        comments_df = contains_keyword(comments_df, 'text', keyword, alt_spellings.get(keyword, keyword))

    return news_df, related_videos_df, comments_df, keywords

def main():
    this_dir_path = os.path.dirname(os.path.abspath(__file__))
    data_dir_path = os.path.join(this_dir_path, "..", "data")

    news_df, related_videos_df, comments_df, keywords = get_keyword_dfs()

    stats = {}

    batch_size = 4
    for j in range(0, len(keywords), batch_size):
        # plot timeline of mentions
        nrows = min(batch_size, len(keywords) - j)
        fig, axes = plt.subplots(nrows=nrows, ncols=1, figsize=(20, 10))
        for i, keyword in enumerate(keywords[j:j+nrows]):
            if nrows == 1:
                ax = axes
            else:
                ax = axes[i]

            stats[keyword] = {}
            news_data = news_df[['publish_date', 'text', f'contains_{keyword}']].resample('SMS', on='publish_date').agg({'text': 'count', f'contains_{keyword}': 'sum'})
            news_data['percentage'] = 100 * news_data[f'contains_{keyword}'] / news_data['text']
            stats = add_stats(stats, keyword, news_data['percentage'], 'news')
            ln_news = ax.plot(news_data.index, news_data[f'contains_{keyword}'], label='News')

            related_video_data = related_videos_df[['createtime', 'desc', f'contains_{keyword}']].resample('SMS', on='createtime').agg({'desc': 'count', f'contains_{keyword}': 'sum'})
            related_video_data['percentage'] = 100 * related_video_data[f'contains_{keyword}'] / related_video_data['desc']
            stats = add_stats(stats, keyword, related_video_data['percentage'], 'video')
            ln_video = ax.plot(related_video_data.index, related_video_data[f'contains_{keyword}'], label='TikTok Videos')

            t_axis = ax.twinx()
            t_axis._get_lines.prop_cycler = ax._get_lines.prop_cycler
            comments_data = comments_df[['createtime', 'text', f'contains_{keyword}']].resample('SMS', on='createtime').agg({'text': 'count', f'contains_{keyword}': 'sum'})
            comments_data['percentage'] = 100 * comments_data[f'contains_{keyword}'] / comments_data['text']
            stats = add_stats(stats, keyword, comments_data['percentage'], 'comment')
            ln_comment = t_axis.plot(comments_data.index, comments_data[f'contains_{keyword}'], label='TikTok Comments')

            lns = ln_news + ln_video + ln_comment
            labs = [l.get_label() for l in lns]
            ax.legend(lns, labs, loc=0)
            ax.set_title(f'Timeline of mentions of {keyword} in News vs TikTok')
            ax.set_xlabel('Date')
            ax.set_ylabel('Number of news/videos mentioning')
            t_axis.set_ylabel('Number of comments mentioning')

        fig.tight_layout()
        fig.savefig(os.path.join(data_dir_path, 'outputs', f'{j}_google_news_timeline.png'))

    # with open(os.path.join(data_dir_path, 'keyword_stats.json'), 'w') as f:
        # json.dump(stats, f, indent=4)

    # get_examples(news_df, related_videos_df, comments_df, keywords, data_dir_path)

if __name__ == '__main__':
    main()