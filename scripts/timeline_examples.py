import datetime
import json
import os

import pandas as pd

import comparison_timelines

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

def main():
    this_dir_path = os.path.dirname(os.path.abspath(__file__))
    data_dir_path = os.path.join(this_dir_path, '..', 'data')
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

    news_df, related_videos_df, comments_df, keywords = comparison_timelines.get_keyword_dfs()

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

if __name__ == '__main__':
    main()