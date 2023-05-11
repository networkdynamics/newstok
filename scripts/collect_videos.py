import os

import pandas as pd

from pytok import utils

def main():
    this_dir_path = os.path.dirname(os.path.abspath(__file__))
    data_dir_path = os.path.join(this_dir_path, '..', 'data')
    all_video_path = os.path.join(data_dir_path, 'all_videos.csv')
    all_video_df = utils.get_video_df(all_video_path)

    hashtags_path = os.path.join(data_dir_path, 'hashtags_apr_23')
    searches_path = os.path.join(data_dir_path, 'searches_apr_23')
    new_video_file_paths = [os.path.join(hashtags_path, file_path) for file_path in os.listdir(hashtags_path)] \
        + [os.path.join(searches_path, file_path) for file_path in os.listdir(searches_path)]
    new_video_file_paths.reverse()

    apr_video_path = os.path.join(data_dir_path, 'apr_videos.csv')
    apr_video_df = utils.get_video_df(apr_video_path, file_paths=new_video_file_paths)

    apr_all_video_df = pd.concat([all_video_df, apr_video_df])
    apr_all_video_df = apr_all_video_df.drop_duplicates(subset=['video_id'])
    old_new_related_video_path = os.path.join(data_dir_path, 'apr_all_videos.csv')
    apr_all_video_df.to_csv(old_new_related_video_path, index=False)

if __name__ == '__main__':
    main()