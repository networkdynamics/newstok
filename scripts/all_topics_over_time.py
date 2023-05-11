from datetime import datetime
import json
import os

import matplotlib.pyplot as plt
import pandas as pd

def main():
    this_dir_path = os.path.dirname(os.path.abspath(__file__))
    all_outputs_dir_path = os.path.join(this_dir_path, '..', 'data', 'outputs')

    clusterings = {
        'google_news': os.path.join(all_outputs_dir_path, 'google_news_40_mpnet_base'),
        'related_videos_bernice': os.path.join(all_outputs_dir_path, 'all_related_videos_40_bernice_base'),
        'related_comments_bernice': os.path.join(all_outputs_dir_path, 'related_comments_40_bernice_base'),
        'related_videos_bertweet': os.path.join(all_outputs_dir_path, 'all_related_videos_40_bertweet_base'),
        'related_comments_bertweet': os.path.join(all_outputs_dir_path, 'related_comments_40_bertweet_base'),
    }

    for clustering in clusterings:
        outputs_dir_path = clusterings[clustering]
        topics = None
        top_n_topics = 40
        batch_size = 5
        custom_labels = False

        topics_over_time = pd.read_csv(os.path.join(outputs_dir_path, 'topics_over_time.csv'))
        topics_over_time['Timestamp'] = pd.to_datetime(topics_over_time['Timestamp'])

        freq_df = pd.read_csv(os.path.join(outputs_dir_path, 'topic_freqs.csv'))

        with open(os.path.join(outputs_dir_path, 'topic_labels.json'), 'r') as f:
            topic_labels = json.load(f)

        start_date = datetime(2022, 1, 1)
        end_date = datetime(2023, 4, 1)
        topics_over_time = topics_over_time[topics_over_time['Timestamp'] >= start_date]
        topics_over_time = topics_over_time[topics_over_time['Timestamp'] <= end_date]

        topic_names = {int(key): value[value.index('_')+1:].replace('_', ', ')
                        for key, value in topic_labels.items()}
        topics_over_time["Name"] = topics_over_time['Topic'].map(topic_names)

        freq_df = freq_df.loc[freq_df.Topic != -1, :]

        fig, axes = plt.subplots(nrows=top_n_topics // batch_size, ncols=1, figsize=(15, 25))

        for i in range(top_n_topics // batch_size):
            ax = axes[i]

            if topics is not None:
                selected_topics = list(topics)
            elif top_n_topics is not None:
                selected_topics = sorted(freq_df['Topic'].to_list()[i*batch_size:(i+1)*batch_size])
            else:
                selected_topics = sorted(freq_df['Topic'].to_list())

            # Prepare data
            data = topics_over_time.loc[topics_over_time['Topic'].isin(selected_topics), :].sort_values(["Topic", "Timestamp"])

            normalize_frequency = False
            # Add traces
            data = data.set_index('Timestamp')
            data = data.pivot_table(index=['Timestamp'], columns=['Name'], values=['Frequency'], fill_value=0).droplevel(0, axis=1)
            data = data.resample('SMS').sum()

            all_count = topics_over_time[['Timestamp', 'Frequency']].resample('SMS', on='Timestamp').sum()
            data = data.div(all_count['Frequency'], axis=0)

            for column in data.columns:
                ax.plot(data.index, data[column], label=column)

            ax.set_xlabel('Time')
            ax.set_ylabel('Share')
            ax.legend()

        fig.tight_layout()
        fig.savefig(os.path.join(all_outputs_dir_path, f'{clustering}_topics_over_time.png'))


if __name__ == '__main__':
    main()