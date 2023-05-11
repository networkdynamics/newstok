import datetime
import json
import os

import matplotlib.pyplot as plt
import pandas as pd

def main():
    this_dir_path = os.path.dirname(os.path.abspath(__file__))
    all_outputs_dir_path = os.path.join(this_dir_path, '..', 'data', 'outputs')

    clusterings = {
        'google_news': os.path.join(all_outputs_dir_path, 'google_news_40_mpnet_base'),
        'related_videos': os.path.join(all_outputs_dir_path, 'related_videos_40_bernice_base'),
    }

    topic_months = {
        'related_videos': {
            'putin, usa, nato': [1, 2, 5, 7],
            'news, russia, russian': [1, 4, 6],
            'stopthewar, people, standwithukraine': [2],
            'president, zelenskyy': [3, 6],
            'rusia, ukraina, balas, nato': [2, 3, 5],
            'alaska, alaskan': [4, 6],
            'geopolitics, russia': [1, 2],
        },
        'google_news': {
            'oil, gas': [5, 7, 10],
            'nuclear, weapons': [1, 5, 9],
            'biden, trump': [2, 4, 5, 7, 10],
            'zelenskyy, ukraine': [3, 4, 9],
            'nato, ukraine, russia, finland': [2, 3, 5, 11],
            'china, xi': [2, 4, 9],
            'idaho, police': [11],
            'icc, crimes': [3, 6],
            'athletes, olympic': [4],
            'mcdonald, ': [6, 8]
        }
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
        freq_df = freq_df.loc[freq_df.Topic != -1, :]

        with open(os.path.join(outputs_dir_path, 'topic_labels.json'), 'r') as f:
            topic_labels = json.load(f)

        topic_names = {int(key): value[value.index('_')+1:].replace('_', ', ')
                        for key, value in topic_labels.items()}
        topics_over_time["Name"] = topics_over_time['Topic'].map(topic_names)

        for topic, months in topic_months[clustering].items():
            periods = []
            if len(months) == 1:
                periods.append((months[0], months[0]+1))
            else:
                # add continuous month periods
                i = 0
                while i < len(months):
                    start = months[i]
                    end = months[i]
                    while i+1 < len(months) and months[i+1] == end+1:
                        end += 1
                        i += 1
                    periods.append((start, end+1))
                    i += 1

            figwidth = 8 + 2 * len(periods)
            fig, axes = plt.subplots(nrows=1, ncols=len(periods), figsize=(12, 5))

            for i, (start, end) in enumerate(periods):
                ax = axes[i] if len(periods) > 1 else axes

                start_date = datetime.datetime(2022, start, 1)
                end_date = datetime.datetime(2022, end, 1)
                topics_over_period = topics_over_time[(topics_over_time['Timestamp'] >= start_date) & (topics_over_time['Timestamp'] <= end_date)]

                if topics_over_period.empty:
                    continue

                # Prepare data
                data = topics_over_period.loc[topics_over_period['Name'].str.contains(topic, regex=False), :].sort_values(["Topic", "Timestamp"])

                normalize_frequency = False
                # Add traces
                data = data.set_index('Timestamp')
                data = data.pivot_table(index=['Timestamp'], columns=['Name'], values=['Frequency'], fill_value=0).droplevel(0, axis=1)
                data = data.resample('W').sum()

                all_count = topics_over_period[['Timestamp', 'Frequency']].resample('W', on='Timestamp').sum()
                data = data.div(all_count['Frequency'], axis=0)

                for column in data.columns:
                    ax.plot(data.index, data[column], label=column)

                ax.set_xlabel('Time')
                ax.tick_params(axis='x', labelrotation=45)
                ax.set_ylabel('Share')
                
            if len(periods) > 1:
                axes[0].legend()
            else:
                axes.legend()

            fig.tight_layout()
            fig.savefig(os.path.join(all_outputs_dir_path, 'zoomed', f'{clustering}_{topic.split(", ")[0]}_topic_over_period.png'))


if __name__ == '__main__':
    main()