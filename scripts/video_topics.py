import json
import os
import re

from bertopic import BERTopic
from bertopic.backend._utils import select_backend
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from bertopic.vectorizers import ClassTfidfTransformer

from pytok import utils

HANDLE_RE = re.compile(r"@\w+")

def process_text(t):
    t = HANDLE_RE.sub("@USER", t)
    return t

def main():
    this_dir_path = os.path.dirname(os.path.abspath(__file__))
    data_dir_path = os.path.join(this_dir_path, '..', 'data')

    df_path = os.path.join(data_dir_path, 'related_videos.csv')
    video_df = utils.get_video_df(df_path)
    apr_df_path = os.path.join(data_dir_path, 'related_apr_videos.csv')
    apr_video_df = utils.get_video_df(apr_df_path)
    video_df = pd.concat([video_df, apr_video_df])
    video_df = video_df.drop_duplicates(subset=['video_id'])

    # process usernames
    video_df['desc_processed'] = video_df['desc'].apply(process_text)

    docs = list(video_df['desc_processed'].values)
    timestamps = list(video_df['createtime'].values)

    # Train the model on the corpus.
    pretrained_model = 'jhu-clsp/bernice'

    num_topics = 40
    vectorizer_model = CountVectorizer(stop_words="english")
    ctfidf_model = ClassTfidfTransformer(reduce_frequent_words=True)
    topic_model = BERTopic(embedding_model=pretrained_model, nr_topics=num_topics, vectorizer_model=vectorizer_model, ctfidf_model=ctfidf_model)

    # get embeddings so we can cache
    embeddings_cache_path = os.path.join(data_dir_path, 'cache', 'all_related_videos_bernice_embeddings.npy')
    if os.path.exists(embeddings_cache_path):
        with open(embeddings_cache_path, 'rb') as f:
            embeddings = np.load(f)
    else:
        topic_model.embedding_model = select_backend(pretrained_model,
                                        language=topic_model.language)
        topic_model.embedding_model.embedding_model.max_seq_length = 128
        embeddings = topic_model._extract_embeddings(docs,
                                                    method="document",
                                                    verbose=topic_model.verbose)

        with open(embeddings_cache_path, 'wb') as f:
            np.save(f, embeddings)

    topics, probs = topic_model.fit_transform(docs, embeddings)

    this_run_name = f'all_related_videos_{num_topics}_bernice_base'
    run_dir_path = os.path.join(data_dir_path, 'outputs', this_run_name)
    if not os.path.exists(run_dir_path):
        os.mkdir(run_dir_path)

    with open(os.path.join(run_dir_path, 'topics.json'), 'w') as f:
        json.dump([int(topic) for topic in topics], f)

    with open(os.path.join(run_dir_path, 'probs.npy'), 'wb') as f:
        np.save(f, probs)

    topic_df = topic_model.get_topic_info()
    topic_df.to_csv(os.path.join(run_dir_path, 'topic_info.csv'))
    
    hierarchical_topics = topic_model.hierarchical_topics(docs)
    hierarchical_topics.to_csv(os.path.join(run_dir_path, 'hierarchical_topics.csv'))

    tree = topic_model.get_topic_tree(hierarchical_topics)
    with open(os.path.join(run_dir_path, f'{num_topics}_cluster_tree.txt'), 'w') as f:
        f.write(tree)

    day_timestamps = [pd.Timestamp(t).date() for t in timestamps]
    topics_over_time = topic_model.topics_over_time(docs, day_timestamps)
    topics_over_time.to_csv(os.path.join(run_dir_path, 'topics_over_time.csv'))

    freq_df = topic_model.get_topic_freq()
    freq_df.to_csv(os.path.join(run_dir_path, 'topic_freqs.csv'))

    with open(os.path.join(run_dir_path, 'topic_labels.json'), 'w') as f:
        json.dump(topic_model.topic_labels_, f)


if __name__ == '__main__':
    main()