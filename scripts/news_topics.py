import json
import os

from bertopic import BERTopic
from bertopic.backend._utils import select_backend
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer


def main():
    this_dir_path = os.path.dirname(os.path.abspath(__file__))
    data_dir_path = os.path.join(this_dir_path, '..', 'data')

    df_path = os.path.join(data_dir_path, 'all_google_news.csv')
    news_df = pd.read_csv(df_path)

    news_df = news_df.dropna(subset=['title', 'text', 'publish_date'])

    news_df['all_text'] = news_df['title'] + ' ' + news_df['text']

    docs = list(news_df['all_text'].values)
    timestamps = list(news_df['publish_date'].values)

    # Train the model on the corpus.
    pretrained_model = 'all-mpnet-base-v2'

    num_topics = 40
    vectorizer_model = CountVectorizer(stop_words="english")
    topic_model = BERTopic(embedding_model=pretrained_model, nr_topics=num_topics, vectorizer_model=vectorizer_model)

    # get embeddings so we can cache
    embeddings_cache_path = os.path.join(data_dir_path, 'cache', 'all_google_news_mpnet_embeddings.npy')
    if os.path.exists(embeddings_cache_path):
        with open(embeddings_cache_path, 'rb') as f:
            embeddings = np.load(f)
    else:
        topic_model.embedding_model = select_backend(pretrained_model,
                                        language=topic_model.language)
        topic_model.embedding_model.embedding_model.max_seq_length = 511
        embeddings = topic_model._extract_embeddings(docs,
                                                    method="document",
                                                    verbose=topic_model.verbose)

        with open(embeddings_cache_path, 'wb') as f:
            np.save(f, embeddings)

    topics, probs = topic_model.fit_transform(docs, embeddings)

    this_run_name = f'google_news_{num_topics}_mpnet_base'
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

    topics_over_time = topic_model.topics_over_time(docs, timestamps)
    topics_over_time.to_csv(os.path.join(run_dir_path, 'topics_over_time.csv'))

    freq_df = topic_model.get_topic_freq()
    freq_df.to_csv(os.path.join(run_dir_path, 'topic_freqs.csv'))

    with open(os.path.join(run_dir_path, 'topic_labels.json'), 'w') as f:
        json.dump(topic_model.topic_labels_, f)


if __name__ == '__main__':
    main()