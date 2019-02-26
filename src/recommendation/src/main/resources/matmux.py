import numpy as np
import pandas as pd
from sys import argv
import os

from scipy import sparse


def recommend_k_items(top_k=10, sort_top_k=False):
    """Recommend top K items for all users which are in the test set

    Args:
        test (pd.DataFrame): user to test
        top_k (int): number of top items to recommend
        sort_top_k (bool): flag to sort top k results
    Returns:
        pd.DataFrame: top k recommendation items for each user
    """

    # get user / item indices from test set
    X = pd.read_parquet('./items-parquet', engine='pyarrow')
    Y = pd.read_parquet('./users-parquet', engine='pyarrow')

    Xpp = pd.DataFrame(index=range(0, int(X.itemID.max()) + 1)).join(
        pd.DataFrame(np.stack(X.itemAffinities), X.itemID)).fillna(0)

    Ypp = pd.DataFrame(index=range(0, int(Y.customerID.max()) + 1)).join(
        pd.DataFrame(np.stack(Y.flatList), Y.customerID)).fillna(0)

    # extract only the scores for the test users

    user = Xpp
    items = Ypp
    test_scores = Ypp.dot(Xpp)
    # test_scores = np.matmul(Ypp, Xpp)

    # ensure we're working with a dense matrix
    if isinstance(test_scores, sparse.spmatrix):
        test_scores = test_scores.todense()

    # get top K items and scores
    # this determines the un-ordered top-k item indices for each user
    top_items = np.argpartition(test_scores, -top_k, axis=1)[:, -top_k:]
    top_scores = test_scores[np.arange(test_scores.shape[0])[:, None], top_items]

    if sort_top_k:
        sort_ind = np.argsort(-top_scores)
        top_items = top_items[np.arange(top_items.shape[0])[:, None], sort_ind]
        top_scores = top_scores[np.arange(top_scores.shape[0])[:, None], sort_ind]

    df = pd.DataFrame(
        {
            'customerId': np.repeat(
                user['customerId'].drop_duplicates().values, top_k
            ),
            'productId': [
                np.array(top_items).flatten()
            ],
            'rating': np.array(top_scores).flatten(),
        }
    )

    # ensure datatypes are correct
    df = df.astype(
        dtype={
            'customerId': str,
            'productId': str,
            'rating': test_scores.dtype,
        }
    )

    # drop seen items
    return df.replace(-np.inf, np.nan).dropna()


def main(args):
    user_parquet = pd.read_parquet('./users-parquet', engine='pyarrow')
    item_parquet = pd.read_parquet('./items-parquet', engine='pyarrow')

    user_df = pd.DataFrame(index=range(0, int(user_parquet.customerID.max()) + 1)).join(
        pd.DataFrame(np.stack(user_parquet.flatList), index=user_parquet.customerID).fillna(0))
    item_df = pd.DataFrame(np.stack(item_parquet.itemAffinities).T, columns=item_parquet.itemID).fillna(0)
    df = user_df.dot(item_df).T

    def write_to_parquet(df, filename):
        if not os.path.exists("sampleout"):
            os.makedirs("sampleout")

        df.columns = ['id', 'ratings']
        df.to_parquet(filename, engine='pyarrow')

    k = int(args[1])
    output_list = []
    part = 0

    for i, r in enumerate(df):
        top_k_df = df[r].sort_values(ascending=False)[:k].index.tolist()
        temp_list = []
        for index in top_k_df:
            temp_list.append([index, df[r][index]])
        output_list.append((i, temp_list))
        if (i + 1) % 25000 == 0:
            write_to_parquet(pd.DataFrame(output_list),
                             "sampleout/parquet {}".format(part))
            output_list = []
            part += 1

    if output_list:
        write_to_parquet(pd.DataFrame(output_list),
                         "sampleout/parquet {}".format(part))


if __name__ == '__main__':
    main(argv)
    # recommend_k_items()
