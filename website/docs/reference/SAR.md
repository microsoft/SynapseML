---
title: Smart Adaptive Recommendations (SAR) Algorithm
hide_title: true
sidebar_label: SAR Algorithm
---


# Smart Adaptive Recommendations (SAR) Algorithm

The following document is a subset of the implemented logic, the orginal can be found [here](https://github.com/Microsoft/Product-Recommendations/blob/master/doc/sar.md)

**SAR** is a fast scalable adaptive algorithm for personalized recommendations based on user transactions history and items description. It produces easily explainable / interpretable recommendations.

The overall architecture of SAR is shown in the following diagram:

![SAR Diagram](https://i.imgur.com/AMPShWl.jpg)

## Input

The input to SAR consists of:

-   transaction (usage) data
-   catalog data

**Transaction data**, also called **usage data**, contains information on interactions between users and items and has the following schema:

`<User Id>,<Item Id>,<Time>`

Each row represents a single interaction between a user and an item, which we call a transaction. Here is an example of usage data (displayed as a table for readability, but note that it must be provided in the comma-separated format above):

| User ID | Item ID | Time                |
| ------- | ------- | ------------------- |
| User 1  | Item 1  | 2015/06/20T10:00:00 |
| User 1  | Item 1  | 2015/06/28T11:00:00 |
| User 1  | Item 2  | 2015/08/28T11:01:00 |
| User 1  | Item 2  | 2015/08/28T12:00:01 |

Note that SAR does not require explicit user ratings, which are often noisy and unreliable, and is focused to work with implicit events (transactions), which are more telling of user intent.

## Collaborative Filtering

SAR is based on a simple yet powerful collaborative filtering approach. In the heart of the algorithm is computation of two matrices:

-   Item-to-Item similarity matrix
-   User-to-Item affinity matrix

**Item-to-Item similarity matrix** contains for each pair of items a numerical value of similarity between these two items. A simple measure of item similarity is _co-occurrence_, which is the number of times two items appeared in a same transaction. Let's look at the following example:

|            | Item 1 | Item 2 | Item 3 | Item 4 | Item 5 |
| ---------- | :----: | :----: | :----: | :----: | :----: |
| **Item 1** |    5   |    3   |    4   |    3   |    2   |
| **Item 2** |    3   |    4   |    3   |    2   |    1   |
| **Item 3** |    4   |    3   |    4   |    3   |    1   |
| **Item 4** |    3   |    2   |    3   |    4   |    2   |
| **Item 5** |    2   |    1   |    1   |    2   |    3   |

Here, for example, _cooccur(Item 1, Item 2) = 3_, meaning that items 1 and 2 were together in a transaction 3 times. Note that the Item-to-Item similarity matrix is symmetric. Diagonal elements, _occ(Item i)_, simply represent the number of occurrences of each item.
The advantage of co-occurrence is that it is very easy to update. However, it favors predictability, and the most popular items will be recommended most of the time. To alleviate that, two additional similarity measures are used: _lift_ and _Jaccard_. They can be thought of as normalized co-occurrences.

_Lift_ measures how much the co-occurrence of two items is higher than it would be by chance, i.e., what is the contribution of interaction of the two items. It is obtained as

_lift(Item i, Item j) = cooccur(Item i, Item j) / (occ(Item i) \* occ(Item j))_ .

Lift favors serendipity / discoverability. For example, items 2 and 5 have the same co-occurrence with item 4, but item 5 in general occurs less frequently than item 2 and will be favored by lift.

_Jaccard_ measure is defined as the number of transaction in which two items appear together divided by the number of transactions in which either of them appears:

_Jaccard(Item 1, Item 2) = cooccur(Item1, Item 2) / (occ(Item 1) + occ(Item 2) - cooccur(Item 1, Item 2))_ .

Jaccard measure is a tradeoff between co-occurrence and lift and is the default in SAR.

If one or both items are _cold items_, i.e., for which there are no transactions yet or the number of transactions is very low (below the SupportThreshold, which is configurable), their item-to-item similarity cannot be estimated from the transactions data and item features must be used. A linear learner is trained using warm items, where the features of the model are (partial) matches on corresponding features of a pair of items and the target is the computed similarity based on normalized co-occurrences of those items.
The model is then used to predict similarities between cold and cold/warm items.

**User-to-Item affinity matrix** contains for each user-item pair an affinity score of the user towards the item. Affinity score is computed as a weighted number of transactions in which the user and the item appear together, where newer transactions are weighted more than the older transactions. Also, weights are adjusted for the event type. For example, "Purchase" event may be valued 4 times more than the "Click" event. Therefore, affinity score takes into account frequency, recency and the event type of transactions in which the user appears with the item.

<!---
**User-to-Item affinity matrix** contains for each user-item pair an affinity score of the user towards the item. Affinity score takes into account frequency, recency and the event type of transactions in which the user appears with the item. Each transaction contributes to the affinity score with some weight. Newer transactions are weighted more than the older transactions. Also, weights are adjusted for the event type. For example, "Purchase" event may be valued 4 times more than the "Click" event.
--->

An example User-to-Item affinity matrix is shown in the following table:

|            | Item 1 | Item 2 | Item 3 | Item 4 | Item 5 |     |
| ---------- | :----: | :----: | :----: | :----: | :----: | --- |
| **User 1** |  5.00  |  3.00  |  2.50  |        |        |     |
| **User 2** |  2.00  |  2.50  |  5.00  |  2.00  |        |     |
| **User 3** |  2.50  |        |        |  4.00  |  4.50  |     |
| **User 4** |  5.00  |        |  3.00  |  4.50  |        |     |
| **User 5** |  4.00  |  3.00  |  2.00  |  4.00  |  3.50  |     |
| **User 6** |        |        |        |        |  2.00  |     |
| **User 7** |        |  1.00  |        |        |        |     |

Here, we can see that User 1 has the highest affinity to Item 1, followed by items 2 and 3, while this user did not have transaction involving items 4 and 5. On the other hand, User 2 has the highest affinity to Item 3, followed by Item 2 and then items 1 and 4.

## Making Recommendations

SAR can produce two types of recommendations:

-   **User recommendations**, which recommend items to individual users based on their transaction history, and

### User Recommendations

Personalized recommendations for a single user are obtained by multiplying the Item-to-Item similarity matrix with a user affinity vector. The user affinity vector is simply a transposed row of the affinity matrix corresponding to that user. For example, User 1 affinity vector is

|            | User 1 aff |
| ---------- | ---------- |
| **Item 1** | 5.00       |
| **Item 2** | 3.00       |
| **Item 3** | 2.50       |
| **Item 4** |            |
| **Item 5** |            |

By pre-multiplying this vector with the Item-to-Item similarity matrix, User 1 recommendation scores are obtained:

|            | User 1 rec |
| ---------- | :--------: |
| **Item 1** |     44     |
| **Item 2** |    34.5    |
| **Item 3** |     39     |
| **Item 4** |  **28.5**  |
| **Item 5** |  **15.5**  |

Note that the recommendation score of an item is purely based on its similarity to Item 5 in this case. Assuming that a same item is 
not recommended again, items 1 and 4 have the highest score and would be recommended before items 2 and 3. 

Now, if this user adds Item 2 to the shopping cart, affinity vector (assuming weight 2 for this transaction) will be

|            | New User aff |
| ---------- | :----------: |
| **Item 1** |       0      |
| **Item 2** |       0      |
| **Item 3** |       0      |
| **Item 4** |       0      |
| **Item 5** |       1      |

resulting in recommendation scores:

|            | New User rec |
| ---------- | :----------: |
| **Item 1** |     **2**    |
| **Item 2** |     **1**    |
| **Item 3** |     **1**    |
| **Item 4** |     **2**    |
| **Item 5** |       3      |

Note that the recommendation score of an item is purely based on its similarity to Item 5 in this case.
Assuming that a same item is not recommended again, items 1 and 4 have the highest score and would be recommended before items 2 and 3. 
Now, if this user adds Item 2 to the shopping cart, affinity vector (assuming weight 2 for this transaction) will be

|            | New User aff |
| ---------- | :----------: |
| **Item 1** |       0      |
| **Item 2** |       2      |
| **Item 3** |       0      |
| **Item 4** |       0      |
| **Item 5** |       1      |

resulting in recommendation scores:

|            | New User rec |
| ---------- | :----------: |
| **Item 1** |     **8**    |
| **Item 2** |       9      |
| **Item 3** |     **7**    |
| **Item 4** |     **6**    |
| **Item 5** |       5      |

Now, Item 1 has the highest score, followed by Item 3 and then Item 4. Note that the advantage of SAR is that the updated scores are obtained without any retraining (i.e., without having to re-compute Item-to-Item similarities).

#### Interpretability

SAR provides explainable recommendations. For example, let's look at the reasoning behind the recommendation score for User 1 and  Item 4:

_rec(User 1, Item 4)_  
\*= **sim(Item 4, Item 1) \* aff(User 1, Item 1)\***  
_+ sim(Item 4, Item 2) \* aff(User 1, Item 2)_  
_+ sim(Item 4, Item 3) \* aff(User 1, Item 3)_  
_+ sim(Item 4, Item 4) \* aff(User 1, Item 4)_  
_+ sim(Item 4, Item 5) \* aff(User 1, Item 5)_  
_= **3 \* 5** + 2 \* 3 + 3 \* 2.5 + 4 \* 0 + 2 \* 0_  
\*= **15** + 6 + 7.5 + 0 + 0 = **28.5\***

Clearly, the first term (highlighted) has the highest contribution to the score and we can say that "Item 4 is recommended to User 1 because it is very similar to Item 1, to which User 1 has high affinity". A message like this can be displayed automatically for each recommendation.
