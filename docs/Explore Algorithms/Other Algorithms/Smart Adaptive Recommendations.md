---
title: Smart Adaptive Recommendations (SAR) Algorithm
hide_title: true
sidebar_label: SAR Algorithm
---


# Smart Adaptive Recommendations (SAR) Algorithm

This page describes the collaborative-filtering subset of SAR implemented in SynapseML. The [original algorithm description](https://github.com/Microsoft/Product-Recommendations/blob/master/doc/sar.md) also covers catalog-based recommendations, which are not implemented by SynapseML's SAR estimator.

**SAR** produces personalized recommendations from user-item interaction history. SynapseML computes item similarities and user affinities from those interactions; it does not use item descriptions or other catalog features.

The following diagram shows the original SAR architecture. Its item-catalog and cold-item learner paths are not implemented in SynapseML; only the interaction-based collaborative-filtering path applies here:

![SAR Diagram](https://i.imgur.com/AMPShWl.jpg)

## Input

Pass one Spark DataFrame of user-item interactions to `SAR.fit()`. Configure the user and item identifier columns with `userCol` and `itemCol`; both string and numeric identifiers are supported. There is no separate catalog DataFrame argument or parameter for item descriptions or other item features.

Each row represents an interaction, also called a transaction. Columns matching the configured `ratingCol` and `timeCol` names (defaults: `rating` and `time`) are optional. SAR detects these columns by exact, case-sensitive name, even when Spark's column resolution is case-insensitive. If a configured name is absent or misspelled, SAR silently omits that column's rating weights or time decay. A matching rating column must be numeric; its values scale the interaction's contribution to user-item affinity. Without a matching rating column, each interaction has weight one before any time decay.

If a time column is present, its values are converted to strings and parsed using `activityTimeFormat` (default: `yyyy/MM/dd'T'h:mm:ss`). For 24-hour times such as those below, set `activityTimeFormat` to `yyyy/MM/dd'T'HH:mm:ss`. The default uses the 12-hour `h` field without an AM/PM marker, so it reads `12:xx` as `00:xx`. Choose a pattern matching the time column's string representation; for a Spark timestamp column, use a pattern such as `yyyy-MM-dd HH:mm:ss`.

Time decay is relative to `startTime`, parsed with the separate `startTimeFormat` (default: `EEE MMM dd HH:mm:ss Z yyyy`). When specifying a reference time, make sure `startTime` matches `startTimeFormat`; leave both unset to use the current time. The `timeDecayCoeff` parameter specifies the half-life in days (default: 30). For historical or offline data, set `startTime` close to the most recent interaction time. Affinities are stored as 32-bit floats, so events many half-lives older than the reference time can lose precision or contribute zero weight.

The following table illustrates interaction data with timestamps. CSV is not required; SAR consumes the DataFrame, regardless of how it was loaded:

| User ID | Item ID | time                |
| ------- | ------- | ------------------- |
| User 1  | Item 1  | 2015/06/20T10:00:00 |
| User 1  | Item 1  | 2015/06/28T11:00:00 |
| User 1  | Item 2  | 2015/08/28T11:01:00 |
| User 1  | Item 2  | 2015/08/28T12:00:01 |

Note that SAR doesn't require explicit user ratings, which are often noisy and unreliable. It also focuses on implicit events (transactions), which are more telling of user intent.

## Collaborative Filtering

SAR is based on a simple yet powerful collaborative filtering approach. In the heart of the algorithm is computation of two matrices:

-   Item-to-Item similarity matrix
-   User-to-Item affinity matrix

**Item-to-Item similarity matrix** contains for each pair of items a numerical value of similarity between these two items. In SynapseML, _co-occurrence_ counts the distinct users who interacted with both items; repeated interactions by the same user do not increase this count. The following co-occurrence matrix uses the seven users in the affinity table below, where each nonblank cell indicates that the user interacted with that item:

|            | Item 1 | Item 2 | Item 3 | Item 4 | Item 5 |
| ---------- | :----: | :----: | :----: | :----: | :----: |
| **Item 1** |    5   |    3   |    4   |    4   |    2   |
| **Item 2** |    3   |    4   |    3   |    2   |    1   |
| **Item 3** |    4   |    3   |    4   |    3   |    1   |
| **Item 4** |    4   |    2   |    3   |    4   |    2   |
| **Item 5** |    2   |    1   |    1   |    2   |    3   |

Here, for example, _cooccur(Item 1, Item 2) = 3_, meaning that three distinct users interacted with both items 1 and 2. The Item-to-Item similarity matrix is symmetric. Diagonal elements, _occ(Item i)_, count the distinct users who interacted with each item.
The advantage of co-occurrence is that it's easy to update. However, it favors predictability, and the most popular items will be recommended most of the time. To alleviate that, two more similarity measures are used: _lift_ and _Jaccard_. They can be thought of as normalized co-occurrences.

_Lift_ measures how much the co-occurrence of two items is higher than it would be by chance, that is, what is the contribution of interaction of the two items. It is obtained as

_lift(Item i, Item j) = cooccur(Item i, Item j) / (occ(Item i) \* occ(Item j))_ .

Lift favors serendipity / discoverability. For example, items 2 and 5 have the same co-occurrence with item 4, but item 5 in general occurs less frequently than item 2 and will be favored by lift.

_Jaccard_ similarity divides the number of distinct users who interacted with both items by the number who interacted with either item:

_Jaccard(Item 1, Item 2) = cooccur(Item1, Item 2) / (occ(Item 1) + occ(Item 2) - cooccur(Item 1, Item 2))_ .

Jaccard measure is a tradeoff between co-occurrence and lift and is the default in SAR.

Set `similarityFunction` to `jaccard` (default), `lift`, or `cooc` for raw co-occurrence. Values are case-sensitive; the implementation also treats any other string as raw co-occurrence.

SynapseML sets an item pair's similarity to zero when its co-occurrence count is below `supportThreshold` (default: 4 distinct users). This threshold also applies to self-pairs. It does not select a catalog-based fallback: the content-based cold-start learner described in the original SAR algorithm is not implemented here.

**User-to-Item affinity matrix** contains for each user-item pair an affinity score of the user towards the item. Affinity score is computed as a weighted number of transactions in which the user and the item appear together, and, when a time column is present, newer transactions are weighted more than older ones. Event-type weighting can be represented by supplying numeric values in the configured `ratingCol`; SAR has no separate event-type parameter. For example, a caller can assign a purchase a rating four times that of a click. Affinity therefore reflects interaction frequency, optional time decay, and any supplied rating weights.

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

Here, we can see that User 1 has the highest affinity to Item 1, followed by items 2 and 3. This user didn't have a transaction that included items 4 and 5. On the other hand, User 2 has the highest affinity to Item 3, followed by Item 2 and then items 1 and 4.

## Making Recommendations

The following examples illustrate item recommendations for individual users based on their interaction history. They use `similarityFunction="cooc"` and `supportThreshold=1`, not the defaults `similarityFunction="jaccard"` and `supportThreshold=4`.

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
| **Item 4** |  **33.5**  |
| **Item 5** |  **15.5**  |

If the caller filters out items that User 1 has already interacted with (items 1, 2, and 3), Item 4 is recommended before Item 5. SAR does not automatically exclude previously seen items.

`recommendForAllUsers(numItems)` and `recommendForUserSubset(dataset, numItems)` return only the top `numItems` items, including seen items, before caller-side filtering. Request extra candidates, filter out seen items, then keep the desired number; otherwise the result can be short or empty. In this example, request all five candidates to retain both unseen items for User 1, since the top three are already seen.

For another example, consider a user with a single interaction of weight 1 with Item 5:

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

The recommendation score of an item is purely based on its similarity to Item 5 in this case.
Assuming that a same item isn't recommended again, items 1 and 4 have the highest score and would be recommended before items 2 and 3.
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

After excluding the already-interacted items 2 and 5, Item 1 has the highest score, followed by Item 3 and then Item 4. Mathematically, these updated scores reuse the same Item-to-Item similarity matrix. This illustrates the scoring calculation, not an incremental-update API: a fitted SynapseML `SARModel` does not automatically incorporate new interactions.

#### Interpretability

SAR provides explainable recommendations. For example, let's look at the reasoning behind the recommendation score for User 1 and  Item 4:

_rec(User 1, Item 4)_
\*= **sim(Item 4, Item 1) \* aff(User 1, Item 1)\***
_+ sim(Item 4, Item 2) \* aff(User 1, Item 2)_
_+ sim(Item 4, Item 3) \* aff(User 1, Item 3)_
_+ sim(Item 4, Item 4) \* aff(User 1, Item 4)_
_+ sim(Item 4, Item 5) \* aff(User 1, Item 5)_
_= **4 \* 5** + 2 \* 3 + 3 \* 2.5 + 4 \* 0 + 2 \* 0_
\*= **20** + 6 + 7.5 + 0 + 0 = **33.5\***

Clearly, the first term (highlighted) has the highest contribution to the score. We can say that "The algorithm recommends Item 4 to User 1 because it's similar to Item 1, to which User 1 has high affinity". A message like this can be displayed automatically for each recommendation.
