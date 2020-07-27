# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from typing import List, Set, Optional, Tuple
import pandas as pd
import random

from mmlspark.cyber.anomaly.collaborative_filtering import AccessAnomalyConfig


class DataFactory:
    def __init__(
            self,
            num_hr_users: int = 7,
            num_hr_resources: int = 30,
            num_fin_users: int = 5,
            num_fin_resources: int = 25,
            num_eng_users: int = 10,
            num_eng_resources: int = 50,
            single_component: bool = True):

        self.hr_users = ['hr_user_' + str(i) for i in range(num_hr_users)]
        self.hr_resources = ['hr_res_' + str(i) for i in range(num_hr_resources)]

        self.fin_users = ['fin_user_' + str(i) for i in range(num_fin_users)]
        self.fin_resources = ['fin_res_' + str(i) for i in range(num_fin_resources)]

        self.eng_users = ['eng_user_' + str(i) for i in range(num_eng_users)]
        self.eng_resources = ['eng_res_' + str(i) for i in range(num_eng_resources)]

        if single_component:
            self.join_resources = ['ffa']
        else:
            self.join_resources = []

        self.rand = random.Random(42)

    def to_pdf(self, users: List[str], resources: List[str], likelihoods: List[float]) -> pd.DataFrame:
        return pd.DataFrame(
            data={
                AccessAnomalyConfig.default_user_col: [str(u) for u in users],
                AccessAnomalyConfig.default_res_col: [str(r) for r in resources],
                AccessAnomalyConfig.default_likelihood_col: [float(s) for s in likelihoods]
            }
        )

    def tups2pdf(self, tup_arr: List[Tuple[str, str, float]]) -> pd.DataFrame:
        user_lst = [tup[0] for tup in tup_arr]
        res_lst = [tup[1] for tup in tup_arr]
        likelihood_lst = [tup[2] for tup in tup_arr]

        return self.to_pdf(user_lst, res_lst, likelihood_lst)

    def edges_between(
            self,
            users: List[str],
            resources: List[str],
            ratio: float,
            full_node_coverage: bool,
            not_set: Optional[Set[Tuple[str, str]]] = None) -> List[Tuple[str, str, float]]:

        import itertools

        if len(users) == 0 or len(resources) == 0:
            return []

        required_edge_cnt = len(users) * len(resources) * ratio

        tups = []
        seen = set([])
        seen_users = set([])
        seen_resources = set([])

        # optimization for creating dense access patterns (fill all the possible pairs in advance)
        cart = list(itertools.product(range(len(users)), range(len(resources)))) if ratio >= 0.5 else None

        while len(tups) < required_edge_cnt \
                or (full_node_coverage and (len(seen_users) < len(users)) or (len(seen_resources) < len(resources))):

            if cart is not None:
                assert len(cart) > 0, cart
                ii = self.rand.randint(0, len(cart) - 1)
                ui, ri = cart[ii]
                cart[ii] = cart[-1]
                cart.pop()
            else:
                assert len(users) > 0, users
                assert len(resources) > 0, resources
                ui = self.rand.randint(0, len(users) - 1)
                ri = self.rand.randint(0, len(resources) - 1)

            user = users[ui]
            res = resources[ri]

            if ((ui, ri) in seen) or ((not_set is not None) and ((user, res) in not_set)):
                continue

            seen.add((ui, ri))
            seen_users.add(ui)
            seen_resources.add(ri)

            assert users[ui] is not None
            assert resources[ri] is not None

            score = self.rand.randint(500, 1000)
            tups.append((user, res, score))

        return tups

    def create_clustered_training_data(self, ratio: float = 0.25):
        return self.tups2pdf(
            self.edges_between(self.hr_users, self.join_resources, 1.0, True) +
            self.edges_between(self.fin_users, self.join_resources, 1.0, True) +
            self.edges_between(self.eng_users, self.join_resources, 1.0, True) +

            self.edges_between(self.hr_users, self.hr_resources, ratio, True) +
            self.edges_between(self.fin_users, self.fin_resources, ratio, True) +
            self.edges_between(self.eng_users, self.eng_resources, ratio, True)
        )

    def create_clustered_intra_test_data(self, train: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        not_set = set(
            [(row[AccessAnomalyConfig.default_user_col],
              row[AccessAnomalyConfig.default_res_col]) for _, row in train.iterrows()]
        ) if train is not None else None

        return self.tups2pdf(
            self.edges_between(self.hr_users, self.join_resources, 1.0, True) +
            self.edges_between(self.fin_users, self.join_resources, 1.0, True) +
            self.edges_between(self.eng_users, self.join_resources, 1.0, True) +

            self.edges_between(self.hr_users, self.hr_resources, 0.025, False, not_set) +
            self.edges_between(self.fin_users, self.fin_resources, 0.05, False, not_set) +
            self.edges_between(self.eng_users, self.eng_resources, 0.035, False, not_set)
        )

    def create_clustered_inter_test_data(self) -> pd.DataFrame:
        return self.tups2pdf(
            self.edges_between(self.hr_users, self.join_resources, 1.0, True) +
            self.edges_between(self.fin_users, self.join_resources, 1.0, True) +
            self.edges_between(self.eng_users, self.join_resources, 1.0, True) +

            self.edges_between(self.hr_users, self.fin_resources, 0.025, False) +
            self.edges_between(self.hr_users, self.eng_resources, 0.025, False) +
            self.edges_between(self.fin_users, self.hr_resources, 0.05, False) +
            self.edges_between(self.fin_users, self.eng_resources, 0.05, False) +
            self.edges_between(self.eng_users, self.fin_resources, 0.035, False) +
            self.edges_between(self.eng_users, self.hr_resources, 0.035, False)
        )

    def create_fixed_training_data(self) -> pd.DataFrame:
        users = [
            6, 2, 8, 6, 7, 8, 2, 8, 10, 3, 11, 6, 7, 3, 2, 10, 11, 1, 8, 4, 9, 3, 5, 6, 7
        ]

        resources = [
            2, 8, 2, 8, 2, 6, 4, 1, 5, 5, 6, 1, 6, 7, 6, 3, 3, 4, 4, 8, 2, 1, 7, 7, 5
        ]

        likelihoods = [
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
            46.77678298950195,
            35.94552993774414,
            36.470367431640625,
            33.59884262084961,
            10.0,
            43.84599304199219,
            14.908903121948242,
            10.0,
            19.817806243896484,
            21.398120880126953,
            14.908903121948242
        ]

        return self.to_pdf(users, resources, likelihoods)
