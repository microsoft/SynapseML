---
title: Estimators - Core
sidebar_label: Core
hide_title: true
---


import AutoML, {toc as AutoMLTOC} from './core/_AutoML.md';

<AutoML/>


import Featurize, {toc as FeaturizeTOC} from './core/_Featurize.md';

<Featurize/>


import IsolationForest, {toc as IsolationForestTOC} from './core/_IsolationForest.md';

<IsolationForest/>


import NN, {toc as NNTOC} from './core/_NN.md';

<NN/>


import Recommendation, {toc as RecommendationTOC} from './core/_Recommendation.md';

<Recommendation/>


import Stages, {toc as StagesTOC} from './core/_Stages.md';

<Stages/>

import Train, {toc as TrainTOC} from './core/_Train.md';

<Train/>

export const toc = [...AutoMLTOC, ...FeaturizeTOC, ...IsolationForestTOC,
...NNTOC, ...RecommendationTOC, ...StagesTOC, ...TrainTOC]
