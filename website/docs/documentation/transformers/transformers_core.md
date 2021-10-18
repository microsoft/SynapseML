---
title: Transformers - Core
sidebar_label: Core
hide_title: true
---


import Explainers, {toc as ExplainersTOC} from './core/_Explainers.md';

<Explainers/>


import Featurize, {toc as FeaturizeTOC} from './core/_Featurize.md';

<Featurize/>


import Image, {toc as ImageTOC} from './core/_Image.md';

<Image/>


import IO, {toc as IOTOC} from './core/_IO.md';

<IO/>


import SuperpixelTransformer, {toc as LIMETOC} from './core/_SuperpixelTransformer.md';

<SuperpixelTransformer/>


import Stages, {toc as StagesTOC} from './core/_Stages.md';

<Stages/>


import Train, {toc as TrainTOC} from './core/_Train.md';

<Train/>

export const toc = [...ExplainersTOC, ...FeaturizeTOC, ...ImageTOC,
...IOTOC, ...LIMETOC, ...StagesTOC, ...TrainTOC]