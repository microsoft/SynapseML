---
title: Transformers - Cognitive
sidebar_label: Cognitive
hide_title: true
---


import TextAnalytics, {toc as TextAnalyticsTOC} from './cognitive/_TextAnalytics.md';

<TextAnalytics/>


import Translator, {toc as TranslatorTOC} from './cognitive/_Translator.md';

<Translator/>


import ComputerVision, {toc as ComputerVisionTOC} from './cognitive/_ComputerVision.md';

<ComputerVision/>


import FormRecognizer, {toc as FormRecognizerTOC} from './cognitive/_FormRecognizer.md';

<FormRecognizer/>


import AnomalyDetection, {toc as AnomalyDetectionTOC} from './cognitive/_AnomalyDetection.md';

<AnomalyDetection/>


import Face, {toc as FaceTOC} from './cognitive/_Face.md';

<Face/>


import SpeechToText, {toc as SpeechToTextTOC} from './cognitive/_SpeechToText.md';

<SpeechToText/>


import AzureSearch, {toc as AzureSearchTOC} from './cognitive/_AzureSearch.md';

<AzureSearch/>


import BingImageSearch, {toc as BingImageSearchTOC} from './cognitive/_BingImageSearch.md';

<BingImageSearch/>


export const toc = [...TextAnalyticsTOC, ...TranslatorTOC, ...ComputerVisionTOC,
...FormRecognizerTOC, ...AnomalyDetectionTOC, ...FaceTOC, ...SpeechToTextTOC,
...AzureSearchTOC, ...BingImageSearchTOC]
