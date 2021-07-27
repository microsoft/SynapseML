---
title: Processors
sidebar_label: About
---

Benthos processors are functions applied to messages passing through a pipeline. The function signature allows a processor to mutate or drop messages depending on the content of the message. There are many types on offer but the most powerful is the [`bloblang` processor][processor.bloblang].

Processors are set via config, and depending on where in the config they are placed they will be run either immediately after a specific input (set in the input section), on all messages (set in the pipeline section) or before a specific output (set in the output section). Most processors apply to all messages and can be placed in the pipeline section:

```yaml
pipeline:
  threads: 1
  processors:
    - label: my_cool_mapping
      bloblang: |
        root.message = this
        root.meta.link_count = this.links.length()
```

The `threads` field in the pipeline section determines how many parallel processing threads are created. You can read more about parallel processing in the [pipeline guide][pipelines].

## Labels

Processors have an optional field `label` that can uniquely identify them in observability data such as metrics and logs. This can be useful when running configs with multiple nested processors, otherwise their metrics labels will be generated based on their composition. For more information check out the [metrics documentation][metrics.about].

import ComponentsByCategory from '@theme/ComponentsByCategory';

## Categories

<ComponentsByCategory type="processors"></ComponentsByCategory>

## Error Handling

Some processors have conditions whereby they might fail. Rather than throw these messages into the abyss Benthos still attempts to send these messages onwards, and has mechanisms for filtering, recovering or dead-letter queuing messages that have failed which can be read about [here][error_handling].

## Batching and Multiple Part Messages

All Benthos processors support multiple part messages, which are synonymous with batches. This enables some cool [windowed processing][windowed_processing] capabilities.

Many processors are able to perform their behaviours on specific parts of a message batch, or on all parts, and have a field `parts` for specifying an array of part indexes they should apply to. If the list of target parts is empty these processors will be applied to all message parts.

Part indexes can be negative, and if so the part will be selected from the end counting backwards starting from -1. E.g. if part = -1 then the selected part will be the last part of the message, if part = -2 then the part before the last element will be selected, and so on.

Some processors such as [`dedupe`][processor.dedupe] act across an entire batch, when instead we might like to perform them on individual messages of a batch. In this case the [`for_each`][processor.for_each] processor can be used.

You can read more about batching [in this document][batching].

[error_handling]: /docs/configuration/error_handling
[batching]: /docs/configuration/batching
[windowed_processing]: /docs/configuration/windowed_processing
[pipelines]: /docs/configuration/processing_pipelines
[processor.bloblang]: /docs/components/processors/bloblang
[processor.split]: /docs/components/processors/split
[processor.dedupe]: /docs/components/processors/dedupe
[processor.for_each]: /docs/components/processors/for_each
