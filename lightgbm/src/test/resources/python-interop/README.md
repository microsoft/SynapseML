# Python model interoperability fixture

Synthetic regression model for Python-to-Scala input encoding. No user data.

Generated with Python 3.11.8, LightGBM 3.3.5, NumPy 1.26.4, pandas 2.0.3,
SciPy 1.17.1, scikit-learn 1.9.1 and wheel 0.48.0 on Linux x86-64.
The native parity check uses lightgbmlib 3.3.510. This records one tested pair,
not compatibility between arbitrary LightGBM releases.

From the repository root in that environment:

```bash
python lightgbm/src/test/resources/python-interop/generate.py --output /tmp/python-lightgbm-interop
```

The output directory must be new. `python-model.txt` and `expected.tsv` must
match these SHA256 values:

- model: `767aef4e256ddb3535b1f690a87043d96c3f5e9ba7ddf4623b430d7c8fae192e`
- expected: `00185629cc179d5374345678e25baea2015b026f499648033508fb427fb7464c`

Feature order: numeric_missing, numeric_complete, category_missing,
category_complete. Both categorical columns use red=0, blue=1, green=2.
The first numeric and categorical features are trained with missing values;
the second pair are not. The model has 40 trees and explicitly sets
`use_missing=true`, `zero_as_missing=false`.

Each TSV row contains a case name, four encoded feature values, a raw score,
a normal score, then 40 leaf indices. `nan` denotes IEEE NaN. Ten rows reflect
pandas input conversion; four additionally measure raw native categorical
codes 3 and -1. Those four are observations, not recommended encodings.
Missing and unseen strings should instead be converted to NaN as Python does.

The Scala suite reads the committed fixture, so ordinary upstream tests do
not require Python LightGBM. The generator needs the declared environment;
SynapseML's general environment does not install that extra Python package.
