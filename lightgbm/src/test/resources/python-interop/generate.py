# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

"""Regenerate the synthetic Python-to-Scala fixture in its pinned environment.

Python 3.11.8, lightgbm 3.3.5, numpy 1.26.4, pandas 2.0.3,
scipy 1.17.1, scikit-learn 1.9.1, wheel 0.48.0.
This optional generator is not run by the upstream Scala test suite.
"""

import argparse
import hashlib
import importlib.metadata
import itertools
import json
import math
import platform
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd


FEATURES = [
    "numeric_missing",
    "numeric_complete",
    "category_missing",
    "category_complete",
]
CATEGORIES = ["red", "blue", "green"]


def dataframe(rows):
    frame = pd.DataFrame(rows, columns=FEATURES)
    for name in FEATURES[2:]:
        frame[name] = pd.Categorical(frame[name], categories=CATEGORIES)
    return frame


def encoded(frame):
    values = frame.copy()
    for name in FEATURES[2:]:
        codes = values[name].cat.codes.astype(float)
        values[name] = codes.where(codes >= 0, np.nan)
    return values.to_numpy(dtype=float)


def distinguish(a, b):
    return bool(np.any(np.abs(a - b) > 1e-6))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    destination = parser.parse_args().output
    destination.mkdir(parents=True, exist_ok=False)
    assert lgb.__version__ == "3.3.5"
    assert platform.python_version() == "3.11.8"
    assert np.__version__ == "1.26.4"
    assert pd.__version__ == "2.0.3"
    for package, version in (
        ("scipy", "1.17.1"),
        ("scikit-learn", "1.9.1"),
        ("wheel", "0.48.0"),
    ):
        assert importlib.metadata.version(package) == version, package
    rows = (
        list(
            itertools.product(
                [-1.0, 0.0, 1.0, np.nan],
                [-1.0, 0.0, 1.0],
                ["red", "blue", "green", None],
                CATEGORIES,
            )
        )
        * 2
    )
    train = dataframe(rows)
    category_effect = {"red": 0.0, "blue": 9.0, "green": -8.0, None: 40.0}
    complete_effect = {"red": 0.0, "blue": 15.0, "green": -15.0}
    labels = [30.0 if math.isnan(a) else 4.0 * a for a, _, _, _ in rows]
    labels = np.asarray(
        [
            value + 3.0 * b + category_effect[c] + complete_effect[d]
            for value, (_, b, c, d) in zip(labels, rows)
        ]
    )
    dataset = lgb.Dataset(train, label=labels, categorical_feature=FEATURES[2:])
    model = lgb.train(
        {
            "objective": "regression",
            "learning_rate": 0.3,
            "num_leaves": 31,
            "min_data_in_leaf": 1,
            "min_data_per_group": 1,
            "cat_smooth": 0,
            "cat_l2": 0,
            "max_cat_to_onehot": 4,
            "num_threads": 1,
            "deterministic": True,
            "force_col_wise": True,
            "verbosity": -1,
            "use_missing": True,
            "zero_as_missing": False,
            "seed": 874,
        },
        dataset,
        num_boost_round=40,
    )
    text = model.model_to_string()
    metadata = [
        line for line in text.splitlines() if line.startswith("pandas_categorical:")
    ]
    assert len(metadata) == 1
    assert json.loads(metadata[0].split(":", 1)[1]) == [CATEGORIES, CATEGORIES]
    model_path = destination / "python-model.txt"
    model_path.write_text(text, encoding="utf-8")
    reloaded = lgb.Booster(model_file=str(model_path))
    cases = [
        ("ordinary", [1.0, 0.0, "red", "red"]),
        ("numeric_nan", [np.nan, 0.0, "red", "red"]),
        ("numeric_zero", [0.0, 0.0, "red", "red"]),
        ("category_missing", [1.0, 0.0, None, "red"]),
        ("category_blue", [1.0, 0.0, "blue", "red"]),
        ("wrong_blue_mapping", [1.0, 0.0, "green", "red"]),
        ("complete_category_nan", [1.0, 0.0, "red", None]),
        ("complete_category_blue", [1.0, 0.0, "red", "blue"]),
        ("complete_numeric_nan", [1.0, np.nan, "red", "red"]),
        ("all_missing", [np.nan, np.nan, None, None]),
    ]
    frame = dataframe([row for _, row in cases])
    numeric = encoded(frame)
    raw = reloaded.predict(frame, raw_score=True)
    normal = reloaded.predict(frame)
    leaves = reloaded.predict(frame, pred_leaf=True)
    np.testing.assert_allclose(
        raw, model.predict(frame, raw_score=True), atol=1e-10, rtol=0
    )
    np.testing.assert_allclose(
        raw, reloaded.predict(numeric, raw_score=True), atol=1e-10, rtol=0
    )
    np.testing.assert_array_equal(leaves, reloaded.predict(numeric, pred_leaf=True))
    unknown_frame = dataframe([[1.0, 0.0, "purple", "red"]])
    assert np.isnan(encoded(unknown_frame)[0, 2])
    # These are native numeric inputs, without pandas converting them to NaN.
    native_cases = [
        ("native_missing_feature_code3", [1.0, 0.0, 3.0, 0.0]),
        ("native_missing_feature_negative1", [1.0, 0.0, -1.0, 0.0]),
        ("native_complete_feature_code3", [1.0, 0.0, 0.0, 3.0]),
        ("native_complete_feature_negative1", [1.0, 0.0, 0.0, -1.0]),
    ]
    extra = np.asarray([row for _, row in native_cases], dtype=float)
    numeric = np.concatenate((numeric, extra))
    raw = np.concatenate((raw, reloaded.predict(extra, raw_score=True)))
    normal = np.concatenate((normal, reloaded.predict(extra)))
    leaves = np.concatenate((leaves, reloaded.predict(extra, pred_leaf=True)))
    cases += native_cases
    assert len(cases) == 14 and len({name for name, _ in cases}) == 14
    assert np.isfinite(raw).all() and np.isfinite(normal).all()
    index = {name: i for i, (name, _) in enumerate(cases)}
    controls = [
        ("numeric_nan", "numeric_zero"),
        ("category_missing", "ordinary"),
        ("category_blue", "wrong_blue_mapping"),
    ]
    for left, right in controls:
        assert distinguish(raw[index[left]], raw[index[right]]), (left, right, raw)
    # Record the actual no-training-missing behavior rather than assuming a branch.
    observations = {
        left
        + "__vs__"
        + right: {"left": float(raw[index[left]]), "right": float(raw[index[right]])}
        for left, right in controls
        + [
            ("complete_category_nan", "ordinary"),
            ("complete_numeric_nan", "ordinary"),
            ("native_missing_feature_code3", "category_missing"),
            ("native_missing_feature_code3", "ordinary"),
            ("native_missing_feature_negative1", "category_missing"),
            ("native_missing_feature_negative1", "ordinary"),
            ("native_complete_feature_code3", "complete_category_nan"),
            ("native_complete_feature_code3", "ordinary"),
            ("native_complete_feature_negative1", "complete_category_nan"),
            ("native_complete_feature_negative1", "ordinary"),
        ]
    }
    lines = []
    for i, (name, _) in enumerate(cases):
        fields = [name] + [
            "nan" if np.isnan(v) else format(v, ".17g") for v in numeric[i]
        ]
        fields += [format(raw[i], ".17g"), format(normal[i], ".17g")]
        fields += [str(int(leaf)) for leaf in np.atleast_1d(leaves[i])]
        lines.append("\t".join(fields))
    (destination / "expected.tsv").write_text("\n".join(lines) + "\n", encoding="utf-8")
    result = {
        "versions": {
            name: importlib.metadata.version(name)
            for name in (
                "lightgbm",
                "numpy",
                "pandas",
                "scipy",
                "scikit-learn",
                "wheel",
            )
        },
        "python": platform.python_version(),
        "features": FEATURES,
        "pandas_categorical": [CATEGORIES, CATEGORIES],
        "category_missing_encoding": "NaN",
        "pandas_unknown_category_encoding_verified": "NaN; separate from direct native codes3/-1",
        "rows": len(cases),
        "trees": int(model.num_trees()),
        "controls": observations,
        "model_sha256": hashlib.sha256(model_path.read_bytes()).hexdigest(),
        "expected_sha256": hashlib.sha256(
            (destination / "expected.tsv").read_bytes()
        ).hexdigest(),
        "qualification": "Python reference only; native Scala parity remains separate",
    }
    assert model_path.stat().st_size < 1048576
    (destination / "python-reference.json").write_text(
        json.dumps(result, indent=2, allow_nan=False) + "\n"
    )
    print(json.dumps(result, allow_nan=False))


if __name__ == "__main__":
    main()
