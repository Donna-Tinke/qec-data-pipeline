# Part II QEC decoder report

## Inputs and evaluation

Part II uses the two ML tables produced in Part I. The syndrome table contains 75,598 aggregated examples representing 70 million physical observations. The Google table contains 250,000 hardware shots.

The supplied train, validation and test splits were used without modification. For Task A, physical sample weights were used during training and evaluation. Model thresholds were selected using only validation data, while test data was used for the final reported metrics.

Within each task and distance, all compared models are evaluated on the same held-out test examples.

## Model choices

| Task | Input | Target | Model | Why the model fits the table |
|---|---|---|---|---|
| A | 16 ordered syndrome bits | Logical-error label | Weighted logistic regression | The table has a small fixed binary feature vector and a binary target. Logistic regression can use these features directly. The aggregated rows also provide `sample_weight`, which is used during fitting and evaluation so that physical observation counts are preserved without expanding the data. |
| B | Detector-event density and four supplied decoder predictions | Actual observable flip | Logistic regression | The input is a small five-feature numeric/binary vector with a binary target, so a simple linear classifier provides a clear way to combine the decoder signals. Distance 3 and distance 5 are trained separately. |
| C | 200 unpacked detector bits | Actual observable flip | Small MLP | The raw input is a larger binary vector. A small MLP can learn nonlinear combinations of detector bits while keeping the experiment bounded and simple. |

## Results summary

| Task | Model | Distance | Logical-error rate | Balanced accuracy | Brier score | Training (s) | Prediction (s) |
|---|---|---:|---:|---:|---:|---:|---:|
| A | Weighted prior | - | 0.10395 | 0.50000 | 0.09691 | 0.00019 | 0.00004 |
| A | Weighted logistic regression | - | 0.19554 | 0.62946 | 0.09096 | 0.23207 | 0.00056 |
| B | Majority prior | 3 | 0.49295 | 0.50000 | 0.24995 | 0.00012 | 0.00004 |
| B | Belief matching | 3 | 0.40133 | 0.59854 | N/A | N/A | N/A |
| B | Correlated matching | 3 | 0.42197 | 0.57792 | N/A | N/A | N/A |
| B | PyMatching | 3 | 0.43636 | 0.56346 | N/A | N/A | N/A |
| B | Tensor network contraction | 3 | 0.39668 | 0.60312 | N/A | N/A | N/A |
| B | Combined logistic regression | 3 | 0.39722 | 0.60251 | 0.23515 | 0.03744 | 0.00895 |
| B | Majority prior | 5 | 0.50152 | 0.50000 | 0.25001 | 0.00009 | 0.00003 |
| B | Belief matching | 5 | 0.39944 | 0.60057 | N/A | N/A | N/A |
| B | Correlated matching | 5 | 0.42260 | 0.57741 | N/A | N/A | N/A |
| B | PyMatching | 5 | 0.45480 | 0.54518 | N/A | N/A | N/A |
| B | Tensor network contraction | 5 | 0.39524 | 0.60476 | N/A | N/A | N/A |
| B | Combined logistic regression | 5 | 0.39768 | 0.60265 | 0.23406 | 0.01217 | 0.00039 |
| C | Raw detector MLP | 3 | 0.49728 | 0.50279 | 0.27120 | 0.44356 | 0.00585 |

For the four supplied Google decoders, training time, prediction time and Brier score are reported as N/A. We receive their binary predictions in the input data and do not train or execute the original decoder implementations. `metrics.json` separately records the local time needed to extract and evaluate those provided predictions.

## Task A - Weighted syndrome decoder

The weighted positive logical-error proportion in the training data was 0.0426. The weighted prior baseline achieved a logical-error rate of 0.10395, balanced accuracy of 0.50000, and Brier score of 0.09691.

The weighted logistic regression achieved a logical-error rate of 0.19554, balanced accuracy of 0.62946, and Brier score of 0.09096. The threshold selected on the validation split was 0.10.

The logistic regression has a higher logical-error rate than the prior baseline, but its balanced accuracy is clearly better. The dataset is imbalanced, so a majority-based prediction can achieve a low total error while performing poorly on the minority class. This is why we report balanced accuracy together with logical-error rate.

## Task B - Google decoder comparison

Distance 3 and distance 5 were evaluated separately. For each distance we compared a majority baseline, the four supplied decoder predictions, and a combined logistic regression using detector-event density and the four decoder outputs.

### Distance 3

The majority baseline had a logical-error rate of 0.49295. Tensor network contraction achieved 0.39668, while the combined logistic regression achieved 0.39722.

The supplied decoders disagreed on 0.55384 of the test shots. All four were wrong together on 0.14440 of the shots. This shows that the decoders do not always make the same mistakes and there is some complementary information in their predictions.

### Distance 5

The majority baseline had a logical-error rate of 0.50152. Tensor network contraction achieved 0.39524, while the combined logistic regression achieved 0.39768.

The decoder disagreement rate was 0.64780. All four decoders were wrong together on 0.10852 of the test shots.

For both distances, the combined model performs much better than the majority baseline. Tensor network contraction is slightly better than the combined model on the final test data. This is still a valid result because the goal is a correct and repeatable ML pipeline rather than outperforming the supplied decoders.

## Task C - Raw detector prototype

Task C uses only distance-3 rows with `shot_index < 12500`. Each model input contains 200 unpacked detector bits.

The MLP achieved a logical-error rate of 0.49728, balanced accuracy of 0.50279, and Brier score of 0.27120. Training took 0.444 seconds and prediction took 0.006 seconds.

The result is close to random classification. A flat vector of 200 detector bits does not explicitly represent detector positions, neighbourhood relationships, or how detector events change across QEC rounds. This limits what the simple MLP can learn from the representation.

The MLP used all 20 configured training iterations. In this run scikit-learn also reported a ConvergenceWarning because the optimizer reached this limit before fully converging. We kept this small training budget because Task C is intended as a bounded prototype rather than a model-optimization experiment.

## Reproducibility

The pipeline records the random seed, dependency versions, hashes of both ML input tables, feature order, split rules and timings. It saves the fitted models, predictions and complete metrics under `results/part2/`.

Automated tests check the ML schemas, supplied split rules, syndrome weights, little-endian detector-bit order, detector event counts, feature order, repeatability and metric calculation.
