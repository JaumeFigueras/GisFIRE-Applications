import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import confusion_matrix
from sklearn.metrics import precision_recall_fscore_support
from sklearn.model_selection import KFold
import numpy as np
import argparse
import csv

if __name__ == "__main__":  # pragma: no cover
    # Config the program arguments
    # noinspection DuplicatedCode
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--positive-file', help='Positive examples lightnings file')
    parser.add_argument('-n', '--negative-file', help='Negative examples of the same day lightnings')
    parser.add_argument('-o', '--negative-other-file', help='Negative examples of other day lightnings')
    parser.add_argument('-r', '--results-file', help='Results filename')
    args = parser.parse_args()

    # Load all data
    positive_examples = pd.read_csv(args.positive_file)
    positive_examples['FIRE'] = 1
    negative_examples = pd.read_csv(args.negative_file)
    negative_examples['FIRE'] = 0
    negative_other_examples = pd.read_csv(args.negative_other_file)
    negative_other_examples['FIRE'] = 0

    confusion = list()
    scores = list()
    scores_header = ['TEST_POINT', 'TN', 'FP', 'FN', 'TP', 'FN_RATIO', 'PRECISION', 'RECALL', 'F_SCORE']
    scores.append(scores_header)
    K_FOLDS = 5
    for i in range(1, 11):
        # Merge all the dataframes
        examples = positive_examples.copy()
        examples = examples.append(negative_examples.iloc[::i].copy())
        examples = examples.append(negative_other_examples.iloc[::i].copy())
        examples.reset_index(drop=True, inplace=True)

        # Drop non useful columns see correlation analysis
        examples.drop('NUMBER_OF_SENSORS', axis=1, inplace=True)
        examples.drop('HIT_GROUND', axis=1, inplace=True)
        # examples.drop('AVG_REL_HUMIDITY_1_DAY', axis=1, inplace=True)
        # examples.drop('AVG_REL_HUMIDITY_3_DAY', axis=1, inplace=True)
        examples.drop('AVG_REL_HUMIDITY_5_DAY', axis=1, inplace=True)
        examples.drop('AVG_REL_HUMIDITY_10_DAY', axis=1, inplace=True)
        examples.drop('AVG_REL_HUMIDITY_15_DAY', axis=1, inplace=True)
        # examples.drop('AVG_TEMPERATURE_1_DAY', axis=1, inplace=True)
        examples.drop('AVG_TEMPERATURE_3_DAY', axis=1, inplace=True)
        examples.drop('AVG_TEMPERATURE_5_DAY', axis=1, inplace=True)
        examples.drop('AVG_TEMPERATURE_10_DAY', axis=1, inplace=True)
        examples.drop('AVG_TEMPERATURE_15_DAY', axis=1, inplace=True)
        # examples.drop('SUM_RAIN_1_DAY', axis=1, inplace=True)
        # examples.drop('SUM_RAIN_3_DAY', axis=1, inplace=True)
        examples.drop('SUM_RAIN_5_DAY', axis=1, inplace=True)
        examples.drop('SUM_RAIN_10_DAY', axis=1, inplace=True)
        examples.drop('SUM_RAIN_15_DAY', axis=1, inplace=True)
        # Many weather station do not have this data
        examples.drop('SOLAR_IRRADIANCE', axis=1, inplace=True)
        examples.drop('SUM_SOLAR_IRRADIANCE_1_DAY', axis=1, inplace=True)
        examples.drop('SUM_SOLAR_IRRADIANCE_3_DAY', axis=1, inplace=True)
        examples.drop('SUM_SOLAR_IRRADIANCE_5_DAY', axis=1, inplace=True)
        examples.drop('SUM_SOLAR_IRRADIANCE_10_DAY', axis=1, inplace=True)
        examples.drop('SUM_SOLAR_IRRADIANCE_15_DAY', axis=1, inplace=True)
        examples.drop('WIND', axis=1, inplace=True)
        examples.drop('SUM_WIND_1_DAY', axis=1, inplace=True)
        examples.drop('SUM_WIND_3_DAY', axis=1, inplace=True)
        examples.drop('SUM_WIND_5_DAY', axis=1, inplace=True)
        examples.drop('SUM_WIND_10_DAY', axis=1, inplace=True)
        examples.drop('SUM_WIND_15_DAY', axis=1, inplace=True)
        rows_before = len(examples.index)
        # Drop rows with NaN
        examples.dropna(inplace=True)
        rows_after = len(examples.index)
        # print(rows_after, rows_before)

        # Remap LandCover
        land_cover_values = list(examples['LAND_COVER'].unique())
        land_cover_values.sort()
        land_cover_dict = {land_cover_values[i]: i for i in range(len(land_cover_values))}
        examples.replace({'LAND_COVER': land_cover_dict}, inplace=True)

        # Create Learning and test  datasets
        learn, test = train_test_split(examples, train_size=0.8, shuffle=True,
                                       random_state=np.random.RandomState(1234567890))
        learn_data = learn.copy()
        learn_data.drop('ID', axis=1, inplace=True)
        learn_data.drop('DATE', axis=1, inplace=True)
        test_data = test.copy()
        test_data.drop('ID', axis=1, inplace=True)
        test_data.drop('DATE', axis=1, inplace=True)

        intermediate_confusion = list()
        intermediate_scores = list()
        X = learn_data.iloc[:, :-1]
        Y = learn_data.iloc[:, -1]
        kf = KFold(n_splits=K_FOLDS)
        kf.get_n_splits(X)
        for train_index, cv_index in kf.split(X):
            # print("TRAIN:", train_index, "TEST:", cv_index)
            x_train_data, x_cv_data = X.values[train_index], X.values[cv_index]
            y_train_data, y_cv_data = Y.values[train_index], Y.values[cv_index]
            logistic_regression = LogisticRegression(random_state=np.random.RandomState(1234567890), solver='liblinear', verbose=1)
            classifier = logistic_regression.fit(x_train_data, y_train_data)
            result = classifier.predict(x_cv_data)
            tn, fp, fn, tp = confusion_matrix(y_cv_data, result).ravel()
            intermediate_confusion.append([tn, fp, fn, tp])
            precision, recall, f_score, support = precision_recall_fscore_support(y_cv_data, result, average='binary')
            intermediate_scores.append([precision, recall, f_score])
        column_average = [i]
        column_average += [sum(sub_list) / len(sub_list) for sub_list in zip(*intermediate_confusion)]
        column_average += [column_average[2] / sum(column_average)] # FN / all
        column_average += [sum(sub_list) / len(sub_list) for sub_list in zip(*intermediate_scores)]
        scores.append(column_average)

        if i == 10:
            logistic_regression = LogisticRegression(random_state=np.random.RandomState(1234567890), solver='liblinear', verbose=1)
            classifier = logistic_regression.fit(learn_data.iloc[:, :-1], learn_data.iloc[:, -1])
            result = classifier.predict(test_data.iloc[:, :-1])

            tn, fp, fn, tp = confusion_matrix(test_data.iloc[:, -1], result).ravel()
            precision, recall, f_score, support = precision_recall_fscore_support(test_data.iloc[:, -1], result,
                                                                                  average='binary')
            scores.append([-1, tn, fp, fn, tp, fn / (tn + fp + fn + tp), precision, recall, f_score])

    with open(args.results_file, 'w') as file:
        writer = csv.writer(file)
        writer.writerows(scores)


