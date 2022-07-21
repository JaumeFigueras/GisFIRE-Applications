import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import confusion_matrix
from sklearn.metrics import precision_recall_fscore_support
import numpy as np
import argparse

if __name__ == "__main__":  # pragma: no cover
    # Config the program arguments
    # noinspection DuplicatedCode
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--positive-file', help='Positive examples lightnings file')
    parser.add_argument('-n', '--negative-file', help='Negative examples of the same day lightnings')
    parser.add_argument('-o', '--negative-other-file', help='Negative examples of other day lightnings')
    args = parser.parse_args()

    # Load all data
    positive_examples = pd.read_csv(args.positive_file)
    positive_examples['FIRE'] = 1
    negative_examples = pd.read_csv(args.negative_file)
    negative_examples['FIRE'] = 0
    negative_other_examples = pd.read_csv(args.negative_other_file)
    negative_other_examples['FIRE'] = 0

    # Merge all the dataframes
    examples = positive_examples.copy()
    examples = examples.append(negative_examples.copy())
    examples = examples.append(negative_other_examples.copy())
    examples.reset_index(drop=True, inplace=True)

    # Drop columns with NaN (wind and solar irradiance)
    # examples.dropna(axis=1, how='any', inplace=True)
    # Drop non useful columns
    examples.drop('NUMBER_OF_SENSORS', axis=1, inplace=True)
    examples.drop('HIT_GROUND', axis=1, inplace=True)
    # examples.drop('AVG_REL_HUMIDITY_1_DAY', axis=1, inplace=True)
    # examples.drop('AVG_REL_HUMIDITY_3_DAY', axis=1, inplace=True)
    examples.drop('AVG_REL_HUMIDITY_5_DAY', axis=1, inplace=True)
    examples.drop('AVG_REL_HUMIDITY_10_DAY', axis=1, inplace=True)
    examples.drop('AVG_REL_HUMIDITY_15_DAY', axis=1, inplace=True)
    # examples.drop('AVG_TEMPERATURE_1_DAY', axis=1, inplace=True)
    # examples.drop('AVG_TEMPERATURE_3_DAY', axis=1, inplace=True)
    examples.drop('AVG_TEMPERATURE_5_DAY', axis=1, inplace=True)
    examples.drop('AVG_TEMPERATURE_10_DAY', axis=1, inplace=True)
    examples.drop('AVG_TEMPERATURE_15_DAY', axis=1, inplace=True)
    # examples.drop('SUM_RAIN_1_DAY', axis=1, inplace=True)
    # examples.drop('SUM_RAIN_3_DAY', axis=1, inplace=True)
    examples.drop('SUM_RAIN_5_DAY', axis=1, inplace=True)
    examples.drop('SUM_RAIN_10_DAY', axis=1, inplace=True)
    examples.drop('SUM_RAIN_15_DAY', axis=1, inplace=True)
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
    # Drop rows with NaN
    examples.dropna(inplace=True)


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

    # print(learn_data)
    # print(test_data)

    logistic_regression = LogisticRegression(random_state=np.random.RandomState(1234567890), solver='liblinear', verbose=1)
    # print(learn_data.iloc[:, :-1])
    classifier = logistic_regression.fit(learn_data.iloc[:, :-1], learn_data.iloc[:, -1])
    result = classifier.predict(test_data.iloc[:, :-1])

    tn, fp, fn, tp = confusion_matrix(test_data.iloc[:, -1], result).ravel()
    precision, recall, f_score, support = precision_recall_fscore_support(test_data.iloc[:, -1], result, average='binary')
    print(tn, fp, fn, tp)
    print(precision, recall, f_score, support)
    print(fn / (tn + fp + fn + tp))

    # print(result)
    # print(test_data.iloc[:, -1])




