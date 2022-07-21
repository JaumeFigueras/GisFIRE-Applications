import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
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
    negative_examples = pd.read_csv(args.negative_file)
    negative_other_examples = pd.read_csv(args.negative_other_file)

    # Merge all the dataframes
    examples = positive_examples.copy()
    examples = examples.append(negative_examples.copy())
    examples = examples.append(negative_other_examples.copy())
    examples.reset_index(drop=True, inplace=True)

    # Drop non useful columns
    examples.drop('ID', axis=1, inplace=True)
    examples.drop('NUMBER_OF_SENSORS', axis=1, inplace=True)
    examples.drop('HIT_GROUND', axis=1, inplace=True)
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

    multi_correlations = examples.corr()
    sns.heatmap(multi_correlations.abs(), annot=True, cmap="YlGnBu")
    plt.show()
