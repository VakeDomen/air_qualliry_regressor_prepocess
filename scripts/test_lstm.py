import pandas as pd
import numpy as np
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import RobustScaler
from tqdm import tqdm
import pickle
from sklearn.metrics import r2_score
import csv
from tensorflow import keras


def load_dataset(test_file):
    test_df = pd.read_csv(test_file)

    test_dataset_X, test_dataset_Y = [], []
    for group in tqdm(test_df.groupby("window_id")):
        data = group[1].values
        test_dataset_X.append(data[:, 1:-1])  # All rows, all but the last column
        test_dataset_Y.append(data[:, -1])  # Last row, last column

    # Reshape to the format [samples, timesteps, features]
    test_dataset_X = np.array(test_dataset_X).reshape(-1, 180, test_dataset_X[0].shape[1])

    # Reshape Y to be [samples, 1]
    test_dataset_Y = np.array(test_dataset_Y).reshape(-1, 180, 1)

    return (test_dataset_X, test_dataset_Y)


def train_and_test_model(test_file):
    print("Loading datasets...")
    # Load datasets
    #print(count_column_values(train_file, 'window_id'))
    (testX, testY) = load_dataset(test_file)


    # Reshape, fit and transform the training and test input data
    scaler_file = 'scaler_1.pkl'  # Replace 'model_number' with the actual model number you used while saving
    with open(scaler_file, 'rb') as file:
        scaler_X = pickle.load(file)
    testX = scaler_X.transform(testX.reshape(-1, testX.shape[-1])).reshape(testX.shape)

    print("Creating model...")
    # Create and fit the LSTM network
    model = keras.models.load_model(f"./model_2_f1.h5")

    print("Making predictions...")
    # Make predictions
    testPredict = model.predict(testX)

    print("Calculating RMSE...")
    testY = testY[:,-1]
    testScore = np.sqrt(mean_squared_error(testY, testPredict))
    print('Test Score: %.2f RMSE' % (testScore))

    print("Calculating R^2...")
    testScore = r2_score(testY, testPredict)
    print('Test Score: %.2f R2' % (testScore))
    pred_file = f"predictions_1.pkl"
    with open(pred_file, 'wb') as file:
        pickle.dump(testPredict, file)
    print(f"Predictions saved as {pred_file}")


if __name__ == "__main__":
    test_file = f"./fold_1/test.csv"
    train_and_test_model(test_file)
