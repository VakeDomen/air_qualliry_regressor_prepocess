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

def load_dataset(train_file):
    train_df = pd.read_csv(train_file)
    train_dataset_X, train_dataset_Y = [], []
    for group in tqdm(train_df.groupby("window_id")):
        data = group[1].values
        train_dataset_X.append(data[:, 1:-1])  # All rows, all but the last column
        train_dataset_Y.append(data[:, -1])  # Last row, last column

    # Reshape to the format [samples, timesteps, features]
    train_dataset_X = np.array(train_dataset_X).reshape(-1, 180, train_dataset_X[0].shape[1])

    # Reshape Y to be [samples, 1]
    train_dataset_Y = np.array(train_dataset_Y).reshape(-1, 180, 1)

    return (train_dataset_X, train_dataset_Y)


def train_and_test_model(train_file, test_file, model_number):
    print("Loading datasets...")
    # Load datasets
    #print(count_column_values(train_file, 'window_id'))
    (trainX, trainY) = load_dataset(train_file)
    
    print(f'trainX shape: {trainX.shape}, trainY shape: {trainY.shape}')  # print shapes
    print("Scaling data")
    

    #instantiate the RobustScaler object for X
    scaler_X = RobustScaler()
    # Reshape, fit and transform the training and test input data
    trainX = scaler_X.fit_transform(trainX.reshape(-1, trainX.shape[-1])).reshape(trainX.shape)

    # Save the scaler object to a file
    scaler_file = f"scaler_{model_number}.pkl"
    with open(scaler_file, 'wb') as file:
        pickle.dump(scaler_X, file)
    print(f"Scaler saved as {scaler_file}")



if __name__ == "__main__":
    for i in range(1, 11):
        print(f"Processing fold {i}...")
        train_file = f"./fold_{i}/train.csv"
        test_file = f"./fold_{i}/test.csv"
        train_and_test_model(train_file, test_file, i)
