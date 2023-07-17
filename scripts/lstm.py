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

def load_dataset(train_file, test_file):
    train_df = pd.read_csv(train_file)
    test_df = pd.read_csv(test_file)

    train_dataset_X, train_dataset_Y, test_dataset_X, test_dataset_Y = [], [], [], []
    for group in tqdm(train_df.groupby("window_id")):
        data = group[1].values
        train_dataset_X.append(data[:, 1:-1])  # All rows, all but the last column
        train_dataset_Y.append(data[:, -1])  # Last row, last column
    for group in tqdm(test_df.groupby("window_id")):
        data = group[1].values
        test_dataset_X.append(data[:, 1:-1])  # All rows, all but the last column
        test_dataset_Y.append(data[:, -1])  # Last row, last column

    # Reshape to the format [samples, timesteps, features]
    train_dataset_X = np.array(train_dataset_X).reshape(-1, 180, train_dataset_X[0].shape[1])
    test_dataset_X = np.array(test_dataset_X).reshape(-1, 180, test_dataset_X[0].shape[1])

    # Reshape Y to be [samples, 1]
    train_dataset_Y = np.array(train_dataset_Y).reshape(-1, 180, 1)
    test_dataset_Y = np.array(test_dataset_Y).reshape(-1, 180, 1)

    return (train_dataset_X, train_dataset_Y), (test_dataset_X, test_dataset_Y)


def create_model(input_shape):
    model = Sequential()
    model.add(LSTM(256, input_shape=input_shape))
    model.add(Dense(1))
    model.compile(loss='mean_squared_logarithmic_error', optimizer='adam', metrics=["mse"])
    return model


def train_and_test_model(train_file, test_file, model_number):
    print("Loading datasets...")
    # Load datasets
    #print(count_column_values(train_file, 'window_id'))
    (trainX, trainY), (testX, testY) = load_dataset(train_file, test_file)
    
    print(f'trainX shape: {trainX.shape}, trainY shape: {trainY.shape}')  # print shapes
    print(f'testX shape: {testX.shape}, testY shape: {testY.shape}')  # print shapes
    print("Scaling data")
    

    #instantiate the RobustScaler object for X
    scaler_X = RobustScaler()
    # Reshape, fit and transform the training and test input data
    trainX = scaler_X.fit_transform(trainX.reshape(-1, trainX.shape[-1])).reshape(trainX.shape)
    testX = scaler_X.transform(testX.reshape(-1, testX.shape[-1])).reshape(testX.shape)

    # If your output data is continuous and needs scaling
    print("Creating model...")
    # Create and fit the LSTM network
    model = create_model((trainX.shape[1], trainX.shape[2]))
    model.fit(trainX, trainY, epochs=10, batch_size=64, verbose=1)

    print("Making predictions...")
    # Make predictions
    trainPredict = model.predict(trainX)
    testPredict = model.predict(testX)
    print("Calculating RMSE...")
    trainY = trainY[:,-1]
    testY = testY[:,-1]
    # Calculate root mean squared error
    trainScore = np.sqrt(mean_squared_error(trainY, trainPredict))
    print('Train Score: %.2f RMSE' % (trainScore))
    testScore = np.sqrt(mean_squared_error(testY, testPredict))
    print('Test Score: %.2f RMSE' % (testScore))
    
    print("Calculating R^2...")
    # Calculate R^2 (regression score function)
    trainScore = r2_score(trainY, trainPredict)
    print('Train Score: %.2f R2' % (trainScore))
    testScore = r2_score(testY, testPredict)
    print('Test Score: %.2f R2' % (testScore))
    
    print("saving model")
    model.save(f"./model_2_f{model_number}.h5")

if __name__ == "__main__":
    for i in range(1, 11):
        print(f"Processing fold {i}...")
        train_file = f"./fold_{i}/train.csv"
        test_file = f"./fold_{i}/test.csv"
        train_and_test_model(train_file, test_file, i)
