import pandas as pd
import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import matplotlib.pyplot as plt
import pickle

# Load data
data = pd.read_csv('../out/fold_1/test.csv')

# Ensure the data is numeric
for column in data.columns:
    data[column] = pd.to_numeric(data[column], errors='coerce')

with open('../out/predictions_1.pkl', 'rb') as f:
    predictions = pickle.load(f)


# Extract true values and time
true_values = data.groupby(data.iloc[:,0]).last().iloc[:,-1].values
print(predictions, true_values)
time = data.groupby(data.iloc[:,0]).last().iloc[:,14].values // 60  # Convert to minutes

# Calculate evaluation scores
mae = mean_absolute_error(true_values, predictions)
mse = mean_squared_error(true_values, predictions)
rmse = np.sqrt(mse)
r2 = r2_score(true_values, predictions)

print(f"Mean Absolute Error (MAE): {mae}")
print(f"Mean Squared Error (MSE): {mse}")
print(f"Root Mean Squared Error (RMSE): {rmse}")
print(f"R^2 Score: {r2}")


# Calculate errors for each prediction
errors = np.abs(true_values - predictions)

# Downsample the data for plotting
downsample_rate = 100  # adjust this value as needed
time = time[::downsample_rate]
errors = errors[::downsample_rate]

# Convert time to hh:mm format and plot
plt.figure(figsize=(12, 6))
time = [f"{t//60:02d}:{t%60:02d}" for t in time]
plt.plot(time, errors)

plt.xticks(rotation=45)
plt.xlabel('Time (HH:MM)')
plt.ylabel('Absolute Error')
plt.title('Error by Time of Day')
plt.show()