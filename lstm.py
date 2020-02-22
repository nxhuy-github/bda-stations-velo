import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import math
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error


def build_model(train, test):
    look_back = 1
    batch_size = 5
    
    # prepare the train and test datasets for modeling
    trainX, trainY = create_dataset(train, look_back)
    testX, testY = create_dataset(test, look_back)

    # reshape input to be [samples, time steps, features]
    trainX = np.reshape(trainX, (trainX.shape[0], trainX.shape[1], 1))
    testX = np.reshape(testX, (testX.shape[0], testX.shape[1], 1))

    # create and fit the LSTM network
    model = Sequential()
    model.add(LSTM(5, input_shape=(look_back, 1)))
    model.add(Dense(1))
    model.compile(loss='mean_squared_error', optimizer='adam')
    model.fit(trainX, trainY, epochs=10, batch_size = batch_size, verbose=2)

    return model

def preprocessing(df, station):
    _t = df[(df['ID'] == station)]
    _t = _t.drop(['ID', 'time-stamp', 'hour', 'day-of-week', 'available-bike-stands'], axis=1)
    dataset = _t.values
    dataset = dataset.astype('float32')
    return dataset

def train_test_split(dataset):
    scaler = MinMaxScaler(feature_range=(0, 1))
    datasetScaled = scaler.fit_transform(dataset)
    train_size = int(len(datasetScaled) * 0.67)
    test_size = len(datasetScaled) - train_size
    train, test = datasetScaled[0:train_size,:], datasetScaled[train_size:len(datasetScaled), :]
    return train, test

def create_dataset(dataset, look_back=1):
    dataX, dataY = [], []
    for i in range(len(dataset)-look_back-1):
        a = dataset[i:(i+look_back), 0]
        dataX.append(a)
        dataY.append(dataset[i + look_back, 0])
    return np.array(dataX), np.array(dataY)


def main():
    file = './VeloVformatted.sample.csv'
    df = pd.read_csv(file, sep=';')
    stations = df['ID'].unique()
    dfCopied = df.copy(deep = True)
    dfCopied = dfCopied.sort_values(['ID', 'time-stamp'])

    d = {}
    for i, station in enumerate(stations):
        # print('STATION {}'.format(i))
        _tdf = dfCopied.copy(deep=True)
        dataset = preprocessing(_tdf, station)
    
        train, test = train_test_split(dataset)

        model = build_model(train, test)
        d = {station: model}
    # print(d)
    return d

if __name__ == '__main__':
    d = main()
    
