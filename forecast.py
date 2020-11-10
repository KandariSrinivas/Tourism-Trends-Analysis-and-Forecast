import tensorflow as tf
print(tf.__version__)

from tensorflow.keras.layers import Dense, LSTM, Dropout, Input
from tensorflow.keras.models import Model, load_model
import pandas as pd
import numpy as np
import time
from elasticSearch import ELK

## TODO: data files
 ## add country

# data = [{"date":"2019-05-01","country":"singapore","trends_score":"23","footfall":1200,"words":{"marina":12,"abc park":31}}] *100
def makeModel(country, X, Y):
    ## creates a NN model for the country specified
 
    i = Input(shape = X[0].shape)
    x = Dense(10)(i)
    x = Dense(10)(x)
    x = Dense(1)(x)
    model = Model(i, x)
    model.compile(optimizer='adam', loss='mean_squared_error', metrics=['accuracy'])
    
    r = model.fit(x = X, y= Y, epochs= 50)
    return model

def saveModel(model, name):
    ## saves the model on disk
    model.save(name)

def loadModel(name):
    ## loads the model from disk
    return load_model(name)

def retrainModel(model, X, Y):
    ## trains already trained model with new data
    model.fit(X, Y, epochs=150)

def streamJob(model):
    # use this to retrain existing model on new streamed data
    cloudID = "bda:dXMtZWFzdDEuZ2NwLmVsYXN0aWMtY2xvdWQuY29tOjkyNDMkNDQwZjI0MDg3YTk1NGRmMGJhMmUyNmFjMmYxMmVjYWUkM2RmYTc4NDg2YzczNGVmM2I0ZTFhNTBhZTYwYWQ2YTM="
    username = "elastic"
    password = "QtXdCDEPbjSuhzwWN1Ss33tL"
    elastic = ELK(cloudID, username, password)
    data = elastic.getData("hypothesis_output", country, "stream")

    df = pd.DataFrame(data)
    df = df.join(df['words'].apply(pd.Series)).drop(['words', 'country', 'date'], axis=1)
    X = np.array(df.drop(['footfall'], axis=1))
    Y = np.array(df['footfall'])

    X = np.asarray(X).astype(np.float32)
    Y = np.asarray(Y).astype(np.float32)

    model.fit(X,Y)

if __name__ = "__main__":
    cloudID = "bda:dXMtZWFzdDEuZ2NwLmVsYXN0aWMtY2xvdWQuY29tOjkyNDMkNDQwZjI0MDg3YTk1NGRmMGJhMmUyNmFjMmYxMmVjYWUkM2RmYTc4NDg2YzczNGVmM2I0ZTFhNTBhZTYwYWQ2YTM="
    username = "elastic"
    password = "QtXdCDEPbjSuhzwWN1Ss33tL"
    elastic = ELK(cloudID, username, password)
    data = elastic.getData("hypothesis_output", country)


    df = pd.DataFrame(data)
    df = df.join(df['words'].apply(pd.Series)).drop(['words', 'country', 'date'], axis=1)
    X = np.array(df.drop(['footfall'], axis=1))
    Y = np.array(df['footfall'])

    X_train = np.asarray(X).astype(np.float32)[:-30]
    X_test = np.asarray(X).astype(np.float32)[-30:]

    Y_train = np.asarray(Y).astype(np.float32)[:-30]
    Y_test = np.asarray(Y).astype(np.float32)[-30:]

    model = makeModel("Singapore", X_train, Y_train)
    saveModel(model, "Singapore" + str(time.time()))
    ## load model when needed
    Y_pred = model.predict(X_test)

    df['footfall'][-30:] = list(Y_pred)

    # df = df[-30:]

    for key, val in zip(df[-30:].date, ddf[-30:]f.footfall):
        elastic.indexData("forecast",{key: val})

    
    ## use streamJob for CI when needed
