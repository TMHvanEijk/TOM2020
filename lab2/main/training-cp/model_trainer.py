# MLP for Pima Indians Dataset saved to single file
# see https://machinelearningmastery.com/save-load-keras-deep-learning-models/
from keras.layers import Dense
from keras.models import Sequential
import json


# load pima indians dataset
def train(dataset):
    # split into input (X) and output (Y) variables
    X = dataset[:, 0:8]
    Y = dataset[:, 8]
    # define model
    model = Sequential()
    model.add(Dense(12, input_dim=8, activation='relu'))
    model.add(Dense(8, activation='relu'))
    model.add(Dense(1, activation='sigmoid'))
    # compile model
    model.compile(loss='binary_crossentropy', optimizer='adam', metrics=['accuracy'])
    # Fit the model
    model.fit(X, Y, epochs=150, batch_size=10, verbose=0)
    # evaluate the model
    scores  = model.evaluate(X, Y, verbose=0)
    print(model.metrics_names)
    text_out = {
        "accuracy:": scores[1],
        "loss": scores[0],
    }
    # save model and architecture to single file
    model.save("model.h5")
    print("Saved model to disk")
    return json.dumps(text_out, sort_keys=False, indent=4)
