# load and evaluate a saved model
import json

from keras.models import load_model


# load model
def predict(dataset):
    model = load_model('model.h5')
    val_set2 = dataset.copy()
    result = model.predict(dataset)
    y_classes = result.argmax(axis=-1)
    val_set2['class'] = y_classes.tolist()
    dic = val_set2.to_dict(orient='records')
    return json.dumps(dic, indent=4, sort_keys=False)
