import pandas as pd
from flask import Flask, json, request, Response
import requests
import model_trainer

app = Flask(__name__)
app.config["DEBUG"] = True


@app.route('/training-cp/<model>', methods=['POST'])
def train_models(model):
    r = requests.get('http://localhost:5000/training-db/diabetes')
    j = r.json()
    df = pd.DataFrame.from_dict(j)
    if model == "mlp":
        js = model_trainer.train(df.values)
        resp = Response(js, status=200, mimetype='application/json')
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.headers['Access-Control-Allow-Methods'] = 'POST'
        resp.headers['Access-Control-Max-Age'] = '1000'
        return resp
    else:
        return json.dumps({'message': 'the given model is not supported dropped'},
                          sort_keys=False, indent=4), 400


app.run(host='0.0.0.0', port=5001)
