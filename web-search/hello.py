from flask import Flask, render_template, flash, request
from wtforms import Form, TextField, TextAreaField, validators, StringField, SubmitField
import pandas as pd
import pyarrow.parquet as pq
import json

#TODO: implement page rank

# app configuration
DEBUG = True
app = Flask(__name__)
app.config.from_object(__name__)
app.config['SECRET_KEY'] = '5i11yg00s3'


PQPATH='/Users/lxu213/data/ad-free-search-engine/spark-warehouse/output_warc/part-00000-91c58b49-e707-444a-9be2-335b8d9f3aa5-c000.snappy.parquet'
PQPATH='/Users/lxu213/data/ad-free-search-engine/spark-warehouse/add_count/tf_idf.parquet'
data = pq.read_table(PQPATH, nthreads=4).to_pandas()


class SearchBox(Form):
    query = TextField(validators=[validators.required()])


@app.route("/", methods=['GET', 'POST'])
def hello():
    form = SearchBox(request.form)  
    print form.errors
    query = '' 
    kw_dict = []

    if request.method == 'POST':
        query = request.form['query']
        kw_data = data[['val', 'tf-idf']].loc[data['keywords'].isin(query.lower().split())][:50]
        for row in kw_data.sort_values('tf-idf', ascending=False)['val']:
            kw_dict.append(json.loads(row))

        if not form.validate():
            flash('A search query is required.')

    return render_template('searchpage.html', form=form, query=query, data=kw_dict)

if __name__ == "__main__":
    app.run(host='0.0.0.0')








