#Ingesting Data from BigQuery or GCS into Datalab or AI Notebooks
## GCS Bucket pull

import google.datalab.storage as storage
import pandas as pd
from io import BytesIO

mybucket = storage.Bucket('<bucket>')
data_csv = mybucket.object('<object>')

uri = data_csv.uri
get_ipython().run_line_magic('gcs', 'read --object $uri --variable data')

df = pd.read_csv(BytesIO(data)
#,skiprows=81 #If there are headers
)
df.head()

## BQ pull with SQL

# (Spin up Datalab VM on Cloud Shell with a command similar to the following:)
#datalab create babyweight --zone us-central1-a --no-create-repository


# Python (will run into a credential issue if not run on a Datalab VM spun up on project with BQ)
get_ipython().system('pip install --upgrade google-cloud-bigquery')

from google.cloud import bigquery

client = bigquery.Client()

sql = """
SELECT creative_id,
la.description as labels
FROM `sauron-230322.oculi_30602.image_label_detection`, UNNEST(label_annotations) as la
WHERE la.description NOT IN ('Font','Text','Brand','Logo','Vehicle','Banner','Car','Advertising','Line','Graphics')
ORDER BY Creative_ID
"""
df = client.query(sql).to_dataframe()
df.head()
