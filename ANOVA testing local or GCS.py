#Dependencies
import pandas as pd
import statsmodels as sm
import numpy as np
from statsmodels.formula.api import ols
from statsmodels.stats.anova import anova_lm
from statsmodels.graphics.factorplots import interaction_plot
import matplotlib.pyplot as plt
from scipy import stats

#Upload data from local
datafile = "<local_path>"
df = pd.read_csv(datafile,
#skiprows=30 #If there are headers/header rows
)

#OR upload from a GCS bucket
import os
from google.cloud import storage
CLOUD_STORAGE_BUCKET = os.environ.get('gs://ceasars-test-dco-anova')

def get(self):
    bucket_name = os.environ.get('BUCKET_NAME',
                                 app_identity.get_default_gcs_bucket_name())

    self.response.headers['Content-Type'] = 'text/plain'
    self.response.write('Demo GCS Application running from Version: '
                        + os.environ['CURRENT_VERSION_ID'] + '\n')
    self.response.write('Using bucket name: ' + bucket_name + '\n\n')

#Define Anova functions and "Pivot"/Overview tables
#Impressions, Conversions and Conversion Rate are used here for testing

def conv_rate_pivot_tbl_xy(x,y):
    # two-way pivot table
    # args (x,y): x = variable 1 , y = variable 2
    pvdf_xy = df.groupby([x,y],as_index=False)[('Impressions','Conversions')].sum()
    pvdf_xy['ConvRate'] = pvdf_xy.Conversions/pvdf_xy.Impressions
    return pvdf_xy

def conv_rate_pivot_tbl_x(x):
    # one-way pivot table
    # arg (x): x = variable
    pvdf_x = df.groupby([x],as_index=False)[('Impressions','Conversions')].sum()
    pvdf_x['ConvRate'] = pvdf_x.Conversions/pvdf_x.Impressions
    return pvdf_x

def two_way_anova_test(x,y):
    # two-way pivot table
    # args (x,y): x = variable 1 , y = variable 2
    formula = 'ConvRate ~ x + y + x:y'
    model = ols(formula,data=df).fit()
    aov_table = anova_lm(model, typ=1)
    return aov_table

def one_way_anova_test(x):
    # one-way anova test
    # arg (x): x = variable
    formula = 'ConvRate ~ x'
    model = ols(formula,data=df).fit()
    aov_table = anova_lm(model, typ=2)
    return aov_table

# Add conversion rate column
df['ConvRate']=df.Conversions / df.Impressions

# Pivot Tables
print('<dimension1> & <dimension2>''\n\n',
      conv_rate_pivot_tbl_xy(df.<dimension1>,df.<dimension2),
     '\n')
print('Layout''\n\n',
      conv_rate_pivot_tbl_x(df.<dimension1>),
     '\n')
print('Version''\n\n',
      conv_rate_pivot_tbl_x(df.<dimension2>))

#Anova Tests
aov_table = two_way_anova_test(df.<dimension1>,df.<dimension2>)

print('One Way''\n\n',
      one_way_anova_test(df.<dimension1>),'\n')
print('Two Way''\n\n',
      two_way_anova_test(df.<dimension1,df.<dimension2>))

# Extraction from table and output
p_v0 = aov_table.iloc[0,4]
p_v1 = aov_table.iloc[1,4]

if p_v0 < .05:
    print(aov_table.index.values[0], 'Result IS significant')
else:
    print(aov_table.index.values[0], 'Result IS NOT significant')

if p_v1 < .05:
    print(aov_table.index.values[1], 'Result IS significant')
else:
    print(aov_table.index.values[1], 'Result IS NOT significant')
