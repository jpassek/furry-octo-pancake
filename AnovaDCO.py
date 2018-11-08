
# coding: utf-8

# In[6]:


# Import libraries
# import pandas as pd
# import statsmodels as sm
# from statsmodels.formula.api import ols
# from statsmodels.stats.anova import anova_lm
# from statsmodels.graphics.factorplots import interaction_plot
# import matplotlib.pyplot as plt
# from scipy import stats
# import gcsfs


# In[32]:


# Upload
#datafile = "/Users/justinpassek/Documents/6469_Caesars_Dynamic_Elements_20181030_055039_2000227069.csv"
#df = pd.read_csv(datafile,skiprows=81)
fs = gcsfs.GCSFileSystem(project='anovadco-220919')
with fs.open('ceasars-test-dco-anova/6469_Caesars_Dynamic_Elements_20181103_055047_2003198075.csv') as f:
    df = pd.read_csv(f,skiprows=81)
print(df)


# In[25]:


# Mutate
# change column names
new_names = {'Mercury Res Module - Sales : RES Step 5 (Confirmation Page): Total Conversions':'Conversions',
            'Feed 1 - Reporting dimension 2 value':'CTA',
            'Feed 1 - Reporting dimension 1 value':'Image'}
df.rename(columns=new_names,inplace=True)
# add conversion rate column
df['ConvRate']=df.Conversions / df.Impressions


# In[26]:


# Clean
# 5000 impression minimum
df = df[df["Impressions"]>5000]
# rm unidentifiable images and cta
df = df[df.Image != '(not set)']
df = df[df.CTA != '(not set)']
df = df[df.Image != 'Test Headline']
df = df[df.CTA != 'Test Headline']
df = df[df.Image != '---']
df = df[df.CTA != '---']
# rm nas
df = df.dropna(subset=["CTA", "Image"])
# narrow to relevant columns
imp_columns = ["Image","CTA","ConvRate","Impressions","Conversions"]
df = df[imp_columns]


# In[27]:


# "pivot" (pv) tables to see Conversion Rate for each Image/CTA combination
## v0 refers to variable 0 which is Image
## v1 refers to variable 1 which is CTA
pvdf = df.groupby(['Image','CTA'],as_index=False)[('Impressions','Conversions')].sum()
pvdf['ConvRate'] = pvdf.Conversions/pvdf.Impressions
pvdf_v0 = df.groupby(['Image'],as_index=False)[('Impressions','Conversions')].sum()
pvdf_v0['ConvRate'] = pvdf_v0.Conversions/pvdf_v0.Impressions
pvdf_v1 = df.groupby(['CTA'],as_index=False)[('Impressions','Conversions')].sum()
pvdf_v1['ConvRate'] = pvdf_v1.Conversions/pvdf_v1.Impressions


# In[29]:


# anova test
ConvRate_Total = sum(df.Conversions)/sum(df.Impressions)
formula = 'ConvRate ~ C(Image) + C(CTA) + C(Image):C(CTA)'
model = ols(formula,data=df).fit()
aov_table = anova_lm(model, typ=1)
print(aov_table)


# In[15]:


# extraction from table and output
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

