#Set up environment
import pandas as pd
from scipy import stats

#Upload data from local drive
datafile = "<path>"
<Variable>_Detection = pd.read_csv(datafile)

#Create "cuts"/buckets of the dataset to test
<Variable> = <Variable>_Detection[<Variable>_Detection.<Variable>_Detection == "<Variable>"]
No_<Variable> = <Variable>_Detection[<Variable>_Detection.<Variable>_Detection == "No_<Variable>"]

#Run the Welch T-test
stats.ttest_ind(
    <Variable>.<Metric>,
    No_<Variable>.<Metric>,
    equal_var = False)
