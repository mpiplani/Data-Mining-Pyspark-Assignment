
import sys
import time
import json

from pyspark import SparkContext

sc = SparkContext("local[*]","PySpark Tutorial")


input_path = sys.argv[1]
output_path = sys.argv[2]

data = sc.textFile(input_path)

## Loading the data in json format.
json_data = data.map(lambda f: json.loads(f))



# In[208]:


json_data_1 = json_data.map(lambda f : f["retweet_count"])

# In[213]:


mean_retweet = json_data_1.mean()


# In[214]:


std_retweet = json_data_1.stdev()



# In[212]:


max_retweet = json_data_1.sortBy(lambda x : x ,False).take(1)




# In[215]:


output_dict_1 = {"mean_retweet":mean_retweet,"max_retweet":max_retweet[0],"stdev_retweet":std_retweet}




# In[217]:


with open(output_path, 'w') as file:
    json.dump(output_dict_1, file)
    
sc.stop()