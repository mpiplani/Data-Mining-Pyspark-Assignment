
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


# In[12]:


json_data = json_data.map(lambda f : (f['user'], f['created_at']))


# In[128]:


total_tweets=json_data.count()


# In[129]:


unique_users= json_data.map(lambda f: f[0]["id"]).distinct().count()




# In[13]:


top_followers= json_data.map(lambda f:[f[0]["screen_name"],f[0]["followers_count"]]).sortBy(lambda x: x[1], False)



# In[158]:


top_result=top_followers.take(3)


# In[149]:


tues_tweets = json_data.filter(lambda f: "Tue" in f[1]).count()


# In[160]:


output_dict = {"n_tweet":total_tweets,"n_user":unique_users,"popular_users":top_result,"Tuesday_Tweet":tues_tweets}


with open(output_path, 'w') as file:
    json.dump(output_dict, file)
    
sc.stop()

