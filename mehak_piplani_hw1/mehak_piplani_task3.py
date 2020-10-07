import sys
import time
import json
from pyspark import SparkContext

sc = SparkContext("local[*]","PySpark Tutorial")

input_path = sys.argv[1]
output_path = sys.argv[2]
data = sc.textFile(input_path)

## Loading the data in json format.


# In[3]:


lines=data.flatMap(lambda x: x.split(' '))


# In[17]:


chunk = lines.filter(lambda word: word=="|********************").count()



# In[19]:


frequent_list = lines.map(lambda word: (word, 1)).reduceByKey(lambda a, b:a + b).sortBy(lambda x: x[1], False)


# In[20]:


frequent_list.count()


# In[21]:


max_word = frequent_list.take(1)


# In[22]:



count_mindless = lines.filter(lambda word: word=="mindless").count()



# In[244]:


output_dict_1 = {"max_word":max_word[0],"mindless_count":count_mindless,"chunk_count":chunk}


# In[245]:



with open(output_path, 'w') as file:
    json.dump(output_dict_1, file)


sc.stop()
