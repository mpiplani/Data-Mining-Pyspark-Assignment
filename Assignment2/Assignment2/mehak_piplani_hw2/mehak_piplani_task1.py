import sys
import time
import json
import pandas as pd
import collections
import itertools
from itertools import chain, combinations

input_path = sys.argv[1]
output_path = sys.argv[2]
support = int(sys.argv[4])
interest = float(sys.argv[3])
#support=30
#interest=0.5

time_start=time.time()

#Read data
data=pd.read_csv(input_path,encoding='utf-8')
data = data.drop(data[(data.rating<5.0)].index)
#print(data.head())

#get frequency of items   
def get_item_counter(basket_name):
    item_counter = collections.Counter()
    for basket in basket_name:
        items = basket_name[basket]
        item_counter.update(items)
    return item_counter
    
#calculate support
def getSupport(item):
    if len(item)==1:
        return item_counter[item[0]]
    else:
        item=tuple(item)
        return frequency_set[item]
#check whether subsets are frequent              
def check(c,items):
    if len(c) == 2:
        if c[0] in items and c[1] in items:
            return True
        else:
            return False
    
    subset_of_c =itertools.combinations(c, len(c)-1)
    
    if any([ tuple(sorted(x)) not in items for x in subset_of_c]):
    
        return False
    return True
#generate tuples    
def generate_ck(items, k):
    if k == 2:
        combs = list(set([tuple(sorted([x, y])) for x in items for y in items if len((x, y)) == k and x != y]))
    else:
        test=set()
        for x in items:
            for y in items: 
                val=tuple(sorted(set(x).union(y)))
                if len(val)==k and x!=y:
                    if check(val,items):
                        test.add(val)
        
        combs=list(test)     
    #combs=[x for x in combs if check(x,items)]
    
      
    return combs


 
   
#making baskets
basket_name = {}
basket = []
baskets = []
#print(len(data))
rules=[]
for i in data.iterrows():
    if i[1][0] not in basket_name:
        basket_name[i[1][0]]=[i[1][1]]
    else:
        basket_name[i[1][0]].append(i[1][1])
   
#print(basket_name.keys())

#finding frequent items
item_counter =get_item_counter(basket_name)
itemsets = sorted([(int(k), v) for k, v in item_counter.items() if v >= support], key=lambda x: x[1], reverse=True)

frequent_1 = [x[0] for x in itemsets]
#print("bs",frequent_1)
for basket in basket_name:
    items = basket_name[basket]
    items_filterd = [item for item in items if item in frequent_1]
    basket_name[basket]=items_filterd
time_start = time.time()
#applying apriori
k=1
total_set={}
frequency_set={}
while frequent_1:
    #print(len(frequent_1))
    ck_1=generate_ck(frequent_1,k+1)
    if len(ck_1)==0:
        break
    #print(len(ck_1))
    item_count={}
    intersect=[]
    for j in ck_1:
        for basket in basket_name:
            if set(j).issubset(basket_name[basket]):
                if j in item_count:
                    item_count[(tuple(sorted(j)))] += 1
                else:
                    item_count[(tuple(sorted(j)))] =1             
                     
    lk_next = list()
    for item in item_count:
   
        if item_count[item] >= support:
           
            lk_next.append(item)
            
            subsets=list(chain(*[combinations(item, i + 1) for i, j in enumerate(item)]))
      
            for element in subsets:
                remain = tuple(set(item)-set(element))
                if len(remain) == 1:
                
                    confidence = item_count[item]/getSupport(element)
                    interest_current= confidence-float(item_counter[remain[0]])/len(basket_name)
                    if interest_current >= interest:
                        rules.append([list(element), list(remain)[0], interest_current, item_count[item]])
        
    frequency_set.update(item_count)
    if not lk_next:  
        break
    
    total_set[k+1]=lk_next
    
    frequent_1=lk_next
    if len(ck_1)<k+1:
        break
    k += 1
                    
#print(time.time()-time_start)

  
rules.sort(key=lambda x: (-x[2], -x[3], x[0], x[1]))
#print(time.time()-time_start)
import json
with open(output_path, 'w') as file:
    json.dump(rules, file)
print(time.time()-time_start)