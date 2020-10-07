import numpy as np
import pandas as pd
import sys
import time

input_path = sys.argv[1]
train_file=sys.argv[2]
test_file=sys.argv[3]
output_path=sys.argv[4]

train=pd.read_csv(train_file,encoding='utf-8')

start_time=time.time()
#dimensions for the matrix
users = list(set(train["userId"]))
items = list(set(train["movieId"]))

#indexing dictionaries
movie_index = {items[i]: i for i in range(len(items))}
users_index = {users[i]: i for i in range(len(users))}

user_list = train["userId"].tolist()
movie_list = train["movieId"].tolist()
rating_list = train["rating"].tolist()
ratings = [[0]*(len(items)) for user in users]

#matrix_formulation
for i in range(0,len(train)):
    item = movie_list[i]
    user = user_list[i]
    value = rating_list[i]
    
    ratings[users_index[user]][movie_index[item]] = value


global_rating=sum(rating_list)/len(rating_list)
#print(global_rating)


def prediction(P,Q):
    #print(np.dot(P.T,Q))
    return np.dot(P.T,Q)
 
   
lmbda = 0.02
lmbda_1=0.01
k = 8
m, n = len(ratings),len(ratings[0]) 
n_epochs = 38
alpha=0.002
alpha_1=0.0001
#P =  np.random.normal(scale=1./k,size=(k,m)) 
P = np.random.rand(k,m)

bias_user=np.random.normal(0, 0.5 ,m)

#Q =  np.random.normal(scale=1./k,size=(k,n)) 
Q = np.random.rand(k,n)
bias_item=np.random.normal(0, 0.5 ,n)
           


#svd
for epoch in range(n_epochs):
    for i in range(len(ratings)): #user
        for j in range(len(ratings[0])):#item
            if ratings[i][j]>0:
              
                e = ratings[i][j] - prediction(P[:,i],Q[:,j])  
                
                P[:,i] += alpha * ( e * Q[:,j] - lmbda * P[:,i]) 
                Q[:,j] += alpha * ( e * P[:,i] - lmbda * Q[:,j])  
                bias_user[i]+=alpha_1*(e-lmbda_1*(bias_user[i]))
                bias_item[j]+=alpha_1*(e-lmbda_1*(bias_item[j]))
    
    
        
#print(time.time()-start_time)   

data1=pd.read_csv(test_file,encoding='utf-8')

final_result=[]
timestamp=[]
for index,row in data1.iterrows():
   
    user_id =users_index[row[0]]
    rating_user = 0
    length_user = 0
    for i in ratings[user_id]:
        if i != 0:
            rating_user += i
            length_user += 1
    user_bias=rating_user/length_user
    if row[1] not in movie_index:
        value=user_bias#+bias_user[user_id]/len(bias_user)
               
        final_result.append(value)
        
    else:
        movie=movie_index[row[1]]
       
        value =np.dot(P[:,user_id].T, Q[:,movie])#+global_rating
        value+=bias_item[movie]/len(bias_item)
        value+=bias_user[user_id]/len(bias_user)
        #value+=user_bias-global_rating
        #value+=bias_user[user_id]+bias_item[movie]+global_rating
        if value>5.0:
            value=5.0
        if value<0.5:
            value=0.5
        final_result.append(value)
    timestamp.append(time.time())
    
    
    
data1["rating"]=final_result
data1["timestamp"]=timestamp
data1.to_csv(output_path, encoding='utf-8',index=False)
print(time.time()-start_time)
