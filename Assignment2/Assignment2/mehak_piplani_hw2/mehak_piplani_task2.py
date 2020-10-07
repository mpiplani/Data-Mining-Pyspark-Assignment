import sys
import time
import json
import pandas as pd
import collections
import itertools
from itertools import chain, combinations
import scipy.sparse
import scipy
import numpy as np

input_path = sys.argv[1]
train_file=sys.argv[2]
test_file=sys.argv[3]
output_path=sys.argv[4]

start_time=time.time()
data=pd.read_csv(input_path, encoding='utf-8')
movie_shingles={}
genre_dict={}
#print(data.head())
row_idx = []
col_idx=[]
map_movie_id={}
hash_size =  19

genre_set=set()
for j,i in data.iterrows():
    shingles=[]
    map_movie_id[i[0]]=j
    for genre in i[2].split("|"):
       
        shingles.append(hash(genre)%hash_size)          
    movie_shingles[i[0]]=shingles

id_map_movie = {v: k for k, v in map_movie_id.items()}

N = 14
B = 30
R = 3
def get_hash_config(k_shingle):
    c  = 1048583   #131101 #
        
    rnds = np.random.choice(2**20, (2, k_shingle), replace=False) 
    
    return rnds[0], rnds[1], c

count = len(movie_shingles.keys())
#print(count)
(a, b, c) = get_hash_config(90)

#print(a,b,c)
a = a.reshape(1, -1)
M = np.zeros((90, count), dtype=int)
for i,s in enumerate(list(movie_shingles)):
    row_idx = np.asarray(movie_shingles[s]).reshape(-1, 1)
    
    m = (np.matmul(row_idx, a) + b) % c
    m_min = np.min(m, axis=0)
    M[:, i] = m_min

#print(M.shape)
    

def LSH(M, b, r, k_band):
	lines = M.shape[1]

	bucket_list = []
	for band_index in range(b):
		row_idx = []
		col_idx = []

		row_start = band_index * r
		for c in range(lines):
			v = M[row_start:(row_start+r), c]
			v_hash = hash(tuple(v.tolist())) % k_band
			row_idx.append(v_hash)
			col_idx.append(c)
			#m[v_hash, c] = 1
		data_ary = [True] * len(row_idx)

		m = scipy.sparse.csr_matrix((data_ary, (row_idx, col_idx)), shape=(k_band, lines), dtype=bool)
		bucket_list.append(m)

	return bucket_list
k_band=2**5
bucket_list = LSH(M, B, R, k_band)
#print(len(bucket_list))


data1=pd.read_csv(test_file, encoding='utf-8')

train_file = pd.read_csv(train_file, encoding='utf-8')
ratings=train_file.drop(columns=["timestamp"])
ratings = ratings.set_index("userId")
final_result=[]
timestamp=[]
start_time = time.time()
for index,row in data1.iterrows():

    user_id=row[0]
    movie=row[1]
    movie_id=map_movie_id[movie]
    
    movie_match=ratings.loc[user_id].set_index("movieId").to_dict()['rating']
    
    movie_list=set(movie_match.keys())
    
    
    candidates = set()
    for band_index in range(B):
        row_start = band_index * R
        v = M[row_start:(row_start+R), movie_id]
        v_hash = hash(tuple(v.tolist())) % k_band

        m = bucket_list[band_index]
       
        candidates = candidates.union(m[v_hash].indices)
    

    
    movie_list=set([map_movie_id[x] for x in movie_list])
  
    
    intersection=candidates.intersection(movie_list)
    
    if len(intersection)==0:
        final_result.append(np.sum(list(movie_match.values()))/len(movie_match))
    else:
        sim_list=0
        rating_list=0
        
        query_set = set(movie_shingles[movie])
       
        for col_idx in intersection:           
            movie=id_map_movie[col_idx]
            col_set = set(movie_shingles[movie])
        
            sim = len(query_set & col_set) / len(query_set | col_set)
           
            if sim>=0.3:
                rating_list+=movie_match[movie]*sim
                sim_list+=sim
        if sim_list == 0:
            final_result.append(np.sum(list(movie_match.values()))/len(movie_match))
        else:
            final_result.append(rating_list/sim_list)
    if final_result[-1]<0.5:
        final_result[-1]=0.5
    if final_result[-1]>5.0:
        final_result[-1]=5.0
    timestamp.append(time.time())
     
    
   
data1["rating"]=final_result
data1["timestamp"]=timestamp
data1.to_csv(output_path, encoding='utf-8',index=False)
print(time.time()-start_time)