import sys
import pdb
import math
import itertools
from pyspark import SparkContext

def get_baskets_python(filename):
    # Equilvent to get_baskets_spark only if the input data is sorted by the first column, which is true for our dataset
    # Since it's sorted, read content line by line
    # If a new user_id is found:
    #     1. Save the current basket to baskets
    #     2. Create a new basket, with one item artist_id
    # Else:
    #     1. Add artist_id to the current basket
    # Remember to save the current basket to baskets at the end
    basket_name = None # user_id, owner of the bucket
    basket = []
    baskets = []

    for line in open(filename, 'r', encoding='utf-8'):
        pair = process_line(line[:-1])

        if len(pair) == 0:
            continue
        else:
            user_id, artist_id = pair[0] #accommodate return format of flatMap

        if user_id != basket_name:
            if basket_name is not None:
                baskets.append((basket_name, basket))
            basket = [artist_id]
            basket_name = user_id
        else:
            basket.append(artist_id)

    baskets.append((basket_name, basket))
    #the format of baskets is:
    #[
    # (user_id1, [artist_id1, artist_id2, artist_id3, ...]),
    # (user_id2, [artist_id4, artist_id5, artist_id6, artist_id7, ...]),
    # ...,
    #]
    return baskets

def process_line(line):
    #Convert each line into a 4 elements list
    #1. Assume line ending '\n' has been removed. In spark this is removed automatically, in python, it's done manually by line[:-1]
    #2. Split a line by separator: '\t'. tsv stands for tab separated values, csv stands for comma separated values
    #3. Remove empty string by comparing to ''. .split('\t') will give you empty string if two `\t` are together. You could try it out by: 'abc\t\t123'.split('\t')
    #4. If the number of tokens equals 4, return the first 2 tokens, they are user_id and artist_id
    #5. If not, some field is missing, skip that line by returning []
    tokens = [t for t in line.split('\t') if t != '']
    if len(tokens) == 4:
        return [tokens[:2]] #one-element list.
    else:
        return []

def get_baskets_spark_rdd(filename, sc, partition):
    #.flatMap expect a list of items is returned. Could be zero or many items. Each item would be in format: (user_id, artist_id)
    #.groupByKey assume the first element is the key. Items having the same key will be merged to a list, the result format is: (user_id, [artist_id1, artist_id2, artist_id3, ...])
    baskets_rdd = sc.textFile(filename, partition)\
        .flatMap(process_line)\
        .groupByKey()

    return baskets_rdd

def get_baskets_spark(filename):
    # Can't use the same logic as in pure python, because spark will read file in parallel.
    # Use groupByKey to merge artist_id liked by the same user_id into a list.
    # Another benfits is, this logic allows the file lines been shuffled, which is not allowed in the python logic
    sc = SparkContext("local","PySpark Tutorial") #In Q1, Q2, Q3, Q4 spark is only used for reading data

    # Use only 1 core
    baskets = get_baskets_spark_rdd(filename, sc, 1).collect()

    sc.stop()
    return baskets

def get_item_dict(baskets):
    # Assign each item (artist_id) an integer to be used as index in the matrix
    item_dict = {}
    for basket in baskets:
        items = basket[1] #basket[0] is user_id, basket[1] is a list of artist_id
        for item in items:
            if item not in item_dict:
                #len(item_dict) is the size of dictionary
                #When adding the first item, it evaluates to 0, adding the second item, it evaluates to 1
                #So the range of assigned integers is [0, #items-1]
                item_dict[item] = len(item_dict)
    return item_dict

def inverse_dict(d):
    # {key: value} will become {value: key}
    return {v: k for k, v in d.items()}

def tuple_wrapper(s):
    if type(s) is not tuple:
        s = (s, )
    return s

def get_possible_k(item_dict, k):
    possible_k = {}
    for pair in itertools.combinations(item_dict.keys(), 2):
        pair_set = set()
        for i in range(2):
            pair_set = pair_set.union(tuple_wrapper(pair[i]))
        if len(pair_set) == k:
            possible_k[frozenset(pair_set)] = [pair[0], pair[1]]
    return possible_k

# Wrap in a function to be reused in Q3
def triangular_matrix_method(baskets, support, item_dict=None, k=2):
    if item_dict is None:
        item_dict = get_item_dict(baskets)  #item -> integer
    else:
        # Only used in Q4, Q5
        # item_dict has been computed -> it's aprior method. Filter baskets to remove infrequent items
        # When k=2, infrequent single items have been removed from baskets. No need to do it again when k>=3
        # baskets will be modified!
        if k == 2:
            for i in range(len(baskets)):
                basket = baskets[i]
                items = basket[1]
                items_filterd = [item for item in items if item in item_dict]
                baskets[i] = (basket[0], items_filterd)

    item_dict_inv = inverse_dict(item_dict) #integer -> item. Inverse dict will be used when printing results
    n = len(item_dict)

    # Only used in Q4, Q5
    if k >= 3:
        possible_k = get_possible_k(item_dict, k)

    # Storage space is pre-allocated. Similiar to ArrayList
    # Convert 2D index to 1D index
    # Conversion logic: https://stackoverflow.com/questions/27086195/linear-index-upper-triangular-matrix
    tri_matrix = [0] * (n * (n-1) // 2) # n * (n-1) always be even for n >= 2, use true division to make it a int

    # Key logic: Upper Triangular Matrix Method
    for basket in baskets:
        # Take a basket (user), iterate all items (artist)
        items = basket[1]

        # Checkout https://docs.python.org/3.6/library/itertools.html#itertools.combinations
        # Equivalent to a double loop, but more concise
        for kpair in itertools.combinations(items, k):
            # kpair is a k element tuple, kpair[i] is item (string)
            if k >= 3:
                pair_set = frozenset(kpair)

                # Now kpair is a 2 element pair
                kpair = possible_k.get(pair_set, None)
                if kpair is None:
                    continue

            # i, j is integer index
            i = item_dict[kpair[0]]
            j = item_dict[kpair[1]]

            # Keep sorted in upper triangular order
            if i > j:
                j, i = i, j

            # Convert 2D index to 1D index
            idx = int((n*(n-1)/2) - (n-i)*((n-i)-1)/2 + j - i - 1)
            # Increase count by 1
            tri_matrix[idx] += 1

    # Extract results
    frequent_itemset_list = []
    for idx in range(len(tri_matrix)):
        # Convert 1D index to 2D index
        i = int(n - 2 - math.floor(math.sqrt(-8*idx + 4*n*(n-1)-7)/2.0 - 0.5))
        j = int(idx + i + 1 - n*(n-1)/2 + (n-i)*((n-i)-1)/2)

        count = tri_matrix[idx]
        item_i = item_dict_inv[i]
        item_j = item_dict_inv[j]

        # Keep sorted in ascii order. item_i, item_j are strings or tuple of strings
        # This implementation is ready for k>=3
        item_all = set()
        for item in (item_i, item_j):
            item_all = item_all.union(tuple_wrapper(item))

        item_all = tuple(sorted(list(item_all)))

        # apply support threshold
        if count >= support:
            frequent_itemset_list.append((item_all, count))

    # First sorted by the occurrence count in decreasing order
    # Then sort by ascii order of the first item, in ascending order
    # Then sort by ascii order of the second item, in ascending order
    frequent_itemset_list = sorted(frequent_itemset_list, key=lambda x: [-x[1]] + list(x[0]))
    return frequent_itemset_list

def print_frequent_itemsets(itemsets):
    for frequent_itemset in itemsets:
        print(frequent_itemset)

    print(f'Total: {len(itemsets)}')

# python q1.py 10 lastfm/sample.tsv
if __name__ == '__main__':
    support = int(sys.argv[1]) #Increase if run on large dataset
    filename = sys.argv[2]

    # You should be able to understand both logic
    if len(sys.argv) >= 4 and sys.argv[3] == 'spark':
        baskets = get_baskets_spark(filename)
    else:
        baskets = get_baskets_python(filename)
    print("hey")
    print(baskets)

    # call the algorithm function
    itemsets = triangular_matrix_method(baskets, support)
    print_frequent_itemsets(itemsets)
