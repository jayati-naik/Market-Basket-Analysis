import itertools
import sys
import time

from pyspark import SparkConf, SparkContext


def get_final_frequent_itemsets(iterator, candidates):
    final = []
    counter = {}

    for sample in iterator:
        for candidate in candidates:
            if set(candidate).issubset(sample[1]):
                if candidate not in counter:
                    counter[candidate] = 1
                else:
                    counter[candidate] += 1

    for i in counter:
        if i not in final:
            final.append((i, counter[i]))

    return final


def generate_powersets(baskets, index, dataset, result, size, s):
    powersets = {}
    current_pairs = []

    pair = list(
        itertools.chain.from_iterable(itertools.combinations(list(dataset), r) for r in range(size, size + 1)))

    for p in pair:
        for basket in baskets:
            if set(p).issubset(basket[1]):
                if p not in powersets:
                    powersets[p] = 1
                else:
                    powersets[p] += 1

    print(str(index) + ".powersets: " , len(powersets))

    for s1 in powersets:
        if powersets[s1] >= int(s):
            s1 = tuple(sorted(s1))
            current_pairs.append(s1)
            result.append(s1)

    current_candidates = list(set(itertools.chain.from_iterable(current_pairs)))
    print(str(index) + ".current_candidates: " + str(len(current_candidates)))

    return current_candidates


# implement A-priori
def a_priori(iterator, index, s):
    final_iterator = []
    singletons = {}
    prev_candidate_set = []
    result = []
    baskets = []
    print(str(index) + ".support", s)

    for business_user_tuple in iterator:
        baskets.append(list(business_user_tuple))

    for i, j in enumerate(baskets):
        value_ids = j[1]
        for value in value_ids:
            if value not in singletons:
                singletons[value] = 1
            else:
                singletons[value] += 1

    for k in singletons:
        if singletons[k] >= int(s):
            prev_candidate_set.append(k)
            result.append((k,))

    print(str(index) + ".prev_candidate_set", len(prev_candidate_set))
    # start_time = time.time()
    keep_true = True
    counter = 2

    while keep_true:
        curr_candidate_set = generate_powersets(baskets, index, prev_candidate_set, result, counter, s)
        if len(curr_candidate_set) == 0:
            keep_true = False
        prev_candidate_set = curr_candidate_set
        counter += 1

    print(str(index) + ".Counter: " + str(counter))
    # end_time = time.time()
    # time_duration = end_time - start_time
    # print(str(index) + ".Duration generate power sets: " + str(time_duration))

    for i in result:
        if i not in final_iterator:
            final_iterator.append(i)

    return final_iterator

def apply_son_algorithm(customer_product_data, s, p):
    # start_time = time.time()
    # SON Algorithm : pass 1
    candidates = customer_product_data.mapPartitionsWithIndex(lambda index, x: a_priori(x, index, int(s)/p)) \
        .distinct().collect()

    '''
    end_time = time.time()
    time_duration = end_time - start_time
    print("Duration pass 1: " + str(time_duration))
    '''

    # SON Algorithm : pass 2
    # start_time = time.time()
    frequent_itemsets = customer_product_data.mapPartitions(lambda x: get_final_frequent_itemsets(x, candidates)) \
        .reduceByKey(lambda x, y: x + y) \
        .filter(lambda x: x[1] >= int(s)) \
        .map(lambda x: x[0]) \
        .collect()

    '''
    end_time = time.time()
    time_duration = end_time - start_time
    print("Duration Pass 2 : " + str(time_duration))
    '''

    candidate_formatted = {}
    for i in candidates:
        if len(i) not in candidate_formatted:
            candidate_formatted[len(i)] = [i]
        else:
            candidate_formatted[len(i)].append(i)

    frequent_itemsets_formatted = {}
    for j in frequent_itemsets:
        if len(j) not in frequent_itemsets_formatted:
            frequent_itemsets_formatted[len(j)] = [j]
        else:
            frequent_itemsets_formatted[len(j)].append(j)

    file = open(output_file_path, 'w')
    file.write("Candidates:\n")
    for i in candidate_formatted:
        file.write(
            str(sorted(candidate_formatted[i])).replace(",)", ")").replace("[", "").replace("]", "").replace(', ',
                                                                                                             ',') + "\n\n")
    file.write("Frequent Itemsets:\n")
    for j in frequent_itemsets_formatted:
        file.write(
            str(sorted(frequent_itemsets_formatted[j])).replace(",)", ")").replace("[", "").replace("]", "").replace(
                ', ', ',') + "\n\n")
    file.close()


if __name__ == '__main__':
    conf = SparkConf().setAppName("HW2")
    sc = SparkContext(conf=conf)

    cmd_args = str(sys.argv)
    cmd_args = cmd_args.split(", ")
    filter_threshold = cmd_args[1].replace("'", "")
    support = cmd_args[2].replace("'", "")
    input_file_path = cmd_args[3].replace("'", "")
    output_file_path = cmd_args[4].replace("'", "").replace(']', '')

    start_time = time.time()
    # Task 2.1 starts here
    partition_count = 8
    customer_data = sc.textFile(input_file_path,partition_count)
    sc.setLogLevel('ERROR')

    processed_customer_data = customer_data.map(lambda line: line.split(",")) \
        .filter(lambda line: 'TRANSACTION_DT' not in line[0]) \
        .map(lambda x: (str(x[0]).replace('"', '') + '-' + str(int(x[1].replace('"', ''))), str(x[5])))

    # file = open('/Users/jayati/Projects/DSCI553/HW2/customer_product.csv', 'w')
    file = open('customer_product.csv', 'w')
    file.write("DATE-CUSTOMER_ID, PRODUCT_ID\n")
    for i in processed_customer_data.collect():
        file.write(str(i[0]).replace('"', '') + ',' + str(i[1]).replace('"', '') + "\n")
    file.close()

    # Task 2.2 starts here
    customer_product_data = sc.textFile('customer_product.csv', partition_count)
    # customer_product_data = sc.textFile('/Users/jayati/Projects/DSCI553/HW2/customer_product.csv', partition_count)

    #  step 1 create buckets customer => product
    customer_product_data = customer_product_data \
        .map(lambda line: line.split(",")) \
        .filter(lambda line: 'DATE-CUSTOMER_ID' not in line[0]) \
        .map(lambda x: (str(x[0]), str(int(x[1])))) \
        .groupByKey().mapValues(set) \
        .filter(lambda x: len(x[1]) > int(filter_threshold))

    apply_son_algorithm(customer_product_data, int(support), partition_count)

    end_time = time.time()
    time_duration = end_time - start_time
    print("Duration: " + str(time_duration))