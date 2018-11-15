import re
import sys
from pyspark import SparkConf, SparkContext
def reducer(element):
    return 

def matmul(row):
    row_sum = 0
    for i in v_old:
        row_sum += v_old[i] * row[1][i]
    return (row[0], BETA * row_sum)

def parser(line):
    points = line.split()
    if len(points) > 1:
        return (int(points[0]), int(points[1]))

def normalize(element):
    return [(element[0], element[1][0], 1.0 / float(element[1][1]))]

def mapper(element):
    return (element[0], (element[1], element[2]))
def teleport(pair):
    return (pair[0], (0, pair[1] + (1 - BETA) * 1.0))

if __name__ == "__main__":

    BETA = 0.9
    arr = [(i + 1, 0, 1.0) for i in range(1000)]

    conf = SparkConf()
    sc = SparkContext(conf=conf)
    lines = sc.textFile(sys.argv[1])

    edges = lines.map(parser).distinct()
    col_count = edges.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda a, b: a + b).map(lambda x: (x[0], len(x[1])))
    mat = edges.join(col_count)
    norm_mat = mat.flatMap(normalize)
    norm_mat_pair = norm_mat.map(mapper)
    v = sc.parallelize(arr)
    v_old = v
    v_old_pair = v_old.map(mapper)

    for j in range(4):
        matmul_pair = norm_mat_pair.join(v_old_pair)
        print(matmul_pair.collect())
        matmul = matmul_pair.map(lambda x: (x[1][0][0], BETA * x[1][0][1] * x[1][1][1])).reduceByKey(lambda a, b: a + b)
        v_new_pair = matmul.map(teleport)
        v_old_pair = v_new_pair

    top_10 = sorted(v_old_pair.collect(), key=lambda x: -x[1][1])[:10]
    for pair in top_10:
        print("%d\t%f" %(pair[0], pair[1][1]))
    sc.setLogLevel('WARN')
    sc.stop()
