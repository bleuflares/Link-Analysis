import re
import sys
from pyspark import SparkConf, SparkContext

def col_parser(line):
    points = line.split()
    if len(points) > 1:
        return (points[0], [points[1]])

def row_parser(line):
    points = line.split()
    if len(points) > 1:
        return (points[1], [points[0]])

def matmul(row):
    row_sum = 0
    for i in v_old:
        row_sum += v_old[i] * row[1][i]
    return (row[0], BETA * row_sum)

def parser(line):
    points = line.split()
    if len(points) > 1:
        return (points[0], points[1])

def normalize(element):
    return (element[0], element[1][0], 1.0 / float(element[1][1]))

def teleport(pair):
    return (pair[0], pair[1][0] + 0.3 * 0.001)

if __name__ == "__main__":

    BETA = 0.9
    arr = [(i, 0, 0.001) for i in range(1000)]

    conf = SparkConf()
    sc = SparkContext(conf=conf)
    lines = sc.textFile(sys.argv[1])

    edges = lines.map(lambda line: parser(line)).distinct()
    col_count = sc.parallelize(edges.countByKey())
    mat = edges.join(col_count)
    norm_mat = mat.flatMap(normalize)
    v = sc.parallelize(arr)
    v_old = v

    for j in range(50):
        matmuls = norm_mat.cartesian(v_old).filter(lambda x: x[0][1] == x[1][0])
        matmul = matmuls.map(lambda x: (x[0][0], BETA * x[0][2] * x[1][2])).reduceByKey(lambda a, b: a + b)
        v_new = matmul.map(teleport)
        v_old = v_new

    print(sorted(v_old.collect())[-10:])


