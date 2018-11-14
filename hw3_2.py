import sys
import numpy as np
import math

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
    arr = [(i, 1) for i in range(1000)]

    conf = SparkConf()
    sc = SparkContext(conf=conf)
    lines = sc.textFile(sys.argv[1])
    """
    
    col_edges = lines.map(lambda line: col_parser(line)) #map to generate pair for each line
    cols = col_edges.reduceByKey(lambda a, b: a + b)
    """

    edges = lines.map(lambda line: parser(line))

    col_edges = lines.map(lambda line: col_parser(line)) #map to generate pair for each line
    col_count = row_edges.countByKey()
    mat = edges.join(col_count)
    norm_mat = mat.flatmap(normalize)
    v = sc.parllelize(arr)
    v_old = v

    while(1):
        matmul = norm_mat.cartesian(v_old).filter(lambda x: x[0][0] == x[1][1] and x[0][1] == x[1][0]).map(lambda x: (x[0][0], BETA * x[0][2] * x[1][2])).reduceByKey(lambda a, b: a + b)
        v_new = matmul.map(teleport)
        if(v_new == v_old):
	       break
        else:
            v_old = v_new

    print(sorted(v_old.collect())[-5:])
