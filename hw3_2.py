import sys
from pyspark import SparkConf, SparkContext

#max value to store
h_max_val = 0.0
a_max_val = 0.0

#mapper for parsing input
def parser(line):
    points = line.split()
    if len(points) > 1:
        return (int(points[0]), (int(points[1]), 1.0))
#mapper for parsing input for transposed matrix
def parser_t(line):
    points = line.split()
    if len(points) > 1:
        return (int(points[1]), (int(points[0]), 1.0))

#mapper for changing formation of rdd to compute matrix multiplication
def mapper(element):
    return (element[0], (element[1], element[2]))
#normalize by making the maximum 1
def h_normalize(vector):
    return (vector[0], (0, vector[1] / h_max_val[1]))
#normalize by making the maximum 1
def a_normalize(vector):
    return (vector[0], (0, vector[1] / a_max_val[1]))

if __name__ == "__main__":
    arr = [(i + 1, 0, 1.0) for i in range(1000)] #initial h

    conf = SparkConf()
    sc = SparkContext(conf=conf)
    lines = sc.textFile(sys.argv[1])

    link = lines.map(parser).distinct() #obtain distict inputs and create matrix
    link_t = lines.map(parser_t).distinct()

    h = sc.parallelize(arr)
    h_pair = h.map(mapper)
    a_pair = None

    for j in range(50):
        h_matmul_pair = link.join(h_pair) #get pairs to multiply
        h_matmul = h_matmul_pair.map(lambda x: (x[1][0][0], x[1][0][1] * x[1][1][1])).reduceByKey(lambda a, b: a + b)  #matrix multiplication
        h_max_val = h_matmul.max(key= lambda x: x[1])
        a_pair = h_matmul.map(h_normalize) #normalize vector

        a_matmul_pair = link_t.join(a_pair) #get pairs to multiply
        a_matmul = a_matmul_pair.map(lambda x: (x[1][0][0], x[1][0][1] * x[1][1][1])).reduceByKey(lambda a, b: a + b)  #matrix multiplication
        a_max_val = a_matmul.max(key= lambda x: x[1])
        h_pair = a_matmul.map(a_normalize) #normalize vector

    h_top_10 = sorted(h_pair.collect(), key=lambda x: -x[1][1])[:10] #get top 10
    a_top_10 = sorted(a_pair.collect(), key=lambda x: -x[1][1])[:10] #get top 10

    for pair in h_top_10:
        print("%d\t%f" %(pair[0], pair[1][1])) # print top 10
    for pair in a_top_10:
        print("%d\t%f" %(pair[0], pair[1][1])) # print top 10

    sc.setLogLevel('WARN')
    sc.stop()
