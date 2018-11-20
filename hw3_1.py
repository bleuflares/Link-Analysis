import sys
from pyspark import SparkConf, SparkContext

#mapper for parsing input
def parser(line):
    points = line.split()
    if len(points) > 1:
        return (int(points[0]), int(points[1]))

#mapper for normalizing each column of matrix
def normalize(element):
    return [(element[0], element[1][0], 1.0 / float(element[1][1]))]

#mapper for changing formation of rdd to compute matrix multiplication
def mapper(element):
    return (element[0], (element[1], element[2]))

#mapper for taxation
def teleport(pair):
    return (pair[0], (0, pair[1] + (1 - BETA) * 0.001))

if __name__ == "__main__":

    BETA = 0.9
    arr = [(i + 1, 0, 0.001) for i in range(1000)] #initial v

    conf = SparkConf()
    sc = SparkContext(conf=conf)
    lines = sc.textFile(sys.argv[1])

    edges = lines.map(parser).distinct() #obtain distict inputs and create matrix
    col_count = edges.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda a, b: a + b).map(lambda x: (x[0], len(x[1]))) #count the number of nonzeros to normalize
    mat = edges.join(col_count)
    norm_mat = mat.flatMap(normalize) #normalize input matrix
    norm_mat_pair = norm_mat.map(mapper)
    v = sc.parallelize(arr)
    v_old = v
    v_old_pair = v_old.map(mapper)

    for j in range(50):
        matmul_pair = norm_mat_pair.join(v_old_pair) #get pairs to multiply
        matmul = matmul_pair.map(lambda x: (x[1][0][0], BETA * x[1][0][1] * x[1][1][1])).reduceByKey(lambda a, b: a + b)  #matrix multiplication
        v_new_pair = matmul.map(teleport) #taxation applied
        v_old_pair = v_new_pair

    top_10 = sorted(v_old_pair.collect(), key=lambda x: -x[1][1])[:10] #get top 10
    for pair in top_10:
        print("%d\t%f" %(pair[0], pair[1][1])) # print top 10
    sc.setLogLevel('WARN')
    sc.stop()
