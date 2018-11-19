import numpy as np
import sys
def pagerank(G, beta):

    N = G.shape[0]
    d = G.sum(axis=0)

    r = np.zeros(N) + 1./N
    rnew = np.zeros(N) + 1./N

    for k in range(20):
        rprime = np.zeros(N)

        for i in range(N):
            for j in range(N):
                if G[i][j] != 0:
                    rprime[i] += beta * r[j]/d[j]

        S = rprime.sum()

        for i in range(N):
            rnew[i] = rprime[i] + (1 - S)/N

        
        r = rnew * 1.0

    return r

if __name__ == "__main__":
    points = []
    file = open(sys.argv[1])
    for line in file:
        point = line.split()
        if len(point) > 1:
            points.append((int(point[1]), int(point[0])))

    M = np.zeros((1000, 1000))
    for point in points:
        M[point[0] - 1][point[1] - 1] = 1.0

    pgrank = pagerank(M, 0.9).tolist()
    indexed_pgrank = []
    for i in range(len(pgrank)):
        indexed_pgrank.append([i + 1, pgrank[i]])
    top_10 = sorted(indexed_pgrank, key=lambda x: -x[1])[:10]
    for pair in top_10:
        print("%d\t%f" %(pair[0], pair[1])) # print top 10