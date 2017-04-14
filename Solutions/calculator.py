
import math
from operator import add
from math import sqrt

def calculate_RMSE(difference,counts):

    return (sqrt(difference.map(lambda x: x[1]*x[1]).reduce(add) / counts))

def cal_mean(data):
    for item in data:
        n= r = 0
        for i in item[1]:
            n += 1
            r += i[1]
        ratemean = r/float(n)
        yield (item[0],ratemean)

def get_simi(data):
    for d in data:
        x = y = 0
        coRatedmovies=[]
        i = d[0]
        j = d[1]
        while(x < len(i[1]) and y < len(j[1])):
            if(i[1][x][0] < j[1][y][0]):
                x += 1
            elif(i[1][x][0] > j[1][y][0]):
                y += 1
            else:
                coRatedmovies.append((i[1][x][0],(i[1][x][1],j[1][y][1])))
                y += 1
                x += 1
        if(len(coRatedmovies)!=0):
            meanx = meany = 0
            for c in coRatedmovies:
                meanx += c[1][0]
                meany += c[1][1]
            meanx = meanx/len(coRatedmovies)
            meany = meany/len(coRatedmovies)
            p0 = p1 = p2 = 0
            for c in coRatedmovies:
                px = c[1][0] - meanx
                py = c[1][1] - meany
                p0 = p0 + px*py
                p1 = p1 + px*px
                p2 = p2 + py*py
            if(math.sqrt(p1) == 0 or math.sqrt(p2) == 0):
                yield((i[0],j[0]),3.65)
            else:
                sim = p0/(math.sqrt(p1) * math.sqrt(p2))
                yield ((i[0],j[0]),sim)
        else:
            yield((i[0],j[0]),0)


