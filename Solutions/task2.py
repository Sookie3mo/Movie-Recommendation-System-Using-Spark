#Yiming Liu
#inf553 homework3
#User based CF/ Spark/ Recommendation System
from time import time
from pyspark import SparkContext,SparkConf
import numpy as np
from helper import *
from calculator import *

def find_first_user(user_pair, movie_sim_data):

    (user1,user2) = user_pair
    return user1,(user2,movie_sim_data)

def cosine_simi(user_pair, rating_pairs):
    #x : co-raters count
    sum_x, sum_xy, sum_y,x = (0.0, 0.0, 0.0,0)

    for ratingPair in rating_pairs:
        sum_x += np.float(ratingPair[0]) * np.float(ratingPair[0])
        sum_y += np.float(ratingPair[1]) * np.float(ratingPair[1])
        sum_xy += np.float(ratingPair[0]) * np.float(ratingPair[1])
        x += 1
    cos_sim = cosine(sum_xy,np.sqrt(sum_x),np.sqrt(sum_y))
    yield user_pair, cos_sim


def predict_rating(input):
        for item in input:
            prex = prey = 0
            for i in item[1]:
                prex += (i[2] - mean[i[3] - 1][1]) * i[1]
                prey = prey + abs(i[1])
                if(item[1].index(i) + 1 < len(item[1])):
                   if(i[0]!= item[1][item[1].index(i) + 1][0]):
                        if(prey != 0):
                            pred = mean[item[0] - 1][1] + prex / prey
                        else:
                            pred = mean[item[0] - 1][1]
                        prex = prey = 0
                        yield((item[0],i[0]),pred)
                else:
                    if(prey != 0):
                        pred = mean[item[0] - 1][1] + prex / prey
                    else:
                        pred = mean[item[0] - 1][1]
                    yield((item[0],i[0]),pred)

def cosine(dot_product,rating1_norm_squared,rating2_norm_squared):

    num = dot_product
    den = rating1_norm_squared * rating2_norm_squared

    yield (num / (float(den))) if den else 0.0


def near_neighbor(movie_id, movie_and_sims, n):

    movie_and_sims.sort(key = lambda x: x[1][0],reverse = True)

    yield movie_id, movie_and_sims[:n]

if __name__== "__main__":
    start = time()
    print("Start: " + str(start))
# Initializaiton of Spark

    conf = SparkConf().setMaster("local[*]").setAppName("User-Based CF")
    sc = SparkContext(conf = conf)

# Getting test data from "test_small.csv". Remove header. str to int

    testfile = sc.textFile("testing_small.csv")
    testheader = testfile.first()
    testmapper = testfile.filter(lambda x: x != testheader).map(lambda x: x.split(',')).map(lambda x:((int(x[0]), int(x[1])), -1))
    testData = testmapper.map(lambda x:(x[0][0], (x[0][1], x[1]))).groupByKey().\
        map(lambda x: (x[0], sorted(list(x[1])))).sortByKey()

# Getting train data by removing test data from all data
    ratefile = sc.textFile("ratings_small.csv")
    rateheader = ratefile.first()
    # train data : groupby user  (1,(u,m,r)) join user
    # (1, (u m r u m r))
    ratemapper = ratefile.filter(lambda x: x != rateheader).map(lambda x: x.split(',')).map(lambda x:((int(x[0]), int(x[1])), float(x[2]))).leftOuterJoin(testmapper)

# real answer for test data(for calculating rmse later)
    actual = ratemapper.filter(lambda x: None not in x[1]).map(lambda x:(x[0], x[1][0]))

    # filter u1<u2
    # map (u,u)list(m r )list(m r)
    ratefilter = ratemapper.filter(lambda x: None in x[1]).\
        map(lambda x:(x[0][0], (x[0][1], x[1][0])))

    trainData = ratefilter.groupByKey().map(lambda x: (x[0], sorted(list(x[1])))).sortByKey().map(lambda x:(1, x))

# Compute average rating for every user based on all movies an individual rated
    mean = trainData.map(lambda x: x[1]).mapPartitions(cal_mean).collect()

    # func:  sort 2 list by movie
    # find co movie  calculate w   no co movie w = 0
# return user pair similarity based on pearson similarity method

    preSimi = trainData.join(trainData).filter(lambda x: x[1][0][0] < x[1][1][0])

    simi = preSimi.map(lambda x: x[1]).mapPartitions(get_simi)

# changing format for generating predictions
    testmapper2 = testmapper.map(lambda x: (x[0][1], (x[0][0], x[1])))

    trainmapper2 = ratefilter.map(lambda x: (x[1][0], (x[0], x[1][1])))

    flag = testmapper2.map(lambda x: (((x[1][0], x[0]), x[1][1])))

    # test   m (u r)  groupby  m list(u r) --> list(u r) m
    # user user movie  -1 rate
    # if u1> u2  sort u2 u1 rate -1
#getting predictions using pearson prediction formula
    predMovies = trainmapper2.join(testmapper2).mapPartitions(change_format).join(simi)

    groupPredMovies = predMovies.mapPartitions(marker).groupByKey()

    getPredMovies = groupPredMovies.map(lambda x: (x[0], sorted(list(x[1]), reverse = True))).mapPartitions(predict_rating)

    predictions = flag.leftOuterJoin(getPredMovies).filter(lambda x: None in x[1])

    ans = predictions.map(lambda x: (x[0], mean[x[0][0] - 1][1])).union(getPredMovies).sortByKey()

#calculate difference between predicted ratings and actual ratings
    diff = ans.join(actual).map(lambda x: (x[0], abs(x[1][0] - x[1][1])))

    rateDiff = diff.mapPartitions(gather_data).sortByKey().reduceByKey(add).sortByKey().collect()
    counts = diff.count()
    #RMSE = calculate_RMSE(diff,counts)
    #calculate rmse
    RMSE = sqrt(diff.map(lambda x: x[1] * x[1]).reduce(add) / counts)

    writer = open("Yiming_Liu_result_task2.txt", 'wb')
    writer.write("UserId,MovieId,Pred_rating\n")
    for i in ans.collect():
        writer.write(str(i[0][0]) + "," + str(i[0][1]) + "," + str(i[1]) + "\n")
    writer.close()

    print ">=0 and <1:",rateDiff[0][1]
    print ">=1 and <2:",rateDiff[1][1]
    print ">=2 and <3:",rateDiff[2][1]
    print ">=3 and <4:",rateDiff[3][1]
    print ">=4:",rateDiff[4][1]
    print "RMSE = ",RMSE

    sc.stop()
    stop = time()
    print("Stop: " + str(stop))
    print("time: " + str(stop - start) + "s")



