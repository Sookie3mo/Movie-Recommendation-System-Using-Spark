from itertools import combinations


def find_user_pairs(movie_id, users_with_rating):
    for user1,user2 in combinations(users_with_rating,2):
        return (user1[0],user2[0]),(user1[1],user2[1])


def find_first_user(user_pair, movie_sim_data):

    (user1,user2) = user_pair
    return user1,(user2,movie_sim_data)

def marker(input):
    for d in input:
        if(d[1][0][1] == -1):
            yield((d[0][0],(d[1][0][0],d[1][1],d[1][0][2],d[0][1])))
        elif(d[1][0][2]== -1):
            yield((d[0][1],(d[1][0][0],d[1][1],d[1][0][1],d[0][0])))

def near_neighbor(movie_id, movie_and_sims, n):

    movie_and_sims.sort(key = lambda x: x[1][0],reverse = True)

    yield movie_id, movie_and_sims[:n]

def change_format(input):
    for i in input:
        if(i[1][0][0] < i[1][1][0]):

            yield((i[1][0][0],i[1][1][0]),(i[0],i[1][0][1],i[1][1][1]))
        else:
            yield((i[1][1][0],i[1][0][0]),(i[0],i[1][1][1],i[1][0][1]))

def cal_cosine(dot_product,rating1_norm_squared,rating2_norm_squared):

    num = dot_product
    den = rating1_norm_squared * rating2_norm_squared

    yield (num / (float(den))) if den else 0.0

def cal_jaccard(a,b):
    x = set(a)
    y = set(b)
    common = x.intersection(y)
    if len(x)+len(y)-len(common)==0:
        return 0
    return len(common)*1.0/(len(x)+len(y)-len(common))

def gather_data(item):
    field1 = field2 = field3 = field4 = field5 = 0
    for field in item:
        if(field[1] < 1):
            field1 = field1 + 1
        elif(field[1] < 2):
            field2 = field2 + 1
        elif(field[1] < 3):
            field3 = field3 + 1
        elif(field[1] < 4):
            field4 = field4 + 1
        else:
            field5 = field5 + 1
    yield ((0,1),field1)
    yield ((1,2),field2)
    yield ((2,3),field3)
    yield ((3,4),field4)
    yield ((4,5),field5)
