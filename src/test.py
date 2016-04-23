from pyspark.sql import SQLContext
import pickle
import math
import random
import sys
import numpy as np
random_seed = 15
random.seed(random_seed)
'''
weights_pickle = "/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/train_results/weights.pickle"
priors_pickle = "/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/train_results/priors.pickle"
attributes_prob_pickle = "/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/train_results/attributes_probability.pickle"
correlation_pickle = "/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/train_results/correlation.pickle"
graphs_pickle = "/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/pickles/graph.pickle"
rating_dict_file = '/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/pickles/ratings_test_dict.pickle'
ratings_train_dict_file = '/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/pickles/ratings_dict.pickle'
user_businesses_file = '/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/pickles/user_businesses_test.pickle'
users_file = '/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/CSVs/user_test.csv'
ratings_file = '/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/CSVs/Ratings_test.csv'
business_file = '/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/CSVs/Business.csv'
user_friends_file = '/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/CSVs/Edges_test.txt'
#b_users_pickle = "/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/pickles/b_users.pickle"
#naive_pickle = "/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/pickles/naive.pickle"
user_complete_file = '/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/CSVs/User.csv'
'''

weights_pickle = "pickles/weights.pickle"
priors_pickle = "pickles/priors.pickle"
attributes_prob_pickle = "pickles/attributes_probability.pickle"
correlation_pickle = "pickles/correlation.pickle"
graphs_pickle = "pickles/graph.pickle"
rating_dict_file = 'pickles/ratings_test_dict.pickle'
ratings_train_dict_file = 'pickles/ratings_dict.pickle'
user_businesses_file = 'pickles/user_businesses_test.pickle'
users_file = 'csvs/user_test.csv'
ratings_file = 'csvs/Ratings_test.csv'
business_file = 'csvs/Business.csv'
user_friends_file = 'csvs/Edges_test.txt'
user_complete_file = 'csvs/User.csv'






"""f = open(naive_pickle)
naive_dict = pickle.load(f)
f.close()"""

"""f = open(b_users_pickle)
business_users_dict = pickle.load(f)
f.close()"""

f = open(weights_pickle)
weights_dict = pickle.load(f)
f.close()

f = open(priors_pickle)
priors_dict = pickle.load(f)
f.close()

f = open(attributes_prob_pickle)
attributes_probabilitiy_dict = pickle.load(f)
f.close()

f = open(correlation_pickle)
correlation_dict = pickle.load(f)
f.close()

f = open(graphs_pickle)
graph_dict = pickle.load(f)
f.close()

f = open(rating_dict_file)
rating_dict = pickle.load(f)
f.close()

f = open(ratings_train_dict_file)
rating_train_dict = pickle.load(f)
f.close()

f = open(user_businesses_file)
user_businesses = pickle.load(f)
f.close()

user = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(users_file)
user.registerTempTable("Users")

ratings  = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(ratings_file)
ratings.registerTempTable("Ratings")

business  = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(business_file)
business.registerTempTable("Business")

user_friends = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(user_friends_file)
user_friends.registerTempTable("User_Friends")

user_complete = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(user_complete_file)
user_complete.registerTempTable("User_Data")

user_rating_list = sqlContext.sql("SELECT user_id, business_id, stars FROM Ratings ORDER BY user_id")

user_rating_actual_rdd = user_rating_list.map(lambda p: p[2])
user_rating_actual = user_rating_list.map(lambda p: p[2]).collect()

num_of_attributes = 154

businesses = sqlContext.sql("SELECT * FROM Business")

users_complete_data = sqlContext.sql("SELECT user_id, average_stars FROM User_Data")
users_avg_rating_rdd = users_complete_data.map(lambda x: (x.user_id, x.average_stars))
user_avg_rating = {}
cnt = 0

for value in users_avg_rating_rdd.collect():
    user_avg_rating[value[0]] = ( cnt ,float(value[1]))
    cnt += 1

'''
f = open("pickles/user_avg_ratings.pickle", "w")
pickle.dump(user_avg_rating, f)
f.close()
'''

def parseAttributes(x):
    b_id = x[0]
    attr = map(int, x[1].split())
    return (b_id, attr)

bussiness_attr = businesses.map(parseAttributes)

business_attr_dict = {}
cnt = 0
for value in bussiness_attr.collect():
    business_attr_dict[value[0]] = (cnt,value[1])
    cnt+=1

def randomSelect(l):
    m = max(l)
    c = l.count(m)
    if c == 1:
        return l.index(m) + 1
    r = random.randint(1,c)
    cnt = 0
    for j in range(5):
        if l[j] == m:
            cnt+=1
        if cnt == r:
            return j+1
    return l.index(m)+1

def calculateNB(x):
    u = x[0]
    b = x[1]
    probabilities = [0] * 5
    if priors_dict.has_key(u):
        rating_probabilities = priors_dict[u]
    else:
        rating_probabilities = [0.2] * 5
    for r in range(1, 6, 1):
        if attributes_probabilitiy_dict.has_key(u+"~"+str(r)):
            attributes_probability = attributes_probabilitiy_dict[u+"~"+str(r)]
        else:
            attributes_probability = [0.5] * num_of_attributes
        attributes = business_attr_dict[b][1]
        prob = math.log(rating_probabilities[r-1])
        i=0
        for attr in attributes:
            prob = prob + attr*math.log(attributes_probability[i]) + (1 - attr) * math.log(1 - attributes_probability[i])
            i+=1
        probabilities[r-1] = prob
    return probabilities

#nb_probabilities_rdd = user_rating_list.map(calculateNB)
#nb_ratings = nb_probabilities_rdd.map(lambda x : x.index(max(x))+1).collect()

business_user_rdd = user_rating_list.map(lambda p: (p[1], p[0])).groupByKey()

naive_matrix = np.zeros( (len(business_attr_dict),len(user_avg_rating)) )
def getRating(user, business_id):
    bindex = business_attr_dict[business_id][0]
    uindex = user_avg_rating[user][0]
    rating = 1.0
    if rating_train_dict.has_key(user):
        rating = rating_train_dict[user]
    else:
        key = user+"_"+business_id
        if naive_matrix[bindex][uindex] <= 0.0:
            probabilities = calculateNB((user, business_id))
            rating = randomSelect(probabilities)
            naive_matrix[bindex, uindex] = rating
        else:
            rating = naive_matrix[bindex, uindex]
    return rating

"""business_users_dict = {}
for value in business_user_rdd.collect():
    business_id = value[0]
    users = {}
    for user in value[1]:
        if user not in users:
            users[user] = getRating(user, business_id)
        if user not in graph_dict:
            continue
        friends = graph_dict[user]
        for friend in friends:
            if friend not in users:
                users[friend] = getRating(friend, business_id)
    business_users_dict[business_id] = users

f = open("/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/pickles/business_users.pickle", "w")
pickle.dump(business_users_dict, f)
f.close()"""

def generateBusinessUserRatings():
    business_list = business_attr_dict.keys()
    users_list = user_businesses.keys()
    total = len(business_list)* len (user_avg_rating.keys())
    rating_matrix = np.zeros(( len(business_list),len(user_avg_rating) ))
    cnt = 0
    for business in business_list:
        bindex = business_attr_dict[business][0]
        for user in users_list:
            uindex = user_avg_rating[user][0]
            cnt+=1
            sys.stdout.write('\rProgress: %0.3f '%(float(cnt)*100/total)+"%")
            sys.stdout.flush()
            if rating_matrix[bindex, uindex] <= 0.0:
                rating_matrix[ bindex, uindex ] = getRating(user, business)
            if user not in graph_dict:
                continue
            friends = graph_dict[user]
            for friend in friends:
                findex = user_avg_rating[friend][0]
                if rating_matrix[bindex,findex] <= 0.0:
                    rating_matrix[bindex, findex] = getRating(friend, business)
        #b_users[business] = u_ratings
    f = open("pickles/naive.pickle", "w")
    pickle.dump(naive_matrix, f)
    #f.close()
    f = open("pickles/b_users.pickle", "w")
    pickle.dump(rating_matrix, f)
    f.close()
    return rating_matrix

generateBusinessUserRatings()

sys.exit(0)

def getDistantFriendsRating(user, friends, business_id):
    rating = 0.0
    total_prob = 0.0
    flag = False
    for r in range(1, 6, 1):
        product = 1.0 * r
        for friend in friends:
            if rating_train_dict.has_key(friend+"_"+business_id):
                if user+"_"+friend in correlation_dict and friend in business_users_dict[business_id]:
                    flag = True
                    rvi =  business_users_dict[business_id][friend]
                    k_rvi = correlation_dict[user+"_"+friend][r - rvi] 
                    length = correlation_dict[user+"_"+friend]['l'] 
                    product *= (1.0 * k_rvi) / length
        rating += product
        total_prob += product/r
    if flag:
        if total_prob == 0.0:
            business_users_dict[business_id][user] = getRating(user, business_id)
        else:
            business_users_dict[business_id][user] = rating/total_prob
    else:
        business_users_dict[business_id][user] = getRating(user, business_id)

def genarateFinalRatings():
    for business_id in business_users_dict.keys():
        users = business_users_dict[business_id].keys()
        for m in range(1, 6, 1):
            random.shuffle(users)
            for user in users:
                if not rating_train_dict.has_key(user+"_"+business_id):
                    if user not in graph_dict:
                        continue
                    friends = graph_dict[user]
                    getDistantFriendsRating(user, friends, business_id)

genarateFinalRatings()

"""def evaluate(predicted,actual):
    correct = 0
    error = 0.0
    for i in range(len(predicted)):
        error += abs(predicted[i] - actual[i])
        if predicted[i] == actual[i]:
            correct+=1
    return error*1.0/len(predicted),correct*1.0/len(predicted)"""

def evaluateResults():
    correct = 0
    error = 0.0
    count = 0 
    cnt = { k : 0 for k in range(1,10)}
    for user in user_businesses.keys():
        for business in user_businesses[user]:
            count += 1
            key = user + "_" + business
            actual = rating_dict[key]
            predicted = round(business_users_dict[business][user])
            cnt [int(predicted)] += 1
            error += abs(int(actual) - int(predicted))
            if(int(actual) == int(predicted)):
                correct += 1
    print cnt
    return error*1.0/count, correct*1.0/count

err,acc = evaluateResults()
print "accuracy for classification", acc
print "mean absolute error", err

def getRecommendationList(user):
    ratings = sorted([(b, float(d[user])) for b,d in business_users_dict if user+"_"+b not in rating_train_dict],key = lambda t : -t[1])
    avg_rating = user_avg_rating[user]
    #ratings = [(x[0], int(round(x[1]))) for x in ratings if x[1] >= avg_rating]
    recommendations_list = [x[0] for x in ratings if x[1] >= avg_rating]
    return recommendations_list

def evaluateScore(user, recommendations_list):
    business_list = user_businesses[user]
    if len(business_list) == 0:
        return 0.0
    common_recommendations = set(recommendations_list).intersection(set(business_list))
    score = float(len(common_recommendations)) / len(business_list)
    return score

def evaluateRecommendations():
    score = 0.0
    cnt = 0
    for user in user_businesses.keys():
        score += evaluateScore(user, getRecommendationList(user))
        cnt += 1
    return score / cnt

accuracy = evaluateRecommendations()
print "accuracy : ", accuracy
