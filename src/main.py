from pyspark.sql import SQLContext
from pyspark.mllib.stat import Statistics
import scipy
import numpy as np
from scipy.stats import pearsonr
import pickle

sqlContext = SQLContext(sc)

users_file = '/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/CSVs/User.csv'
ratings_file = '/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/CSVs/Ratings.csv'
business_file = '/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/CSVs/Business.csv'
user_friends_file = '/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/CSVs/Edges.txt'
rating_dict_file = '/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/pickles/ratings_dict.pickle'
user_businesses_file = '/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/pickles/user_businesses.pickle'

user = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(users_file)
user.registerTempTable("Users")

ratings  = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(ratings_file)
ratings.registerTempTable("Ratings")

business  = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(business_file)
business.registerTempTable("Business")

user_friends = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(user_friends_file)
user_friends.registerTempTable("User_Friends")

f = open(rating_dict_file)
rating_dict = pickle.load(f)
f.close()

f = open(user_businesses_file)
user_businesses = pickle.load(f)
f.close()

user_friends_list = sqlContext.sql("SELECT * FROM User_Friends")

""" Converting SQl DataFrame to RDD """
user_friends_rdd = user_friends_list.map(lambda p: (p.User, p.Friend))

def getWeights(u_v_pair):
	u = u_v_pair[0]
	v = u_v_pair[1]
	u_businesses = user_businesses[u]
	v_businesses = user_businesses[v]
	final_businesses = u_businesses.intersection(v_businesses) 
	if len(final_businesses) == 0:
		return (u,v,0.0)
	rating_u = [float(rating_dict[u+"_"+k]) for k in final_businesses ]
	rating_v = [float(rating_dict[v+"_"+k]) for k in final_businesses ]
	x = scipy.array(rating_u)
	y = scipy.array(rating_v)
	r_row, p_value = pearsonr(x, y)
	return (u, v, p_value)

weights_rdd = user_friends_rdd.map(getWeights)

user_rating_list = sqlContext.sql("SELECT user_id, stars, count(*) cnt FROM Ratings GROUP BY user_id, stars ORDER BY user_id")

user_rating_dict = user_rating_list.map(lambda x: (x.user_id,(x.stars,x.cnt))).groupByKey()

def sumCalculator(x):
	sum = 0
	for val in x[1]:
		sum = sum + val[1]
	return (x[0], sum, x[1])

user_rating_update = user_rating_dict.map(sumCalculator)

def getProbability(x):
	list_probability = [(1.0 / (x[1] + 5))] * 5
	for val in x[2]:
		list_probability[(val[0] - 1)] = ((1.0 + val[1])/ (x[1] + 5))
	return (x[0], tuple(list_probability))

user_rating_priors = user_rating_update.map(getProbability)

user_rating_businesses = sqlContext.sql("SELECT concat(r.user_id, '~', r.stars) as user_stars, b.attributes FROM Ratings r, Business b WHERE r.business_id = b.business_id ORDER BY user_id")

def parseAttributes(x):
	user_star = x[0]
	attr = map(int, x[1].split())
	return (user_star, attr)

user_rating_bussiness_attr = user_rating_businesses.map(parseAttributes).groupByKey()

def getSumOfAttributes(x):
	return (x[0], map(sum, zip(*x[1])), x[1])

user_rating_bussiness_attr_sum = user_rating_bussiness_attr.map(getSumOfAttributes)

def getAttributesProbability(x):
	list_probability = []
	for index in range(0, len(x[1]), 1):
		probability = (x[1][index] + 1.0) / (len(x[2]) + 2)
		list_probability.append(probability)
	return (x[0], list_probability)

user_rating_bussiness_attr_prob = user_rating_bussiness_attr_sum.map(getAttributesProbability)

def correlationCalculator(u_v_pair):
	u = u_v_pair[0]
	v = u_v_pair[1]
	u_businesses = user_businesses[u]
	v_businesses = user_businesses[v]
	final_businesses = u_businesses.intersection(v_businesses) 
	if len(final_businesses) == 0:
		return (u,v,0,{})
	rating_u = [float(rating_dict[u+"_"+k]) for k in final_businesses ]
	rating_v = [float(rating_dict[v+"_"+k]) for k in final_businesses ]
	x = np.array(rating_u)
	y = np.array(rating_v)
	diff = x-y
	dictCorr = { i : 0 for i in range(-4,5)}
	for item in diff:
		dictCorr[int(item)] += 1 
	return (u, v, len(final_businesses),dictCorr)

user_friends_correlation = user_friends_list.map(correlationCalculator).filter(lambda p: p[2]>=3)

dictCorr = {}
for item in user_friends_correlation.collect():
	dictCorr[item[0]+"_"+item[1]] = item[3]
	dictCorr[item[1]+"_"+item[0]] = {-k:v for k,v in item[3].items()}
	dictCorr[item[0]+"_"+item[1]]["l"] = item[2]
	dictCorr[item[1]+"_"+item[0]]["l"] = item[2]

