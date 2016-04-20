from pyspark.sql import SQLContext
import pickle
import math

weights_pickle = "/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/train_results/weights.pickle"
priors_pickle = "/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/train_results/priors.pickle"
attributes_prob_pickle = "/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/train_results/attributes_probability.pickle"
correlation_pickle = "/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/train_results/correlation.pickle"
rating_dict_file = '/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/pickles/ratings_test_dict.pickle'
user_businesses_file = '/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/pickles/user_businesses_test.pickle'
users_file = '/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/CSVs/user_test.csv'
ratings_file = '/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/CSVs/Ratings_test.csv'
business_file = '/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/CSVs/Business.csv'
user_friends_file = '/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/CSVs/Edges_test.txt'

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

f = open(rating_dict_file)
rating_dict = pickle.load(f)
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

user_rating_list = sqlContext.sql("SELECT user_id, business_id, stars FROM Ratings ORDER BY user_id")

user_rating_actual = user_rating_list.map(lambda p: p[2]).collect()

num_of_attributes = 154

businesses = sqlContext.sql("SELECT * FROM Business b")

def parseAttributes(x):
	b_id = x[0]
	attr = map(int, x[1].split())
	return (b_id, attr)

bussiness_attr = businesses.map(parseAttributes)

business_attr_dict = {}
for value in bussiness_attr.collect():
	business_attr_dict[value[0]] = value[1]

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
		attributes = business_attr_dict[b]
		prob = math.log(rating_probabilities[r-1])
		i=0
		for attr in attributes:
			prob = prob + attr*math.log(attributes_probability[i]) + (1 - attr) * math.log(1 - attributes_probability[i])
			i+=1
		probabilities[r-1] = prob
 	return probabilities

nb_probabilities_rdd = user_rating_list.map(calculateNB)