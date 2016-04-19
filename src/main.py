from pyspark.sql import SQLContext
from pyspark.mllib.stat import Statistics
import scipy
from scipy.stats import pearsonr
import pickle

sqlContext = SQLContext(sc)

users_file = '/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/CSVs/User.csv'
business_category_file = '/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/CSVs/business_category_map.csv'
ratings_file = '/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/CSVs/Ratings.csv'
business_file = '/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/CSVs/Business.csv'
category_file = '/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/CSVs/Category.csv'
user_friends_file = '/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/CSVs/Edges.txt'
rating_dict_file = '/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/pickles/ratings_dict.pickle'
user_businesses_file = '/media/jarvis/16FABB77FABB51AB/Courses/Semester 2/Business Intelligence/Projects/Capstone/pickles/user_businesses.pickle'

user = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(users_file)
user.registerTempTable("Users")

business_category = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(business_category_file)
business_category.registerTempTable("Business_Category")

ratings  = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(ratings_file)
ratings.registerTempTable("Ratings")

business  = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(business_file)
business.registerTempTable("Business")

category = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(category_file)
category.registerTempTable("Category")

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

"""
def getWeights(u_v_pair):
	u = u_v_pair[0]
	v = u_v_pair[1]
	try:
		businesses = sqlContext.sql("SELECT business_id, user_id, stars FROM Ratings WHERE user_id = '%s' OR user_id = '%s'" %(u, v))
		businesses_rdd = businesses.map(lambda p: (p.business_id, (p.user_id, p.stars))).groupByKey().filter(lambda p: len(p[1]) > 1)
		ratings_rdd = businesses_rdd.map(lambda p: sorted(p[1], key=lambda x: x[0]))
		ratings_u = ratings_rdd.map(lambda p: p[0][1])
		ratings_v = ratings_rdd.map(lambda p: p[1][1])
		w = Statistics.corr(ratings_u, ratings_v, method="pearson")
		return (u, v, w)
	except Exception, e:
		return (u, v, 0)
"""

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