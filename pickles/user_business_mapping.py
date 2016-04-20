import csv
import pickle

user_business_dict = {}
rating_dict = {}

rating = open('Ratings_train.csv','r')

headers = rating.readline()

i=1
for row in rating:
    review_id,user_id,business_id,stars = row.strip().split(',')
    if user_id not in user_business_dict:
        user_business_dict[user_id] = set()
    user_business_dict[user_id].add(business_id)
    rating_key = user_id+'_'+business_id
    if rating_key not in rating_dict:
        rating_dict[rating_key] = stars
    print i 
    i+=1

f1 = open('user_businesses.pickle','w')
pickle.dump(user_business_dict,f1)


f2 = open('ratings_dict.pickle','w')
pickle.dump(rating_dict,f2)
f1.close()
f2.close()

user_business_dict = {}
rating_dict = {}

rating = open('Ratings_test.csv','r')

headers = rating.readline()

i=1
for row in rating:
    review_id,user_id,business_id,stars = row.strip().split(',')
    if user_id not in user_business_dict:
        user_business_dict[user_id] = set()
    user_business_dict[user_id].add(business_id)
    rating_key = user_id+'_'+business_id
    if rating_key not in rating_dict:
        rating_dict[rating_key] = stars
    print i
    i+=1

f1 = open('user_businesses_test.pickle','w')
pickle.dump(user_business_dict,f1)


f2 = open('ratings_test_dict.pickle','w')
pickle.dump(rating_dict,f2)
f1.close()
f2.close()
