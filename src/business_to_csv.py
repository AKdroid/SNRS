import csv
import sys
import json

j = json.JSONDecoder()

f = open('restaurant_business.txt')

fw = open('Business.csv','w')

headers = ['business_id','name', 'full_address', 'stars', 'review_count','good_for_kids', 'good_for_groups', 'price_range',
    'alcohol','waiter_service','parking','ambience','take_out','drive_thru','delivery','good_for','open']

writer = csv.DictWriter(fw, fieldnames = headers)
writer.writeheader()

categories = set()

for line in f:
    business = j.decode(line)
    for category in business["categories"]:
        categories.add(category)
    newdict = {}
    newdict["business_id"] = business["business_id"]
    newdict["review_count"] = business["review_count"]
    newdict["name"] = business["name"].encode("utf-8")
    newdict["stars"] = business["stars"]
    newdict["full_address"] = business["full_address"].encode("utf-8")
    if "Good for Kids" in business:
        newdict["good_for_kids"]= business["Good for Kids"]
    else:
        newdict["good_for_kids"]=False
    if "Good For Groups" in business:
        newdict["good_for_groups"] = business["Good For Groups"]
    else:
        newdict["good_for_groups"] = False
    if "Price Range" in business:
        newdict["price_range"] = business["Price Range"]
    else:
        newdict["price_range"] = 3 #Default price range
    if "Alcohol" in business:
        newdict["alcohol"] = business["Alcohol"]
    else:
        newdict["alcohol"] = False
    if "Waiter Service" in business:
        newdict["waiter_service"] = business["Waiter Service"]
    else:
        newdict["waiter_service"] = False
    if "Drive-Thru" in business:
        newdict["drive_thru"] = business["Drive-Thru"]
    else:
        newdict["drive_thru"] = False
    newdict["open"] = business["open"]
    if "Delivery" in business:
        newdict["delivery"]=business["Delivery"]
    else:
        newdict["delivery"]=False
    if "Delivery" in business:
        newdict["take_out"] = business["Take-out"]
    else:
        newdict["take_out"] = False
    if "Parking" in business:
        newdict["parking"] = True in business["Parking"].values()
    else:
        newdict["parking"] = False
    if "Ambience" in business:
        newdict["ambience"] = business["Ambience"].values.count(True)
    else:
        newdict["ambience"] = 0
    if "Good For" in business:
        newdict["good_for"] = business["Good For"].values.count(True)
    else:
        newdict["good_for"] = 0
    writer.writerow(newdict);

i=1

f_cat = open('Category.csv','w')
writer_cat = csv.DictWriter(f_cat, fieldnames = ["category_id","name"])

writer_cat.writeheader()
for cat in categories:
    newdict = {}
    newdict["category_id"] = i
    newdict["name"] = cat
    writer_cat.writerow(newdict)
    i=i+1

f_cat.close()
f.close()
fw.close()

