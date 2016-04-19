import csv

relevantUsers = []
with open('User.csv', 'rb') as csvfile:
	lines = csv.reader(csvfile, delimiter=',')
	relevantUsers = [id[0] for id in lines]

alreadyAdded = [];	

with open('Friend.csv', 'rb') as csvfile:
	f = open("Edges.txt", 'w')
	for lines in csvfile:
		lis = lines.split(",",1)
		if lis[0] in relevantUsers:
			friends =  lis[1][1:-2].split(",")
			for friend in friends:
				friend = friend.strip()[2:-1]
				if friend in relevantUsers and friend is not '':
					alreadyAdded.append((lis[0].strip(),friend))
					found = [x for x in alreadyAdded if x[1] == lis[0].strip() and x[0] == friend]
					if(len(found) == 0):
						f.write(lis[0].strip()+","+friend+ '\n')
