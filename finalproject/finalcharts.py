#import pymongo module
import pymongo
#import pygal graphing module
import pygal

#Connect to the mongodb server
#change username and password below with your own
myclient = pymongo.MongoClient(host = "mongodb://localhost:27017/",
serverSelectionTimeoutMS = 3000,
username="Username",
password='Password')


db = myclient["finalproject"]


employeesCollection = db["employees"]

customersCollection = db["customers"]

productsCollection = db["products"]

orderCollection = db["orders"]


query = [{"$group": {"_id": "salesRepEmployeeNumber", "count":{"$sum":1}}},
{"$sort":{"count":-1}}]

cursor = customersCollection.aggregate(query)
customers = []
employees = []

for query in cursor:
	employees.append(query["_id"])
	customers.append(query["count"])

bar_chart = pygal.Bar()
bar_chart.title = "Number of customers per employee"
bar_chart.x_labels = map(str,employees)
bar_chart.add("Number of customers per employee",customers)
bar_chart.render_to_file("images/q1.svg")
print("question one executed successfully")




query = [{"$unwind":"$orderDetails"},{"$group":{"_id":"orderDetails.productName","count":{"$sum":1}}},{"$limit":10}]

cursor = orderCollection.aggregate(query)
products = []
orders = [] 


for query in cursor:
    orders.append(query["_id"])
    products.append(query["count"])

bar_chart = pygal.Bar()

bar_chart.title = "top 10 products by name orders"
bar_chart.x_labels = map(str,orders)
bar_chart.add("top 10 products",products)
bar_chart.render_to_file("images/q2.svg")
print("question two executed successfully")


'''
query = [{"$unwind": "$orderDetails"), {"$group:"{:{"_id": "$orderDetails.productName", "dollarValue": {"$sum": ("$multiply": ["$orderDetails.quantityOrdered",
"$orderDetails.priceEach"]}}}}}, {"$sort": {"dollarValue": -1}}, {"$limit":10}]

cursor = ordersCollection.aggregate(query)
products = []
orders = []

for query in cursor:
    orders.append(query["_id"])
    products.append(query["dollarValue"])

bar_chart = pygal.Bar()
bar_chart.title = "top 10 products with highest dollar value by name"
bar_chart.x_labels = map(str,orders)
bar_chart.add("top 10 products",products)
bar_chart.render_to_file("images/q3.svg")
print("question one executed successfully")
'''

query = [{"$group": {"_id": "$customerName", "count":{"$sum":1}}}, {"$sort":{"count": 1}}]

cursor = orderCollection.aggregate(query)
customers = []
orders = []

for query in cursor:
    customers.append(query["_id"])
    orders.append(query["count"])

bar_chart = pygal.HorizontalBar()
bar_chart.title = "customer that placed the most orders"
bar_chart.x_labels = map(str,customers)
bar_chart.add("customers that placed the most orders",orders)
bar_chart.render_to_file("images/q5.svg")
print("question five executed successfully")

query = [{"$unwind": "$payments"},{"$group":{"_id":"$customerName","totalPayments":{"$sum":"$payments.amount"}}}, {"$sort":{"totalPayments":-1}}, {"$limit":10}]

cursor = orderCollection.aggregate(query)
payments = []
customers = []

for query in cursor:
    payments.append(query["_id"])
    customers.append(query["count"])

bar_chart = pygal.Bar()
bar_chart.title = "top 10 customers name"
bar_chart.x_labels = map(str,customers)
bar_chart.add("payments collected",customers)
bar_chart.render_to_file("images/q9.svg")
print("question nine executed successfully")



query = [{"$group":{"_id":"$state", "count":{"$sum":1}}}, {"$sort":{"count":-1}}]

cursor = customersCollection.aggregate(query)
customers2 = []
customers = []

for query in cursor:
    customers2.append(query["_id"])
    customers.append(query["count"])

bar_chart = pygal.Pie()
bar_chart.title = "customers by state"
bar_chart.x_labels = map(str,customers)
bar_chart.add("payments collected",customers)
bar_chart.render_to_file("images/q10.svg")
print("question ten executed successfully")




query = [{"$group":{"_id":"$reportsTo","count":{"$sum":1}}},{"$sort":{"count":1}}]
cursor = employeesCollection.aggregate(query)
employee2 = []
employee = []

for query in cursor:
    employee2.append(query["_id"])
    employee.append(query["count"])

bar_chart = pygal.Bar()
bar_chart.title = "employees that manage the most employees"
bar_chart.x_labels = map(str,customers)
bar_chart.add("employees",employee)
bar_chart.render_to_file("images/q11.svg")
print("question q11 executed successfully")
