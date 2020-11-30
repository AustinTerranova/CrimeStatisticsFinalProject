#import mysql connector and pygal
import mysql.connector
import pymongo
import pygal
import json
##################### SETUP  DATABASE CONNECTIONS ###############

#connect to your mysql datbase
mydb = mysql.connector.connect(
  host="159.65.185.126",
  user="austin",
  passwd="austin13",
  database="classicmodels"
)

#connect to the mongo database
mongo_client = pymongo.MongoClient(host = "mongodb://localhost:27017/",
serverSelectionTimeoutMS = 3000,
username="Username",
password='Password')

#Select mongo datacbase and collections
mongo_db = mongo_client["finalproject"]
employee_collection = mongo_db["employees"]
customer_collection = mongo_db["customers"]
product_collection = mongo_db["products"]
order_collection = mongo_db["orders"]
#The cursor will execute queries on your MySql Datbase
mycursor = mydb.cursor()

################# QUERIES TO POPULATE EMPLOYEE COLLECTION  ####################

#sql query generates a list of managers. People who have other employees report to them
sql_query = '''select employeeNumber, firstName, lastName
from employees
where employeeNumber IN (select reportsTo from employees);'''




#Execute the query
mycursor.execute(sql_query)

#Get the query result
query_result = mycursor.fetchall()

#Create managers dictionary
managers = {}
'''query_result is a list of tuples. The loop below converts the list of tuples into
a dictionary where the dictionary key is the employeeNumber and the value is the 
first and last name'''
for result in query_result:
    managers[result[0]] = result[1] + " " + result[2]

#Select all data from the employees and offices tables
sql_query = '''SELECT *
FROM employees e, offices o
WHERE e.officeCode = o.officeCode;'''


#Execute the query
mycursor.execute(sql_query)

#Get the query results
query_result = mycursor.fetchall()

sql_query = '''SELECT *
FROM employees e, offices o
WHERE e.officeCode = o.officeCode;'''


#Execute the query
mycursor.execute(sql_query)

#Get the query results
record_result = mycursor.fetchall()
#################### WRITE DOCUMENTS TO INSERT INTO EMPLOYEE COLLECTIONS ################

#list to store employee documents
employees = []
customers = []
products = []
orders = []
list_of_payment_documents = []
#loop through tuples in query_result. Write data to json format to store in our json file
for record in query_result:
    office_document = {
    "officeCode": record[8],
    "city": record[9],
    "phone": record[10],
    "addressLine1": record[11],
    "addressLine2": record[12],
    "state": record[13],
    "country" : record[14],
    "postalCode": record[15],
    "territory": record[16]
    }

    '''for employees who don't report to anyone, set the reportsTo value to N/A
    for other employees with a manager get the manager name from the managers dictionary
    using the the employeeNumber as the dictionary key'''
    if record[6] == None:
        manager = "N/A"
    else:
        manager = managers[int(record[6])]

    employee_document = {
    "_id": record[0],
    "lastName": record[1],
    "firstName": record[2],
    "extension": record[3],
    "email": record[4],
    "reportsTo": manager,
    "jobTitle": record[7],
    "office": office_document
    }
    employees.append(employee_document)



#insert employee documents into the employees collection in the mongo database
employee_collection.drop()
employee_collection.insert_many(employees)

#write to mongo formated data to a json file
json_file = open("employees.json", "w") #open the file
json_file.write("[\n") #write the opening bracket forthe list
counter = 1 #use counter to determine id the last document is being written

#loop through the list of employee documents
for employee_doc in employees:
    json_file.write(json.dumps(employee_doc)) #convert a python dictionary to a json object
    #write a comma after the document if it is not the last in the list
    if counter != len(employees):
        json_file.write(",\n")
        counter += 1
    else: #don't write a comma after the document if it is the last in the list
        json_file.write("\n")

json_file.write("]\n")
json_file.close()



print("\nScript executed successfully")



#sql query generates a list of managers. People who have other employees report to them
sql_query = '''select c.*, e.firstName,e.lastName FROM customers c, employees e where c.salesRepEmployeeNumber = e.employeeNumber;'''




#Execute the query
mycursor.execute(sql_query)

#Get the query result
query_result = mycursor.fetchall()




'''customer_record is a list of tuples. The loop below converts the list of tuples into
a dictionary where the dictionary key is the employeeNumber and the value is the 
first and last name'''
#select all from payments table iterate through query result attaching the customer records to the proper payments I think 
for customer_record in query_result:
	payment_query = "SELECT * FROM payments WHERE customerNumber = "+" "+ str(customer_record[0])


#Execute the query
	mycursor.execute(payment_query)

#Get the query results
	query_result = mycursor.fetchall()

	for payment_record in record_result:
		#print(payment_record)
		payment_doc = {
		"checkNumber": payment_record[1],
		"paymentDate": str(payment_record[2]),
		"amount": str(payment_record[3]),
		}
		list_of_payment_documents.append(payment_doc)

	customer_doc = {
	"_id": customer_record[0],
	"customerName": customer_record[1],
	"contactLastName": customer_record[2],
	"contactFirstName": customer_record[3],
	"phone": customer_record[4],
	"addressLine1": customer_record[5],
	"addressLine2": customer_record[6],
	"city": customer_record[7],
	"state": customer_record[8],
	"postalCode": customer_record[9],
	"country": customer_record[10],
	"salesRep": customer_record[13] + " " + customer_record[14],
	"creditLimit": float(customer_record[12]),
	"payments": list_of_payment_documents
	}

	customers.append(customer_doc)

customer_collection.drop()
customer_collection.insert_many(customers)

json_file = open("customers.json", "w") #open the file
json_file.write("[\n") #write the opening bracket forthe list
count = 1
for customer_doc in customers:
    json_file.write(json.dumps(customer_doc)) #convert a python dictionary to a json object
    #write a comma after the document if it is not the last in the list
    if count != len(customers):
        json_file.write(",\n")
        count += 1
    else: #don't write a comma after the document if it is the last in the list
        json_file.write("\n")

json_file.write("]\n")
json_file.close()


print("\nScript executed successfully")




sql_query = '''SELECT products.*, productlines.* FROM products,productlines WHERE products.productline = productlines.productline ;'''
production = {}
#for result in query_result:
#	production[result[0]] = str(result[1]) + " " + str(result[2])
#Execute the query
mycursor.execute(sql_query)

#Get the query result
query_result = mycursor.fetchall()
product = []
for production_records in query_result:
	productLine_doc = {
    "line":production_records[9],
    "textDescription":production_records[10],
    "htmlDescription":production_records[11],
    "image":production_records[12],
    }


	product_doc ={
    "_id":production_records[0] ,
    "productName": production_records[1],
    "productScale": production_records[3],
    "productVendor": production_records[4],
    "productDescription": production_records[5],
    "quantityInStock": production_records[6],
    "buyPrice":float(production_records[7]),
    "MSRP": float(production_records[8]),
	"productLine":  productLine_doc,
	}
	product.append(product_doc)


product_collection.drop()
product_collection.insert_many(product)
#write_json_file
json_file = open("products.json", "w") #open the file
json_file.write("[\n") #write the opening bracket forthe list
count = 1
for product_doc in product:
    json_file.write(json.dumps(product_doc)) #convert a python dictionary to a json object
    #write a comma after the document if it is not the last in the list
    if count != len(product):
        json_file.write(",\n")
        count += 1
    else: #don't write a comma after the document if it is the last in the list
        json_file.write("\n")

json_file.write("]\n")
json_file.close()


print("\nScript executed successfully")

sql_query = '''SELECT orders.*, customers.customerName,employees.firstName,employees.lastName
FROM orders JOIN customers ON orders.customerNumber = customers.customerNumber JOIN employees ON customers.salesRepEmployeeNumber = employees.employeeNumber;'''
production = {}
#for result in query_result:
#   production[result[0]] = str(result[1]) + " " + str(result[2])
#Execute the query
mycursor.execute(sql_query)
orders=[]
list_of_order_details = []
#Get the query result
result_query = mycursor.fetchall()
for result in result_query:
	sql_query = '''SELECT orderdetails.*, products.productName FROM orderdetails JOIN products ON orderdetails.productCode = products.productCode
	Where orderdetails.orderNumber = '''+str(result[0]) +";"
	mycursor.execute(sql_query)

	query_result = mycursor.fetchall()
#	list_of_order_details = []

#orders = [] 
	for order_records in query_result:
		orderDetails_doc = {
    	"productName": order_records[5],
    	"quantityOrdered": order_records[4],
    	"priceEach": float(order_records[3])
		}
		list_of_order_details.append(orderDetails_doc)



	orders_doc = {
	"_id":result[0] ,
    "orderDate": str(result[1]),
    "requiredDate": str(result[2]),
    "shippeddDate": str(result[3]),
    "status": result[6],
    "comments": result[7],
    "customerName": result[8],
    "employeeName": result[9],
    "orderDetails": list_of_order_details

	}
	print(orders_doc)
	orders.append(orders_doc)

order_collection.drop()
order_collection.insert_many(orders)
#write_json_file
json_file = open("orders.json", "w") #open the file
json_file.write("[\n") #write the opening bracket forthe list
count = 1
for orders_doc in orders:
    json_file.write(json.dumps(orders_doc)) #convert a python dictionary to a json object
    #write a comma after the document if it is not the last in the list
    if count != len(orders):
        json_file.write(",\n")
        count += 1
    else: #don't write a comma after the document if it is the last in the list
        json_file.write("\n")

json_file.write("]\n")
json_file.close()


print("\nScript executed successfully")
