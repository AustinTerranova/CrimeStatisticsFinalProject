db.orders.find({"orderDetails.priceEach": {gte: 30000}},{customerName:1,employeeName:1})

db.customers.find({"payments.amount": { $gte: 100000 } },  {customerName:1 , contactFirstName:1,contactLastName:1} )

db.employees.find({"office.country": {$not: /USA/}})