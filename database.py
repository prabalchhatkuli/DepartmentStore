import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId
import redis


#Submitted by: Prabal Chhatkuli
#Database Project 2


# The following functions are REQUIRED - you should REPLACE their implementation
# with the appropriate code to interact with your Mongo database.
def initialize():
    # this function will get called once, when the application starts.
    # this would be a good place to initalize your connection!
    # You might also want to connect to redis...
    global CUSTOMERS
    global ORDERS
    global PRODUCTS

    global r
    client = MongoClient()
    CUSTOMERS = client.project2.CUSTOMERS
    PRODUCTS = client.project2.PRODUCTS
    ORDERS = client.project2.ORDERS

    #redis
    r = redis.StrictRedis(host='redis-19959.c15.us-east-1-2.ec2.cloud.redislabs.com', port=19959, password='W7WC2OpZwsa06YrIYNN2ru2NGfnwBJuw', charset="utf-8", decode_responses=True)
    
    print('Nothing to do here...')

####################################################CUSTOMERS######################################
#done
#yields a dictionary for each customer 
def get_customers():
    totalCustomers = CUSTOMERS.find({})
    for each in totalCustomers:
        yield each#yield a dictionary each time

#done
def get_customer(id):
    singleCustomer = CUSTOMERS.find_one({'_id': ObjectId(id)})
    return singleCustomer 

#done
def upsert_customer(customer):
    #jsonObject=customer;
    jsonObject = {'firstName':customer['firstName'], 'lastName' : customer['lastName'], 'street' : customer['street'], 'city' : customer['city'], 'state' : customer['state'], 'zip' : customer['zip']}
    #insert if id does not exist
    if '_id' not in customer:
        CUSTOMERS.insert_one(jsonObject)
    #update if exists
    else:
        if r.exists(customer['_id']):
            r.delete(customer['_id'])
        oldDetails = get_customer(customer['_id'])
        CUSTOMERS.update_one(oldDetails,{'$set':jsonObject})

#done
def delete_customer(id):#cascade on delete fk
    #ORDERS.delete_many({'customerId': ObjectId(id)})
    if r.exists(id):
        r.delete(id)
    ordersOfCustomer = ORDERS.find({'customerId': ObjectId(id)})
    for orders in ordersOfCustomer:
        if r.exists(orders['_id']):
            r.delete(orders['_id'])
    CUSTOMERS.delete_one({'_id':ObjectId(id)})
    #if a product is deleted delete any order with that porduct
    ORDERS.delete_many({'customerId': ObjectId(id)})

#########################################################PRODUCTS###########################################
#done
def get_products():
    totalProducts = PRODUCTS.find({})
    for each in totalProducts:
        yield each

#done
def get_product(id):
    singleProduct = PRODUCTS.find_one({'_id':ObjectId(id)})
    return singleProduct


def upsert_product(product):
    #jsonObject = product;
    jsonObject = {'name':product['name'], 'price' : product['price']}
    #insert if id does not exist
    if '_id' not in product:
        PRODUCTS.insert_one(jsonObject)
    #update if exists
    else:
        if r.exists(product['_id']):
            r.delete(product['_id'])
        oldDetails = get_product(product['_id'])
        PRODUCTS.update_one(oldDetails,{'$set':jsonObject})


#done
def delete_product(id):#cascade on delete fk
    if r.exists(id):
        r.delete(id)
    ordersOfProduct = ORDERS.find({'productId': ObjectId(id)})
    for orders in ordersOfProduct:
        if r.exists(orders['_id']):
            r.delete(orders['_id'])
    PRODUCTS.delete_one({'_id':ObjectId(id)})
    #if a product is deleted delete any order with that porduct
    r.flushall()
    ORDERS.delete_many({'productId': ObjectId(id)})
#####################################################ORDERS#################################################
#done
def get_orders():
    totalOrders = ORDERS.find()
    for orderInfo in totalOrders:
        orderInfo['product'] = get_product(orderInfo['productId'])
        orderInfo['customer'] = get_customer(orderInfo['customerId'])
        yield orderInfo

#done
def get_order(id):
    singleOrder = ORDERS.find_one({'_id':ObjectId(id)})
    return singleOrder

def upsert_order(order):
    #jsonObject = order;
    jsonObject = {'customerId' : ObjectId(order['customerId']), 'productId' : ObjectId(order['productId']), 'date' : order['date']}
    #insert if id does not exist
    if '_id' not in order:
        ORDERS.insert_one(jsonObject)
    #update if exists
    else:
        if r.exists(order['_id']):
            r.delete(order['_id'])
        oldDetails = get_order(order['_id'])
        ORDERS.update_one(oldDetails,{'$set':jsonObject})

#done
def delete_order(id):
    ORDERS.delete_one({'_id':ObjectId(id)})
    if r.exists(id):
        r.delete(id)

def customer_report(id):
    customer = get_customer(id)
    orders = get_orders()
    customer['orders'] = [singleOrder['_id'] for singleOrder in ORDERS.find({'customerId': id})]
    return customer

# Pay close attention to what is being returned here.  Each product in the products
# list is a dictionary, that has all product attributes + last_order_date, total_sales, and 
# gross_revenue.  This is the function that needs to be use Redis as a cache.

# - When a product dictionary is computed, save it as a hash in Redis with the product's
#   ID as the key.  When preparing a product dictionary, before doing the computation, 
#   check if its already in redis!
def sales_report():
    allProducts = PRODUCTS.find({})
    for each in allProducts:
        productId = each['_id']
        #get all the hash
        productHash = r.hgetall(productId)
        if not r.exists(productId):
            lisOfOrder = ORDERS.find({'productId': productId})
            lisOfOrder = sorted(lisOfOrder, key=lambda k: k['date'])
            each['total_sales']=len(lisOfOrder)
            each['gross_revenue'] = each['price']*len(lisOfOrder)
            if len(lisOfOrder)>0:
                each['last_order_date'] = lisOfOrder[-1]['date']
            else:
                continue
                #continue
            r.hmset(productId, each)
            productHash = each
        yield productHash

initialize()