from flask import Flask, render_template, request, redirect
from bson.objectid import ObjectId
from .database import *
app = Flask(__name__)

initialize()

@app.route('/')
def index():
    return render_template('index.html')


#########################################################################
## Customer Routes
@app.route('/customer/')
def customer_index():
    customers = get_customers()
    return render_template('customers/index.html', customers=customers)

@app.route('/customer/new', methods=['GET', 'POST'])
def customer_new():
    if request.method == 'GET':
        return render_template('customers/new.html')
    else:
        n = request.form.copy().to_dict()
        upsert_customer(n)
        return redirect("/customer/", code=302)    

@app.route('/customer/edit/<id>', methods=['GET', 'POST'])
def customer_edit(id):
    if request.method == 'GET':
        customer = get_customer(id)
        return render_template('customers/edit.html', customer=customer)
    else:
        edit = request.form.copy().to_dict()
        upsert_customer(edit)
        return redirect("/customer/", code=302)    

@app.route('/customer/delete/<id>', methods=['GET', 'POST'])
def customer_delete(id):
    if request.method == 'GET':
        customer = get_customer(id)
        return render_template('customers/delete.html', customer=customer)
    else:
        delete_customer((id))
        return redirect("/customer/", code=302)    

#########################################################################


#########################################################################
## Product Routes
@app.route('/product/')
def product_index():
    products = get_products()
    return render_template('products/index.html', products=products)

@app.route('/product/new', methods=['GET', 'POST'])
def product_new():
    if request.method == 'GET':
        return render_template('products/new.html')
    else:
        n = request.form.copy().to_dict()
        n['price'] = float(n['price'])
        print(n)
        upsert_product(n)
        return redirect("/product/", code=302)    

@app.route('/product/edit/<id>', methods=['GET', 'POST'])
def product_edit(id):
    if request.method == 'GET':
        product = get_product(id)
        return render_template('products/edit.html', product=product)
    else:
        edit = request.form.copy().to_dict()
        edit['price'] = float(edit['price'])
        upsert_product(edit)
        return redirect("/product/", code=302)    

@app.route('/product/delete/<id>', methods=['GET', 'POST'])
def product_delete(id):
    if request.method == 'GET':
        product = get_product(id)
        return render_template('products/delete.html', product=product)
    else:
        delete_product(id)
        return redirect("/product/", code=302)    
#########################################################################


#########################################################################
## Order Routes
@app.route('/order/')
def order_index():
    orders = get_orders()
    return render_template('orders/index.html', orders=orders)

@app.route('/order/new', methods=['GET', 'POST'])
def order_new():
    if request.method == 'GET':
        return render_template('orders/new.html', customers=get_customers(), products=get_products())
    else:
        n = request.form.copy().to_dict()
        n['customerId'] = n['customerId']
        n['productId'] = n['productId']
        n['date'] = n['year'] + '-' + n['month'] + '-' + n['day']
        upsert_order(n)
        return redirect("/order/", code=302)    

@app.route('/order/delete/<id>', methods=['GET', 'POST'])
def order_delete(id):
    if request.method == 'GET':
        order = get_order(id)
        return render_template('orders/delete.html', order=order)
    else:
        delete_order(id)
        return redirect("/order/", code=302)    
#########################################################################


@app.route('/reports/product', methods=['GET'])
def get_sales_report():
    products = sales_report()
    return render_template('sales.html',products=products)
#########################################################################