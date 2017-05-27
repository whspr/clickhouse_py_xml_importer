__author__ = 'whspr'
from lxml import etree
from time import time
import datetime
from infi.clickhouse_orm import models as md
from infi.clickhouse_orm import fields as fd
from infi.clickhouse_orm import engines as en
from infi.clickhouse_orm.database import Database


class Data(md.Model):
    """
    structure of your data
    """
    # describes datatypes and fields
    available = fd.StringField()
    category_id = fd.StringField()
    currency_id = fd.StringField()
    delivery = fd.StringField()
    description = fd.StringField()
    item_id = fd.StringField()
    modified_time = fd.DateField()
    name = fd.StringField()
    oldprice = fd.StringField()
    picture = fd.StringField()
    price = fd.StringField()
    sales_notes = fd.StringField()
    topseller = fd.StringField()
    # creating an sampled MergeTree
    engine = en.MergeTree('modified_time', ('available', 'category_id', 'currency_id', 'delivery',
                                        'description', 'item_id', 'name', 'oldprice', 'picture',
                                        'price', 'sales_notes', 'topseller'))


def safely_get_data(element, key):
    """
    Get value or return and error value
    :param element: branch name with 'key: value' couple
    :param key: key name
    :return: value of 'key: value' couple or error message
    """
    try:
        for child in element:
            if child.tag == key:
                return child.text
    except:
        return "not found"


def parse_clickhouse_xml(filename, db_name, db_host):
    """
    Parse xml file and insert it into db
    :param filename: file name
    :param db: database name
    :param db_host: database host and port. Example: http://localhost:8123
    """
    data_buffer = []
    t = time()
    # start read file
    for event, offer in etree.iterparse(filename, tag="offer"):
        # getting values
        available = offer.attrib['available']
        category_id = safely_get_data(offer, 'categoryId')
        currency_id = safely_get_data(offer, 'currencyId')
        delivery = safely_get_data(offer, 'delivery')
        description = safely_get_data(offer, 'description')
        item_id = offer.attrib['id']
        modified_time = safely_get_data(offer, 'modified_time')
        name = safely_get_data(offer, 'name')
        oldprice = safely_get_data(offer, 'oldprice')
        picture = safely_get_data(offer, 'picture')
        price = safely_get_data(offer, 'price')
        sales_notes = safely_get_data(offer, 'sales_notes')
        topseller = safely_get_data(offer, 'top_seller')
        # convert datatime from unix datetime style
        modified_time = datetime.datetime.fromtimestamp(int(modified_time)).strftime('%Y-%m-%d')
        # inserting data into clickhouse model representation
        insert_data = Data(
            available=      available,
            category_id=    category_id,
            currency_id=    currency_id,
            delivery=       delivery,
            description=    description,
            item_id=        item_id,
            modified_time=  modified_time,
            name=           name,
            oldprice=       oldprice,
            picture=        picture,
            price=          price,
            sales_notes=    sales_notes,
            topseller=      topseller
        )
        # appends data into couple
        data_buffer.append(insert_data)
        offer.clear()
    # print elasped time value to prepare a couple of data instances
    print "time to prepare %s data %s" % (len(data_buffer), time() - t)
    # open database with database name and database host values
    db = Database(db_name, db_url=db_host)
    # create table to insert prepared data
    db.create_table(Data)
    t = time()
    # insert prepared data into database
    db.insert(data_buffer)
    print "time to insert %s" % (time() - t)


if __name__ == '__main__':
    parse_clickhouse_xml(
        'data.xml',
        'database',
        'http://localhost:8123'
    )