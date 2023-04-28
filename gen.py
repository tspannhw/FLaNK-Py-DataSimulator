# https://github.com/tspannhw/FLiP-PY-FakeDataPulsar

from time import sleep
from math import isnan
import time
import sys
import datetime
import subprocess
import sys
import os
from subprocess import PIPE, Popen
import traceback
import math
import base64
import json
from time import gmtime, strftime
import random, string
import psutil
import base64
import uuid
import socket 
import time
import logging
from faker import Faker
from faker.providers import internet, address, automotive, barcode, company, date_time, geo, job, misc, person
from faker.providers import phone_number, user_agent
from kafka import KafkaProducer
from kafka.errors import KafkaError

fake = Faker()
fake.add_provider(internet)
fake.add_provider(address)
fake.add_provider(automotive)
fake.add_provider(barcode)
fake.add_provider(company)
fake.add_provider(date_time)
fake.add_provider(geo)
fake.add_provider(job)
fake.add_provider(misc)
fake.add_provider(person)
fake.add_provider(phone_number)
fake.add_provider(user_agent)

producer = KafkaProducer(key_serializer=str.encode, value_serializer=lambda v: json.dumps(v).encode('ascii'),bootstrap_servers='kafka:9092',retries=3)

MAX_COUNT = int(5000)
counter = int(0)
while (counter < MAX_COUNT):

    uuid_key = '{0}_{1}'.format(strftime("%Y%m%d%H%M%S",gmtime()),uuid.uuid4())
    #userRecord.created_dt = fake.date() 
    #userRecord.user_id = uuid_key
    #userRecord.ipv4_public = fake.ipv4_public()
    #userRecord.email = fake.ascii_email()
    #userRecord.user_name = fake.user_name()
    #userRecord.cluster_name = fake.slug()
    #userRecord.city = fake.city() 
    #userRecord.country = fake.country()
    #userRecord.postcode = fake.postcode()
    #userRecord.street_address = fake.street_address()
    #userRecord.license_plate = fake.license_plate()
    #userRecord.ean13 = fake.ean13()
    #userRecord.response = fake.catch_phrase() 
    #userRecord.comment = fake.bs() 
    #userRecord.company = fake.company()
    #userRecord.latitude = float(fake.latitude())
    #userRecord.longitude = float(fake.longitude())
    #userRecord.job = fake.job()
    #userRecord.email_me = fake.boolean()
    #userRecord.secret_code = fake.md5()
    #userRecord.password = fake.password()
    #userRecord.first_name = fake.first_name()
    #userRecord.last_name = fake.last_name()
    #userRecord.phone_number = fake.phone_number()
    #userRecord.user_agent = fake.user_agent()
    print(userRecord)
    #producer.send(userRecord,partition_key=str(uuid_key))
    producer.send("fakeuser", key=uuid_key, value= 
    {'uuid': uuid_key, 'symbol': stockitem['s'], 'ts': float(stockitem['t']), 
    'currentts': float(strftime("%Y%m%d%H%M%S",gmtime())), 
    'useragent': str(fake.user_agent()),
    'price': float(stockitem['p']), 
    'tradeconditions': ','.join(stockitem['c'])  }  )
    producer.flush()
    counter += 1
    
    
