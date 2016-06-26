#! /usr/bin/env python

from datetime import datetime
import json
import random
import os
import sys
import time
import urllib

class IPGenerator:
    def __init__(self, session_count, session_length):
        self.session_count = session_count
        self.session_length = session_length
        self.sessions = {}

    def get_ip(self):
        self.session_gc()
        self.session_create()
        ip = self.sessions.keys()[random.randrange(len(self.sessions))]
        self.sessions[ip] = self.sessions[ip] + 1
        return ip

    def session_create(self):
        while len(self.sessions) < self.session_count:
            self.sessions[self.random_ip()] = 0

    def session_gc(self):
        for (ip, count) in self.sessions.items():
            if count >= self.session_length:
                del self.sessions[ip]

    def random_ip(self):
        octets = []
        octets.append(str(random.randrange(223) + 1))
        for i in range(3):
            octets.append(str(random.randrange(255)))
        return ".".join(octets)

class LogGenerator:
    PRODUCTS = {}

    REQUESTS = {
        "/departments": 40,
        "/department/*DEPARTMENT*/categories": 20,
        "/department/*DEPARTMENT*/products": 10,
        "/categories/*CATEGORY*/products": 5,
        "/product/*PRODUCT*": 10,
        "/add_to_cart/*PRODUCT*": 5,
        "/login": 5,
        "/logout": 2,
        "/checkout": 3,
        "/support": 1
    }

    EXTENSIONS = {
        'html': 40,
        'php': 30,
        'png': 15,
        'gif': 10,
        'css': 5,
    }

    RESPONSE_CODES = {
        200: 92,
        404: 5,
        503: 3,
    }

    USER_AGENTS = {
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36": 11,
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:30.0) Gecko/20100101 Firefox/30.0": 6,
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36": 5,
        "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36": 4,
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.77.4 (KHTML, like Gecko) Version/7.0.5 Safari/537.77.4": 4,
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36": 3,
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:30.0) Gecko/20100101 Firefox/30.0": 3,
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.76.4 (KHTML, like Gecko) Version/7.0.4 Safari/537.76.4": 3,
        "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36": 2,
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36": 2,
        "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:30.0) Gecko/20100101 Firefox/30.0": 2,
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36": 2,
        "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko": 2,
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36": 2,
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:30.0) Gecko/20100101 Firefox/30.0": 1,
        "Mozilla/5.0 (Windows NT 6.1; rv:30.0) Gecko/20100101 Firefox/30.0": 1
    }

    DEPARTMENTS = {
    }

    CATEGORIES = {
    }

    def __init__(self, ipgen):
        self.ipgen = ipgen
        self.set_products()
        self.set_departments()
        self.set_categories()
 
    def set_products(self):
        cwd = os.getcwd()
        json_text = open(cwd + '/data/products.json', 'r').read()
        products = json.loads(json_text)
        for p in products:
            self.PRODUCTS[p['product_id']] = int(5000/p['product_price'])

    def set_departments(self):
        cwd = os.getcwd()
        json_text = open(cwd + '/data/departments.json', 'r').read()
        depts = json.loads(json_text)
        for p in depts:
            self.DEPARTMENTS[p['department_name']] = 100/len(depts)

    def set_categories(self):
        cwd = os.getcwd()
        json_text = open(cwd + '/data/categories.json', 'r').read()
        cats = json.loads(json_text)
        for p in cats:
            self.CATEGORIES[p['category_name']] = 100/len(cats)

    def write_qps(self, dest, qps):
        sleep = 1.0 / qps
        while True:
            self.write(dest, 1)
            time.sleep(sleep)

    def write(self, dest, count):
        for i in range(count):
            ip = self.ipgen.get_ip()
            request = self.pick_weighted_key(self.REQUESTS)
            product = self.pick_weighted_key(self.PRODUCTS)
            dept    = self.pick_weighted_key(self.DEPARTMENTS)
            cat     = self.pick_weighted_key(self.CATEGORIES)

            request = urllib.quote(request.replace("*PRODUCT*",str(product)).replace("*DEPARTMENT*",dept).replace("*CATEGORY*",cat).lower())

            ext = self.pick_weighted_key(self.EXTENSIONS)
            resp_code = self.pick_weighted_key(self.RESPONSE_CODES)
            resp_size = random.randrange(2 * 1024) + 192;
            ua = self.pick_weighted_key(self.USER_AGENTS)
            date = datetime.now().strftime("%d/%b/%Y:%H:%M:%S -0800") # Hard-coded as Python has no standard timezone implementation
            dest.write("%(ip)s - - [%(date)s] \"GET %(request)s HTTP/1.1\" %(resp_code)s %(resp_size)s \"-\" \"%(ua)s\"\n" %
                {'ip': ip, 'date': date, 'request': request, 'resp_code': resp_code, 'resp_size': resp_size, 'ua': ua})
            dest.flush()

    def pick_weighted_key(self, hash):
        total = 0
        for t in hash.values():
            total = total + t
        rand = random.randrange(total)

        running = 0
        for (key, weight) in hash.items():
            if rand >= running and rand < (running + weight):
                return key
            running = running + weight

        return hash.keys()[0]

ipgen = IPGenerator(100, 10)
LogGenerator(ipgen).write_qps(sys.stdout, 1)

