#!/usr/bin/env python2.7
#encoding:utf-8
import sys
import os
import linecache
from pykafka import KafkaClient
import logging 
import urllib2
import time
import atexit
import lxml.html as HTML
import json
from lxml import etree as etree
from signal import SIGTERM 
from logging.handlers import TimedRotatingFileHandler
from logging.handlers import RotatingFileHandler

reload(sys)
sys.setdefaultencoding('utf8')

query_file = "data/base/baike_for_google_final.txt"
point_file = "data/point"
failed_file = "data/base/failed_word.txt"
msglimit = 200000
sleep_limit = 10800 

brokers = "127.0.0.0.1:9092"
zookeepers = "127.0.0.1:2181"
seeds_topic = "seeds"

Rthandler = TimedRotatingFileHandler(filename = './logs/produce', when = 'D', interval = 1, backupCount = 5)
Rthandler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
Rthandler.setFormatter(formatter)
logging.getLogger('').addHandler(Rthandler)

user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36'
headers={'User-Agent':user_agent,
			 'referer': "https://www.google.com.hk",
			 "cookie":"HSID=AcDOcvRTEXjSf4eSv; SSID=A9jfk8MQ3e2lW5FNL; APISID=tCaB8F5pvjc5ibFx/AAduclcz4W-Dlcola; SAPISID=yQONLeXABaBeVB7c/ASKaJKdq8FetvXlxv; SID=vgLFOV9E7hBab7uf1B1ttG1Ab5Z4sDq4tb6_Gj1eRtku5vZo3CZCQJWYLxkgexAi-ODXUw.; NID=81=BdMMXxhaQD_mG14xQ-ndcQhB3O3JVHo2nVQOfxV3Y7yjXUz8DgeNSuYrcEO8KCUy7u418SSRrXeT-XPjSamGsg0uh6xUKbkawTnTeLjIeV1qv9qQ9DXN3pgRVkh-FAbgWb2xS74NniqCOtl9qrFp1IanZ6b8XOzIELZWuZCvnnUehuCw5EviEWfD7uh7ocYx6zkyvyJxclFXX6yQxvv5Y8w17EBddPZni16AMUaeJZqJPXtX9FmoEDKPDY-gqqYS8uQ-VUiIh-5faP4QZ-UhfElOup_THFwCxjRDvN8yLa-E1o1U"}

def post_msgs():
	client = KafkaClient(hosts=brokers, ssl_config=None)
	topic_obj = client.topics[seeds_topic]
	try:
		point_fd = file(point_file,"r")
		point = int(point_fd.read().strip())
		point_fd.close()
	except:
		point = 0
	query_words = linecache.getlines(query_file)
	query_words = query_words[point:]
	print len(query_words)
	fail_fd = open(failed_file, "a")
	with topic_obj.get_sync_producer() as producer:
		for word in query_words:
			try:
				res = producer.produce(word.strip())
				logging.warning("append msg success:" + str(point) + " " + str(word))
			except:
				logging.error("append msg error:" + str(word))
				fail_fd.write(word + "\n")
			point = point + 1
			point_fd = open(point_file,"w")
			point_fd.write(str(point))
			point_fd.close()
	fail_fd.close()

if __name__ == "__main__":
	post_msgs()
