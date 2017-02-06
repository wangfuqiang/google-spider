#!/usr/bin/env python2.7
#encoding:utf-8
import sys
import os
from pykafka import KafkaClient
import logging 
import urllib2
import Queue
import socket
import time
import datetime
import random
from random import choice
import atexit
import lxml.html as HTML
import json
from lxml import etree as etree
from signal import SIGTERM 
from daemon import Daemon
from logging.handlers import TimedRotatingFileHandler
from logging.handlers import RotatingFileHandler

reload(sys)
sys.setdefaultencoding('utf8')

brokers = ["127.0.0.0.1:9092"]
zookeepers = "127.0.0.1:2181"
crawl_result_topic = "fruits"
crawl_query_topic = "seeds"
crawl_dog_topic = "dog"
# UA info
UA_file = "/home/ec2-user/deamon/spider/data/base/ua.list"

#for debug 
run_env = "online"

Rthandler = TimedRotatingFileHandler(filename = './logs/crawl', when = 'H', interval = 1, backupCount = 10)
Rthandler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
Rthandler.setFormatter(formatter)
logging.getLogger('').addHandler(Rthandler)

uas = []

def load_UA():
    uas = []
    uas = open(UA_file, "r").readlines()
    try:
        uas = open(UA_file, "r").readlines()
    except:
        logging.error("open ua file error:" + UA_file)
    if len(uas) == 0:
        exit(1)
    return uas
def time_convert(strtime,format):
    inttime = time.mktime(time.strptime(strtime,format))
    inttime = inttime + 28800
    strtime = time.strftime(format,time.localtime(inttime))
    return strtime

def Worker(word, uas): 
    msgs = []
    query = word.replace(" ","+")
    redirect_url = "https://www.google.com.hk/search?newwindow=1&safe=strict&hl=zh-CN&biw=929&bih=931&site=imghp&tbm=isch&gbv=2&q=" + query.decode("gbk")
    url_for_log = redirect_url.encode("gbk", "ignore")
    logging.warning("Crawl url is " +  url_for_log)
    """
    proxy = urllib2.ProxyHandler({'https':'wangfuqiang.sogou-inc.com:3128'})
    opener = urllib2.build_opener(proxy)
    urllib2.install_opener(opener)
    """
    crawl_time = time.mktime(datetime.datetime.utcnow().timetuple()) + 28800
    user_agent = choice(uas).strip()
    #logging.warning("choose a random ua:" + user_agent)
    headers={'User-Agent':user_agent,
        'referer': "https://www.google.com.hk"
    }
    req = urllib2.Request(redirect_url, headers=headers)    
    try:
        response = urllib2.urlopen(req, timeout=100).read()
    except:
        logging.error("urlopen error:" + url_for_log)
        return msgs
    if isinstance(response, unicode):
        pass
    else:
        response = response.decode('utf-8', "ignore")
    try:
        htmlSource = etree.HTML(response)
    except:
        logging.error("etree error:" + url_for_log)
    #print response
    ele = htmlSource.findall(".//div[@class='rg_meta']")
    if len(ele) == 0:
        fd = open("/home/ec2-user/deamon/spider/index.html" ,"a")
        fd.write(response.encode("utf8","ignore"))
        fd.close()
    seq = 1
    for n in ele:
        pic = n.text
        json_obj = json.loads(pic)

        pic_url = json_obj["ou"]
        page_url = json_obj["ru"]
        try:
            htmltitle = json_obj["pt"]
        except:
            logging.error("json decode error:" + url_for_log)
            continue
        seq = seq + 1
        try:
            msg = word.decode("gbk","ignore") + "\t" + pic_url + "\t" + page_url + "\t" + str(seq) + "\t"  + htmltitle + "\t" + str(crawl_time)
        except:
            logging.error("combine msg error:" + url_for_log)
        msgs.append(msg)
    return msgs 
                

def run_worker():
    uas = load_UA()
    client = KafkaClient(hosts=random.choice(brokers))
    word_topic = client.topics[crawl_query_topic]
    spider_topic = client.topics[crawl_result_topic]
    dog_topic = client.topics[crawl_dog_topic]
    dog_name = socket.gethostname() 
    fail_num = 0
    while True:
        #consumer = word_topic.get_simple_consumer(auto_commit_enable=True, reset_offset_on_start=False)
        try:
            consumer = word_topic.get_balanced_consumer(
                    reset_offset_on_start=False,
                    consumer_group="1",
                    auto_commit_enable=True,
                    auto_commit_interval_ms=10,
                    rebalance_max_retries=1000,
                    zookeeper_connect=zookeepers)
        except:
            logging.error("get_balanced_consumer error")
            continue
        for message in consumer:
            if message is not None and len(message.value) != 0:
                start_time = time.time()
                try:
                    word = message.value.split("\t")[0].strip()
                except:
                    continue
                time.sleep(random.uniform(5,7))
                msgs = Worker(word, uas)  
                retry_times = 2 
                while len(msgs) == 0 and retry_times:
                    msgs = Worker(word, uas)  
                    retry_times = retry_times - 1
                dog_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                dog_time = time_convert(dog_time, "%Y-%m-%d %H:%M:%S")
                print dog_time
                dog_food = word + "\t" +  dog_time + "\t" + str(len(msgs)) + "\t" + dog_name
                with dog_topic.get_producer(delivery_reports=False,linger_ms=0, min_queued_messages=1) as dog_producer:
                    dog_producer.produce(dog_food)
                    logging.warning("put query to dog queue" + word)
                
                """
                if len(msgs) == 0:
                    fail_num = fail_num + 1
                    with word_topic.get_sync_producer() as word_producer:
                        word_producer.produce(word)
                        logging.warning("put word back to queue:" + word)
                    continue
                """

                with spider_topic.get_producer(delivery_reports=True, linger_ms=0,min_queued_messages=1) as producer:
                    for msg in msgs:
                        res = producer.produce(str(msg))
                    while True:
                        try:
                            msg, exc = producer.get_delivery_report(block=False)
                            if exc is not None:
                                logging.error("failed to put result to fruits queue:" + word)
                            else:
                                pass
                        except Queue.Empty:
                            break
                cost_time = str(int(time.time() - start_time))
                print cost_time
                logging.warning(cost_time + "s crawl query word is " + word + " ,crawled pic_nums is " + str(len(msgs)))
        time.sleep(1)

def test():
    uas = load_UA()
    query_word = open("test.txt","r").readline().strip()
    msgs = Worker(query_word, uas)  

if __name__ == "__main__":
    if run_env == "debug":
        test()
        sys.exit(0)
    run_worker()
