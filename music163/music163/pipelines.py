# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo

class Music163Pipeline(object):
    def process_item(self, item, spider):
        return item


class MongoPipeline(object):

    def __init__(self, mongo_url, mongo_port, mongo_db, mongo_db_collection):
        self.mongo_url = mongo_url
        self.mongo_port = mongo_port
        self.mongo_db = mongo_db
        self.mongo_db_collection = mongo_db_collection

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_url=crawler.settings.get('MONGO_URL'),
            mongo_port = crawler.settings.get('MONGO_PORT'),
            mongo_db=crawler.settings.get('MONGO_DB'),
            mongo_db_collection = crawler.settings.get('MONGO_DB_COLLECTION')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(host=self.mongo_url, port=self.mongo_port)
        self.db = self.client[self.mongo_db]
        self.post  = self.db[self.mongo_db_collection]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        self.post.insert_one(dict(item))
        return item