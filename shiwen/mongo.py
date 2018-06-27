import pymongo
import threading

class Mongo:
    _mongo_clock = threading.Lock()
    host = '127.0.0.1'
    port = 27017
    db = 'python'

    def __new__(cls, *args, **kwargs):
        # 双重锁单例，避免多线程爬取时并发问题
        if not hasattr(Mongo, "_conn"):
            with Mongo._mongo_clock:
                if not hasattr(Mongo, "_conn"):
                    Mongo._conn = Mongo.mongo_conn()
        return object.__new__(cls)

    @staticmethod
    def mongo_conn():
        print('mongo_conn')
        client = pymongo.MongoClient(Mongo.host, Mongo.port)
        return client[Mongo.db]

    # def __init__(self):
    #     client = pymongo.MongoClient(self.host, self.port)
    #     self.conn = client[self.db]
        # print('mongo_conn')

    def __adds(self, collection, info):
        return self._conn[collection].insert_many(info)

    def __add(self, collection, info):
        return self._conn[collection].insert_one(info)

    def __remove(self, collection, info):
        return self._conn[collection].remove(info)

    def __find_one(self, collection, info):
        return self._conn[collection].find_one(info)

    def mg(self, option, collection, info):
        if option == 'add':
            return self.__add(collection, info)
        elif option == 'adds':
            return self.__adds(collection, info)
        elif option == 'remove':
            return self.__remove(collection, info)
        elif option == 'find_one':
            return self.__find_one(collection, info)
        else:
            return False

mongo_obj = Mongo()
