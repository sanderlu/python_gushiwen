from pymemcache.client.base import Client
import threading


class Memcache:
    _instance_clock = threading.Lock()
    host = '127.0.0.1'
    port = 12306

    def __new__(cls, *args, **kwargs):
        # 双重锁单例，避免多线程爬取时并发问题
        if not hasattr(Memcache, '_conn'):
            with Memcache._instance_clock:
                if not hasattr(Memcache, '_conn'):
                    Memcache._conn = Memcache.memcache_conn()
        return object.__new__(cls)

    @staticmethod
    def memcache_conn():
        print('mem_conn')
        return Client((Memcache.host, Memcache.port))

    def __set(self, key, value):
        return self._conn.set(key, value)

    def __get(self, key):
        get_res = self._conn.get(key)
        if get_res:
            return str(self._conn.get(key), encoding='utf-8')
        else:
            return get_res

    def __flush_all(self):
        return self._conn.flush_all()

    def mc(self, option, key='', value=''):
        if option == 'set':
            return self.__set(key, value)
        elif option == 'get':
            return self.__get(key)
        elif option == 'flush_all':
            return self.__flush_all()
        else:
            return False

mem_obj = Memcache()
