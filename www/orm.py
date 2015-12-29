#!/usr/bin/env python3
# coding = utf-8

from orm import Model,StringField,IntegerField

@asyncio.coroutine
def create_pool(loop, **kw):
    """创建一个全局的连接池，每个HTTP请求都可以从连接池中直接获取数据库连接
    连接池由全局变量__pool存储，缺省情况下将编码设置为utf8，自动提交事务"""
    logging.info('create datebase connection pool...')
    global __pool
    __pool = yield from aiomysql.create_pool(
            host = kw.get('host', 'localhost'),
            port = kw.get('port', 3306),
            user = kw['user'],
            password = kw['password'],
            db = kw['db'],
            charset = kw.get('charset', 'utf-8'),
            autocommit = kw.get('autocommit', True),
            maxsize = kw.get('maxsize', 10),
            minsize = kw.get('minsize', 1),
            loop = loop
            )

@asyncio.coroutine
def select(sql, args, size=None):
    """执行SELECT语句，用select函数执行，需要传入SQL语句和SQL参数"""
    log(sql, args)
    global __pool
    with (yield from __pool) as conn:
        cur = yield from conn.cursor(aiomysql.DictCursor)
        yield from cur.execute(sql.replace('?', '%s'), args or ())
        if size:
            rs  = yield from cur.fetchmany(size)
        else:
            rs = yield from cur.fetchall()
        yield from cur.close()
        logging.info('rows returned : %s' % len(rs))
        return rs

@asyncio.coroutine
def execute(sql, args):
    """执行INSERT、UPDATE、DELETE语句"""
    log(sql)
    with (yield from __loop) as conn:
        try:
            cur = yield from conn.cursor()
            yield from cur.execute(sql.replace('?', '%s'), args)
            affected = cur.rowcount
            yield from cur.close()
        except BaseException as e:
            raise
        return affected

class User(Model):
    """定义一个User对象，然后把数据库表users和它关联起来"""
    __table__ = 'users'

    id = IntegerField(primary_key=True)
    name = StringField()

class Model(dict, metaclass=ModelMetaclass):
    """定义所有ORM映射的基类Model"""
    def __init__(self, **kw):
        super(Model,self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value


