from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, Float, Text , Boolean, text, or_, desc

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from datetime import datetime
import json
import os
import sqlalchemy.orm.session
import types

from functools import wraps
class ScopedSession(object):
    def __init__(self, sm, db, **kwargs):
        self.sm = sm
        self.db = db
        self.session = self.sm(**kwargs)
        self.session.scoped = self


    def __enter__(self):
        return self.session

    def remove(self):
        self.sm.remove()

    def __exit__(self, type, value, traceback):

        #self.db._update_tables(self.s)
        #self.s.commit()
        self.remove()

    def delete(self, m, commit=False):
        self.session.delete(m)
        if commit:
            self.session.commit()

    def add(self, m):
        self.session.add(m)
        self.session.commit()
        return m


    def add_model(self, model, payload, commit=False):
        m = model(payload)
        self.session.add(m)
        if commit:
            self.session.commit()
        return m




class DBWrapper (object):
    def __init__(self, file_, base, sql_type, create=False, scoped_thread=False, tables_checking=[], **kwargs):
        self.tables_checking = tables_checking
        args = None
        engine_name = None
        connect_args = {}
        if sql_type == 'mysql':
            engine_name = sql_type + '+pymysql://'
        elif sql_type == 'sqlite':
            engine_name = 'sqlite:///'
            connect_args['check_same_thread'] =scoped_thread
        else:
            raise ValueError("Unknown engine type: {}".format(sql_type))

        engine_name = engine_name + file_

        self.file = file_
        self.engine = create_engine(engine_name, connect_args = connect_args, **kwargs)
        _Session = sessionmaker(bind=self.engine)

        self.SCOPED  = scoped_thread
        if not scoped_thread:
            self.Session = _Session
            self.session = self.Session()

        else:

            self.Session = scoped_session(_Session)
            self.session = None

        #maybe throw an error when use 'session' variable if scoped_thread=1



        if create:
            session = self.session
            if not session:
                session = self.Session()

            print("Creating tables...")
            base.metadata.create_all(self.engine)

            session.commit()



    def scoped_session_func(db=None, attr=None, expire_on_commit=False):
        def decorator(f):
            @wraps(f)
            def decorated(*args, **kwargs):
                session = kwargs.get('session', None)
                is_root_session = False
                db = args[0]
                if attr:
                    db = getattr(db, attr)

                if not session:
                    session = db.scoped_session(expire_on_commit=expire_on_commit).session
                    is_root_session = True
                    kwargs['session'] = session

                r = None
                error = None
                try:
                    r = f(*args, **kwargs)
                except Exception as e:
                    error = e
                    session.rollback()
                else:
                    if is_root_session:
                        session.commit()

                finally:
                    if is_root_session:
                        session.scoped.remove()
                    if error:
                        raise error
                    return r

            return decorated
        return decorator

    def scoped_session(self, **kwargs):
        if not self.SCOPED:
            raise Exception("Enable scoped_thread in DBWrapper.")
        return ScopedSession(self.Session, self, **kwargs)


    def get_updated_rows(self, session, table, update_time, limit=None, **kwargs):
        q = session.query(table)\
                .filter(table._update_time>update_time)

        if kwargs:
            for key in kwargs.keys():
                q = q.filter(getattr(table, key)==kwargs[key])

        if limit:
            q = q.order_by(desc(table._update_time)).limit(limit)

        return q.all()


    def query_raw(self, raw, session=None):
        if not session:
            session = self.session

        sql = text(raw)
        result = session.execute(sql)
        data = []
        for row in result:
            data.append(dict(row))
        return data

    def get_tables(self):
        pass
    def get_all(self, model):
        return self.session.query(model).all()

    def find_by_id(self, model, id_):
        return self.session.query(model).filter_by(id=id_).first()

    def add(self, model, payload):
        m = model(payload)
        self.session.add(m)
        self.session.commit()
        return m

    def delete(self, m):
        self.session.delete(m)
        self.session.commit()

    def as_dict(self, model, cols=[], remove_cols=[]):
        if not cols:
            cols = [col.name for col in model.__table__.columns if not col.name in remove_cols]

        return {col: getattr(model, col) for col in cols}

    def as_json(self, model, cols=[], remove_cols=[]):
        m = self.as_dict(model, cols, remove_cols)
        return self.to_json(m)

    def to_json(self, dic):
        d = {}
        for col in dic.keys():
            val = dic[col]
            try:
                if type(val) in [str, int, float]:
                    pass
                json.dumps([val])
            except:
                val = str(val)
            d[col] = val
        return d

    def _update_tables(self, session):
            dirty = session.dirty
            if dirty:
                for obj in dirty:
                    if type(obj) in self.tables_checking:
                        obj._update_time = datetime.utcnow()
                        print( "dirty task in tables: {}".format(obj))
                session.commit()

    def commit(self):
        self.master_db.session.commit()

    @staticmethod
    def get_changed_rows(session):
        return session.dirty

    def update(self, m):
        self._update_tables(self.session)
