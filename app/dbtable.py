from app.config import Config
from sqlalchemy import create_engine  
from sqlalchemy import Column, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base  
from sqlalchemy.orm import sessionmaker
import json
import uuid

#use the declarative syntax of sqlalchemy
base = declarative_base()

class MapState(base):  
    __tablename__ = Config.MAPSTATE_TABLENAME
    uuid = Column(String, primary_key=True, unique=True)
    data = Column(JSONB)

class ScaffoldState(base):  
    __tablename__ = Config.SCAFFOLDSTATE_TABLENAME
    uuid = Column(String, primary_key=True, unique=True)
    data = Column(JSONB)

class Table:
  def __init__(self, databaseURL, state):
        db = create_engine(databaseURL)
        global base
        base.metadata.create_all(db)
        Session = sessionmaker(db)
        self._session = Session()
        self._state = state

  #push the state into the database and return an unique id
  def pushState(self, input, commit = False):
        id = uuid.uuid4().hex[:8]
        #get a new key in the rare case of duplication
        while self._session.query(self._state).filter_by(uuid=id).first() is not None:
            id = uuid.uuid4().hex[:8]
        newState = self._state(uuid=id, data=input)
        self._session.add(newState)
        if commit:
            self._session.commit()
        return id

  def pullState(self, id):
    result = self._session.query(self._state).filter_by(uuid=id).first()
    if result:
        return result.data
    else:
        return None

class MapTable(Table):
    def __init__(self, databaseURL):
        Table.__init__(self, databaseURL, MapState)

class ScaffoldTable(Table):
    def __init__(self, databaseURL):
        Table.__init__(self, databaseURL, ScaffoldState)
