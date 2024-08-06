from app.config import Config
from sqlalchemy import create_engine, inspect, desc
from sqlalchemy import Column, String
from sqlalchemy.dialects.postgresql import DATE, JSONB
from sqlalchemy.ext.declarative import declarative_base  
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

base = declarative_base()


class MonthlyStatsState(base):  
    __tablename__ = Config.MONTHLYSTATS_TABLENAME
    sending_date = Column('sending_date', DATE, index=False, primary_key=True)
    data = Column('info', JSONB, index=False)


class Table:
  def __init__(self, databaseURL, state):
      db = create_engine(databaseURL)
      global base
      base.metadata.create_all(db)
      Session = sessionmaker(db)
      self._session = Session()
      self._state = state

  def getNumberOfRow(self):
      rows = self._session.query(self._state).count()
      return rows

  def pullLatestDate(self):
      #This will return the date of the latest entry in datetime format
      try:
          result = self._session.query(self._state).order_by(desc(self._state.sending_date)).limit(1).all()
          if result:
              return result
          else:
              return None
      except SQLAlchemyError:
          self._session.rollback()
          return None

  def pushState(self, dateIn, input, commit=False):
      #Push the provided date and data
      newState = self._state(sending_date=dateIn, data=input)
      self._session.add(newState)
      if commit:
          try:
              self._session.commit()
          except SQLAlchemyError:
              self._s

  #This determines whether the monthly stats should be send
  def sendingRequired(self, dateIn):
      #This will handle the very first entry.
      if self.getNumberOfRow() == 0:
          return True    
      #only return true if month or year has incremented
      recordDate = self.pullLatestDate()
      if recordDate:
          lastDate = recordDate[0].sending_date
          if dateIn.year > lastDate.year or \
          (dateIn.year >= lastDate.year and dateIn.month > lastDate.month): 
            return True
      return False


class MonthlyStatsTable(Table):
    def __init__(self, databaseURL):
        Table.__init__(self, databaseURL, MonthlyStatsState)

    
