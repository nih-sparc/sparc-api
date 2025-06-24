from app.config import Config
from sqlalchemy import create_engine, asc
from sqlalchemy import Column, String
from sqlalchemy.dialects.postgresql import DATE, JSONB
from sqlalchemy.ext.declarative import declarative_base  
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
import uuid
from datetime import datetime

#use the declarative syntax of sqlalchemy
base = declarative_base()

class AnnotationState(base):
    __tablename__ = Config.ANNOTATIONSTATE_TABLENAME
    uuid = Column(String, primary_key=True, unique=True)
    data = Column(JSONB, index=False)
    sending_date = Column('sending_date', DATE, index=False, primary_key=False)


class MapState(base):  
    __tablename__ = Config.MAPSTATE_TABLENAME
    uuid = Column(String, primary_key=True, unique=True)
    data = Column(JSONB)


class ScaffoldState(base):  
    __tablename__ = Config.SCAFFOLDSTATE_TABLENAME
    uuid = Column(String, primary_key=True, unique=True)
    data = Column(JSONB)


class FeaturedDatasetIdSelectorState(base):  
    __tablename__ = Config.FEATURED_DATASET_ID_SELECTOR_TABLENAME
    uuid = Column(String, primary_key=True, unique=True)
    data = Column(JSONB)

class ProtocolMetricsState(base):
    __tablename__ = Config.PROTOCOL_METRICS_TABLENAME
    uuid = Column(String, primary_key=True, unique=True)
    data = Column(JSONB)

class Table:
    def __init__(self, databaseURL, state):
        self.databaseURL = databaseURL
        self._state = state
        self._engine = None
        self._Session = None
        self._init_db()

    def _init_db(self):
        self._engine = create_engine(self.databaseURL, pool_pre_ping=True, pool_recycle=300)
        base.metadata.create_all(self._engine)
        self._Session = sessionmaker(bind=self._engine)

    def getNumberOfRow(self):
        try:
            with self._Session() as session:
                return session.query(self._state).count()
        except SQLAlchemyError as e:
            print(f"Error counting rows: {e}")
            return 0

    def pushState(self, input, commit=False):
        id = uuid.uuid4().hex[:8]
        try:
            with self._Session() as session:
                while session.query(self._state).filter_by(uuid=id).first() is not None:
                    id = uuid.uuid4().hex[:8]
                newState = self._state(uuid=id, data=input)
                session.add(newState)
                if commit:
                    session.commit()
            return id
        except SQLAlchemyError as e:
            print(f"Error pushing state: {e}")
            return None

    #update the state with the given id, or push a new state with that id if none is found
    def updateState(self, id, input, commit=False):
        try:
            with self._Session() as session:
                record = session.query(self._state).filter_by(uuid=id).first()
                if record is None:
                    session.add(self._state(uuid=id, data=input))
                else:
                    session.query(self._state).filter_by(uuid=id).update({'data': input}, synchronize_session=False)
                if commit:
                    session.commit()
            return input
        except SQLAlchemyError as e:
            print(f"Error updating state: {e}")
            return None

    def pullState(self, id):
        try:
            with self._Session() as session:
                result = session.query(self._state).filter_by(uuid=id).first()
                if result:
                    return result.data
        except SQLAlchemyError as e:
            print(f"Error pulling state: {e}")
        return None


class AnnotationTable(Table):
    def __init__(self, databaseURL):
        super().__init__(databaseURL, AnnotationState)
        self._expiryDuration = 30  # days

    #push the state into the database and return an unique id
    def pushState(self, input, commit=False):
        id = uuid.uuid4().hex[:8]
        inputDate = datetime.now().date()
        try:
            with self._Session() as session:
                while session.query(self._state).filter_by(uuid=id).first() is not None:
                    id = uuid.uuid4().hex[:8]
                newState = self._state(uuid=id, data=input, sending_date=inputDate)
                session.add(newState)
                if commit:
                    session.commit()
            return id
        except SQLAlchemyError as e:
            print(f"Error pushing annotation state: {e}")
            return None

    def removeExpiredState(self):
        try:
            with self._Session() as session:
                results = session.query(self._state).order_by(asc(self._state.sending_date)).limit(200).all()
                now = datetime.now().date()
                for result in results:
                    if (now - result.sending_date).days > self._expiryDuration:
                        session.delete(result)
                    else:
                        break
                session.commit()
            return True
        except SQLAlchemyError as e:
            print(f"Error removing expired state: {e}")
            return False



class MapTable(Table):
    def __init__(self, databaseURL):
        super().__init__(databaseURL, MapState)


class ScaffoldTable(Table):
    def __init__(self, databaseURL):
        super().__init__(databaseURL, ScaffoldState)


class FeaturedDatasetIdSelectorTable(Table):
    def __init__(self, databaseURL):
        super().__init__(databaseURL, FeaturedDatasetIdSelectorState)


class ProtocolMetricsTable(Table):
    def __init__(self, databaseURL):
        super().__init__(databaseURL, ProtocolMetricsState)
