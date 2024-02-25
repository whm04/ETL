# database.py
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from config.config import DATABASE_URL, TABLE_NAME

Base = declarative_base()

class Offer(Base):
    __tablename__ = TABLE_NAME

    offer_number = Column(String, primary_key=True)
    offer_address = Column(String)
    title = Column(String)
    publication_date = Column(DateTime)
    language = Column(String)
    soft_skills = Column(String)
    hard_skills = Column(String)
    salary = Column(String)
    company = Column(String)
    profile = Column(String)

    def __repr__(self):
        return f"<Offer(offer_number='{self.offer_number}', title='{self.title}')>"

class Database:
    def __init__(self, database_url):
        self.database_url = database_url
        self.engine = create_engine(database_url)
        self.Session = sessionmaker(bind=self.engine)

    def create_table(self):
        Base.metadata.create_all(self.engine)

    def drop_table(self):
        Base.metadata.drop_all(self.engine)

    def insert_data(self, df):
        session = self.Session()
        try:
            for _, row in df.iterrows():
                offer = Offer(
                    offer_number=row['offer_number'],
                    offer_address=row['offer_address'],
                    title=row['title'],
                    publication_date=row['publication_date'],
                    language=row['language'],
                    soft_skills=row['soft_skills'],
                    hard_skills=row['hard_skills'],
                    salary=row['salary'],
                    company=row['company'],
                    profile=row['profile']
                )
                session.add(offer)
            session.commit()
        except IntegrityError as e:
            session.rollback()
            raise e
        finally:
            
            session.close()
    
    def get_first_10_offers(self):
        session = self.Session()
        try:
            query = session.query(Offer).limit(10).all()
            return query
        finally:
            session.close()
            
