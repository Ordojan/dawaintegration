#!/usr/bin/python

import requests
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

engine = create_engine('mysql+oursql://root:admin@127.0.0.1:1521/SAMMYEMPTY', echo=True)
Base = declarative_base(engine)

class Houseunit(Base):
    __tablename__ = 'SAM_HOUSEUNITS'
    __table_args__ = {'autoload':True}

class Kommune(Base):
    __tablename__ = 'SAM_KOMMUNE'
    __table_args__ = {'autoload':True}

    def __init__(self, id=None, name=None):
        self.id = id
        self.name = name

def loadSession():
    metadata = Base.metadata
    Session = sessionmaker(bind=engine)
    session = Session()

    return session

def importCommuneInformation(session):
    response = requests.get('http://dawa.aws.dk/kommuner')
    data = response.json()

    communes = []
    for element in data:
        id = element['kode']
        name = element['navn']
        communes.append(Kommune(id, name))

    [session.add(commune) for commune in communes]
    session.commit()

def importAdressInformation(session):
    communes = session.query(Kommune).all()

        def getAddressesInCommune(commune):
            #http://dawa.aws.dk/adresser?kommunekode=0326
            url = 'http://dawa.aws.dk/adresser'
            payload = {'kommunekode': commune.id}
            headers = {'content-type': 'Accept-Encoding: gzip, deflate'}


def main():
    session = loadSession()
    importCommuneInformation(session)
    importAdressInformation(session)

if __name__ == "__main__":
    main()
