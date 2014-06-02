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

    session.add_all(communes)
    session.commit()

def importAdressInformation(session):
    communes = session.query(Kommune).all()

    def insertHouseunit(address):
        uuid = address['id']
        communeID = address['kommune']['kode']
        roadID = address['vejstykke']['kode']
        houseID = address['husnr']
        # equalno
        x = address['adgangspunkt']['koordinater'][0]
        y = address['adgangspunkt']['koordinater'][1]
        doorcount = 1
        sognenr = address['sogn']['kode']
        zip = address['postnummer']['nr']
        sognenavn = address['sogn']['navn']

        houseunit = Houseunit()
        houseunit.ADGANGSADRESSE_UUID = uuid
        houseunit.KOMMUNEID = communeID
        houseunit.ROADID = roadID
        houseunit.HOUSEID = houseID
        houseunit.X = x
        houseunit.Y = y
        houseunit.DOORCOUNT = doorcount
        houseunit.SOGNENR = sognenr
        houseunit.ZIP = zip
        houseunit.SOGNENAVN = sognenavn

        session.add(houseunit)
        session.commit()

    def getAddressesInCommune(commune):
        url = 'http://dawa.aws.dk/adresser'
        #parameters = {'kommunekode': commune.id}
        headers = {'Accept-Encoding': 'gzip, deflate'}

        response = requests.get(url, params={'kommunekode': '0430'}, headers=headers)
        return response.json()

    addressData = getAddressesInCommune(None)

    id = addressData[0]["adgangsadresse"]['id']

    result = session.query(Houseunit).filter_by(ADGANGSADRESSE_UUID = id).first()

    address = addressData[0]["adgangsadresse"]

    def incrementDoorCountForHouseunit(address):
        result = session.query(Houseunit).filter_by(ADGANGSADRESSE_UUID = address['id']).first()
        result.DOORCOUNT = result.DOORCOUNT + 1
        session.commit()

    if result is None:
        insertHouseunit(address)
    else:
        incrementDoorCountForHouseunit(address)



    def insertOrUpdateHouseunits(address):
#check uuid if it exists if it does update else insert
        session.query(Houseunit).filter_by(ADGANGSADRESSE_UUID = address['adgangsadresse']['id'])


# is already in table?
#no? insert
#yes? update doorcount by one

#sam area


def main():
    session = loadSession()
    #importCommuneInformation(session)
    importAdressInformation(session)

if __name__ == "__main__":
    main()
