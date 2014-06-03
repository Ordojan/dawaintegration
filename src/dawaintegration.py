#!/usr/bin/python

import requests, re
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

engine = create_engine('mysql+oursql://root:admin@127.0.0.1:1521/SAMMYEMPTY', echo=False)
Base = declarative_base(engine)

class Houseunit(Base):
    __tablename__ = 'SAM_HOUSEUNITS'
    __table_args__ = {'autoload':True}

class Area(Base):
    __tablename__ = 'SAM_AREA'
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

def importDistrictInformation(session):
    districts = []

    response = requests.get('http://dawa.aws.dk/kommuner')
    data = response.json()

    for element in data:
        area = Area()

        area.AREATYPEID = 'KOM'
        area.AREANAME = element['navn']
        area.AREACODE = element['kode']
        area.KOMMUNEID = element['kode']
        area.AREAID = "{0}{1}".format(area.AREATYPEID, area.AREACODE)

        districts.append(area)

    response = requests.get('http://dawa.aws.dk/sogne')
    data = response.json()

    for element in data:
        area = Area()

        area.AREATYPEID = 'SOGN'
        area.AREANAME = element['navn']
        area.AREACODE = element['kode']
        area.KOMMUNEID = element['kode']
        area.AREAID = "{0}{1}".format(area.AREATYPEID, area.AREACODE)

        districts.append(area)

    response = requests.get('http://dawa.aws.dk/postnumre')
    data = response.json()

    for element in data:
        area = Area()

        area.AREATYPEID = 'POST'
        area.AREANAME = element['navn']
        area.AREACODE = element['nr']
        area.KOMMUNEID = element['kommuner'][0]['kode']
        area.AREAID = "{0}{1}".format(area.AREATYPEID, area.AREACODE)

        districts.append(area)

    session.add_all(districts)
    session.commit()

def importAdressInformation(session):
    communes = session.query(Kommune).all()

    def getAddressesInCommune(commune):
        url = 'http://dawa.aws.dk/adresser'
        #parameters = {'kommunekode': commune.id}
        headers = {'Accept-Encoding': 'gzip, deflate'}

        response = requests.get(url, params={'kommunekode': '101'}, headers=headers)
        return response.json()

    addresses = getAddressesInCommune(None)

    for address in addresses:
        accessAddress = address["adgangsadresse"]
        houseunit = session.query(Houseunit).filter_by(ADGANGSADRESSE_UUID = accessAddress['id']).first()
        if houseunit is None:
            try:
                houseunit = Houseunit()

                houseunit.ADGANGSADRESSE_UUID = accessAddress['id']
                houseunit.KOMMUNEID = accessAddress['kommune']['kode']
                houseunit.ROADID = accessAddress['vejstykke']['kode']

                houseID = accessAddress['husnr']
                houseunit.HOUSEID = houseID

                houseNumber = re.findall(r'\d+', houseID)[0]
                houseunit.EQUALNO = int(houseNumber) % 2

                coordinates = accessAddress['adgangspunkt']['koordinater']
                houseunit.X = coordinates[0]
                houseunit.Y = coordinates[1]

                houseunit.DOORCOUNT = 1
                houseunit.SOGNENR = accessAddress['sogn']['kode']
                houseunit.ZIP = accessAddress['postnummer']['nr']
                houseunit.SOGNENAVN = accessAddress['sogn']['navn']

                session.add(houseunit)
            except:
                # Log error
                continue
        else:
            houseunit.DOORCOUNT = houseunit.DOORCOUNT + 1

        session.commit()

def main():
    session = loadSession()
    #importCommuneInformation(session)
    #importDistrictInformation(session)
    importAdressInformation(session)

if __name__ == "__main__":
    main()
