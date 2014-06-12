#!/usr/bin/python
# -*- coding: utf-8 -*-

"""DAWA integration

Usage:
  dawaintegration.py [--chunksize=<size>]
  dawaintegration.py [--maxworkercount=<count>]

Options:
  -h --help     Show this screen.
  --chunksize=<size>  size of the data chunks that will be requested from DAWA [default: 10000].
  --maxworkercount=<count>  the maximum number of workers isnerting records into the database [default: 3].
"""
from docopt import docopt

import requests, re, time, threading
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from threading import *

engine = create_engine('mysql+oursql://root:admin@127.0.0.1:1521/SAMMYEMPTY', echo=False)
Base = declarative_base(engine)
metadata = Base.metadata
Session = sessionmaker(bind=engine, autocommit=False, autoflush=True)

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

def importCommuneInformation():
    session = Session()

    response = requests.get('http://dawa.aws.dk/kommuner')
    data = response.json()

    communes = []
    for element in data:
        id = element['kode']
        name = element['navn']
        communes.append(Kommune(id, name))

    session.add_all(communes)
    session.commit()

    session.close()

def importDistrictInformation():
    session = Session()
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

    session.close()

def getAddressChunksInCommune(commune, pageNumber, chunkSize):
    url = 'http://dawa.aws.dk/adresser'
    parameters = {'kommunekode': commune.id, 'side': pageNumber, 'per_side': chunkSize}
    headers = {'Accept-Encoding': 'gzip, deflate'}

    response = requests.get(url, params=parameters, headers=headers)

    return response.json()

def processAddresses(addresses, session):
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
                continue
        else:
            houseunit.DOORCOUNT = houseunit.DOORCOUNT + 1

    session.commit()

    session.close()

def importAddressInformation(maxWorkerCount, chunkSize):
    session = Session()

    communes = session.query(Kommune).all()

    workers = []
    def removeDeadWorkers(waitTime):
        while len(workers) >= maxWorkerCount:
            time.sleep(waitTime)
            [workers.remove(w) for w in workers[:] if not w.isAlive()]

    pageNumber = 1
    for commune in communes:

        while True:
            addressData = getAddressChunksInCommune(commune, pageNumber, chunkSize)

            removeDeadWorkers(1)

            worker = threading.Thread(target=processAddresses, args=(addressData, Session()))
            workers.append(worker)
            worker.daemon = True
            worker.start()

            if len(addressData) == 0:
                break
            pageNumber = pageNumber + 1

    removeDeadWorkers(3)

    session.close()

def main(args):
    maxWorkerCount = int(arguments['--maxworkercount'])

    importCommuneInformation()
    importDistrictInformation()
    importAddressInformation(maxWorkerCount, args['--chunksize'])

if __name__ == "__main__":
    arguments = docopt(__doc__)
    main(arguments)
