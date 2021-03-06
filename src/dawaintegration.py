#!/usr/bin/python
# -*- coding: utf-8 -*-

"""DAWA integration

Usage:
  dawaintegration.py import [options]
  dawaintegration.py import communes <communeid>... [options]

Options:
  -h --help     Show this screen.
  --chunksize=<size>  size of the data chunks that will be requested from DAWA [default: 10000].
  --maxworkercount=<count>  the maximum number of workers isnerting records into the database [default: 3].
  --importareas    imports the areas
  --importcommunes    imports communes
"""
from docopt import docopt

import requests
import re
import time
import threading
import config
import logging
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

mainLogger = logging.getLogger('dawaintegration')

engine = create_engine(config.DB_URL, echo=False)
Base = declarative_base(engine)
metadata = Base.metadata
Session = sessionmaker(bind=engine, autocommit=False, autoflush=True)


class Houseunit(Base):
    __tablename__ = 'SAM_HOUSEUNITS'
    __table_args__ = {'autoload':True}


class Area(Base):
    __tablename__ = 'SAM_AREA'
    __table_args__ = {'autoload':True}

    def __eq__(self, other):
        return int(self.AREACODE) == int(other.AREACODE)


class Kommune(Base):
    __tablename__ = 'SAM_KOMMUNE'
    __table_args__ = {'autoload':True}

    def __init__(self, id=None, name=None):
        self.id = id
        self.name = name


def importCommuneInformation():
    mainLogger.debug('Starting the commune import procedure.')

    session = Session()

    response = requests.get(config.SERVER_URL + 'kommuner')
    data = response.json()

    communes = []
    for element in data:
        id = element['kode']
        name = element['navn']
        communes.append(Kommune(id, name))

    mainLogger.debug('{0} communes found.'.format(len(communes)))

    session.add_all(communes)
    session.commit()

    session.close()

    mainLogger.debug('Ending the commune import procedure.')


def importAreaInformation():
    mainLogger.debug('Starting the area import procedure.')

    session = Session()

    mainLogger.debug('Importing communes...')

    newAreas = []

    areas = session.query(Area).filter_by(AREATYPEID = 'KOM').all()

    response = requests.get(config.SERVER_URL + 'kommuner')
    data = response.json()

    for element in data:
        area = Area()

        area.AREACODE = int(element['kode'])

        if area in areas:
            continue

        area.AREATYPEID = 'KOM'
        area.AREANAME = element['navn']
        area.KOMMUNEID = element['kode']
        area.AREAID = "{0}{1}".format(area.AREATYPEID, area.AREACODE)

        newAreas.append(area)

    mainLogger.debug('{0} new commune(s) found.'.format(len(newAreas)))

    session.add_all(newAreas)
    session.commit()

    mainLogger.debug('Done.')

    mainLogger.debug('Importing parishes...')

    newAreas = []

    areas = session.query(Area).filter_by(AREATYPEID = 'SOGN').all()

    [session.delete(area) for area in areas if area.KOMMUNEID == 9999]

    response = requests.get(config.SERVER_URL + 'sogne')
    data = response.json()

    for element in data:
        area = Area()

        area.AREACODE = int(element['kode'])

        if area in areas:
            continue

        area.AREATYPEID = 'SOGN'
        area.AREANAME = element['navn']

        url = config.SERVER_URL + 'adgangsadresser'
        parameters = {'sognekode': area.AREACODE, 'side': 1, 'per_side': 1}
        response = requests.get(url, params=parameters)
        try:
            if response.json():
                area.KOMMUNEID = response.json()[0]['kommune']['kode']
            else:
                area.KOMMUNEID = 9999
        except ValueError:
            continue

        area.AREAID = "{0}{1}".format(area.AREATYPEID, area.AREACODE)

        mainLogger.debug('Added new parish {0}'.format(area.AREANAME.encode('utf-8')))
        newAreas.append(area)

    mainLogger.debug('{0} new parishe(s) found.'.format(len(newAreas)))

    session.add_all(newAreas)
    session.commit()

    mainLogger.debug('Done.')

    mainLogger.debug('Importing postal numbers...')

    newAreas = []

    areas = session.query(Area).filter_by(AREATYPEID = 'POST').all()

    response = requests.get(config.SERVER_URL + 'postnumre')
    data = response.json()

    for element in data:
        area = Area()

        area.AREACODE = int(element['nr'])

        if area in areas:
            continue

        area.AREATYPEID = 'POST'
        area.AREANAME = element['navn']
#TODO multiple kommunes same postal code?
        area.KOMMUNEID = element['kommuner'][0]['kode']
        area.AREAID = "{0}{1}".format(area.AREATYPEID, area.AREACODE)

        newAreas.append(area)

    mainLogger.debug('{0} new postal number(s) found.'.format(len(newAreas)))

    session.add_all(newAreas)
    session.commit()

    mainLogger.debug('Done.')

    mainLogger.debug('Importing electoral areas...')

    newAreas = []

    areas = session.query(Area).filter_by(AREATYPEID = 'VALG').all()

    response = requests.get(config.SERVER_URL + 'opstillingskredse')
    data = response.json()

    for element in data:
        area = Area()

        area.AREACODE = int(element['kode'])

        if area in areas:
            continue

        area.AREATYPEID = 'VALG'
        area.AREANAME = element['navn']
        area.KOMMUNEID = 9999
        area.AREAID = "{0}{1}".format(area.AREATYPEID, area.AREACODE)

        newAreas.append(area)

    mainLogger.debug('{0} new elecroral district(s) found.'.format(len(newAreas)))

    session.add_all(newAreas)
    session.commit()

    mainLogger.debug('Done.')

    session.close()

    mainLogger.debug('Ending the area import procedure.')


def getAddressChunksInCommune(commune, pageNumber, chunkSize):
    mainLogger.debug('''Starting the procedure to query a chunk of address data.
                    Commune id: {0}
                    Commune name: {1}
                    Page number: {2}'''
                    .format(commune.id, commune.name.encode('utf-8'), pageNumber))

    url = config.SERVER_URL + 'adresser'
    parameters = {'kommunekode': commune.id, 'side': pageNumber, 'per_side': chunkSize}
    headers = {'Accept-Encoding': 'gzip, deflate'}

    response = requests.get(url, params=parameters, headers=headers)

    output = response.json()

    mainLogger.debug('{0} records found.'.format(len(output)))
    mainLogger.debug('Ending the procedure to query a chunk of address data.')

    return output


def processCommune(commune, chunkSize, session):
    logger = logging.getLogger('worker: {0}'.format(threading.current_thread().name))

    pageNumber = 1
    while True:
        addresses = getAddressChunksInCommune(commune, pageNumber, chunkSize)

        if len(addresses) == 0:
            break
        pageNumber = pageNumber + 1

        for address in addresses:
            accessAddress = address["adgangsadresse"]
            houseunit = session.query(Houseunit).filter_by(ADGANGSADRESSE_UUID = accessAddress['id']).first()
            if houseunit is None:
                try:
                    houseunit = Houseunit()

                    houseunit.ADGANGSADRESSE_UUID = accessAddress['id']
                    houseunit.KOMMUNEID = accessAddress['kommune']['kode']

                    roadID = accessAddress['vejstykke']['kode']
                    if roadID is None:
                        raise
                    houseunit.ROADID = roadID
                    houseunit.roadName = accessAddress['vejstykke']['navn']

                    houseID = accessAddress['husnr']
                    houseunit.HOUSEID = houseID

                    houseNumber = re.findall(r'\d+', houseID)[0]
                    if int(houseNumber) % 2 == 0:
                        houseunit.EQUALNO = 1
                    else:
                        houseunit.EQUALNO = 0

                    c = accessAddress['adgangspunkt']['koordinater']
                    if c:
                        houseunit.X, houseunit.Y = c[0], c[1]
                    else:
                        houseunit.X, houseunit.Y = None, None

                    houseunit.DOORCOUNT = 1

                    postNumber = accessAddress['postnummer']['nr']
                    if postNumber is None:
                        raise
                    houseunit.ZIP = postNumber

                    parish = accessAddress['sogn']
                    if parish:
                        houseunit.SOGNENR = parish['kode']
                        houseunit.SOGNENAVN = parish['navn']
                    else:
                        houseunit.SOGNENR = 9999
                        houseunit.SOGNENAVN = 'Ukendt (Sogn) Sogn'

                    valgkreds = accessAddress['opstillingskreds']
                    if valgkreds:
                        houseunit.valgkreds = int(valgkreds['kode'])
                    else:
                        houseunit.valgkreds = 9999

                    session.add(houseunit)
                except:
                    logger.error('Encountered erroneous record with "adgangsadresse id" of {0}'.format(accessAddress['id']))
                    logger.error(accessAddress)
                    continue
            else:
                houseunit.DOORCOUNT = houseunit.DOORCOUNT + 1

        session.commit()

    session.close()


def importAddressInformation(maxWorkerCount, chunkSize, communeIds):
    mainLogger.debug('Starting the address import procedure.')

    session = Session()

    if communeIds:
        communes = session.query(Kommune).filter(Kommune.id.in_(communeIds)).all()
    else:
        communes = session.query(Kommune).all()

    session.close()

    mainLogger.debug('Found {0} commune(s) in the database.'.format(len(communes)))

    workers = []

    def checkForDeadWorkers():
        SLEEP_TIME = 10
        mainLogger.debug('Checking for dead workers.')
        mainLogger.debug('Workers: {0}'.format(len(workers)))

        while len(workers) >= maxWorkerCount:
            time.sleep(SLEEP_TIME)
            [workers.remove(w) for w in workers[:] if not w.isAlive()]

    for index, commune in enumerate(communes):
        checkForDeadWorkers()

        mainLogger.debug('Assigning "{0}" commune to be processed.'.format(commune.name.encode('utf-8')))
        mainLogger.info('{0} out of {1} communes processed.'.format(index, len(communes)))

        worker = threading.Thread(target=processCommune, args=(commune, chunkSize, Session()))
        workers.append(worker)
        worker.daemon = True
        worker.start()

    mainLogger.debug('Waiting for workers to finish.')
    while True:
        [workers.remove(w) for w in workers[:] if not w.isAlive()]
        mainLogger.debug('Workers: {0}'.format(len(workers)))
        if len(workers) == 0:
            break
        time.sleep(20)

    mainLogger.debug('Ending the address import procedure.')


def main(args):
    mainLogger.info('Application starting.')

    maxWorkerCount = int(arguments['--maxworkercount'])

    mainLogger.info('Application arguments: \n{0}'.format(args))

    if args['--importcommunes']:
        importCommuneInformation()

    if args['--importareas']:
        importAreaInformation()

    communeIds = [int(id) for id in args['<communeid>']]
    importAddressInformation(maxWorkerCount, args['--chunksize'], communeIds)

    mainLogger.info('Application ending.')

if __name__ == "__main__":
    arguments = docopt(__doc__)
    main(arguments)
