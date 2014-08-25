import sqlite3
import threading
import sys
import os.path
import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

@pytest.fixture(scope='module')
def dawaintegration(request):
# setup
#verify database is new and empty +
# create sqlite3 db +
# create schema in it +
# create webserver
# create data that will be returned by the server
# create expected data that will be save in on the sqlite db
# map routes to the server to match what the app will be calling
# assert
# varify that the data int the database is the expected data.
# check if log.log contains proper information
#teardown
# delete database
# remove log.log

    databaseName = 'test.db'

    try:
        os.remove(databaseName)
    except OSError:
        pass

    conn = sqlite3.connect(databaseName)

    conn.execute('''CREATE TABLE `SAM_AREA` (
                      `AREAID` varchar(10) NOT NULL default '',
                      `AREATYPEID` varchar(10) NOT NULL,
                      `AREANAME` varchar(40) NOT NULL,
                      `AREACODE` varchar(10) NOT NULL,
                      `KOMMUNEID` smallint(5) NOT NULL,
                      `COORDINATES` mediumtext ,
                      PRIMARY KEY  (`AREAID`)
                    )''')

    conn.execute('''CREATE TABLE `SAM_KOMMUNE` (
                      `id` int(11) NOT NULL,
                      `name` varchar(45) default NULL,
                      PRIMARY KEY  (`id`)
                    )''')

    conn.execute('''CREATE TABLE `SAM_HOUSEUNITS` (
                      `ADGANGSADRESSE_UUID` varchar(36) NOT NULL default '',
                      `KOMMUNEID` decimal(10,0) NOT NULL default '0',
                      `ROADID` decimal(10,0) NOT NULL default '0',
                      `HOUSEID` varchar(10) NOT NULL default '',
                      `EQUALNO` decimal(10,0) default NULL,
                      `X` decimal(18,15) default NULL,
                      `Y` decimal(18,15) default NULL,
                      `DOORCOUNT` decimal(10,0) default NULL,
                      `SOGNENR` decimal(10,0) NOT NULL,
                      `ZIP` decimal(10,0) NOT NULL,
                      `SOGNENAVN` varchar(75) NOT NULL,
                      `X_SYS34` decimal(10,0) NOT NULL default '0',
                      `Y_SYS34` decimal(10,0) NOT NULL default '0',
                      PRIMARY KEY  (`ADGANGSADRESSE_UUID`, `KOMMUNEID`,`ROADID`,`HOUSEID`)
                    )''')

    conn.commit()

    import web
    import testdata

    urls = (
            '/kommuner', 'kommuner'
            )

    class kommuner:
        def GET(self):
            return testdata.COMMUNES

    app = web.application(urls, globals())
    thread = threading.Thread(target=app.run())
    thread.daemon = True
    thread.start()

    import config

    config.DB_URL = "sqlite:///{0}".format(databaseName)
    config.SERVER_URL = "http://localhost:8080/"

    import dawaintegration

    def fin():
        try:
            os.remove(databaseName)
            os.remove('log.log')
        except OSError:
            pass

    request.addfinalizer(fin)

    return dawaintegration

def test_answer(dawaintegration):
    dawaintegration.importCommuneInformation()
