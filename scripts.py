import logging
import sys
from datetime import datetime
from kuf_messdaten_auswertung.baulaerm.v2 import erstelle_baulaerm_auswertung # import get_project_data, erstelle_baulaerm_auswertung
from kuf_messdaten_auswertung.baulaerm.shared import get_project_data
from kuf_messdaten_auswertung.foo import get_project_via_rest
from kuf_messdaten_auswertung.default.v1 import werte_beurteilungszeitraum_aus
from kuf_messdaten_auswertung.db_insert import insert_auswertung_via_psycopg2
from kuf_messdaten_auswertung.models import Ergebnisse
import uuid

FORMAT = '%(filename)s %(lineno)d %(asctime)s %(levelname)s %(message)s'
logging.basicConfig(
    level=logging.INFO,
    format=FORMAT,
    handlers=[
        logging.StreamHandler(sys.stdout),
    ])


if False:
    t = datetime(2023, 4, 23, 1, 0, 5)
    insert_auswertung_via_psycopg2(t, Ergebnisse(t, t, 0, 0, 0, [], [], [] ,[] ,[], uuid.UUID('4c7be8b7-5515-4ab1-9b49-c5208ff87c08')))

if True:
    for d in range(10, 25+1):
    # get_project_via_rest("abc")
        for h in [
            0, 1, 2, 3, 4, 5, 
                21, 
                22, 
                23]:
            try:
                current_time = datetime.now()
                ausgewerteter_zeitpunkt = datetime(2023, 4, d, h, 30, 0)
                result = werte_beurteilungszeitraum_aus(ausgewerteter_zeitpunkt, "Test-2")
                insert_auswertung_via_psycopg2(ausgewerteter_zeitpunkt, result)
            except Exception as ex:
                logging.exception(ex)


if False:
    for i in range(10, 30):
        for ii in [10, 23]:
            ausgewerteter_zeitpunkt = datetime(2023, 4, i, ii, 0, 0)
            _mps, _ios, laermkategorisierung_an_ios, _ausbreitungsfaktoren = get_project_data()
            erstelle_baulaerm_auswertung(_mps, _ios, laermkategorisierung_an_ios, _ausbreitungsfaktoren, ausgewerteter_zeitpunkt, "8d7e0d22-620c-45b4-ac38-25b63ddf79e0")
