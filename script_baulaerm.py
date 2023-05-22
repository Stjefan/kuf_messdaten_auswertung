import logging
import sys
from datetime import datetime
from src.kuf_messdaten_auswertung.baulaerm.v2 import erstelle_baulaerm_auswertung # import get_project_data, erstelle_baulaerm_auswertung
from src.kuf_messdaten_auswertung.baulaerm.shared import get_project_data
from src.kuf_messdaten_auswertung.foo import get_project_via_rest
from src.kuf_messdaten_auswertung.default.v1 import werte_beurteilungszeitraum_aus
from src.kuf_messdaten_auswertung.db_insert import insert_auswertung_via_psycopg2

FORMAT = '%(filename)s %(lineno)d %(asctime)s %(levelname)s %(message)s'
logging.basicConfig(
    level=logging.DEBUG,
    format=FORMAT,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("baulaerm.log")
    ])


if True:
    for i in range(8, 15+1):
        for ii in [13, 23]:
            try:
                ausgewerteter_zeitpunkt = datetime(2023, 5, i, ii, 0, 0)
                _mps, _ios, laermkategorisierung_an_ios, _ausbreitungsfaktoren = get_project_data()
                erstelle_baulaerm_auswertung(_mps, _ios, laermkategorisierung_an_ios, _ausbreitungsfaktoren, ausgewerteter_zeitpunkt, "8d7e0d22-620c-45b4-ac38-25b63ddf79e0")
            except Exception as ex:
                logging.exception(ex)