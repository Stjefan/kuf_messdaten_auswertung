import logging
import sys
from datetime import datetime
from src.kuf_messdaten_auswertung.baulaerm.v2 import erstelle_baulaerm_auswertung # import get_project_data, erstelle_baulaerm_auswertung
from src.kuf_messdaten_auswertung.baulaerm.shared import get_project_data
from src.kuf_messdaten_auswertung.to_be_replaced import get_project_via_rest
from src.kuf_messdaten_auswertung.default.v1 import werte_beurteilungszeitraum_aus
from src.kuf_messdaten_auswertung.db_insert import insert_auswertung_via_psycopg2
from src.kuf_messdaten_auswertung.models import Ergebnisse
import uuid

if __name__ == "__main__":
    month = 8
    year = 2021
    FORMAT = '%(filename)s %(lineno)d %(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        level=logging.INFO,
        format=FORMAT,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(f"log_sindelfingen_{format(month, '02d')}_{format(year, '02')[-2:]}.log")
        ])


    if False:
        t = datetime(2023, 4, 23, 1, 0, 5)
        insert_auswertung_via_psycopg2(t, Ergebnisse(t, t, 0, 0, 0, [], [], [] ,[] ,[], uuid.UUID('4c7be8b7-5515-4ab1-9b49-c5208ff87c08')))

    if True:
        # d = 11
        # 11, 10, 9, 1, 2, 3, 4, 5, 12
        
        for d in range(1, 31+1):
            for h in [
                    0, 1, 2, 3, 4, 5, 
                        21, 
                        22, 
                        23]:
                try:
                    current_time = datetime.now()
                    ausgewerteter_zeitpunkt = datetime(year, month, d, h, 30, 0)
                    
                    # get_project_via_rest("sindelfingen", "http://localhost:8000")
                    result = werte_beurteilungszeitraum_aus(ausgewerteter_zeitpunkt, "Sindelfingen", "http://localhost:8000/")
                    insert_auswertung_via_psycopg2(ausgewerteter_zeitpunkt, result)
                except Exception as ex:
                    logging.exception(ex)
    if False:
        for d in [11, 10, 9, 8]:
        # get_project_via_rest("abc")
            for h in [
                0, 1, 2, 3, 4, 5, 
                    21, 
                    22, 
                    23]:
                try:
                    current_time = datetime.now()
                    ausgewerteter_zeitpunkt = datetime(2023, 6, d, h, 30, 0)
                    result = werte_beurteilungszeitraum_aus(ausgewerteter_zeitpunkt, "Mannheim", "http://localhost:8000/")
                    insert_auswertung_via_psycopg2(ausgewerteter_zeitpunkt, result)
                except Exception as ex:
                    logging.exception(ex)


    if False:
        for i in range(10, 30):
            for ii in [10, 23]:
                ausgewerteter_zeitpunkt = datetime(2023, 4, i, ii, 0, 0)
                _mps, _ios, laermkategorisierung_an_ios, _ausbreitungsfaktoren = get_project_data()
                erstelle_baulaerm_auswertung(_mps, _ios, laermkategorisierung_an_ios, _ausbreitungsfaktoren, ausgewerteter_zeitpunkt, "8d7e0d22-620c-45b4-ac38-25b63ddf79e0")
