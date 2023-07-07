import logging
import sys
from datetime import datetime
from src.kuf_messdaten_auswertung.baulaerm.v3 import erstelle_baulaerm_auswertung
from src.kuf_messdaten_auswertung.baulaerm.shared import get_project_data


if __name__ == "__main__":
    FORMAT = '%(filename)s %(lineno)d %(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        level=logging.INFO,
        format=FORMAT,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("baulaerm.log")
        ])

    if True:
        logging.info("Script started")
        year = 2023
        month = 7
        for i in range(1, 3+1):
            for ii in [13, 
                       # 23
                       ]:
                try:
                    ausgewerteter_zeitpunkt = datetime(year, month, i, ii, 0, 0)
                    _mps, _ios, laermkategorisierung_an_ios, _ausbreitungsfaktoren = get_project_data()
                    # erstelle_baulaerm_auswertung(_mps, _ios, laermkategorisierung_an_ios, _ausbreitungsfaktoren, ausgewerteter_zeitpunkt, "8d7e0d22-620c-45b4-ac38-25b63ddf79e0")
                    erstelle_baulaerm_auswertung(_mps, ausgewerteter_zeitpunkt, "8d7e0d22-620c-45b4-ac38-25b63ddf79e0")

                except Exception as ex:
                    logging.exception(ex)

        