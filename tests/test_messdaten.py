from unittest import TestCase
from src.kuf_messdaten_auswertung.default import werte_beurteilungszeitraum_aus
from src.kuf_messdaten_auswertung.baulaerm.v3 import erstelle_baulaerm_auswertung
from src.kuf_messdaten_auswertung.baulaerm.shared import get_project_data
from datetime import datetime

import httpretty
import logging

class TestJoke(TestCase):
    def setUp(self) -> None:
        super().setUp()

    @httpretty.activate
    def test_a(self):
        httpretty.register_uri(
        httpretty.GET, 
        "http://localhost:8000/dauerauswertung/projekt/immendingen/",
        json={"name": "John Doe"},
        status=200
        )
        # werte_beurteilungszeitraum_aus(datetime(2023, 9, 5, 10, 0, 0), "immendingen", "http://localhost:8000")
    if False:
        def test_sindelfingen(self):
            werte_beurteilungszeitraum_aus(datetime(2023, 9, 5, 10, 0, 0), "sindelfingen", "localhost:8000")
        
        def test_mannheim(self):
            werte_beurteilungszeitraum_aus(datetime(2023, 9, 5, 10, 0, 0), "mannheim", "localhost:8000")

    def test_b(self):
        logger = logging.getLogger("kuf_messdaten_auswertung")
        logger.addHandler(logging.FileHandler("bla.log"))
        logger.setLevel(logging.DEBUG)
        
        ausgewerteter_zeitpunkt = datetime.now()
        logger.info(f"Test für {ausgewerteter_zeitpunkt}")
        _mps, _ios, laermkategorisierung_an_ios, _ausbreitungsfaktoren = get_project_data()
        # erstelle_baulaerm_auswertung(_mps, _ios, laermkategorisierung_an_ios, _ausbreitungsfaktoren, ausgewerteter_zeitpunkt, "8d7e0d22-620c-45b4-ac38-25b63ddf79e0")
        erstelle_baulaerm_auswertung([_mps[1]], ausgewerteter_zeitpunkt, "8d7e0d22-620c-45b4-ac38-25b63ddf79e0", database_insert=False)

