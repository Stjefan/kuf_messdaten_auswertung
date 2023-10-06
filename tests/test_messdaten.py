from unittest import TestCase
from src.kuf_messdaten_auswertung.default import werte_beurteilungszeitraum_aus
from datetime import datetime

import httpretty

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
        werte_beurteilungszeitraum_aus(datetime(2023, 9, 5, 10, 0, 0), "immendingen", "http://localhost:8000")
    if False:
        def test_sindelfingen(self):
            werte_beurteilungszeitraum_aus(datetime(2023, 9, 5, 10, 0, 0), "sindelfingen", "localhost:8000")
        
        def test_mannheim(self):
            werte_beurteilungszeitraum_aus(datetime(2023, 9, 5, 10, 0, 0), "mannheim", "localhost:8000")

    def test_non_terz_block(self):
        print("Hell yeah")
