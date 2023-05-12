from unittest import TestCase
from kuf_messdaten_auswertung.foo import b
from kuf_messdaten_auswertung.baulaerm_script import baz
class TestJoke(TestCase):
    def setUp(self) -> None:
        super().setUp()

    def test_a(self):
        self.assertAlmostEqual(b, 10)
        
    
    def test_b(self):
        self.assertAlmostEqual(baz(), 32)