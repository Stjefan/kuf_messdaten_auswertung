from collections import defaultdict
from dataclasses import dataclass, field

from enum import Enum

from datetime import datetime


from pandas import Series, DataFrame
import numpy as np


@dataclass
class EinstellungenRichtungsdaten:
    akzeptanz_untere_schranke: int = 5
    akzeptanz_obere_schranke: int = 35


@dataclass
class Koordinaten:
    GKRechtswert: float = 0
    GKHochwert: float = 0


@dataclass
class Immissionsort:
    Id: str
    Bezeichnung: str = "Missing IO name"
    Grenzwert_nacht: float = 100
    Grenzwert_tag: float = 100
    Koordinaten:  Koordinaten = Koordinaten(0, 0)
    ruhezeitzuschlag: bool = False
    shortname_for_excel: str = "" # <= 31 chars
    id_in_db: int = 0

    def get_kurzbezeichnung(self):
        return f"IO {self.Id}"

    def __post_init__(self):
        if self.shortname_for_excel == "":
            self.shortname_for_excel = self.Bezeichnung
@dataclass
class Messpunkt:
    Id: str
    bezeichnung_in_db: str = ""
    Bezeichnung: str = "Missing MP name"
    Koordinaten: Koordinaten = Koordinaten(0, 0)
    Ereignisse: list[str] = field(default_factory=list)
    LWA: float = 0.0 # schallleistungspegel_korrektur
    Filter: list[str] = field(default_factory=list)
    OrdnerMessdaten: str = ""
    column_lr: str = "N/A"
    id_in_db: int = 0
    einstellungen_richtunsdaten: EinstellungenRichtungsdaten = None
    
    

'''
@dataclass
class Messpunkt:
    id: int
    koordinaten: Koordinaten = field(default_factory=Koordinaten)
    ereignisse: list[str] = field(default_factory=list)
    bezeichnung: str = "X"
    lwa: float = 0.0  # schallleistungspegel_korrektur """
'''


@dataclass
class Projekt:
    name: str
    IOs: list[Immissionsort]
    MPs: list[Messpunkt]
    Ausbreitungsfaktoren: dict
    name_in_db: str
    has_mete_data: bool = False
    has_terz_data: bool = False
    mete_station: Messpunkt = None
    gw_lafeq: float = 90
    gw_lafmax: float = 100
    dict_abf_io_ereignis: dict = None
    id_in_db: int = 0
    ursachen_an_ios: dict = field(default_factory=defaultdict)
    filter_mit_ids: dict = field(default_factory=defaultdict)


@dataclass
class DTO_LrPegel:
    time: datetime
    pegel: float
    verursacht: int
    immissionsort: int

@dataclass
class DTO_Rejected:
    time: datetime
    grund: str
    messpunkt: int = 2

@dataclass
class DTO_Detected:
    time: datetime
    duration: int
    messpunkt: int


@dataclass
class DTO_TaktmaximalpegelRichtungsgewertet:
    time: datetime
    pegel: float
    messpunkt: int

@dataclass
class DTO_Maxpegel:
    time: datetime
    pegel: float
    immissionsort: int

@dataclass
class DTO_Schallleistungpegel:
    time: datetime
    pegel: float
    messpunkt: int

@dataclass
class Ergebnisse:
    zeitpunkt_im_beurteilungszeitraum: datetime
    zeitpunkt_durchfuehrung: datetime
    verhandene_messwerte: int
    verwertebare_messwerte: int
    in_berechnung_gewertete_messwerte: int
    detected_set: list[DTO_Detected]
    lrpegel_set: list[DTO_LrPegel]
    rejected_set: list[DTO_Rejected]
    maxpegel_set: list[DTO_Maxpegel]
    schallleistungspegel_set: list[DTO_Schallleistungpegel]
    zuordnung: int


@dataclass
class ErgebnisseBaulaerm:
    zeitpunkt_im_beurteilungszeitraum: datetime
    zeitpunkt_durchfuehrung: datetime
    verhandene_messwerte: int
    verwertebare_messwerte: int
    in_berechnung_gewertete_messwerte: int
    detected_set: list[DTO_Detected]
    lrpegel_set: list[DTO_LrPegel]
    rejected_set: list[DTO_Rejected]
    richtungsgewertetertaktmaximalpegel_set: list[DTO_TaktmaximalpegelRichtungsgewertet]
    zuordnung: int


@dataclass
class Beurteilungszeitraum:
    Beginn: int
    Ende: int
    Stunden_in_beurteilungszeitraum: int


@dataclass
class Detected:
    start: datetime
    end: datetime
    timepoints: np.array
    pattern_id: int
    messpunkt_id: int
    id: int
    score: float

@dataclass
class Auswertungslauf:
    ausgewertetes_datum: datetime
    project: str
    erkennung_set= [] 
    aussortierung_set= []
    beurteilungspegel_set= []
    schallleistungspegel_set= []
    lautestestunde_set= []
    zugeordneter_beurteilungszeitraum: int
    no_verwertbare_messwerte: int = 0
    no_verfuegbare_messwerte: int = 0
    no_gewertete_messwerte: int = 0
    no_aussortiert_wetter: int = 0
    no_aussortiert_sonstiges: int = 0
    zeitpunkt_durchfuehrung: datetime = field(default_factory=datetime.now)
    kennung_auswertungslauf: str = "N/A"
                                  

@dataclass
class Vorbeifahrt:
    beginn: datetime
    ende: datetime
    messpunkt: Messpunkt

@dataclass
class Aussortiert:
    timepoints: Series
    bezeichnug: str
    messpunkt: Messpunkt


@dataclass
class Schallleistungspegel:
    pegel: float
    zeitpunkt: datetime
    messpunkt: Messpunkt

@dataclass
class LautesteStunde:
    pegel: float
    zeitpunkt: datetime
    immissionsort: Immissionsort

@dataclass
class MonatsuebersichtAnImmissionsort:
    immissionsort: Immissionsort
    lr_tag: DataFrame = None
    lr_max_nacht: DataFrame = None
    lauteste_stunde_tag:  DataFrame = None
    lauteste_stunde_nacht: DataFrame = None

@dataclass
class LaermursacheAnImmissionsorten:
    name: str


@dataclass
class LrPegel:
    pegel: float
    immissionsort: Immissionsort
    zeitpunkt: datetime


@dataclass
class Auswertungsergebnis:
    lr: list[LrPegel]
    rejected: list[Aussortiert]
    detected: list[Vorbeifahrt]
    leistungspegel: list[Schallleistungspegel]

    vorhandene_sekunden: int
    verwertbare_sekunden: int
    in_berechnung_gewertete_messwerte: int

    zeitpunkt_ausfuehrung: datetime
    beginn_beurteilungszeitraum: datetime

@dataclass
class SettingsAuswertung:
    has_mete: True
    has_terz: True
    gw_lafeq: 90
    gw_lafmax: 110
    gw_lcfeq: 120