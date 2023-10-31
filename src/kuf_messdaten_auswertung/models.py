from collections import defaultdict
from dataclasses import dataclass, field

from enum import Enum

from datetime import datetime
from typing import Optional


from pandas import Series, DataFrame
import numpy as np

from uuid import UUID

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
    id_in_db: UUID
    Bezeichnung: str = "Missing IO name"
    Grenzwert_nacht: float = 100
    Grenzwert_tag: float = 100
    Koordinaten:  Koordinaten = Koordinaten(0, 0)
    ruhezeitzuschlag: bool = False
    shortname_for_excel: str = "" # <= 31 chars
    

    def get_kurzbezeichnung(self):
        return f"IO {self.Id}"

    def __post_init__(self):
        if self.shortname_for_excel == "":
            self.shortname_for_excel = self.Bezeichnung
@dataclass
class Messpunkt:
    id: str
    id_in_db: UUID
    bezeichnung_in_db: str = ""
    Bezeichnung: str = "Missing MP name"
    Koordinaten: Koordinaten = Koordinaten(0, 0)
    Ereignisse: list[str] = field(default_factory=list)
    LWA: float = 0.0 # schallleistungspegel_korrektur
    Filter: list[str] = field(default_factory=list)
    OrdnerMessdaten: str = ""
    column_lr: str = "N/A"
    
    einstellungen_richtunsdaten: Optional[EinstellungenRichtungsdaten] = None
    
    
    
@dataclass
class LaermkategorisierungMesspunkt:
    id: UUID
    name: str

@dataclass
class MesspunktBaulaerm:
    id: str
    id_in_db: UUID
    ereignisse: list[LaermkategorisierungMesspunkt]
 

@dataclass
class ImmissionsortBaulaerm:
    id: str
    id_in_db: UUID
    name: str = "Missing IO name"
    koordinaten:  Koordinaten = Koordinaten(0, 0)


@dataclass
class Projekt:
    name: str
    IOs: list[Immissionsort]
    MPs: list[Messpunkt]
    Ausbreitungsfaktoren: dict
    name_in_db: str
    has_mete_data: bool = False
    has_terz_data: bool = False
    mete_station: Optional[Messpunkt] = None
    gw_lafeq: float = 90
    gw_lafmax: float = 100
    dict_abf_io_ereignis: Optional[dict] = None
    id_in_db: int = 0
    ursachen_an_ios: dict = field(default_factory=defaultdict)
    filter_mit_ids: dict = field(default_factory=defaultdict)
    id_messpunkt_at_mete_station: Optional[UUID] = None

@dataclass
class ProjektBaulaerm:
    name: str
    ios: list[Immissionsort]
    mps: list[MesspunktBaulaerm]
    ausbreitungsfaktoren: dict
    name_in_db: str

    dict_abf_io_ereignis: dict = field(default_factory=defaultdict)
    id_in_db: Optional[UUID] = None
    ursachen_an_ios: dict = field(default_factory=defaultdict)
    filter_mit_ids: dict = field(default_factory=defaultdict)


@dataclass
class DTO_LrPegel:
    time: datetime
    pegel: float
    verursacht: UUID
    immissionsort: UUID

@dataclass
class DTO_Rejected:
    time: datetime
    grund: str
    messpunkt: Optional[UUID] = None

@dataclass
class DTO_Detected:
    time: datetime
    duration: int
    messpunkt: UUID


@dataclass
class DTO_TaktmaximalpegelRichtungsgewertet:
    time: datetime
    pegel: float
    verursacht_durch: UUID

@dataclass
class FremdgeraeuschMittelungspegel:
    time: datetime
    pegel: float
    verursacht_durch: UUID

@dataclass
class FremdgeraeuschLrpegel:
    time: datetime
    pegel: float
    verursacht_durch: UUID

@dataclass
class MesspunktLrpegel:
    time: datetime
    pegel: float
    verursacht_durch: UUID

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
    in_berechnung_gewertete_messwerte: UUID
    detected_set: list[DTO_Detected]
    lrpegel_set: list[DTO_LrPegel]
    rejected_set: list[DTO_Rejected]
    maxpegel_set: list[DTO_Maxpegel]
    schallleistungspegel_set: list[DTO_Schallleistungpegel]
    zuordnung: UUID

@dataclass
class ErgebnisseBaulaerm:
    zeitpunkt_im_beurteilungszeitraum: datetime
    zeitpunkt_durchfuehrung: datetime
    verhandene_messwerte: int
    verwertebare_messwerte: int
    in_berechnung_gewertete_messwerte: UUID
    detected_set: list[DTO_Detected]
    lrpegel_set: list[DTO_LrPegel]
    rejected_set: list[DTO_Rejected]
    richtungsgewertetertaktmaximalpegel_set: list[DTO_TaktmaximalpegelRichtungsgewertet]
    zuordnung: UUID
    running_mean_fremdgeraeusche_list: list[FremdgeraeuschMittelungspegel]
    lr_fremdgeraeusche_list: list[FremdgeraeuschLrpegel]
    lr_messpunkt_list: list[MesspunktLrpegel]


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
    pattern_id: UUID
    messpunkt_id: UUID
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