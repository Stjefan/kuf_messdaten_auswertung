from datetime import datetime, timedelta
from . import logger
ISOFORMAT = "%Y-%m-%dT%H:%M:%SZ"

FORMAT = '%(filename)s %(asctime)s %(message)s'

frequencies = ['hz20', 'hz25', 'hz31_5', 'hz40', 'hz50', 'hz63', 'hz80', 'hz100', 'hz125', 'hz160', 'hz200', 'hz250',
                            'hz315', 'hz400', 'hz500', 'hz630', 'hz800', 'hz1000', 'hz1250', 'hz1600', 'hz2000', 'hz2500', 'hz3150',
                            'hz4000',
                            'hz5000',
                            'hz6300',
                            'hz8000',
                            'hz10000',
                            'hz12500',
                            'hz16000',
                            'hz20000']


umrechnung_Z_2_A = [-50.381340054288508, -44.808253082486992, -39.51773225684942, -34.529217003689183,
                        -30.265947403466988,
                        -26.214832041312413, -22.390780883566684, -19.139123948958755, -16.184850179682389,
                        -13.24055129346589,
                        -10.844057804713385, -8.67245241535807, -6.6418939418274583, -4.7724139958206253,
                        -3.2466290426057549,
                        -1.9075334826350536, -0.79391638893301053, 0.00065250589591281383, 0.57665182435893092,
                        0.99339037086401527,
                        1.2018394062425557, 1.2710677601627558, 1.2015162813063089, 0.963642706019207,
                        0.55446312883691506,
                        -0.11553410365194017, -1.1468678972384261, -2.4915620797210227, -4.2539930143511926,
                        -6.7061155353482214,
                        -9.3467919167418945]

terzfrequenzen = ["20", "25", "31_5", "40", "50", "63", "80", "100", "125", "160",
                      "200", "250", "315", "400", "500", "630", "800", "1000", "1250", "1600",
                      "2000", "2500", "3150", "4000", "5000", "6300", "8000", "10000", "12500", "16000",
                      "20000"]


cols_directions_vertical= [f"dr{i}_h" for i in range(0,32+1)]
cols_directions_horizontal= [f"dr{i}_v" for i in range(0,16)]

indexed_cols_directions_horizontal = [(i, f"dr{i}_h") for i in range(0,32+1)]
indexed_cols_directions_vertical= [(i, f"dr{i}_v") for i in range(0,16)]



bewertungszeitraum_daten = {
        0: {"Beginn": 0, "Ende": 1 * 3600-1, "stunden_in_beurteilungszeitraum": 1},
        1: {"Beginn": 1 * 3600, "Ende": 2 * 3600-1, "stunden_in_beurteilungszeitraum": 1},
        2: {"Beginn": 2 * 3600, "Ende": 3 * 3600-1, "stunden_in_beurteilungszeitraum": 1},
        3: {"Beginn": 3 * 3600, "Ende": 4 * 3600-1, "stunden_in_beurteilungszeitraum": 1},
        4: {"Beginn": 4 * 3600, "Ende": 5 * 3600-1, "stunden_in_beurteilungszeitraum": 1},
        5: {"Beginn": 5 * 3600, "Ende": 6 * 3600-1, "stunden_in_beurteilungszeitraum": 1},
        6: {"Beginn": 6 * 3600, "Ende": 22 * 3600-1, "stunden_in_beurteilungszeitraum": 16},
        7: {"Beginn": 22 * 3600, "Ende": 23 * 3600-1, "stunden_in_beurteilungszeitraum": 1},
        8: {"Beginn": 23 * 3600, "Ende": 24 * 3600-1, "stunden_in_beurteilungszeitraum": 1},
    }

def get_start_end_beurteilungszeitraum_from_datetime(zeitpunkt: datetime):
    my_beurtielungszeitraum = bewertungszeitraum_daten[get_id_corresponding_beurteilungszeitraum(zeitpunkt)]
    selected_date = datetime(zeitpunkt.year, zeitpunkt.month, zeitpunkt.day)
    print(selected_date)
    return selected_date + timedelta(seconds=my_beurtielungszeitraum["Beginn"]) , selected_date + timedelta(seconds=my_beurtielungszeitraum["Ende"])


def get_interval_beurteilungszeitraum_from_datetime(zeitpunkt: datetime):
    my_beurtielungszeitraum = bewertungszeitraum_daten[get_id_corresponding_beurteilungszeitraum(zeitpunkt)]
    return my_beurtielungszeitraum["Beginn"], my_beurtielungszeitraum["Ende"]

def get_endzeitpunkt_beurteilungszeitraum(year, month, day, id_beurteilungszeitraum):
    return datetime(year, month, day) + timedelta(seconds = bewertungszeitraum_daten[id_beurteilungszeitraum]["Ende"]-1)
    
def tagessekunde_from_date(arg: datetime):
    return arg.hour*3600+arg.minute*60+arg.second

def get_key(dict, func):
    for key, value in dict.items():
        if func(value):
            return key
    raise IOError("Value does not exist")
    
def get_id_corresponding_beurteilungszeitraum(zeitpunkt: datetime):
    logger.info(zeitpunkt)
    second_of_day = zeitpunkt.hour*3600+zeitpunkt.minute*60+zeitpunkt.second
    return get_key(bewertungszeitraum_daten, lambda i: i["Beginn"] <= second_of_day and i["Ende"] >= second_of_day)
