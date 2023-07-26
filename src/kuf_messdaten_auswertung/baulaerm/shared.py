
from ..models import EinstellungenRichtungsdaten, MesspunktBaulaerm, LaermkategorisierungMesspunkt, ImmissionsortBaulaerm
from datetime import datetime, timedelta
from uuid import UUID

def get_beurteilungszeitraum_zeitfenster(zeitpunkt: datetime):
    if zeitpunkt.hour <= 7:
        beginn = datetime(zeitpunkt.year, zeitpunkt.month,
                          zeitpunkt.day) + timedelta(hours=-4)
        ende = datetime(zeitpunkt.year, zeitpunkt.month,
                        zeitpunkt.day) + timedelta(hours=7)
    elif zeitpunkt.hour >= 7 and zeitpunkt.hour < 20:
        beginn = datetime(zeitpunkt.year, zeitpunkt.month,
                          zeitpunkt.day) + timedelta(hours=7)
        ende = datetime(zeitpunkt.year, zeitpunkt.month,
                        zeitpunkt.day) + timedelta(hours=20)
    else:
        beginn = datetime(zeitpunkt.year, zeitpunkt.month,
                          zeitpunkt.day) + timedelta(hours=20)
        ende = datetime(zeitpunkt.year, zeitpunkt.month,
                        zeitpunkt.day) + timedelta(hours=7, days=1)
    return beginn, ende



def get_project_data(richtungsdaten_settings = [(0,33), (10, 25), (0, 33), (0, 33)]):
    r1 = EinstellungenRichtungsdaten(richtungsdaten_settings[0][0], richtungsdaten_settings[0][1])
    r2 = EinstellungenRichtungsdaten(richtungsdaten_settings[1][0], richtungsdaten_settings[1][1])
    r3 = EinstellungenRichtungsdaten(richtungsdaten_settings[2][0], richtungsdaten_settings[2][1])
    r4 = EinstellungenRichtungsdaten(richtungsdaten_settings[3][0], richtungsdaten_settings[3][1])
    mp1 = MesspunktBaulaerm(1, id_in_db=UUID(
        "16b2a784-8b6b-4b7e-9abf-fd2d5a8a0091"), ereignisse=[LaermkategorisierungMesspunkt(UUID("b24b1ff0-17f9-463e-9868-d041d49fa3b0"), "Unkategorisiert")])
    mp2 = MesspunktBaulaerm(2, id_in_db=UUID(
        "965157eb-ab17-496f-879a-55ce924f6252"), ereignisse=[LaermkategorisierungMesspunkt(UUID("396cec54-44b4-4ce4-8b64-a66b1caa3684"), "Unkategorisiert")])
    mp3 = MesspunktBaulaerm(3, id_in_db=UUID(
        "d0aa76cf-36e8-43d1-bb62-ff9cc2c275c0"), ereignisse=[LaermkategorisierungMesspunkt(UUID("0586bff3-c4c9-4100-b862-6a1d51e76652"), "Unkategorisiert")])
    mp4 = MesspunktBaulaerm(4, id_in_db=UUID(
        "ab4e7e2d-8c39-48c2-b80c-b80f6b619657"), ereignisse=[LaermkategorisierungMesspunkt(UUID("4d342e90-c8d2-4163-9907-74a07ab96a8a"), "Unkategorisiert")])

    io1 = ImmissionsortBaulaerm(1, id_in_db=UUID("c4862493-478b-49ec-ba03-a779551bf575"))
    io2 = ImmissionsortBaulaerm(2, id_in_db=UUID("f4311d0b-cd3a-4cf1-a0df-d4f1a5edbef7"))
    io3 = ImmissionsortBaulaerm(3, id_in_db=UUID("c27fe3cd-af55-43ec-9a52-0b2aec78df8b"))
    io4 = ImmissionsortBaulaerm(4, id_in_db=UUID("89b09198-44ee-43b9-bb03-a0a138c6d26a"))
    _ios = [io1, io2, io3, io4]
    _ausbreitungsfaktoren = { # io_id, mp_id
        (1, 1): 0, (1, 2): 100, (1, 3): 100, (1, 4): 100,
        (2, 1): 100, (2, 2): 0, (2, 3): 100, (2, 4): 100,
        (3, 1): 100, (3, 2): 100, (3, 3): 0, (3, 4): 100,
        (4, 1): 100, (4, 2): 100, (4, 3): 100, (4, 4): 0,
    }
    _mps = [mp1,
        mp2,
        mp3,
        mp4]


    # laermkategorisierung_an_immissionsorten_ids = [
    #     "31b9dc20-0f4d-4e15-a530-17b810cada01",
    #     "b324888e-c5d2-473b-80b2-6118c0ddeee3",
    #     "5957e178-2095-4727-930d-6c1a8ded7aa9",
    #     "9d3cedc7-ef9a-4299-bcb3-139cf3ad5979",
    #     "03354e11-690b-415f-9f85-0864c617e174"]
    # laermkategorisierung_an_immissionsorten_extended = [
    #     {"name": f"{k}_mp{mp.Id}", "id": 1} for mp in _mps for k in mp.Ereignisse] + ["Gesamt"]

    # mapping_mp_kategorisierung_io_kategorisierung = dict(zip(
    #     laermkategorisierung_an_messpunkten + [None], laermkategorisierung_an_immissionsorten))


    name_column_beurteilungspegelrelevant = '{mp_id}_{ereignis_name}_TAKT'
    _laermkategorisierung_an_immissionsorten_reloaded = [{"name": f"Stihl MP {mp.id} - Unkategorisiert", "name_messpunkt_kategorisierung": f"{k}_mp{mp.id}", "column_name_ergebnis": name_column_beurteilungspegelrelevant.format(
        ereignis_name=k.name, mp_id=mp.id), "id": None} for mp in _mps for k in mp.ereignisse] + [{"name": "Gesamt", "name_messpunkt_kategorisierung": None, "column_name_ergebnis": "Gesamt", "id": None}]

    laermkategorisierung_an_immissionsorten_datenbank = [{
        "id": "31b9dc20-0f4d-4e15-a530-17b810cada01",
        "name": "Stihl MP 1 - Unkategorisiert",
                "projekt": "8d7e0d22-620c-45b4-ac38-25b63ddf79e0"
    },
        {
        "id": "b324888e-c5d2-473b-80b2-6118c0ddeee3",
        "name": "Stihl MP 2 - Unkategorisiert",
                "projekt": "8d7e0d22-620c-45b4-ac38-25b63ddf79e0"
    },
        {
        "id": "5957e178-2095-4727-930d-6c1a8ded7aa9",
        "name": "Stihl MP 3 - Unkategorisiert",
                "projekt": "8d7e0d22-620c-45b4-ac38-25b63ddf79e0"
    },
        {
        "id": "9d3cedc7-ef9a-4299-bcb3-139cf3ad5979",
        "name": "Stihl MP 4 - Unkategorisiert",
                "projekt": "8d7e0d22-620c-45b4-ac38-25b63ddf79e0"
    },
        {
        "id": "03354e11-690b-415f-9f85-0864c617e174",
        "name": "Gesamt",
                "projekt": "8d7e0d22-620c-45b4-ac38-25b63ddf79e0"
    }]

    for i in laermkategorisierung_an_immissionsorten_datenbank:
        for ii in _laermkategorisierung_an_immissionsorten_reloaded:
            if i["name"] == ii["name"]:
                ii["id"] = i["id"]

    kategorisierte_laermpegel = []



    return _mps, _ios, _laermkategorisierung_an_immissionsorten_reloaded, _ausbreitungsfaktoren
    