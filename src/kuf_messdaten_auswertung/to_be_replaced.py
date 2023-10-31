from .baulaerm.v1 import foo, bar
import requests
from .models import Projekt, Koordinaten, Immissionsort, Messpunkt

from uuid import UUID

from .settings import logger_config_parser

def get_project_via_rest(name: str, server_url: str) -> Projekt:
    custom_zuordnung = {
        'MP1 - Handlingkurs':  'Unkategorisiert - MP 1', 
        'MP2 - Berthafläche': 'Unkategorisiert - MP 2', 'MP3 - Stadtstraße': 'Unkategorisiert - MP 3', 'MP4 - Innenstadt': 'Unkategorisiert - MP 4', 
        'MP5 Ovalkurs - Unkategorisiert': 'Unkategorisiert - MP 5', 'MP5 Ovalkurs - Vorbeifahrt': 'Vorbeifahrt - MP 5', 
        'MP6 - Stadtstraße Heidestrecke': 'Unkategorisiert - MP 6',
        'Gesamt': 'Gesamt'
    }

    dict_abf = {
            (1, 1): -46.3, (5, 1): -51.2, (9, 1): -43.3, (15, 1): -31.8, (17, 1): -25.6,
            (1, 2): -43.1, (5, 2): -41.6, (9, 2): -38.7, (15, 2): -33.7, (17, 2): -30.3,
            (1, 3): -33.3, (5, 3): -31.0, (9, 3): -29.9, (15, 3): -24.7, (17, 3): -31.1,
            (1, 4): -31.2, (5, 4): -36.6, (9, 4): -31.5, (15, 4): -15.9, (17, 4): -17.2,
            (1, 5): -35.1, (5, 5): -35.4, (9, 5): -32.7, (15, 5): -27.8, (17, 5): -21.7, #-20.8, -14.7
            (1, 6): -32.3, (5, 6): -34.6, (9, 6): -32.9, (15, 6): -30.7, (17, 6): -30.2
        }


    dict_abf_mannheim = {
        (4, 2): -15.7, (5, 2): -21.6, (6, 2): -17
    }

    custom_zuordnung_2 = {
        'MP2':  'Unkategorisiert - MP 2', 
        'Gesamt': 'Gesamt'
    }

    zuordnung_ereignis_bezeichnung_datenbank_berechnung_sindelfingen = {
        

        'Gesamt Sindelfingen': 'Gesamt'
    }

    p = requests.get(f"{server_url}/dauerauswertung/projekt/{name}/", timeout=30)
    p.raise_for_status()
    projekt_json = p.json()


    logger_config_parser.info(projekt_json)


    if name == 'Immendingen':
        
        idx = 0

        abfs_json = projekt_json["ausbreitungsfaktoren_set"]

        dict_abf_io_ereignis = {}
        

        abfs = dict(zip([(a["immissionsort"], a["messpunkt"]) for a in abfs_json], [a["ausbreitungskorrektur"] for a in abfs_json]))
            

        mps = [
            Messpunkt(mp_json['id_external'], Bezeichnung=mp_json['name'], Koordinaten=Koordinaten(mp_json["gk_rechts"], mp_json["gk_hoch"]), id_in_db=UUID(mp_json["id"]), Ereignisse=[e["name"] for e in mp_json["laermursacheanmesspunkt_set"]], 
                einstellungen_richtunsdaten=None
                ) 
                for mp_json in projekt_json['messpunkt_set']]
        has_mete_data = any([mp_json["is_meteo_station"] for mp_json in projekt_json['messpunkt_set']])
        ios = [
            Immissionsort(io_json['id_external'],
                        Bezeichnung=io_json["name"],
                        Koordinaten=Koordinaten(io_json["gk_rechts"], io_json["gk_hoch"]), 
                        id_in_db=UUID(io_json["id"]), Grenzwert_nacht=io_json["grenzwert_nacht"], Grenzwert_tag=io_json["grenzwert_tag"]) for io_json in projekt_json['immissionsort_set']]
        for mp in mps:
            mp: Messpunkt
            mp.column_lr = mp.Ereignisse[0]
            for e in mp.Ereignisse:
                for io in ios:
                    
                    dict_abf_io_ereignis[(io.Id, e)] = dict_abf[(io.Id, mp.id)] # 0 # abfs[(io.Id, mp.Id)]
        filters = dict(zip([el["name"] for el in projekt_json["rejections"]], projekt_json["rejections"])) 
        ursachen_an_ios = dict(zip([custom_zuordnung[el["name"]] for el in projekt_json["laermursacheanimmissionsorten_set"]], projekt_json["laermursacheanimmissionsorten_set"])) 
        p1 = Projekt(projekt_json['name'], ios, mps, abfs, "blub", has_mete_data=has_mete_data, dict_abf_io_ereignis = dict_abf_io_ereignis, id_in_db =  projekt_json["id"],ursachen_an_ios=ursachen_an_ios, filter_mit_ids=filters, id_messpunkt_at_mete_station=UUID("50f6a165-3f76-4d26-9f55-1d559e0e6fc8"))
        print(p1)
        return p1
    elif name == 'Mannheim':



        abfs_json = projekt_json["ausbreitungsfaktoren_set"]

        dict_abf_io_ereignis = {}
        

        abfs = dict(zip([(a["immissionsort"], a["messpunkt"]) for a in abfs_json], [a["ausbreitungskorrektur"] for a in abfs_json]))
            

        mps = [
            Messpunkt(mp_json['id_external'], Bezeichnung=mp_json['name'], Koordinaten=Koordinaten(mp_json["gk_rechts"], mp_json["gk_hoch"]), id_in_db=UUID(mp_json["id"]), Ereignisse=[e["name"] for e in mp_json["laermursacheanmesspunkt_set"]], 
                einstellungen_richtunsdaten=None
                ) 
                for mp_json in projekt_json['messpunkt_set']]
        has_mete_data = any([mp_json["is_meteo_station"] for mp_json in projekt_json['messpunkt_set']])
        ios = [
            Immissionsort(io_json['id_external'],
                        Bezeichnung=io_json["name"],
                        Koordinaten=Koordinaten(io_json["gk_rechts"], io_json["gk_hoch"]), 
                        id_in_db=UUID(io_json["id"]), Grenzwert_nacht=io_json["grenzwert_nacht"], Grenzwert_tag=io_json["grenzwert_tag"]) for io_json in projekt_json['immissionsort_set']]
        for mp in mps:
            mp: Messpunkt
            mp.column_lr = mp.Ereignisse[0]
            for e in mp.Ereignisse:
                for io in ios:
                    
                    dict_abf_io_ereignis[(io.Id, e)] = dict_abf_mannheim[(io.Id, mp.id)] # 0 # abfs[(io.Id, mp.Id)]
        filters = dict(zip([el["name"] for el in projekt_json["rejections"]], projekt_json["rejections"])) 
        ursachen_an_ios = dict(zip([custom_zuordnung_2[el["name"]] for el in projekt_json["laermursacheanimmissionsorten_set"]], projekt_json["laermursacheanimmissionsorten_set"])) 
        p1 = Projekt(projekt_json['name'], ios, mps, abfs, "mannheim", has_mete_data=has_mete_data, dict_abf_io_ereignis = dict_abf_io_ereignis, id_in_db =  projekt_json["id"],ursachen_an_ios=ursachen_an_ios, filter_mit_ids=filters)
        
        return p1
    
    elif name == "Sindelfingen":
       

        dict_abf_sindelfingen = {
                    (5,4):-36.6,(4,4):-28.9,(2,4):-14.5,(3,4):-21.5,(1,4):-23.8,
                    (5,3):-34.1,(4,3):-12.3,(2,3):-30.7,(3,3):-24.1,(1,3):-14.5,
                    (5,1):-37.6,(4,1):-18.9,(2,1):-43,(3,1):-33.8,(1,1):-31.9,
                    (5,5):-34.3,(4,5):-29.1,(2,5):-26.9,(3,5):-22,(1,5):-30.7,
                    (5,2):-21,(4,2):-21,(2,2):-40.1,(3,2):-33.9,(1,2):-35.1,
                    # (6,1):-27.5,(6,5):-27.7,(6,3):-5.6,(6,4):-18.3,(6,2):-31.4,
        }

        abfs_json = projekt_json["ausbreitungsfaktoren_set"]

        dict_abf_io_ereignis = {}
        

        abfs = dict(zip([(a["immissionsort"], a["messpunkt"]) for a in abfs_json], [a["ausbreitungskorrektur"] for a in abfs_json]))
            

        mps = [
            Messpunkt(mp_json['id_external'], Bezeichnung=mp_json['name'], Koordinaten=Koordinaten(mp_json["gk_rechts"], mp_json["gk_hoch"]), id_in_db=UUID(mp_json["id"]), Ereignisse=[e["name"] for e in mp_json["laermursacheanmesspunkt_set"]], 
                einstellungen_richtunsdaten=None, LWA=mp_json['lwa']
                ) 
                for mp_json in projekt_json['messpunkt_set']]
        has_mete_data = any([mp_json["is_meteo_station"] for mp_json in projekt_json['messpunkt_set']])
        ios = [
            Immissionsort(io_json['id_external'],
                        Bezeichnung=io_json["name"],
                        Koordinaten=Koordinaten(io_json["gk_rechts"], io_json["gk_hoch"]), 
                        id_in_db=UUID(io_json["id"]), Grenzwert_nacht=io_json["grenzwert_nacht"], Grenzwert_tag=io_json["grenzwert_tag"]) for io_json in projekt_json['immissionsort_set']]
        for mp in mps:
            mp: Messpunkt
            mp.column_lr = mp.Ereignisse[0]
            for e in mp.Ereignisse:
                for io in ios:
                    print(io)
                    dict_abf_io_ereignis[(io.Id, e)] = dict_abf_sindelfingen[(io.Id, mp.id)] # 0 # abfs[(io.Id, mp.Id)]
        filters = dict(zip([el["name"] for el in projekt_json["rejections"]], projekt_json["rejections"])) 
        ursachen_an_ios = dict(zip([zuordnung_ereignis_bezeichnung_datenbank_berechnung_sindelfingen.get(el["name"], el["name"]) for el in projekt_json["laermursacheanimmissionsorten_set"]], projekt_json["laermursacheanimmissionsorten_set"])) 
        print("ursachen_an_ios", ursachen_an_ios)
        p1 = Projekt(projekt_json['name'], ios, mps, abfs, "sindelfingen", has_mete_data=has_mete_data, dict_abf_io_ereignis = dict_abf_io_ereignis, id_in_db =  projekt_json["id"],ursachen_an_ios=ursachen_an_ios, filter_mit_ids=filters)
        
        return p1



