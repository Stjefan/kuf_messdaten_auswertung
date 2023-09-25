import math
from random import random
import uuid
import psycopg2
from pgcopy import CopyManager
from datetime import datetime, timedelta
from .models import (
    Auswertungsergebnis, Detected, LrPegel, 
    Ergebnisse, DTO_LrPegel, ErgebnisseBaulaerm, DTO_Detected, DTO_Rejected,
    DTO_TaktmaximalpegelRichtungsgewertet, FremdgeraeuschMittelungspegel, FremdgeraeuschLrpegel,
    MesspunktLrpegel
)
import requests
import logging
import sys
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv

from .constants import get_start_end_beurteilungszeitraum_from_datetime
from .baulaerm.shared import get_beurteilungszeitraum_zeitfenster
# Connect to an existing database
load_dotenv()

logger = logging.getLogger("kuf_messdaten_auswertung")
# logger.info(f"ENV: {os.getenv('POSTGRES_CS')}")
CS = os.getenv("POSTGRES_CS")

app_name = "dauerauswertung" # "tsdb"

def delete_old_data_via_psycopg2(cursor, ids_old_auswertungslauf, from_date, to_date):
    if True:
        for id_old_auswertungslauf in ids_old_auswertungslauf:
            for tbl in ["lrpegel", "rejected", "detected", "schallleistungpegel", "maxpegel"]:
                pass
                q1 = f"""
                        DELETE FROM {app_name}_{tbl} WHERE berechnet_von_id = '{id_old_auswertungslauf}'::uuid;
                        """
                #  DELETE FROM {app_name}_{tbl} WHERE time >= '{from_date}' and time <= '{to_date}' and berechnet_von_id = {id_old_auswertungslauf};
                cursor.execute(q1)
                logger.info(q1)
                if False:
                    q2 = f"SELECT drop_chunks('{app_name}_{tbl}', '{to_date + timedelta(hours=0)}', '{from_date + timedelta(hours=-0)}');"
                    
                    logger.info(q2)
                    cursor.execute(q2)
            q1 = f"""DELETE FROM {app_name}_Auswertungslauf WHERE id = '{id_old_auswertungslauf}'::uuid"""
            logger.info(q1)
            cursor.execute(q1)
    
def delete_old_baulaerm_data_via_psycopg2(cursor, ids_old_auswertungslauf, from_date, to_date):
    logger.info(ids_old_auswertungslauf)
    if True:
        for id_old_auswertungslauf in ids_old_auswertungslauf:
            for tbl in [
                "dauerauswertung_beurteilungspegelbaulaerm", "dauerauswertung_richtungungsgewertetertaktmaximalpegel", 
                "dauerauswertung_baustellenumgebungspegel", "dauerauswertung_baustellenbeurteilungspegel", "dauerauswertung_baustellenbeurteilungspegelumgebung"
                ]:
                q1 = f"""
                        DELETE FROM {tbl} WHERE berechnet_von_id = '{id_old_auswertungslauf}'::uuid;
                        """
                #  DELETE FROM {app_name}_{tbl} WHERE time >= '{from_date}' and time <= '{to_date}' and berechnet_von_id = {id_old_auswertungslauf};
                cursor.execute(q1)
                logger.info(q1)
            for tbl in ["dauerauswertung_rejected", "dauerauswertung_detected"]:
                q1 = f"""
                        DELETE FROM {tbl} WHERE berechnet_von_baulaerm_id = '{id_old_auswertungslauf}'::uuid;
                        """
                #  DELETE FROM {app_name}_{tbl} WHERE time >= '{from_date}' and time <= '{to_date}' and berechnet_von_id = {id_old_auswertungslauf};
                cursor.execute(q1)
                logger.info(q1)
            q1 = f"""DELETE FROM dauerauswertung_auswertungslaufbaulaerm WHERE id = '{id_old_auswertungslauf}'::uuid"""
            logger.info(q1)
            cursor.execute(q1)

def get_id_old_auswertungslauf(cursor, zuordnung, from_date, to_date):
    q = f"""SELECT id FROM {app_name}_Auswertungslauf WHERE zeitpunkt_im_beurteilungszeitraum  >= '{from_date.astimezone()}' and zeitpunkt_im_beurteilungszeitraum <= '{to_date.astimezone()}' and zuordnung_id = '{zuordnung}'::uuid"""
    logger.info("q")
    cursor.execute(q)
    
    result = cursor.fetchall()

    logger.info(f"OLD ID:  {result}")
    return [r[0] for r in result] if len(result) > 0 else []

def get_id_old_baulaerm_auswertungslauf(cursor, zuordnung, from_date, to_date):
    q = f"""SELECT id FROM dauerauswertung_auswertungslaufbaulaerm WHERE zeitpunkt_im_beurteilungszeitraum  >= '{from_date.astimezone()}' and zeitpunkt_im_beurteilungszeitraum <= '{to_date.astimezone()}' and zuordnung_id = '{zuordnung}'::uuid"""
    cursor.execute(q)
    logger.info(q)
    result = cursor.fetchall()

    logger.info(result)
    return [r[0] for r in result] if len(result) > 0 else []

def insert_new_baulaerm_auswertung(cursor, from_date, to_date, ergebnis: ErgebnisseBaulaerm):
    random_messpunkt_id = "16b2a784-8b6b-4b7e-9abf-fd2d5a8a0091"
    time = from_date
    q = f"""
        INSERT INTO dauerauswertung_auswertungslaufbaulaerm
        (id, zeitpunkt_im_beurteilungszeitraum, zeitpunkt_durchfuehrung, verhandene_messwerte, verwertebare_messwerte, in_berechnung_gewertete_messwerte, zuordnung_id) 
        VALUES 
        ('{uuid.uuid4()}', '{ergebnis.zeitpunkt_im_beurteilungszeitraum.astimezone().isoformat()}', '{datetime.now().astimezone().isoformat()}', {ergebnis.verhandene_messwerte}, {ergebnis.verwertebare_messwerte}, {ergebnis.in_berechnung_gewertete_messwerte}, '{ergebnis.zuordnung}'::uuid) 
        RETURNING id;
        """
    cursor.execute(q)
    new_row_id = cursor.fetchone()
    lr_arr = [[i.time.isoformat(), i.pegel, i.immissionsort, i.verursacht, new_row_id] for i in ergebnis.lrpegel_set]
    rejected_set = [[i.time.isoformat(), i.grund, random_messpunkt_id, new_row_id] for i in ergebnis.rejected_set]
    detected_set = [[i.time.isoformat(), i.duration, i.messpunkt, 1, new_row_id] for i in ergebnis.detected_set]
    richtungsgewertetertaktmaximalpegel_set = [[i.time.isoformat(), i.pegel, i.verursacht_durch, new_row_id] for i in ergebnis.richtungsgewertetertaktmaximalpegel_set]
    fremdgeraeusche_mittelungspegel_set =  [[i.time.isoformat(), i.pegel, i.verursacht_durch, new_row_id] for i in ergebnis.running_mean_fremdgeraeusche_list]
    fremdgeraeusche_lr_set =  [[i.time.isoformat(), i.pegel, i.verursacht_durch, new_row_id] for i in ergebnis.lr_fremdgeraeusche_list]
    messpunkt_lr_set = [[i.time.isoformat(), i.pegel, i.verursacht_durch, new_row_id] for i in ergebnis.lr_messpunkt_list]

    
    # logger.info("lr_arr", lr_arr)
    execute_values(cursor, """INSERT INTO dauerauswertung_beurteilungspegelbaulaerm (time, pegel, immissionsort_id, laermursache_id, berechnet_von_id) VALUES %s""", lr_arr)
    execute_values(cursor, """INSERT INTO dauerauswertung_rejected (time, filter_id, messpunkt_id, berechnet_von_baulaerm_id) VALUES %s""", rejected_set)
    execute_values(cursor, """INSERT INTO dauerauswertung_richtungungsgewertetertaktmaximalpegel (time, pegel, messpunkt_id, berechnet_von_id) VALUES %s""", richtungsgewertetertaktmaximalpegel_set)
    execute_values(cursor, """INSERT INTO dauerauswertung_baustellenumgebungspegel (time, pegel, messpunkt_id, berechnet_von_id) VALUES %s""", fremdgeraeusche_mittelungspegel_set)
    execute_values(cursor, """INSERT INTO dauerauswertung_baustellenbeurteilungspegelumgebung (time, pegel, messpunkt_id, berechnet_von_id) VALUES %s""", fremdgeraeusche_lr_set)
    execute_values(cursor, """INSERT INTO dauerauswertung_baustellenbeurteilungspegel (time, pegel, messpunkt_id, berechnet_von_id) VALUES %s""", messpunkt_lr_set)
    # execute_values(cursor, """INSERT INTO tsdb_detected ( time, dauer, messpunkt_id, typ_id, berechnet_von_id) VALUES %s""", detected_set)
    # execute_values(cursor, """INSERT INTO tsdb_maxpegel ( time, pegel, immissionsort_id, berechnet_von_id) VALUES  %s""", maxpegel_set)
    # execute_values(cursor, """INSERT INTO tsdb_schallleistungpegel (time, pegel, messpunkt_id, berechnet_von_id) VALUES %s""", schallleistungspegel_set)


def insert_new_auswertungslauf(cursor, from_date, to_date, ergebnis: Ergebnisse):

    time = from_date
    q = f"""
        INSERT INTO {app_name}_Auswertungslauf 
        (id, zeitpunkt_im_beurteilungszeitraum, zeitpunkt_durchfuehrung, verhandene_messwerte, verwertebare_messwerte, in_berechnung_gewertete_messwerte, zuordnung_id) 
        VALUES 
        ('{uuid.uuid4()}', '{ergebnis.zeitpunkt_im_beurteilungszeitraum.astimezone()}', '{datetime.now().astimezone()}', {ergebnis.verhandene_messwerte}, {ergebnis.verwertebare_messwerte}, {ergebnis.in_berechnung_gewertete_messwerte}, '{ergebnis.zuordnung}'::uuid) 
        RETURNING id;
        """
    cursor.execute(q)
    new_row_id = cursor.fetchone()
    lr_arr = [[i.time.isoformat(), i.pegel, i.immissionsort, i.verursacht, new_row_id] for i in ergebnis.lrpegel_set]
    rejected_set = [[i.time.isoformat(), i.grund, i.messpunkt, new_row_id] for i in ergebnis.rejected_set]
    detected_set = [[i.time.isoformat(), i.duration, i.messpunkt, uuid.UUID("4645f74d-55ff-4ff8-a781-75a55e5a3230"), new_row_id] for i in ergebnis.detected_set]
    maxpegel_set = [[i.time.isoformat(), i.pegel, i.immissionsort, new_row_id] for i in ergebnis.maxpegel_set]
    schallleistungspegel_set = [[i.time.isoformat(), i.pegel, i.messpunkt, new_row_id] for i in ergebnis.schallleistungspegel_set]

    
    execute_values(cursor, f"""INSERT INTO {app_name}_lrpegel (time, pegel, immissionsort_id, verursacht_id, berechnet_von_id) VALUES %s""", lr_arr)
    execute_values(cursor, f"""INSERT INTO {app_name}_rejected (time, filter_id, messpunkt_id, berechnet_von_id) VALUES %s""", rejected_set)
    execute_values(cursor, f"""INSERT INTO {app_name}_detected ( time, dauer, messpunkt_id, typ_id, berechnet_von_id) VALUES %s""", detected_set)
    execute_values(cursor, f"""INSERT INTO {app_name}_maxpegel ( time, pegel, immissionsort_id, berechnet_von_id) VALUES  %s""", maxpegel_set)
    execute_values(cursor, f"""INSERT INTO {app_name}_schallleistungpegel (time, pegel, messpunkt_id, berechnet_von_id) VALUES %s""", schallleistungspegel_set)


def insert_auswertung_via_psycopg2(time: datetime, ergebnis: Ergebnisse):
    conn = psycopg2.connect(
        CS)
    cursor = conn.cursor()
    
    from_date, to_date = get_start_end_beurteilungszeitraum_from_datetime(time.astimezone())
    from_date, to_date = from_date.astimezone(), to_date.astimezone()
    delete_old_data_via_psycopg2(cursor, get_id_old_auswertungslauf(cursor, ergebnis.zuordnung, from_date, to_date), from_date, to_date)
    insert_new_auswertungslauf(cursor, from_date, to_date, ergebnis)
    conn.commit()
    

def insert_baulaermauswertung_via_psycopg2(time, ergebnis: ErgebnisseBaulaerm):
    conn = psycopg2.connect(
        CS)
    cursor = conn.cursor()
    
    from_date, to_date = get_beurteilungszeitraum_zeitfenster(time)

    delete_old_baulaerm_data_via_psycopg2(cursor, get_id_old_baulaerm_auswertungslauf(cursor, ergebnis.zuordnung, from_date, to_date), from_date, to_date)
    insert_new_baulaerm_auswertung(cursor, from_date, to_date, ergebnis)
    conn.commit()


if __name__ == "__main__":
    FORMAT = '%(filename)s %(lineno)d %(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
    level=logging.INFO, format=FORMAT, handlers=[
        #logging.FileHandler("eval.log"),
        logging.StreamHandler(sys.stdout)
        ]
    )
    url = "http://localhost:8000/tsdb/auswertungslauf/"
    time = datetime(2021, 6, 1, 0, 0, 0)
    # insert_auswertung_via_psycopg2(time)
    results = ErgebnisseBaulaerm(time, datetime.now(), 3600, 3600, 3600, [DTO_Detected(time, 1, 1)], [DTO_LrPegel(time, 100, 1, 1)], [DTO_Rejected(time, 1, 1)] , [DTO_TaktmaximalpegelRichtungsgewertet(time, 50, 1)], 3)
    insert_baulaermauswertung_via_psycopg2(time, results)
    