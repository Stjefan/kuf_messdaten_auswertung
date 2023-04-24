from dataclasses import dataclass
import math
from time import timezone
from typing import List
import uuid
import pandas as pd
from datetime import datetime
import numpy as np
from kuf_messdaten_auswertung.constants import terzfrequenzen, cols_directions_horizontal, cols_directions_vertical, indexed_cols_directions_horizontal, indexed_cols_directions_vertical
from kuf_messdaten_auswertung.models import Messpunkt

import pytz

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import logging


import configparser
config = configparser.ConfigParser()

config.read('../../config.ini')

if True:
    @dataclass
    class MessdatenServiceV3:
        conn_string: str
        alchemyEngine: Engine = None
        


        
        def __post_init__(self):
            # Connect to PostgreSQL server
            self.alchemyEngine = create_engine(
            self.conn_string
            # 'postgresql://postgres:password@127.0.0.1:5432/tsdb'
            )
            self.dbConnection = self.alchemyEngine.connect()
        

        def get_direction_data_single(self, messpunkt: Messpunkt, from_date: datetime, to_date: datetime) -> pd.DataFrame:
            q = f"select {','.join(cols_directions_vertical)}, {','.join(cols_directions_horizontal)}, time from \"dauerauswertung_directiondr\" where messpunkt_id = '{messpunkt.id_in_db}'::uuid and time >= '{from_date.astimezone()}' and time < '{to_date.astimezone()}' ORDER BY TIME"
            print("Query", q)
            direction_df = pd.read_sql(
            q, self.dbConnection)

            mapping_cols_directions_vertical = dict([(i[1], f"T{messpunkt.Id}_dr_vertical_section_{i[0]}") for i in indexed_cols_directions_vertical])
            mapping_cols_directions_horizontal = dict([(i[1], f"T{messpunkt.Id}_dr_horizontal_section_{i[0]}") for i in indexed_cols_directions_horizontal])

            data_dict = {
                "time": "Timestamp",
                **mapping_cols_directions_vertical,
                **mapping_cols_directions_horizontal,
            }

            

            
            direction_df.rename(columns=data_dict, inplace=True)
            # print(resu_df["Timestamp"].iloc[0].tzinfo)
            direction_df['Timestamp'] = direction_df['Timestamp'].dt.tz_convert('Europe/Berlin')
            # cet = pytz.timezone('CET').utcoffset()
            # resu_df['Timestamp'] = resu_df['Timestamp'] + cet
            direction_df['Timestamp'] = direction_df['Timestamp'].dt.tz_localize(None)
            direction_df.set_index("Timestamp", inplace=True)
            print(direction_df)
            logging.debug(direction_df)
            return direction_df

        def get_resudaten_single(self, messpunkt: Messpunkt, from_date: datetime, to_date: datetime) -> pd.DataFrame:
            q = f"select lafeq, lcfeq, lafmax, time from \"dauerauswertung_resu\" where messpunkt_id = '{messpunkt.id_in_db}'::uuid and time >= '{from_date.astimezone()}' and time < '{to_date.astimezone()}' ORDER BY TIME"
            print("Query", q)
            resu_df = pd.read_sql(
            q, self.dbConnection)

            
            data_dict = {
                "lafeq": f"R{messpunkt.Id}_LAFeq",
                "lafmax": f"R{messpunkt.Id}_LAFmax",
                "lcfeq": f"R{messpunkt.Id}_LCFeq",
                "time": "Timestamp"
            }
            resu_df.rename(columns=data_dict, inplace=True)
            # print(resu_df["Timestamp"].iloc[0].tzinfo)
            resu_df['Timestamp'] = resu_df['Timestamp'].dt.tz_convert('Europe/Berlin')
            # cet = pytz.timezone('CET').utcoffset()
            # resu_df['Timestamp'] = resu_df['Timestamp'] + cet
            resu_df['Timestamp'] = resu_df['Timestamp'].dt.tz_localize(None)
            resu_df.set_index("Timestamp", inplace=True)
            logging.debug(resu_df)
            return resu_df


        def get_resu_all_mps(self, ids_only: List[Messpunkt], from_date: datetime, to_date: datetime):

            df_mps = []
            for i in ids_only:
                df_mps.append(self.get_resudaten_single(i, from_date, to_date))
            print(df_mps)
            result = pd.concat(df_mps, axis=1, join="inner")
            return result

        def get_dir_all_mps(self, ids_only, from_date, to_date):

            df_mps = []
            print("get-dir-all-mps")
            for i in ids_only:
                df_mps.append(self.get_direction_data_single(i, from_date, to_date))
            result = pd.concat(df_mps, axis=1, join="inner")
            return result


        def get_terz_all_mps(self, ids_only: List[Messpunkt], from_date, to_date):

            df_mps = []
            for i in ids_only:
                df_mps.append(self.get_terzdaten_single(i, from_date, to_date))
            result = pd.concat(df_mps, axis=1, join="inner")
            return result

        def get_terzdaten_single(self,messpunkt: Messpunkt, from_date: datetime, to_date: datetime) -> pd.DataFrame:
            try:
                terz_prefix = "LZeq"
                available_cols_terz = []
                cols_terz = []
                values_terz = []
                cols_in_db = ["time"]
                for k in terzfrequenzen:
                    available_cols_terz.append(terz_prefix + k)
                    cols_in_db.append("hz" + k)
                    
                t_arr = [f"""T{messpunkt.Id}"""]  # "T2", "T3", "T4", "T5", "T6"]
                for i in available_cols_terz:
                    for j in t_arr:
                        cols_terz.append(f"{j}_{i}")
                q = f"select {','.join(cols_in_db)} from \"dauerauswertung_terz\" where messpunkt_id = '{messpunkt.id_in_db}'::uuid and time >= '{from_date.astimezone()}' and time < '{to_date.astimezone()}' ORDER BY TIME"
                logging.info(q)
                terz_df = pd.read_sql(q, self.dbConnection)



                terz_df.rename(columns={"hz20": f"T{messpunkt.Id}_LZeq20", "hz25": f"T{messpunkt.Id}_LZeq25", "hz31_5": f"T{messpunkt.Id}_LZeq31_5", "hz40": f"T{messpunkt.Id}_LZeq40", "hz50": f"T{messpunkt.Id}_LZeq50", "hz63": f"T{messpunkt.Id}_LZeq63", "hz80": f"T{messpunkt.Id}_LZeq80", "hz100": f"T{messpunkt.Id}_LZeq100", "hz125": f"T{messpunkt.Id}_LZeq125", "hz160": f"T{messpunkt.Id}_LZeq160",
                                        "hz200": f"T{messpunkt.Id}_LZeq200", "hz250": f"T{messpunkt.Id}_LZeq250", "hz315": f"T{messpunkt.Id}_LZeq315", "hz400": f"T{messpunkt.Id}_LZeq400", "hz500": f"T{messpunkt.Id}_LZeq500", "hz630": f"T{messpunkt.Id}_LZeq630", "hz800": f"T{messpunkt.Id}_LZeq800", "hz1000": f"T{messpunkt.Id}_LZeq1000", "hz1250": f"T{messpunkt.Id}_LZeq1250", "hz1600": f"T{messpunkt.Id}_LZeq1600",
                                        "hz2000": f"T{messpunkt.Id}_LZeq2000", "hz2500": f"T{messpunkt.Id}_LZeq2500", "hz3150": f"T{messpunkt.Id}_LZeq3150", "hz4000": f"T{messpunkt.Id}_LZeq4000", "hz5000": f"T{messpunkt.Id}_LZeq5000", "hz6300": f"T{messpunkt.Id}_LZeq6300", "hz8000": f"T{messpunkt.Id}_LZeq8000", "hz10000": f"T{messpunkt.Id}_LZeq10000", "hz12500": f"T{messpunkt.Id}_LZeq12500", "hz16000": f"T{messpunkt.Id}_LZeq16000",
                                        "hz20000": f"T{messpunkt.Id}_LZeq20000", "time": "Timestamp"}, inplace=True)
                
                # print(terz_df)
                terz_df['Timestamp'] = terz_df['Timestamp'].dt.tz_convert('Europe/Berlin')
                terz_df['Timestamp'] = terz_df['Timestamp'].dt.tz_localize(None)
                # cet = pytz.timezone('CET').utcoffset()
                # resu_df['Timestamp'] = resu_df['Timestamp'] + cet

                terz_df.set_index("Timestamp", inplace=True)
                return terz_df
            except Exception as e:
                logging.warning(f"MP {messpunkt.id_in_db} at {from_date.astimezone()} failed")
                logging.warning(e)
                raise e





        def get_metedaten(self, messpunkt: Messpunkt, from_date: datetime, to_date: datetime) -> pd.DataFrame:
            rename_dict = {
                "time": "Timestamp",
                "winddirection": "Windrichtung",
                "rain":"Regen",
                "windspeed": "MaxWindgeschwindigkeit",
                
            }
            mete_df = pd.read_sql(f"select time, rain, temperature, windspeed, pressure, humidity, winddirection from \"dauerauswertung_mete\" where messpunkt_id = '{messpunkt.id_in_db}'::uuid and time >= '{from_date.astimezone()}' and time < '{to_date.astimezone()}' ORDER BY TIME", self.dbConnection)

            mete_df.rename(columns=rename_dict, inplace=True)

            mete_df['Timestamp'] = mete_df['Timestamp'].dt.tz_convert('Europe/Berlin')
            mete_df['Timestamp'] = mete_df['Timestamp'].dt.tz_localize(None)
            mete_df.set_index("Timestamp", inplace=True)

            return mete_df








def assemble_dataframe(mps, df_resu, df_terz, df_mete, df_dir):
    usable_dfs = []
    if df_resu is not None:
        usable_dfs.append(df_resu)
    if df_terz is not None:
        usable_dfs.append(df_terz)
        print("")
    if df_mete is not None:
        usable_dfs.append(df_mete)
        print("")
    print(len(usable_dfs))
    assemled_dataframe = pd.concat(usable_dfs, axis=1, join="inner")
    return assemled_dataframe

if __name__ == "__main__":
    if True:
        cs = "postgresql://stjefan2:p057!Gres@kufi-postgres13.postgres.database.azure.com/foo3"
        from_date = datetime(2023, 4, 10, 0, 0, 0)
        to_date = datetime(2023, 4, 10, 10, 4, 0)
        mp = Messpunkt("1", id_in_db=uuid.UUID("16b2a784-8b6b-4b7e-9abf-fd2d5a8a0091"))
        m = MessdatenServiceV3(cs)
        df = m.get_direction_data_single(mp, from_date, to_date)
        print(df)
    if False:
        from_date = datetime(2023, 2, 1, 0, 0, 0)
        to_date = datetime(2023, 2, 1, 5, 0, 0)
        mp = Messpunkt("1", id_in_db=8)
        m = MessdatenServiceV3("postgresql://postgres:password@127.0.0.1:5432/dauerauswertung")
        df = m.get_direction_data_single(mp, from_date, to_date)
        print(df)
    if False:
        m = RandomMessdatenService()
        from_date = datetime(2022, 10, 2, 6, 0, 0)
        to_date = datetime(2022, 10, 2, 22, 0, 0)
        dirs = m.get_dir_all_mps(p.MPs, from_date, to_date)
        resus = m.get_resu_all_mps(p.MPs, from_date, to_date)
        terze = m.get_terz_all_mps(p.MPs, from_date, to_date)
        print(dirs)
        # print(resus)
        resu_w, terz_w = simple_direction_weighting(p.MPs, resus, terze, dirs)
        assembled = assemble_dataframe(p.MPs, resu_w, terz_w, None, dirs)
        print(assembled)
        # print(m.get_metedaten(2, from_date, to_date))
        # print()
    if False:
        m = MessdatenServiceV3()
        
        print(m.get_resudaten_single(7, from_date, to_date))
        print(m.get_resu_all_mps([1, 3, 5, 6], from_date, to_date))
        print(m.get_metedaten(2, from_date, to_date))
        # print(m.get_terzdaten_single(2, from_date, to_date))
        print(m.get_terz_all_mps([1, 3, 5, 6], from_date, to_date))