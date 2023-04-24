from dataclasses import dataclass
import math
from time import timezone
from typing import List
import uuid
import pandas as pd
from datetime import datetime
import numpy as np
from kuf_messdaten_auswertung.constants import terzfrequenzen, cols_directions_horizontal, cols_directions_vertical, indexed_cols_directions_horizontal, indexed_cols_directions_vertical

from models import Messpunkt
@dataclass
class RandomMessdatenService:
    def get_resudaten_single(self, messpunkt: Messpunkt, from_date: datetime, to_date: datetime) -> pd.DataFrame:

        total_seconds = int((to_date - from_date).total_seconds() + 1)
        dti = pd.date_range(from_date, to_date,
            freq="s", name="Timestamp")
        data_dict = {
            f"R{messpunkt.Id}_LAFeq": 15*np.random.rand(total_seconds) + 60,
            f"R{messpunkt.Id}_LAFmax": 15*np.random.rand(total_seconds) + 80,
            f"R{messpunkt.Id}_LCFeq": 15*np.random.rand(total_seconds) + 100
                        }
        df = pd.DataFrame(data_dict, index=dti)
        print(df)
        return df


    def get_resu_all_mps(self, ids_only, from_date, to_date):

        df_mps = []
        for i in ids_only:
            df_mps.append(self.get_resudaten_single(i, from_date, to_date))
        result = pd.concat(df_mps, axis=1, join="inner")
        return result


    def get_terz_all_mps(self, ids_only, from_date, to_date):
        df_mps = []
        for i in ids_only:
            df_mps.append(self.get_terzdaten_single(i, from_date, to_date))
        result = pd.concat(df_mps, axis=1, join="inner")
        return result
        # return self.get_terzdaten(ids_only, from_date, to_date)


    def get_terzdaten_single(self, messpunkt: Messpunkt, from_date: datetime, to_date: datetime) -> pd.DataFrame:
        total_seconds = int((to_date - from_date).total_seconds()) + 1
        
        terz_prefix = "LZeq"
        available_cols_terz = []
        
        cols_terz = []
        values_terz = []
        for k in terzfrequenzen:
            available_cols_terz.append(terz_prefix + k)
        t_arr = [f"""T{messpunkt.Id}"""]  # "T2", "T3", "T4", "T5", "T6"]
        for i in available_cols_terz:
            for j in t_arr:
                cols_terz.append(f"{j}_{i}")
                values_terz.append(5 * np.random.rand(total_seconds) + 50)
        dti = pd.date_range(
            from_date, to_date,
            freq="s",
            name="Timestamp")
        data_dict = dict(zip(cols_terz, values_terz))
        df = pd.DataFrame(data_dict, index=dti)
        if False:
            df.rename(columns={"hz20": f"T{mp_id}_LZeq20", "hz25": f"T{mp_id}_LZeq25", "hz31_5": f"T{mp_id}_LZeq31_5", "hz40": f"T{mp_id}_LZeq40", "hz50": f"T{mp_id}_LZeq50", "hz63": f"T{mp_id}_LZeq63", "hz80": f"T{mp_id}_LZeq80", "hz100": f"T{mp_id}_LZeq100", "hz125": f"T{mp_id}_LZeq125", "hz160": f"T{mp_id}_LZeq160",
                                    "hz200": f"T{mp_id}_LZeq200", "hz250": f"T{mp_id}_LZeq250", "hz315": f"T{mp_id}_LZeq315", "hz400": f"T{mp_id}_LZeq400", "hz500": f"T{mp_id}_LZeq500", "hz630": f"T{mp_id}_LZeq630", "hz800": f"T{mp_id}_LZeq800", "hz1000": f"T{mp_id}_LZeq1000", "hz1250": f"T{mp_id}_LZeq1250", "hz1600": f"T{mp_id}_LZeq1600",
                                    "hz2000": f"T{mp_id}_LZeq2000", "hz2500": f"T{mp_id}_LZeq2500", "hz3150": f"T{mp_id}_LZeq3150", "hz4000": f"T{mp_id}_LZeq4000", "hz5000": f"T{mp_id}_LZeq5000", "hz6300": f"T{mp_id}_LZeq6300", "hz8000": f"T{mp_id}_LZeq8000", "hz10000": f"T{mp_id}_LZeq10000", "hz12500": f"T{mp_id}_LZeq12500", "hz16000": f"T{mp_id}_LZeq16000",
                                    "hz20000": f"T{mp_id}_LZeq20000"}, inplace=True)
        return df


    def get_direction_data_single(self, messpunkt: Messpunkt, from_date: datetime, to_date: datetime) -> pd.DataFrame:
        total_seconds = int((to_date - from_date).total_seconds()) + 1
        
        
        available_cols_dir = []
        
        cols_dir = []
        values_terz = []
        for k in range(0, 33):
            terz_prefix = "dr_horizontal"
            available_cols_dir.append(f"{terz_prefix}_section_{k}")
        for k in range(0, 16):
            terz_prefix = "dr_vertical"
            available_cols_dir.append(f"{terz_prefix}_section_{k}")
        t_arr = [f"""T{messpunkt.Id}"""]  # "T2", "T3", "T4", "T5", "T6"]
        for i in available_cols_dir:
            for j in t_arr:
                cols_dir.append(f"{j}_{i}")
                values_terz.append(np.random.rand(total_seconds))
        dti = pd.date_range(
            from_date, to_date,
            freq="s",
            name="Timestamp")
        data_dict = dict(zip(cols_dir, values_terz))
        df = pd.DataFrame(data_dict, index=dti)
        df = df.iloc[:,0:32].div(df.iloc[:, 0:32].sum(axis=1), axis=0)
        if False:
            print("A:", df, "B", df.sum(axis=1))
            
            print("C", df)
            print("D", df.sum(axis=1))
        if False:
            df.rename(columns={"hz20": f"T{mp_id}_LZeq20", "hz25": f"T{mp_id}_LZeq25", "hz31_5": f"T{mp_id}_LZeq31_5", "hz40": f"T{mp_id}_LZeq40", "hz50": f"T{mp_id}_LZeq50", "hz63": f"T{mp_id}_LZeq63", "hz80": f"T{mp_id}_LZeq80", "hz100": f"T{mp_id}_LZeq100", "hz125": f"T{mp_id}_LZeq125", "hz160": f"T{mp_id}_LZeq160",
                                    "hz200": f"T{mp_id}_LZeq200", "hz250": f"T{mp_id}_LZeq250", "hz315": f"T{mp_id}_LZeq315", "hz400": f"T{mp_id}_LZeq400", "hz500": f"T{mp_id}_LZeq500", "hz630": f"T{mp_id}_LZeq630", "hz800": f"T{mp_id}_LZeq800", "hz1000": f"T{mp_id}_LZeq1000", "hz1250": f"T{mp_id}_LZeq1250", "hz1600": f"T{mp_id}_LZeq1600",
                                    "hz2000": f"T{mp_id}_LZeq2000", "hz2500": f"T{mp_id}_LZeq2500", "hz3150": f"T{mp_id}_LZeq3150", "hz4000": f"T{mp_id}_LZeq4000", "hz5000": f"T{mp_id}_LZeq5000", "hz6300": f"T{mp_id}_LZeq6300", "hz8000": f"T{mp_id}_LZeq8000", "hz10000": f"T{mp_id}_LZeq10000", "hz12500": f"T{mp_id}_LZeq12500", "hz16000": f"T{mp_id}_LZeq16000",
                                    "hz20000": f"T{mp_id}_LZeq20000"}, inplace=True)
        return df

    def get_dir_all_mps(self, messpunkte, from_date, to_date):
        df_mps = []
        for i in messpunkte:
            df_mps.append(self.get_direction_data_single(i, from_date, to_date))
        result = pd.concat(df_mps, axis=1, join="inner")
        return result

    def read_mete_data_v1(from_date, to_date):
        return RandomMessdatenService.get_metedaten(from_date.year, from_date.month, from_date.day, from_date.hour*3600, (to_date.hour)*3600+to_date.minute*60+to_date.second)

    def get_metedaten(self, messpunkt_id: int, from_date: datetime, to_date: datetime) -> pd.DataFrame:
        cols_mete = ["Regen", "MaxWindgeschwindigkeit", "Windrichtung"]
        total_seconds = int((to_date - from_date).total_seconds() + 1)
        values_mete = [np.floor(np.random.rand(total_seconds) + 0.001), 2 * np.random.rand(total_seconds) + 1.65,
                        np.floor(360*np.random.rand(total_seconds))]
        dti = pd.date_range(
            from_date,
            to_date,
            freq="s", name="Timestamp")
        data_dict = dict(zip(cols_mete, values_mete))
        df = pd.DataFrame(data_dict, index=dti)

        return df
