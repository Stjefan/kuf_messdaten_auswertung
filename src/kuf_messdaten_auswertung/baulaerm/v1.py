def foo():
    print(54)
    return 10
    
def bar():
    return 10

import logging
import random
import sys
from typing import List
from uuid import UUID
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from ..models import Messpunkt, Immissionsort, EinstellungenRichtungsdaten
from ..messdaten.azure import MessdatenServiceV3
from ..db_insert import insert_baulaermauswertung_via_psycopg2, ErgebnisseBaulaerm, DTO_Detected, DTO_LrPegel, DTO_Rejected, DTO_TaktmaximalpegelRichtungsgewertet

from ..constants import terzfrequenzen, indexed_cols_directions_vertical

from .shared import get_beurteilungszeitraum_zeitfenster, get_project_data

def simple_direction_weighting(mps: List[Messpunkt], df_resu, df_terz, df_dir, gewertete_sektionen: List[range]):
    dataframes_resu = []
    weighted_terz = []
  
    for idx, mp in enumerate(mps):
        # weights = pd.Series([1 if c[0] < 30 else 0 for c in indexed_cols_directions_vertical], index=[f"T{mp.Id}_dr_vertical_section_{s[0]}" for s in indexed_cols_directions_vertical])
        weights = pd.Series(gewertete_sektionen[idx], index=[f"T{mp.id}_dr_vertical_section_{s[0]}" for s in indexed_cols_directions_vertical])

        weights = weights # / weights.sum()
        print("weights", weights, weights.sum())
        r_cols_2_adjust = ["LAFeq", "LCFeq", "LAFmax"]
        t_cols_2_adjust = [f"LZeq{f}" for f in terzfrequenzen]
        all_cols = [f"R{mp.id}_{c}" for c in r_cols_2_adjust]

        rows_dir_vertical = df_dir.loc[:, [f"T{mp.id}_dr_vertical_section_{s[0]}" for s in indexed_cols_directions_vertical]] / 100
        # rows_dir_vertical.loc[:, [f"T{mp.Id}_dr_vertical_section_{s[0]}" for s in indexed_cols_directions_vertical]] = 1

        print("DIR", rows_dir_vertical, rows_dir_vertical.sum(axis=1))

        df_resu_energetic = 10**(0.1*df_resu)
        # df_resu_weighted = df_resu.loc[:, all_cols].mul(df_dir.loc[:, [f"T{mp.Id}_dr_vertical_section_{s}" for s in gewertete_sektionen]].sum(axis=1) / 100, axis=0)
        print("z1", rows_dir_vertical.iloc[0, :], weights, rows_dir_vertical.iloc[0, :].dot(weights))
        prod_b = rows_dir_vertical.dot(weights)
        print("prod_b", prod_b)
        df_resu_weighted = 10*np.log10(df_resu_energetic.loc[:, all_cols].mul(prod_b, axis=0))
        print("weighted result", df_resu_weighted)
        dataframes_resu.append(df_resu_weighted)
        

        all_cols =  [f"T{mp.id}_{c}" for c in t_cols_2_adjust]
        df_terz_weighted = df_terz.loc[:, all_cols].mul(df_dir.loc[:, [f"T{mp.id}_dr_vertical_section_{s}" for s in gewertete_sektionen[idx]]].sum(axis=1), axis=0)
        weighted_terz.append(df_terz_weighted)

    weighted_resu = pd.concat(dataframes_resu, axis=1, join="inner")
    weighted_terz = pd.concat(weighted_terz, axis=1, join="inner")
    return weighted_resu, weighted_terz
    if False:
        dfs = []
        for mp in mps:
            richtungsdaten = [f"T{mp.Id}_dr_horizontal_section_{r}" for r in gewertete_sektionen]
            pegelwerte_gewertet = df_dir.loc[:, richtungsdaten].mul(df_resu.loc[:, :], axis=0)
            dfs.append(pegelwerte_gewertet)
        result = pd.concat(dfs, axis=1, join="inner")
        print(result)

def max_pegel_filter_04_23(mps: List[Messpunkt], df_dir, gewertete_sektionen: List[range], id_filter_in_db = "341d5c57-86d2-4e4b-87c8-8331bddea966"):
    s1 = pd.Series(index=df_dir.index, dtype="int")
    s2 = pd.Series(index=df_dir.index, dtype="int")
    filter_result_df = pd.DataFrame(data={"filtered_by": s1, "messpunkt_id": s2})
    
    to_be_kept = pd.Series(data=True, index=df_dir.index, dtype="bool")
    for idx, mp in enumerate(mps):
        logging.info(gewertete_sektionen[idx])
        colsnames_horizontal_for_mp = []
        rename_arr = []
        prefix = "dr_horizontal"
        for k in range(0, 32):
            colsnames_horizontal_for_mp.append(f"""T{mp.id}_{prefix}_section_{k}""")
            rename_arr.append((f"""T{mp.id}_{prefix}_section_{k}""", k))
        cols_from_mp = df_dir[colsnames_horizontal_for_mp]
        renamed_cols_from_arr = cols_from_mp.rename(columns=dict(rename_arr))
        arg_max = renamed_cols_from_arr.idxmax(axis="columns")
        max_is_in_area = arg_max.isin(gewertete_sektionen[idx]) # (arg_max >= 5) & (arg_max <= 20 )
        to_be_kept.loc[max_is_in_area[max_is_in_area].index] = True
        filter_result_df.loc[max_is_in_area[~max_is_in_area].index, :] = [id_filter_in_db, mp.id_in_db]
           
    return to_be_kept, filter_result_df


def svantek_filter_05_23(mps: List[Messpunkt], df_dir, gewertete_sektionen: List[range], id_filter_in_db = "341d5c57-86d2-4e4b-87c8-8331bddea966"):
    s1 = pd.Series(index=df_dir.index, dtype="int")
    s2 = pd.Series(index=df_dir.index, dtype="int")
    filter_result_df = pd.DataFrame(data={"filtered_by": s1, "messpunkt_id": s2})
    
    to_be_kept = pd.Series(data=True, index=df_dir.index, dtype="bool")
    for idx, mp in enumerate(mps):
        logging.info(gewertete_sektionen[idx])
        colsnames_horizontal_for_mp = []
        rename_arr = []
        prefix = "dr_horizontal"
        for k in range(0, 32):
            colsnames_horizontal_for_mp.append(f"""T{mp.id}_{prefix}_section_{k}""")
            rename_arr.append((f"""T{mp.id}_{prefix}_section_{k}""", k))
        cols_from_mp = df_dir[colsnames_horizontal_for_mp]
        renamed_cols_from_arr = cols_from_mp.rename(columns=dict(rename_arr))
        arg_max = renamed_cols_from_arr.idxmax(axis="columns")
        max_is_in_area = arg_max.isin(gewertete_sektionen[idx]) # (arg_max >= 5) & (arg_max <= 20 )
        to_be_kept.loc[max_is_in_area[max_is_in_area].index] = True
        filter_result_df.loc[max_is_in_area[~max_is_in_area].index, :] = [id_filter_in_db, mp.id_in_db]
           
    return to_be_kept, filter_result_df

def erstelle_pegel_nach_laermursache(an_messpunkten_gemessene_werte: pd.DataFrame, mps: List[Messpunkt]):
    intermediate = {}
    for mp in mps:
        for e in mp.Ereignisse:
            intermediate[f"{mp.id}_{e}"] = an_messpunkten_gemessene_werte[f"R{mp.id}_LAFeq"]
    return pd.DataFrame(intermediate)


def erstelle_beurteilungsrelevante_pegel(pegel_nach_laermursache, mps: List[Messpunkt]):

    beurteilungsrelevante_taktmaximalpegel = pegel_nach_laermursache.resample(
        '5s').max()
    rename_dict = dict([(f"{mp.id}_{e}", f"{mp.id}_{e}_TAKT")
                       for mp in mps for e in mp.Ereignisse])
    # print(rename_dict)

    beurteilungsrelevante_taktmaximalpegel = beurteilungsrelevante_taktmaximalpegel.rename(
        columns=rename_dict)

    return beurteilungsrelevante_taktmaximalpegel


def erstelle_baulaerm_auswertung_an_io(zeitpunkt: datetime, io: Immissionsort, beurteilungsrelevante_pegel_von_messpunkten: pd.DataFrame, ausbreitungsfaktoren):
    beginn, ende = get_beurteilungszeitraum_zeitfenster(zeitpunkt)
    print(beginn, ende)
    print(ausbreitungsfaktoren)

    rechenwert_verwertbare_sekunden = (ende-beginn).total_seconds()

    print("rechenwert_verwertbare_sekunden", rechenwert_verwertbare_sekunden)

    laerm_nach_ursachen_an_io_df = beurteilungsrelevante_pegel_von_messpunkten - \
        ausbreitungsfaktoren  # pd.concat(cols_laerm_nach_ursachen_an_io, axis=1)
    print("laerm_nach_ursachen_an_io_df", laerm_nach_ursachen_an_io_df)
    dti3 = pd.date_range(beginn, ende, freq="5s", name="Timestamp")
    df3 = pd.DataFrame(index=dti3)

    df_filled_holes = df3.merge(10**(0.1*laerm_nach_ursachen_an_io_df) /
                                rechenwert_verwertbare_sekunden, how='left', left_index=True, right_index=True)
    df_filled_holes.fillna(0, inplace=True)

    print("Without empty", df_filled_holes)

    dti = pd.date_range(beginn + timedelta(seconds=0),
                        ende, freq="300s", name="Timestamp")
    df1 = pd.DataFrame(index=dti)

    cumsummed_gesamt = 10 * np.log10(df_filled_holes.sum(axis=1).cumsum())
    cumsummed_gesamt.name = "Gesamt"
    df_gesamt_lr = df1.merge(cumsummed_gesamt,
                             how='left', left_index=True, right_index=True)

    print("cumsummed_gesamt", cumsummed_gesamt)

    df_all = df1.merge(10 * np.log10(df_filled_holes.cumsum()),
                       how='left', left_index=True, right_index=True)

    print("df_all", df_all)
    result_df = pd.merge(df_all, df_gesamt_lr,
                         left_index=True, right_index=True)
    print("Result", result_df)

    return result_df


def random_filter(alle_messdaten: pd.DataFrame, id_unsepcfic_filter: str = "7289f26d-b470-45cd-910c-4a6bb8bae84f"):

    rand_arr = (np.random.rand(len(alle_messdaten)) - 0.0) >= 0
    result = alle_messdaten[rand_arr]
    print("Before:", len(alle_messdaten), "After", len(result))
    reason_for_dropping = pd.DataFrame(index=alle_messdaten.index)

    reason_for_dropping.loc[~rand_arr, ["filtered_by", "messpunkt"]] = (
        id_unsepcfic_filter, 0)
    print(reason_for_dropping)
    return result, reason_for_dropping


def random_erkennung(alle_messdaten: pd.DataFrame):

    rand_arr = (np.random.rand(len(alle_messdaten)) - 0.75) >= 0
    result = alle_messdaten[rand_arr]
    print("Before:", len(alle_messdaten), "After", len(result))
    return result


def erstelle_ergebnisse(from_date, arg_mps, arg_ios, laermkategorisierung_an_immissionsorten_reloaded, lr_result_dict, beurteilungsrelevante_taktmaximalpegel_nach_laermursache, rejection_df: pd.DataFrame, number_fully_available_seconds,
                        number_non_dropped_seconds, number_counted_seconds, project_id):
    taktmaximalpegel_list = []
    lr_pegel_list = []
    rejected_list = []
    print(rejection_df)
    for idx, val in rejection_df.iterrows():
        rejected_list.append(DTO_Rejected(idx, val["filtered_by"]))
    for io in arg_ios:

        beurteilungspegel_an_io = lr_result_dict[io.Id]

        for i in laermkategorisierung_an_immissionsorten_reloaded:
            for idx, val in beurteilungspegel_an_io[i["column_name_ergebnis"]].items():
                lr_pegel_list.append(DTO_LrPegel(
                    idx, val, i["id"], io.id_in_db))

        if False:
            for col in beurteilungspegel_an_io.columns:
                id_verursacher = 1
                io_id = 1
                for idx, val in beurteilungspegel_an_io[col].items():
                    lr_pegel_list.append(DTO_LrPegel(
                        idx, val, id_verursacher, io.id_in_db))
    if False:
        for idx, r in beurteilungsrelevante_taktmaximalpegel_nach_laermursache.iterrows():
            for mp, e in [(mp, f"{mp.Id}_{e}_TAKT") for mp in arg_mps for e in mp.Ereignisse]:
                pass
            # taktmaximalpegel_list.append(
            #     DTO_TaktmaximalpegelRichtungsgewertet(idx, r[e], mp.id_in_db))
    print("LrPegel", len(lr_pegel_list))
    results = ErgebnisseBaulaerm(
        from_date,
        datetime.now(),
        number_fully_available_seconds,
        number_non_dropped_seconds,
        number_counted_seconds,
        [],  # [DTO_Detected(from_date, 1, 1)],
        lr_pegel_list,
        rejected_list,  # [DTO_Rejected(from_date, 1, 1)] ,
        taktmaximalpegel_list,
        project_id
    )
    # print(results)
    insert_baulaermauswertung_via_psycopg2(from_date, results)



def merge_berechnungsdaten_aller_messpunkte(resu: pd.DataFrame, terz: pd.DataFrame, mete: pd.DataFrame, has_mete=False, has_terz=True):
    if has_terz:
        df_all_resu_all_terz = pd.merge(
            resu, terz, left_index=True, right_index=True)
        if has_mete:
            df_all_resu_all_terz_all_mete = pd.merge(
                df_all_resu_all_terz, mete, left_index=True, right_index=True)
            return df_all_resu_all_terz_all_mete
        else:
            return df_all_resu_all_terz
    else:
        df_all_resu = resu
        if has_mete:
            df_all_resu_all_mete = pd.merge(
                df_all_resu, mete, left_index=True, right_index=True)
            return df_all_resu_all_mete
        else:
            return df_all_resu


def erstelle_baulaerm_auswertung(arg_mps, arg_ios, laermkategorisierung_an_immissionsorten_reloaded, ausbreitungsfaktoren, zeitpunkt_im_zielreitraum, project_id):
    from_date, to_date = get_beurteilungszeitraum_zeitfenster(
        zeitpunkt_im_zielreitraum)

    m = MessdatenServiceV3(
        "postgresql://stjefan2:p057!Gres@kufi-postgres13.postgres.database.azure.com/foo3")
    # m = RandomMessdatenService()
    resu = m.get_resu_all_mps(arg_mps, from_date, to_date)

    terz = m.get_terz_all_mps(arg_mps, from_date, to_date)
    dirs = m.get_dir_all_mps(arg_mps, from_date, to_date)

    all_messdaten_joined = pd.merge(pd.merge(
        resu, terz, left_index=True, right_index=True), dirs, left_index=True, right_index=True)
    logging.info(f"all {all_messdaten_joined}")

    if True:
        # w_resu, w_terz = simple_direction_weighting(mps, resu, terz, dirs, [[1 for i in range(0, 5)] + [0 for i in range(0, 5)] + [1 for i in range(0, 10)] + [0 for i in range(0, 13)] for mp in mps])
        # print(resu, w_resu)
        w_terz, w_resu = terz, resu
        verfuegbare_messwerte = merge_berechnungsdaten_aller_messpunkte(
            w_resu, w_terz, None, False, True)

        filterergebnisse_gesamt = None

        messwerte_nach_filtern, filterergebnisse = random_filter(
            all_messdaten_joined)

        richtungsfilter_ergebnisse, filterergebnisse_max_pegel = max_pegel_filter_04_23(
            arg_mps, messwerte_nach_filtern, [range(mp.einstellungen_richtunsdaten.akzeptanz_untere_schranke, mp.einstellungen_richtunsdaten.akzeptanz_obere_schranke) for mp in arg_mps])

        messwerte_nach_filtern = messwerte_nach_filtern[richtungsfilter_ergebnisse]

        number_fully_available_seconds = len(verfuegbare_messwerte)
        number_non_dropped_seconds = len(messwerte_nach_filtern)
        number_counted_seconds = len(messwerte_nach_filtern)
        pegel_nach_laermursache = erstelle_pegel_nach_laermursache(
            messwerte_nach_filtern, arg_mps)
        beurteilungsrelevante_taktmaximalpegel_nach_laermursache = erstelle_beurteilungsrelevante_pegel(
            pegel_nach_laermursache, arg_mps)

        filterergebnisse_max_pegel = filterergebnisse_max_pegel.dropna()

        lr_dict = {}
        for io in arg_ios:
            beurteilungspegel_an_io = erstelle_baulaerm_auswertung_an_io(zeitpunkt_im_zielreitraum, io, beurteilungsrelevante_taktmaximalpegel_nach_laermursache, [
                                                                         ausbreitungsfaktoren[(io.Id, mp.Id)] for mp in arg_mps for k in mp.Ereignisse])
            lr_dict[io.Id] = beurteilungspegel_an_io

        erstelle_ergebnisse(
            zeitpunkt_im_zielreitraum,
            arg_mps,
            arg_ios,
            laermkategorisierung_an_immissionsorten_reloaded,
            lr_dict, beurteilungsrelevante_taktmaximalpegel_nach_laermursache,
            filterergebnisse_max_pegel,
            number_fully_available_seconds, number_non_dropped_seconds, number_counted_seconds,
            project_id
        )



def erstelle_auswertung_baulaerm_exported(from_date, richtungsdaten_settings = [(0,33), (10, 25), (0, 33), (0, 33)]):
    _mps, _ios, laermkategorisierung_an_ios, _ausbreitungsfaktoren = get_project_data(richtungsdaten_settings)
    erstelle_baulaerm_auswertung(_mps, _ios, laermkategorisierung_an_ios, _ausbreitungsfaktoren, from_date, "8d7e0d22-620c-45b4-ac38-25b63ddf79e0")




def bar():
    print("432")

if __name__ == "__main__":
    FORMAT = '%(filename)s %(lineno)d %(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        level=logging.DEBUG,
        format=FORMAT,
        handlers=[
            logging.StreamHandler(sys.stdout),
        ])

    ausgewerteter_zeitpunkt = datetime(2023, 4, 17, 23, 0, 0)
    _mps, _ios, laermkategorisierung_an_ios, _ausbreitungsfaktoren = get_project_data()
    erstelle_baulaerm_auswertung(_mps, _ios, laermkategorisierung_an_ios, _ausbreitungsfaktoren, ausgewerteter_zeitpunkt, "8d7e0d22-620c-45b4-ac38-25b63ddf79e0")
