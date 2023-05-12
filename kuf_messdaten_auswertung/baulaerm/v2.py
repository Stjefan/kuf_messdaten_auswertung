from typing import List
from .shared import get_beurteilungszeitraum_zeitfenster
import logging
from ..messdaten.azure import MessdatenServiceV3
from ..models import Messpunkt, Immissionsort, EinstellungenRichtungsdaten, MesspunktBaulaerm, LaermkategorisierungMesspunkt, ImmissionsortBaulaerm

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from ..db_insert import insert_baulaermauswertung_via_psycopg2, ErgebnisseBaulaerm, DTO_Detected, DTO_LrPegel, DTO_Rejected, DTO_TaktmaximalpegelRichtungsgewertet
from uuid import UUID

def svantek_filter_05_23(mps: List[MesspunktBaulaerm], df_all_data: pd.DataFrame, id_filter_in_db = "341d5c57-86d2-4e4b-87c8-8331bddea966"):
    s1 = pd.Series(index=df_all_data.index, dtype="int")
    s2 = pd.Series(index=df_all_data.index, dtype="int")
    filter_result_df = pd.DataFrame(data={"filtered_by": s1, "messpunkt_id": s2})
    
    
    io_to_be_kept_dict = {}

    for idx, mp in enumerate(mps):
        # to_be_kept = pd.Series(data=True, index=df_all_data.index, dtype="bool")

        colsnames_horizontal_for_mp = [f"E{mp.id}_estimated_1", f"R{mp.id}_LAFeq"]


        
        cols_from_mp = df_all_data[colsnames_horizontal_for_mp]

        fremdgeraeusche_hoch = cols_from_mp[f"R{mp.id}_LAFeq"] - 3 >= cols_from_mp[f"E{mp.id}_estimated_1"]
        logging.info(f"Hohe Fremdgeräusche an MP {mp.id}")
        logging.info(fremdgeraeusche_hoch)

        # to_be_kept.loc[fremdgeraeusche_hoch[fremdgeraeusche_hoch].index] = True
        logging.info(fremdgeraeusche_hoch[fremdgeraeusche_hoch])
        io_to_be_kept_dict[mp.id] = fremdgeraeusche_hoch
        df_all_data.loc[fremdgeraeusche_hoch[fremdgeraeusche_hoch].index, f"R{mp.id}_LAFeq"] = 0

    filter_from_all_df = pd.DataFrame(io_to_be_kept_dict)
    print(filter_from_all_df)
    rows_2_be_kept = ~filter_from_all_df.all(axis=1)
    filterergebnisse = pd.DataFrame(index=df_all_data.index)

    filterergebnisse.loc[~rows_2_be_kept, ["filtered_by", "messpunkt"]] = (
        UUID("ed871682-e111-4cf5-9304-21068d2145cc"), 0)
    df_all_data = df_all_data[rows_2_be_kept]
    
    
    return filterergebnisse, df_all_data


def random_filter(alle_messdaten: pd.DataFrame, id_unsepcfic_filter: str = "7289f26d-b470-45cd-910c-4a6bb8bae84f"):

    rand_arr = (np.random.rand(len(alle_messdaten)) - 0.0) >= 0
    result = alle_messdaten[rand_arr]
    print("Before:", len(alle_messdaten), "After", len(result))
    reason_for_dropping = pd.DataFrame(index=alle_messdaten.index)

    reason_for_dropping.loc[~rand_arr, ["filtered_by", "messpunkt"]] = (
        id_unsepcfic_filter, 0)
    print(reason_for_dropping)
    return result, reason_for_dropping

def erstelle_pegel_nach_laermursache(an_messpunkten_gemessene_werte: pd.DataFrame, mps: List[MesspunktBaulaerm]):
    intermediate = {}
    for mp in mps:
        for e in mp.ereignisse:
            intermediate[f"{mp.id}_{e.name}"] = an_messpunkten_gemessene_werte[f"R{mp.id}_LAFeq"]
    return pd.DataFrame(intermediate)

def erstelle_beurteilungsrelevante_pegel(pegel_nach_laermursache, mps: List[MesspunktBaulaerm]):

    beurteilungsrelevante_taktmaximalpegel = pegel_nach_laermursache.resample(
        '5s').max()
    rename_dict = dict([(f"{mp.id}_{e.name}", f"{mp.id}_{e.name}_TAKT")
                       for mp in mps for e in mp.ereignisse])
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



def erstelle_ergebnisse(from_date, arg_mps: List[MesspunktBaulaerm], arg_ios: List[ImmissionsortBaulaerm], laermkategorisierung_an_immissionsorten_reloaded, lr_result_dict, beurteilungsrelevante_taktmaximalpegel_nach_laermursache, rejection_df: pd.DataFrame, number_fully_available_seconds,
                        number_non_dropped_seconds, number_counted_seconds, project_id):
    taktmaximalpegel_list = []
    lr_pegel_list = []
    rejected_list = []
    print(rejection_df)
    for idx, val in rejection_df.iterrows():
        rejected_list.append(DTO_Rejected(idx, val["filtered_by"]))
    for io in arg_ios:

        beurteilungspegel_an_io = lr_result_dict[io.id]

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
    if True:
        # print(beurteilungsrelevante_taktmaximalpegel_nach_laermursache)
        # for mp, e in [(mp, f"{mp.id}_{e.name}_TAKT") for mp in arg_mps for e in mp.ereignisse]:
            
        
        for mp, name, e in [(mp, f"{mp.id}_{e.name}_TAKT", e) for mp in arg_mps for e in mp.ereignisse]:
            print(e)
            logging.info("Verwertbare Taktmaximalpegel")
            logging.info(beurteilungsrelevante_taktmaximalpegel_nach_laermursache[beurteilungsrelevante_taktmaximalpegel_nach_laermursache[name] > 0.0][name] )
            for idx, r in beurteilungsrelevante_taktmaximalpegel_nach_laermursache.iterrows():
                
                taktmaximalpegel_list.append(
                    DTO_TaktmaximalpegelRichtungsgewertet(idx, r[name], e.id))
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

def erstelle_baulaerm_auswertung(
        arg_mps: List[MesspunktBaulaerm],
        arg_ios: List[ImmissionsortBaulaerm], laermkategorisierung_an_immissionsorten_reloaded, ausbreitungsfaktoren, zeitpunkt_im_zielreitraum, project_id):
    from_date, to_date = get_beurteilungszeitraum_zeitfenster(
        zeitpunkt_im_zielreitraum)

    m = MessdatenServiceV3(
        
        "postgresql://stjefan2:p057!Gres@kufi-postgres13.postgres.database.azure.com/foo3")
    svantek_estimated = m.get_richtungswertungsvantek_all_mps(arg_mps, from_date, to_date)
    resu = m.get_resu_all_mps(arg_mps, from_date, to_date)

    terz = m.get_terz_all_mps(arg_mps, from_date, to_date)
    

    all_messdaten_joined = pd.merge(pd.merge(
        resu, terz, left_index=True, right_index=True), svantek_estimated, left_index=True, right_index=True)
    logging.info(f"all before {all_messdaten_joined}")

    filterergebnisse, all_messdaten_nach_filtern = svantek_filter_05_23(arg_mps, all_messdaten_joined,)


    logging.info(f"all after {all_messdaten_nach_filtern}")

    

    #messwerte_nach_filtern, filterergebnisse = random_filter(
    #     all_messdaten_joined)

    number_fully_available_seconds = len(all_messdaten_joined)
    number_non_dropped_seconds = len(all_messdaten_nach_filtern)
    number_counted_seconds = len(all_messdaten_nach_filtern)
    pegel_nach_laermursache = erstelle_pegel_nach_laermursache(
        all_messdaten_nach_filtern, arg_mps)
    beurteilungsrelevante_taktmaximalpegel_nach_laermursache = erstelle_beurteilungsrelevante_pegel(
        pegel_nach_laermursache, arg_mps)


    lr_dict = {}
    for io in arg_ios:
        beurteilungspegel_an_io = erstelle_baulaerm_auswertung_an_io(zeitpunkt_im_zielreitraum, io, beurteilungsrelevante_taktmaximalpegel_nach_laermursache, [
                                                                        ausbreitungsfaktoren[(io.id, mp.id)] for mp in arg_mps for k in mp.ereignisse])
        lr_dict[io.id] = beurteilungspegel_an_io

    filterergebnisse.dropna(inplace=True)
    erstelle_ergebnisse(
        zeitpunkt_im_zielreitraum,
        arg_mps,
        arg_ios,
        laermkategorisierung_an_immissionsorten_reloaded,
        lr_dict, beurteilungsrelevante_taktmaximalpegel_nach_laermursache,
        filterergebnisse,
        number_fully_available_seconds, number_non_dropped_seconds, number_counted_seconds,
        project_id
    )

