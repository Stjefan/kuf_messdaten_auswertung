from src.kuf_messdaten_auswertung.messdaten.azure import MessdatenServiceV3
from datetime import datetime
import uuid

from src.kuf_messdaten_auswertung.models import Messpunkt
cs = "postgresql://stjefan2:p057!Gres@kufi-postgres13.postgres.database.azure.com/foo3"
from_date = datetime(2022, 1, 21, 0, 0, 0)
to_date = datetime(2022, 1, 21, 10, 4, 0)
mp = Messpunkt("1", id_in_db=uuid.UUID(
    #"16b2a784-8b6b-4b7e-9abf-fd2d5a8a0091"
#"d0aa76cf-36e8-43d1-bb62-ff9cc2c275c0"

#"ab4e7e2d-8c39-48c2-b80c-b80f6b619657"
#"965157eb-ab17-496f-879a-55ce924f6252"
"653b313f-83bc-4a49-b4d7-5a917fc54723" # Sindelfingen MP 1 Bau 34
))

"31b773b9-bc96-4363-9af4-38304894ceba" # Sindelfingen MP 2 Bau 46
"2577c507-fdd6-4de0-8ae5-125d125a4692" # Sindelfingen MP 3 Bau 7_4
"ae4d6672-f3be-469c-8ef2-a2302b47ac86" # Sindelfigen MP 4 Bau 50_12
"aefab2b8-1b09-48ad-b229-a4b7e805aa82" # Sindelfingen MP 5 Bau 17_4

mps_sindelfingen = [
    # Messpunkt("1", id_in_db=uuid.UUID("653b313f-83bc-4a49-b4d7-5a917fc54723"), bezeichnung_in_db="Sindelfingen MP 1 Bau 34"),
    Messpunkt("2", id_in_db=uuid.UUID("31b773b9-bc96-4363-9af4-38304894ceba"), bezeichnung_in_db="Sindelfingen MP 2 Bau 46"),
    Messpunkt("3", id_in_db=uuid.UUID("2577c507-fdd6-4de0-8ae5-125d125a4692"), bezeichnung_in_db="Sindelfingen MP 3 Bau 7_4"),
    Messpunkt("4", id_in_db=uuid.UUID("ae4d6672-f3be-469c-8ef2-a2302b47ac86"), bezeichnung_in_db="Sindelfigen MP 4 Bau 50_12"),
    Messpunkt("5", id_in_db=uuid.UUID("aefab2b8-1b09-48ad-b229-a4b7e805aa82"), bezeichnung_in_db="Sindelfingen MP 5 Bau 17_4")

]
mp_2_sindelfingen = Messpunkt("2", id_in_db=uuid.UUID("31b773b9-bc96-4363-9af4-38304894ceba"), bezeichnung_in_db="Sindelfingen MP 2 Bau 46") # Mit Mete
m = MessdatenServiceV3(cs)


df_2 = m.get_resu_all_mps(mps_sindelfingen, from_date, to_date)
print(df_2)

df_terz = m.get_terz_all_mps(mps_sindelfingen, from_date, to_date)
print(df_terz)

df_mete = m.get_metedaten(mp_2_sindelfingen, from_date, to_date)
print(df_mete)