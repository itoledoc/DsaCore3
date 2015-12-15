#!/usr/bin/env python
import time
import os

import pandas as pd
import DsaDataBase3 as Data
import DsaAlgorithm3 as Dsa
import DsaScorers3 as WtoScor
from sqlalchemy import create_engine
from astropy.utils.data import download_file
from astropy.utils import iers
iers.IERS.iers_table = iers.IERS_A.open(
    download_file(iers.IERS_A_URL, cache=True))

engine = create_engine('postgresql://wto:wto2020@dmg02.sco.alma.cl:5432/aidadb')
refr = False
if time.time() - os.path.getmtime('/users/aod/.mwto/') > 3600.:
    refr = True
try:
    datas = Data.DsaDatabase3(refresh_apdm=refr, path='/users/aod/.mwto/',
                              allc2=False, loadp1=False)
except IOError:
    datas = Data.DsaDatabase3(path='/users/aod/.mwto/',
                              allc2=False, loadp1=False)

dsa = Dsa.DsaAlgorithm3(datas)

dsa.write_ephem_coords()
dsa.static_param()
pwv = pd.read_sql('pwv_data', engine).pwv.values[0]
dsa.selector(
    minha=-4., maxha=4., letterg=['A', 'B', 'C'],
    array_id='last', pwv=pwv)
dsa.selection_df['PWV now'] = pwv
dsa.selection_df['PWV now date'] = (
    pd.read_sql('pwv_data', engine).date.values[0] + ' ' +
    pd.read_sql('pwv_data', engine).time.values[0])
dsa.selection_df['date'] = str(dsa._ALMA_ephem.date)
dsa.selection_df['arrayname'] = dsa.arrays.iloc[0, 3]
scorer = dsa.master_dsa_df.apply(
    lambda x: WtoScor.calc_all_scores(
        pwv, x['maxPWVC'], x['Exec. Frac'], x['sbName'], x['array'], x['ARcor'],
        x['DEC'], x['array_ar_cond'], x['minAR'], x['maxAR'], x['Observed'],
        x['EXECOUNT'], x['PRJ_SCIENTIFIC_RANK'], x['DC_LETTER_GRADE'],
        x['CYCLE'], x['HA']), axis=1)

dsa.master_dsa_df['allconfs'] = dsa.obs_param.apply(
    lambda x: ','.join(
        [str(x['C36_1']), str(x['C36_2']), str(x['C36_3']), str(x['C36_4']),
         str(x['C36_5']), str(x['C36_7']), str(x['C36_8'])]), axis=1)

scorer.to_sql('scorer_wto_test', engine, index_label='SBUID',
              if_exists='replace', schema='wto')
print('scorer written')
dsa.inputs.to_sql('inputs_wto_text', engine, index_label='Cycle',
                  if_exists='replace', schema='wto')
print('inputs written')
dsa.selection_df.to_sql('selection_wto_test', engine, index_label='SBUID',
                        if_exists='replace', schema='wto')
print('selection written')
dsa.master_dsa_df.to_sql('master_wto_test', engine, index_label='SBUID',
                         if_exists='replace', schema='wto')
print('master written')
dsa.obs_param.to_sql('staticparam_wto_test', engine, index_label='SBUID',
                     if_exists='replace', schema='wto')
print('stat param written')
# C36-1 default

dsa.selector(
    minha=-4., maxha=4., letterg=['A', 'B', 'C'],
    conf='C36-1', pwv=pwv)
dsa.selection_df['PWV now'] = pwv
dsa.selection_df['PWV now date'] = (
    pd.read_sql('pwv_data', engine).date.values[0] + ' ' +
    pd.read_sql('pwv_data', engine).time.values[0])
dsa.selection_df['date'] = str(dsa._ALMA_ephem.date)
dsa.selection_df['arrayname'] = dsa.bl_arrays.iloc[0, 3]
scorer = dsa.master_dsa_df.apply(
    lambda x: WtoScor.calc_all_scores(
        pwv, x['maxPWVC'], x['Exec. Frac'], x['sbName'], x['array'], x['ARcor'],
        x['DEC'], x['array_ar_cond'], x['minAR'], x['maxAR'], x['Observed'],
        x['EXECOUNT'], x['PRJ_SCIENTIFIC_RANK'], x['DC_LETTER_GRADE'],
        x['CYCLE'], x['HA']), axis=1)

dsa.master_dsa_df['allconfs'] = dsa.obs_param.apply(
    lambda x: ','.join(
        [str(x['C36_1']), str(x['C36_2']), str(x['C36_3']), str(x['C36_4']),
         str(x['C36_5']), str(x['C36_7']), str(x['C36_8'])]), axis=1)

scorer.to_sql('scorer_wto_c361', engine, index_label='SBUID',
              if_exists='replace', schema='wto')
print('scorer written')
dsa.inputs.to_sql('inputs_wto_c361', engine, index_label='Cycle',
                  if_exists='replace', schema='wto')
print('inputs written')
dsa.selection_df.to_sql('selection_wto_c361', engine, index_label='SBUID',
                        if_exists='replace', schema='wto')
print('selection written')
dsa.master_dsa_df.to_sql('master_wto_c361', engine, index_label='SBUID',
                         if_exists='replace', schema='wto')
print('master written')
dsa.obs_param.to_sql('staticparam_wto_c361', engine, index_label='SBUID',
                     if_exists='replace', schema='wto')
print('stat param written')


datas._cursor.close()
datas._connection.close()
