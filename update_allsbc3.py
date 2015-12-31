#!/usr/bin/env python
import DsaDataBase3 as wto
import pandas as pd
import warnings

warnings.simplefilter(action = "ignore", category = RuntimeWarning)

datas = wto.DsaDatabase3(path='/users/aod/.cycle3pt/', loadp1=False)
allsbC3 = pd.merge(
    pd.merge(
        datas.projects.query('CYCLE not in ["2013.1", "2013.A"]'), 
        datas.schedblocks, on='OBSPROJECT_UID'), 
    datas.sb_status, 
    on='SB_UID', how='left', suffixes=["", "_SB"])

allsbC3 = allsbC3.query('SB_STATE not in ["Canceled", "Deleted"]')[
    ['CODE', 'OBSPROJECT_UID', 'sbName', 'SB_UID', 'band', 'repfreq', 
     'array', 'EXEC', 'RA', 'DEC', 'BestConf']]
allsbC3.sort('CODE').to_csv(
        '/data/Cycle3/daily-backup/allC3.sbinfo', sep='\t', index=False,
        header=False)
datas._cursor.close()
datas._connection.close()
