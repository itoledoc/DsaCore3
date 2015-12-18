#!/usr/bin/env python
import DsaDataBase3 as Data
import DsaAlgorithm3 as Dsa
import DsaScorers3 as DsaScore
import pandas as pd
import threading
import time
import datetime as dt

from twisted.web import xmlrpc, server
from astropy.utils.data import download_file
from astropy.utils import iers
from sqlalchemy import create_engine
engine = create_engine('postgresql://wto:wto2020@dmg02.sco.alma.cl:5432/aidadb')


class DSACoreService(xmlrpc.XMLRPC):
    """
    A service to start as XML RPC interface to run the DSA Core
    """

    def __init__(self):
        xmlrpc.XMLRPC.__init__(self)
        self.engine = create_engine(
                'postgresql://wto:wto2020@dmg02.sco.alma.cl:5432/aidadb')

        iers.IERS.iers_table = iers.IERS_A.open(
            download_file(iers.IERS_A_URL, cache=True))
        self.data = Data.DsaDatabase3(
                path='/home/itoledo/sim/',
                refresh_apdm=True, allc2=False, loadp1=False)
        self.dsa = Dsa.DsaAlgorithm3(self.data)

    def xmlrpc_run(self,
                   array_kind='TWELVE-M',
                   bands=('ALMA_RB_03', 'ALMA_RB_04', 'ALMA_RB_06',
                          'ALMA_RB_07', 'ALMA_RB_08', 'ALMA_RB_09',
                          'ALMA_RB_10'),
                   conf='',
                   cal_blratio=False,
                   numant=0,
                   array_id='',
                   horizon=20,
                   minha=-3.,
                   maxha=3.,
                   pwv=0.5,
                   timestring='',
                   update=False):

        if conf == '' or array_kind != 'TWELVE-M':
            conf = None
        else:
            conf = [conf]

        if array_id == '' or array_kind != 'TWELVE-M':
            array_id = None
        elif array_id != '' and array_kind == 'TWELVE-M':
            self.dsa._query_array(array_kind)

        if numant == 0 or array_kind == 'TWELVE-M':
            numant = None

        if timestring != '':
            self.dsa.set_time(timestring)  # YYYY-MM-DD HH:mm:SS
        else:
            self.dsa.set_time_now()

        if update:
            self.dsa.data.update_status()
            self.dsa.write_ephem_coords()
            self.dsa.static_param()

        self.dsa.selector(array_kind=array_kind, minha=minha, maxha=maxha,
                          conf=conf, array_id=array_id,
                          pwv=pwv, horizon=horizon, numant=numant,
                          bands=bands)

        scorer = self.dsa.master_dsa_df.apply(
            lambda x: DsaScore.calc_all_scores(
                pwv, x['maxPWVC'], x['Exec. Frac'], x['sbName'], x['array'], x['ARcor'],
                x['DEC'], x['array_ar_cond'], x['minAR'], x['maxAR'], x['Observed'],
                x['EXECOUNT'], x['PRJ_SCIENTIFIC_RANK'], x['DC_LETTER_GRADE'],
                x['CYCLE'], x['HA']), axis=1)

        fin = pd.merge(
                pd.merge(
                    self.dsa.master_dsa_df[
                        self.dsa.selection_df.ix[:, 1:11].sum(axis=1) == 10],
                    self.dsa.selection_df, on='SB_UID'),
                scorer.reset_index(), on='SB_UID').set_index(
            'SB_UID', drop=False).sort('Score', ascending=0)

        return fin.to_json(orient='index')

    def xmlrpc_get_ar(self, array_id):
        '''
        Only works for 12-m arrays
        :param array_id:
        :return:
        '''

        self.dsa._query_array()
        a = self.dsa._get_bl_prop(array_id)

        return float(a[0])

    def xmlrpc_add_observation(self, sbuid):
        # needs fixing to avoid lost of info
        self.dsa.data.sb_status.ix[sbuid, 'EXECOUNT'] -= 1
        if self.dsa.data.sb_status.ix[sbuid, 'EXECOUNT'] == 0:
            self.dsa.data.sb_status.ix[sbuid, 'SB_STATE'] = 'FullyObserved'
        return ''

    def xmlrpc_get_arrays(self, array_kind):

        self.dsa._query_array(array_kind=array_kind)
        if self.dsa.arrays is None:
            return 'No Arrays'

        return self.dsa.arrays.SE_ARRAYNAME.unique().tolist()

    def xmlrpc_run_full(self,
                   array_kind='TWELVE-M',
                   bands=('ALMA_RB_03', 'ALMA_RB_04', 'ALMA_RB_06',
                          'ALMA_RB_07', 'ALMA_RB_08', 'ALMA_RB_09',
                          'ALMA_RB_10'),
                   conf='',
                   cal_blratio=False,
                   numant=0,
                   array_id='',
                   horizon=20,
                   minha=-3.,
                   maxha=3.,
                   pwv=0.5,
                   timestring=''):

        if conf == '' or array_kind != 'TWELVE-M':
            conf = None
        else:
            conf = [conf]

        if array_id == '' or array_kind != 'TWELVE-M':
            array_id = None
        elif array_id != '' and array_kind == 'TWELVE-M':
            self.dsa._query_array(array_kind)

        if numant == 0 or array_kind == 'TWELVE-M':
            numant = None

        self.dsa.data.update_status()  # to be put on thread

        if timestring != '':
            self.dsa.set_time(timestring)  # YYYY-MM-DD HH:mm:SS
        else:
            self.dsa.set_time_now()

        self.dsa.write_ephem_coords()
        self.dsa.static_param()
        self.dsa.selector(array_kind=array_kind, minha=minha, maxha=maxha,
                     conf=conf, array_id=array_id,
                     pwv=pwv, horizon=horizon, numant=numant,
                     bands=bands)

        scorer = self.dsa.master_dsa_df.apply(
            lambda x: DsaScore.calc_all_scores(
                pwv, x['maxPWVC'], x['Exec. Frac'], x['sbName'], x['array'], x['ARcor'],
                x['DEC'], x['array_ar_cond'], x['minAR'], x['maxAR'], x['Observed'],
                x['EXECOUNT'], x['PRJ_SCIENTIFIC_RANK'], x['DC_LETTER_GRADE'],
                x['CYCLE'], x['HA']), axis=1)

        fin = pd.merge(
                pd.merge(
                    self.dsa.master_dsa_df,
                    self.dsa.selection_df, on='SB_UID'),
                scorer.reset_index(), on='SB_UID').set_index(
            'SB_UID', drop=False).sort('Score', ascending=0)

        return fin.to_json(orient='index')


if __name__ == '__main__':
    from twisted.internet import reactor
    r = DSACoreService()
    reactor.listenTCP(7081, server.Site(r))
    reactor.run()

