#!/usr/bin/env python
import DsaDataBase3 as Data
import DsaAlgorithm3 as Dsa
import DsaScorers3 as DsaScore
import pandas as pd

from twisted.web import xmlrpc, server
from astropy.utils.data import download_file
from astropy.utils import iers


class DSACoreService(xmlrpc.XMLRPC):
    """
    A service to start as XML RPC interface to run the DSA Core
    """

    def __init__(self):
        xmlrpc.XMLRPC.__init__(self)

        iers.IERS.iers_table = iers.IERS_A.open(
            download_file(iers.IERS_A_URL, cache=True))
        self.data = Data.DsaDatabase3(refresh_apdm=True, allc2=False, loadp1=False)

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
                   timestring=''):

        if conf == '' or array_kind != 'TWELVE-M':
            conf = None
        if array_id == '' or array_kind != 'TWELVE-M':
            array_id = None
        if numant == 0 or array_kind == 'TWELVE-M':
            numant = None

        self.data.update_status()
        dsa = Dsa.DsaAlgorithm3(self.data)

        if timestring != '':
            dsa.set_time(timestring)  # YYYY-MM-DD HH:mm:SS
        else:
            dsa.set_time_now()

        dsa.write_ephem_coords()
        dsa.static_param()
        dsa.selector(array_kind=array_kind, minha=minha, maxha=maxha,
                     conf=[conf], array_id=array_id,
                     pwv=0.5, horizon=horizon, numant=numant)

        scorer = dsa.master_dsa_df.apply(
            lambda x: DsaScore.calc_all_scores(
                pwv, x['maxPWVC'], x['Exec. Frac'], x['sbName'], x['array'], x['ARcor'],
                x['DEC'], x['array_ar_cond'], x['minAR'], x['maxAR'], x['Observed'],
                x['EXECOUNT'], x['PRJ_SCIENTIFIC_RANK'], x['DC_LETTER_GRADE'],
                x['CYCLE'], x['HA']), axis=1)

        fin = pd.merge(pd.merge(dsa.master_dsa_df, dsa.selection_df, on='SB_UID'),
                       scorer.reset_index(), on='SB_UID').set_index(
            'SB_UID', drop=False).sort('Score', ascending=0)

        return fin.to_json(orient='index')

    def xlmrpc_get_ar(self, array_id):

        dsa = Dsa.DsaAlgorithm3(self.data)
        dsa._query_array()
        a = dsa._get_bl_prop(array_id)
        return a[0]


if __name__ == '__main__':
    from twisted.internet import reactor
    r = DSACoreService()
    reactor.listenTCP(7080, server.Site(r))
    reactor.run()

