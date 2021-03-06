#!/usr/bin/env python
import DsaDataBase3 as Data
import DsaAlgorithm3 as Dsa
import DsaScorers3 as DsaScore
import pandas as pd
import threading
import time
import logging
import copy

from twisted.web import xmlrpc, server
from astropy.utils.data import download_file
from astropy.utils import iers
from sqlalchemy import create_engine

logging.basicConfig(format='%(asctime)s %(message)s')
logging.getLogger().setLevel(logging.INFO)

engine = create_engine(
    'postgresql://dsacore:dsa2020@tableau.alma.cl:5432/dsa_data')


class RefreshThread(threading.Thread):

    def __init__(self, dsa_core_instance, sleep_time=3600):
        """
        :param dsa_core_instance: DSACoreService
        :return:
        """
        threading.Thread.__init__(self, name="APDM refreshing thread")
        self.__dsa = dsa_core_instance
        self.stop = False
        self.__wait_time = sleep_time

    def run(self):
        logging.info("Starting refreshing thread")
        time.sleep(self.__wait_time)
        while not self.stop:
            try:
                logging.info("Staring DsaDatabase refresh...")
                data = Data.DsaDatabase3(
                    refresh_apdm=True, loadp1=False)
                with self.__dsa.lock:
                    logging.info("Locking, data into dsa")
                    self.__dsa.data = data
                logging.info("Refresh done. Waiting " + str(self.__wait_time) +
                             "seconds until next refresh")
            except Exception as ex:
                logging.info("Problem refreshing the data. Cause: " + str(ex))
                logging.info("Waiting " + str(self.__wait_time) +
                             " seconds until next refresh")

            time.sleep(self.__wait_time)


class DSACoreService(xmlrpc.XMLRPC):
    """
    A service to start as XML RPC interface to run the DSA Core
    """

    def __init__(self):
        xmlrpc.XMLRPC.__init__(self)
        iers.IERS.iers_table = iers.IERS_A.open(
            download_file(iers.IERS_A_URL, cache=True))
        self.lock = threading.Lock()
        self.access_lock = threading.Lock()
        with self.lock:
            self.data = Data.DsaDatabase3(
                    refresh_apdm=True, loadp1=False)

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
        logging.info('Starting algorithm run (%s)' % array_kind)
        with self.lock:
            logging.info('Update status (%s)' % array_kind)
            datacopy = copy.deepcopy(self.data)
            datacopy.update_status()
            dsa = Dsa.DsaAlgorithm3(datacopy)

        logging.info('Completed Status update (%s)' % array_kind)

        if conf == '' or array_kind != 'TWELVE-M':
            conf = None
        else:
            conf = [conf]

        if array_id == '' or array_kind != 'TWELVE-M':
            array_id = None
        elif array_id != '' and array_kind == 'TWELVE-M':
            dsa._query_array(array_kind)

        if numant == 0 or array_kind == 'TWELVE-M':
            numant = None

        if timestring != '':
            dsa.set_time(timestring)  # YYYY-MM-DD HH:mm:SS
        else:
            dsa.set_time_now()

        dsa.write_ephem_coords()
        dsa.static_param()
        logging.info('Starting Selection (%s)' % array_kind)
        dsa.selector(array_kind=array_kind, minha=minha, maxha=maxha,
                     conf=conf, array_id=array_id,
                     pwv=pwv, horizon=horizon, numant=numant,
                     bands=bands)
        logging.info('Completed Selection (%s)' % array_kind)

        scorer = dsa.master_dsa_df.apply(
            lambda x: DsaScore.calc_all_scores(
                pwv, x['maxPWVC'], x['Exec. Frac'], x['sbName'], x['array'],
                x['ARcor'], x['DEC'], x['array_ar_cond'], x['minAR'],
                x['maxAR'], x['Observed'], x['EXECOUNT'],
                x['PRJ_SCIENTIFIC_RANK'], x['DC_LETTER_GRADE'], x['CYCLE'],
                x['HA']), axis=1)

        fin = pd.merge(
                pd.merge(
                    dsa.master_dsa_df[
                        dsa.selection_df.ix[:, 1:11].sum(axis=1) == 10],
                    dsa.selection_df, on='SB_UID'),
                scorer.reset_index(), on='SB_UID').set_index(
            'SB_UID', drop=False).sort_values(by='Score', ascending=0)

        return fin.to_json(orient='index')

    def xmlrpc_get_ar(self, array_id):
        """
        Returns the angular resolution and number of antennae
        :param array_id:
        :return:
        """

        logging.info('Calculating Array AR (%s)' % array_id)
        dsa = Dsa.DsaAlgorithm3(None)
        with self.lock:
            dsa._query_array()
            a = dsa._get_bl_prop(array_id)

        logging.info('Array AR (%s)' % array_id)
        return float(a[0]), int(a[2])

    def xmlrpc_get_arrays(self, array_kind):

        logging.info('Querying Arrays (%s)' % array_kind)
        dsa = Dsa.DsaAlgorithm3(None)
        with self.lock:
            dsa._query_array(array_kind=array_kind)
        if dsa.arrays is None:
            logging.info('No Arrays (%s)' % array_kind)
            return 'No Arrays'

        logging.info('Array List Created (%s)' % array_kind)
        return dsa.arrays.SE_ARRAYNAME.unique().tolist()

    def xmlrpc_update_data(self):
        with self.lock:
            self.data = Data.DsaDatabase3(
                    refresh_apdm=True, loadp1=False)

    def xlmrpc_update_apdm(self, obsproject_uid):
        with self.lock:
            self.data._update_apdm(obsproject_uid)

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

        self.lock.acquire()
        self.data.update_status()  # to be put on thread
        dsa = Dsa.DsaAlgorithm3(copy.deepcopy(self.data))
        self.lock.release()

        if conf == '' or array_kind != 'TWELVE-M':
            conf = None
        else:
            conf = [conf]

        if array_id == '' or array_kind != 'TWELVE-M':
            array_id = None
        elif array_id != '' and array_kind == 'TWELVE-M':
            dsa._query_array(array_kind)

        if numant == 0 or array_kind == 'TWELVE-M':
            numant = None

        if timestring != '':
            dsa.set_time(timestring)  # YYYY-MM-DD HH:mm:SS
        else:
            dsa.set_time_now()

        dsa.write_ephem_coords()
        dsa.static_param()
        dsa.selector(array_kind=array_kind, minha=minha, maxha=maxha,
                     conf=conf, array_id=array_id,
                     pwv=pwv, horizon=horizon, numant=numant,
                     bands=bands)

        scorer = dsa.master_dsa_df.apply(
            lambda x: DsaScore.calc_all_scores(
                pwv, x['maxPWVC'], x['Exec. Frac'], x['sbName'], x['array'],
                x['ARcor'], x['DEC'], x['array_ar_cond'], x['minAR'],
                x['maxAR'], x['Observed'], x['EXECOUNT'],
                x['PRJ_SCIENTIFIC_RANK'], x['DC_LETTER_GRADE'], x['CYCLE'],
                x['HA']), axis=1)

        fin = pd.merge(
                pd.merge(
                    dsa.master_dsa_df,
                    dsa.selection_df, on='SB_UID'),
                scorer.reset_index(), on='SB_UID').set_index(
            'SB_UID', drop=False).sort('Score', ascending=0)

        return fin.to_json(orient='index')

    def xmlrpc_get_pwv(self):

        pwvd = pd.read_sql(
            'SELECT * FROM pwv_data ORDER BY d_id DESC LIMIT 1', engine)
        pwv = 8.0
        try:
            pwv = pwvd.pwv.values[0]
            print pwvd.date.values[0] + ' ' + pwvd.time.values[0]
        except Exception as ex:
            print('Error handling the request to get pwv values. '
                  'Returning default')
            print ex
        return float(pwv)

if __name__ == '__main__':
    from twisted.internet import reactor
    r = DSACoreService()
    thread = RefreshThread(r)
    thread.daemon = True
    thread.start()
    reactor.listenTCP(7080, server.Site(r))
    reactor.run()

