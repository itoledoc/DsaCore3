#!/usr/bin/env python
import DsaDataBase3 as Data
import DsaAlgorithm3 as Dsa
import DsaScorers3 as DsaScore
import pandas as pd
import warnings
import datetime as dt
import time
import os

from sqlalchemy import create_engine
from astropy.utils.data import download_file
from astropy.utils import iers
from optparse import OptionParser

try:
    iers.IERS.iers_table = iers.IERS_A.open(
        download_file(iers.IERS_A_URL, cache=True))
except OSError:
    iers.IERS.iers_table = iers.IERS_A.open(
        download_file(iers.IERS_A_URL, cache=False))
warnings.simplefilter(action="ignore", category=RuntimeWarning)

engine = create_engine(
        'postgresql://dsacore:dsa2020@tableau.alma.cl:5432/dsa_data')

try:
    path = os.environ['APDM_PREFIX']
except KeyError:
    path = os.environ['HOME'] + '/.apdm_'


def simulate_day(
        data_instance, starttime, timelapse, arrayfamily, arrayid='last',
        configuration='', pwv=None, lgrades=("A", "B", "C")):

    endtime = starttime + dt.timedelta(timelapse / 24.)

    if not pwv:
        pwvf = pd.read_csv(
                'http://www.eso.org/gen-fac/pubs/astclim/forecast/gfs/'
                'APEX/forecast/text/gfs_pwv_for.txt', header=None,
                names=['date', 'hourStart', 'offset', 'pwv'],
                delim_whitespace=True)
    else:
        return None

    pwvf.index = pwvf.apply(
            lambda x:
            crea_timestring(x['date'], x['hourStart'], x['offset']),
            axis=1)

    pwvavai = pwvf.index[(pwvf.index > starttime) & (pwvf.index < endtime)]
    if len(pwvavai) == 0:
        return None

    time_sim = starttime
    first = True

    dsa = Dsa.DsaAlgorithm3(data_instance)
    dsa.set_time(starttime.strftime('%Y-%m-%d %H:%M'))  # YYYY-MM-DD HH:mm:SS
    dsa.write_ephem_coords()
    dsa.static_param()
    if arrayid == '' or arrayfamily != 'TWELVE-M':
        arrayid = None
    elif arrayid != '' and arrayfamily == 'TWELVE-M':
        dsa._query_array(arrayfamily)

    results = None

    while time_sim <= endtime:

        if time_sim < pwvf.index[0]:
            pwv_sim = float(pwvf.ix[0, 'pwv'])
        elif time_sim > pwvf.index[-1]:
            pwv_sim = float(pwvf.ix[-1, 'pwv'])

        else:
            pwv_newindex = pwvf.index | pd.Index([time_sim])
            pwvf_inter = pwvf.reindex(pwv_newindex).interpolate(method='time')
            pwv_sim = float(pwvf_inter.ix[time_sim, 'pwv'])

        if first:
            try:
                results = rundsa(
                        dsa, array_kind=arrayfamily,
                        array_id=arrayid,
                        conf=configuration, pwv=pwv_sim,
                        letterg=lgrades).head(1)
            except KeyError, e:
                print e
                print("No results?")
                results = None

            if results is None or len(results) == 0:
                time_sim += dt.timedelta(seconds=900)
                first = True
                print time_sim, pwv_sim
                dsa.set_time(time_sim.strftime('%Y-%m-%d %H:%M'))
                continue

            results['pwv'] = pwv_sim
            results['date_sim'] = time_sim
            est_time = (results.estimatedTime / results.EXECOUNT).values[0]
            results['sim_timelapse'] = est_time
            sb_uid = results.SB_UID.values[0]
            first = False

        else:
            try:
                r2 = rundsa(dsa, array_kind=arrayfamily, array_id=arrayid,
                            conf=configuration, pwv=pwv_sim, letterg=lgrades
                            ).head(1)
            except KeyError, e:
                print e
                print("No results?")
                r2 = None
            if r2 is None or len(r2) == 0:
                time_sim += dt.timedelta(seconds=900)
                print time_sim, pwv_sim
                dsa.set_time(time_sim.strftime('%Y-%m-%d %H:%M'))
                continue
            r2['pwv'] = pwv_sim
            r2['date_sim'] = time_sim
            est_time = (r2.estimatedTime / r2.EXECOUNT).values[0]
            r2['sim_timelapse'] = est_time
            sb_uid = r2.SB_UID.values[0]
            results = pd.concat([results, r2])

        dsa = add_observation(dsa, str(sb_uid), time_sim)
        if est_time > 2.5:
            est_time = 2.2
        time_sim += dt.timedelta(hours=est_time)
        dsa.set_time(time_sim.strftime('%Y-%m-%d %H:%M'))

    return results


def rundsa(dsa_instance,
           array_kind='TWELVE-M',
           array_id='',
           conf='',
           pwv=0.5,
           numant=0,
           letterg=("A", "B", "C"),
           update=False):

    if conf == '' or array_kind != 'TWELVE-M':
        conf = None
    else:
        conf = [conf]

    if numant == 0 or array_kind == 'TWELVE-M':
        numant = None

    dsa_instance.selector(
            array_kind=array_kind, minha=-3., maxha=3.,
            conf=conf, array_id=array_id, letterg=letterg,
            pwv=pwv, horizon=20, numant=numant,
            sb_status=("Ready", "Running"), sim=True)

    scorer = dsa_instance.master_dsa_df.apply(
        lambda x: DsaScore.calc_all_scores(
            pwv, x['maxPWVC'], x['Exec. Frac'], x['sbName'], x['array'],
            x['ARcor'], x['DEC'], x['array_ar_cond'], x['minAR'],
            x['maxAR'], x['Observed'], x['EXECOUNT'],
            x['PRJ_SCIENTIFIC_RANK'], x['DC_LETTER_GRADE'], x['CYCLE'],
            x['HA']), axis=1)

    fin = pd.merge(
            pd.merge(
                dsa_instance.master_dsa_df[
                    dsa_instance.selection_df.ix[:, 1:11].sum(axis=1) == 10],
                dsa_instance.selection_df, on='SB_UID'),
            scorer.reset_index(), on='SB_UID').set_index(
        'SB_UID', drop=False).sort_values(by='Score', ascending=0)

    return fin


def crea_timestring(date, hstart, offset):

    day = str(date)[0:4] + '-' + str(date)[4:6] + '-' + str(date)[6:8]
    timest = dt.datetime.strptime(day + ' 00:00', '%Y-%m-%d %H:%M')
    timest += dt.timedelta((hstart + offset) / 24.)

    return timest


def add_observation(dsa_instance, sb_uid, time_sim):

    try:
        dsa_instance.data.qastatus.ix[sb_uid, 'Pass'] += 1
        dsa_instance.data.qastatus.ix[sb_uid, 'Observed'] += 1
        dsa_instance.data.qastatus.ix[sb_uid, 'last_observed'] = time_sim
    except:
        print("New?")
        dsa_instance.data.qastatus.ix[sb_uid] = [
            1, 0, 0, 1, 0, time_sim, 'Pass', 'SUCCESS']

    return dsa_instance

if __name__ == '__main__':

    parser = OptionParser()
    parser.add_option('-s', '--start_time', type=str, default=None)
    parser.add_option('-a', '--array_family', type=str, default='TWELVE-M')
    parser.add_option('-t', '--timelapse', type=float, default=24.)
    parser.add_option('-c', '--conf', type=str, default='last')
    parser.add_option('--hp', action='store_true', dest='highprio',
                      default=False)

    opts, args = parser.parse_args()

    if opts.start_time:
        inittime = dt.datetime.strptime(opts.start_time, '%Y-%m-%d %H:%M')
    else:
        inittime = dt.datetime.utcnow()

    refr = False
    try:
        if time.time() - os.path.getmtime(path + '_onesim/') > 3600.:
            refr = True
    except OSError:
        os.mkdir(path + '_onesim/')
        refr = True

    try:
        datas = Data.DsaDatabase3(refresh_apdm=refr, path=path + '_onesim/',
                                  allc2=False, loadp1=False)
    except IOError:
        datas = Data.DsaDatabase3(path=path + '_onesim/',
                                  allc2=False, loadp1=False)

    conf = ''
    if opts.conf in ['C36-1', 'C36-2', 'C36-3', 'C36-4', 'C36-5',
                     'C36-6', 'C36-7', 'C36-8']:
        arrayid = ''
        conf = opts.conf
    else:
        arrayid = opts.conf

    grades = ("A", "B", "C")
    if opts.highprio:
        grades = ("A", "B")

    r = simulate_day(datas, inittime, opts.timelapse, opts.array_family,
                     arrayid=arrayid, configuration=conf, lgrades=grades)
    r.to_csv('simulation_result.csv', index=False)
