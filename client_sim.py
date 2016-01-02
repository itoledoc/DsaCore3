import xmlrpclib
import pandas as pd
from sqlalchemy import create_engine
import datetime as dt

pd.options.display.width = 200
pd.options.display.max_columns = 100

client = xmlrpclib.Server('http://localhost:7081/')
engine = create_engine('postgresql://wto:wto2020@dmg02.sco.alma.cl:5432/aidadb')


def simulate_day(
        inittime, timelapse, arrayfamily, arrayid='', configuration='',
        pwv=None):

    starttime = dt.datetime(
            inittime[0], inittime[1], inittime[2],
            inittime[3], inittime[4])
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
                r = rundsa(array_kind=arrayfamily, array_id=arrayid,
                           conf=configuration, pwv=pwv_sim,
                           timestring=time_sim.strftime('%Y-%m-%d %H:%M:%S'),
                           update=True).head(1)
            except:
                print("No results?")
                r = None

            if r is None:
                time_sim += dt.timedelta(seconds=900)
                first = True
                print time_sim, pwv_sim
                continue

            r['pwv'] = pwv_sim
            r['date_sim'] = time_sim
            est_time = (r.estimatedTime / r.EXECOUNT).values[0]
            r['sim_timelapse'] = est_time
            sb_uid = r.SB_UID.values[0]
            first = False

        else:
            try:
                r2 = rundsa(array_kind=arrayfamily, array_id=arrayid,
                            conf=configuration, pwv=pwv_sim,
                            timestring=time_sim.strftime('%Y-%m-%d %H:%M:%S')
                            ).head(1)
            except:
                print("No results?")
                r2 = None
            if r2 is None:
                time_sim += dt.timedelta(seconds=900)
                print time_sim, pwv_sim
                continue
            r2['pwv'] = pwv_sim
            r2['date_sim'] = time_sim
            est_time = (r2.estimatedTime / r2.EXECOUNT).values[0]
            r2['sim_timelapse'] = est_time
            sb_uid = r2.SB_UID.values[0]
            r = pd.concat([r, r2])

        client.add_observation(str(sb_uid))
        if est_time > 2.5:
            est_time = 2.2
        time_sim += dt.timedelta(hours=est_time)

    return r


def rundsa(
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

    r = client.run(array_kind, bands, conf, cal_blratio, numant, array_id,
                   horizon, minha, maxha, pwv, timestring, update)
    try:
        return pd.read_json(r, orient='index').sort_values(by='Score',
                                                           ascending=False)
    except KeyError:
        print len(r)
        print r
        return None


def rundsa_full(
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

    r = client.run_full(array_kind, bands, conf, cal_blratio, numant, array_id,
                        horizon, minha, maxha, pwv, timestring)

    return pd.read_json(r, orient='index').sort_values(by='Score',
                                                           ascending=False)


def get_arrays(array_kind):

    r = client.get_arrays(array_kind)

    return r


def get_ar(array_id):

    r = client.get_ar(array_id)

    return r


def update_data():

    client.update_data()


def update_apdm(obsprojectuid):

    client.update_apdm(obsprojectuid)


def get_pwv():

    return client.get_pwv()


def crea_timestring(date, hstart, offset):

    day = str(date)[0:4] + '-' + str(date)[4:6] + '-' + str(date)[6:8]
    timest = dt.datetime.strptime(day + ' 00:00', '%Y-%m-%d %H:%M')
    timest += dt.timedelta((hstart + offset) / 24.)

    return timest
