import pandas as pd
import ephem
import os
import DsaTools3 as DsaTool
import visibiltyTools as rUV
import datetime as dt
import cx_Oracle

from astropy import units as u
from astropy.coordinates import SkyCoord, EarthLocation, AltAz
from astropy.time import Time

# noinspection PyUnresolvedReferences
ALMA = EarthLocation(
    lat=-23.0262015*u.deg, lon=-67.7551257*u.deg, height=5060*u.m)
TIME = Time.now()
TIME.delta_ut1_utc = 0
TIME.location = ALMA

ALMA1 = ephem.Observer()
# noinspection PyUnresolvedReferences
ALMA1.lat = '-23.0262015'
# noinspection PyUnresolvedReferences
ALMA1.long = '-67.7551257'
# noinspection PyUnresolvedReferences
ALMA1.elev = 5060
# noinspection PyUnresolvedReferences
ALMA1.horizon = ephem.degrees(str('20'))

home = os.environ['HOME']

RECEIVER = {
    'g': {
        'ALMA_RB_03': 0.0,
        'ALMA_RB_04': 0.0,
        'ALMA_RB_06': 0.0,
        'ALMA_RB_07': 0.0,
        'ALMA_RB_08': 0.0,
        'ALMA_RB_09': 1.0,
        'ALMA_RB_10': 1.0},
    'trx': {
        'ALMA_RB_03': 45.0,
        'ALMA_RB_04': 51.0,
        'ALMA_RB_06': 55.0,
        'ALMA_RB_07': 75.0,
        'ALMA_RB_08': 150.0,
        'ALMA_RB_09': 110.0,
        'ALMA_RB_10': 230.0},
    'mintrans': {
        'ALMA_RB_03': 0.7,
        'ALMA_RB_04': 0.7,
        'ALMA_RB_06': 0.7,
        'ALMA_RB_07': 0.7,
        'ALMA_RB_08': 0.6,
        'ALMA_RB_09': 0.5,
        'ALMA_RB_10': 0.5}
}

CONF_LIM = {
    'minbase': {
        'C36-1': 14.7,
        'C36-2': 14.7,
        'C36-3': 14.7,
        'C36-4': 38.6,
        'C36-5': 47.9,
        'C36-6': 77.3,
        'C36-7': 248.3,
        'C36-8': 346.5,
        'C40-1': 15.0,
        'C40-2': 15.0,
        'C40-3': 15.0,
        'C40-4': 15.0,
        'C40-5': 16.6,
        'C40-6': 15.3,
        'C40-7': 80.7,
        'C40-8': 167.0,
        'C40-9': 267.3
    },
    'maxbase': {
        'C36-1': 160.7,
        'C36-2': 376.9,
        'C36-3': 538.9,
        'C36-4': 969.4,
        'C36-5': 1396.4,
        'C36-6': 2299.6,
        'C36-7': 6074.2,
        'C36-8': 9743.7,
        'C40-1': 155.6,
        'C40-2': 272.6,
        'C40-3': 460.0,
        'C40-4': 704.1,
        'C40-5': 1124.3,
        'C40-6': 1813.2,
        'C40-7': 3697.0,
        'C40-8': 6855.2,
        'C40-9': 12645.0
    }
}

CONFRES = {
    'C36-1': 3.35,
    'C36-2': 1.8,
    'C36-3': 1.22,
    'C36-4': 0.7,
    'C36-5': 0.49,
    'C36-6': 0.3,
    'C36-7': 0.12,
    'C36-8': 0.075,
    'C40-1': 3.38,
    'C40-2': 2.41,
    'C40-3': 1.47,
    'C40-4': 0.93,
    'C40-5': 0.53,
    'C40-6': 0.34,
    'C40-7': 0.197,
    'C40-8': 0.115,
    'C40-9': 0.0651}

CONFLAS = {
    'C36-1': 25.31,
    'C36-2': 24.93,
    'C36-3': 22.84,
    'C36-4': 9.42,
    'C36-5': 16.85,
    'C36-6': 3.72,
    'C36-7': 1.78,
    'C36-8': 1.07,
    'C40-1': 24.76,
    'C40-2': 24.76,
    'C40-3': 24.71,
    'C40-4': 24.71,
    'C40-5': 22.33,
    'C40-6': 24.28,
    'C40-7': 4.59,
    'C40-8': 2.22,
    'C40-9': 1.39
}

CYC_NA = {'2012.A': 34,
          '2012.1': 34,
          '2013.A': 34,
          '2013.1': 34,
          '2015.1': 36,
          '2015.A': 36}


# noinspection PyAttributeOutsideInit,PyProtectedMember
class DsaAlgorithm3(object):

    def __init__(self, data):

        self._dsa_path = os.environ['DSA']
        self._conx_str = os.environ['CON_STR']
        if data is not None:
            self.data = data
            self.tau = pd.read_csv(
                self._dsa_path + 'conf/tau.csv',
                sep=',', header=0).set_index('freq')
            self.tsky = pd.read_csv(
                self._dsa_path + 'conf/tskyR.csv', sep=',',
                header=0).set_index(
                    'freq')
            self.pwvdata = pd.read_pickle(
                self._dsa_path + 'conf/pwvdata2.pandas')
            self.schedblocks = self.data.schedblocks.copy()
        else:
            mode = 'Just for some tools...'

        self._pwv = None
        self._array_res = []
        self._date = ephem.now()
        self._availableobs = False
        self._time_astropy = TIME
        self._ALMA_ephem = ALMA1
        self._static_calculated = False

    def set_time_now(self):
        
        """

        """
        self._time_astropy = Time.now()
        self._time_astropy.delta_ut1_utc = 0
        self._time_astropy.location = ALMA
        self._ALMA_ephem.date = ephem.now()

    def set_time(self, time_str):

        """

        Args:
            time_str: 
        """
        self._time_astropy = Time(time_str)
        self._time_astropy.delta_ut1_utc = 0
        self._time_astropy.location = ALMA
        self._ALMA_ephem.date = ephem.date(self._time_astropy.iso)

    def write_ephem_coords(self):

        """

        """
        self.schedblocks['ephem'] = 'N/A'

        ephem_sb = pd.merge(
            self.schedblocks,
            self.data.target_tables.query(
                'solarSystem != "Unspecified" and isQuery == False and '
                'RA == 0'),
            on='SB_UID').drop_duplicates(['SB_UID', 'ephemeris']).set_index(
            'SB_UID', drop=False)

        results = ephem_sb.apply(
            lambda x: DsaTool.calc_ephem_coords(
                x['solarSystem'], x['ephemeris'], x['SB_UID'],
                alma=self._ALMA_ephem),
            axis=1)

        for r in results.iteritems():
            self.schedblocks.ix[r[0], 'RA'] = r[1][0]
            self.schedblocks.ix[r[0], 'DEC'] = r[1][1]
            self.schedblocks.ix[r[0], 'ephem'] = r[1][2]

    def static_param(self, horizon=20):

        """

        Args:
            horizon: 
        """
        if self._static_calculated:
            idx = self.data.target_tables.query(
                'solarSystem != "Unspecified" and isQuery == False and '
                'RA == 0').SB_UID.unique()

            self.obs_param.ix[idx] = self.schedblocks.ix[idx].apply(
                lambda r: DsaTool.observable(
                    r['RA'], r['DEC'], self._ALMA_ephem, r['RA'], r['minAR'],
                    r['maxAR'], r['array'], r['SB_UID'], horizon=horizon),
                axis=1
            )

        else:
            self.obs_param = self.schedblocks.apply(
                lambda r: DsaTool.observable(
                    r['RA'], r['DEC'], self._ALMA_ephem, r['RA'], r['minAR'],
                    r['maxAR'], r['array'], r['SB_UID'], horizon=horizon),
                axis=1
            )

            ind1 = pd.np.around(self.schedblocks.repfreq, decimals=1)
            ind2 = self.schedblocks.apply(
                lambda x: str(
                    int(x['maxPWVC'] / 0.05) * 0.05 +
                    (0.05 if (x['maxPWVC'] % 0.05) > 0.02 else 0.)) if
                x['maxPWVC'] < 8 else '7.95',
                axis=1)

            self.schedblocks['transmission_ot'] = self.pwvdata.lookup(
                ind1, ind2)
            self.schedblocks['tau_ot'] = self.tau.lookup(ind1, ind2)
            self.schedblocks['tsky_ot'] = self.tsky.lookup(ind1, ind2)
            self.schedblocks['airmass_ot'] = self.schedblocks.apply(
                lambda x: calc_airmass(x['DEC'], transit=True), axis=1)
            self.schedblocks['tsys_ot'] = (
                self.schedblocks.apply(
                    lambda x: calc_tsys(x['band'], x['tsky_ot'], x['tau_ot'],
                                        x['airmass_ot']), axis=1))

        self.obs_param.rise.fillna(0, inplace=True)
        self.obs_param['rise datetime'] = self.obs_param.apply(
            lambda x:
            dt.datetime.strptime(
                '2015-01-01 ' + str(int(x['rise'])) + ':' +
                str(int(60*(x['rise'] - int(x['rise'])))),
                '%Y-%m-%d %H:%M'),
            axis=1)

        self._static_calculated = True

    def update_apdm(self, obsproject_uid):

        """

        Args:
            obsproject_uid: 
        """
        self.data._update_apdm(obsproject_uid)
        self.schedblocks = self.data.schedblocks.copy()
        self._static_calculated = False

    def selector(self,
                 array_kind='TWELVE-M',
                 prj_status=("Ready", "InProgress"),
                 sb_status=("Ready", "Suspended", "Running", "CalibratorCheck",
                            "Waiting"),
                 letterg=("A", "B", "C"),
                 bands=("ALMA_RB_03", "ALMA_RB_04", "ALMA_RB_06", "ALMA_RB_07",
                        "ALMA_RB_08", "ALMA_RB_09", "ALMA_RB_10"),
                 check_count=True,
                 conf=None,
                 calc_blratio=False,
                 numant=None,
                 array_id=None,
                 horizon=20.,
                 minha=-3.,
                 maxha=3.,
                 pwv=0.,
                 sim=False):

        """

        Args:
            array_kind: 
            prj_status: 
            sb_status: 
            letterg:
            bands: 
            check_count: 
            conf: 
            calc_blratio: 
            numant: 
            array_id: 
            horizon: 
            minha: 
            maxha: 
            pwv: 
            sim: 

        """
        if float(pwv) > 8:
            pwv = 8.0
        # print self._time_astropy

        if sim:
            self._aggregate_dfs(sim=True)
        else:
            self._aggregate_dfs()

        self.master_dsa_df['array'] = self.master_dsa_df.apply(
            lambda x: 'SEVEN-M' if x['array'] == "ACA" else
            x['array'], axis=1
        )
        self.master_dsa_df['isToo'] = self.master_dsa_df.apply(
            lambda x: True if str(x['CODE']).endswith('.T') else
            False, axis=1
        )

        self.selection_df = self.master_dsa_df[['SB_UID']].copy()

        # select array kind

        self.selection_df['selArray'] = (
            self.master_dsa_df['array'] == array_kind)

        # select valid Prj States
        self.selection_df['selPrjState'] = (
            self.master_dsa_df.apply(
                lambda x: True if x['PRJ_STATUS'] in prj_status else False,
                axis=1))

        # select valid SB States
        self.selection_df['selSBState'] = (
            self.master_dsa_df.apply(
                lambda x: True if x['SB_STATE'] in sb_status else False,
                axis=1))

        # select By grades

        sbuid_ex = self.exception_df.SB_UID.unique()

        self.selection_df['selGrade'] = (
            self.master_dsa_df.apply(
                lambda x: True if
                (x['CYCLE'] in
                 ["2015.1", "2015.A"] and x['DC_LETTER_GRADE'] in letterg) or
                (x['CYCLE'] not in
                 ["2015.1", "2015.A"] and x['DC_LETTER_GRADE'] == "A") or
                (x['SB_UID'] in sbuid_ex) else
                False, axis=1)
        )

        # select by band
        self.selection_df['selBand'] = (
            self.master_dsa_df.apply(
                lambda x: True if x['band'] in bands else False,
                axis=1
            )
        )

        if not sim:
            self.selection_df = self.selection_df.query(
                'selArray == True and selPrjState == True and '
                'selSBState == True and selGrade == True and selBand == True')
            sb_fsel = self.selection_df.SB_UID.unique()
            self.master_dsa_df = self.master_dsa_df.query('SB_UID in @sb_fsel')

        # select if still some observations are left

        self.selection_df['selCount'] = True

        if check_count:
            self.selection_df['selCount'] = (
                self.master_dsa_df.EXECOUNT > self.master_dsa_df.Observed)

        self.selection_df['selConf'] = True

        # Array Configuration Selection (12m)

        if array_kind == "TWELVE-M":

            self.master_dsa_df['blmax'] = self.master_dsa_df.apply(
                lambda row: rUV.compute_bl(row['minAR'] / 0.8, 100.), axis=1)
            self.master_dsa_df['blmin'] = self.master_dsa_df.apply(
                lambda row: rUV.compute_bl(row['LAScor'], 100., las=True) if
                ((row['LAScor'] > row['minAR']) and
                 (row['LAScor'] > 5 * row['ARcordec']))
                else rUV.compute_bl(10., 100., las=True),
                axis=1)

            if conf:
                # Not Working For Exceptions!!!
                qstring = ''
                l = len(conf) - 1
                for i, c in enumerate(conf):
                    col = c.replace('-', '_')
                    if i == l:
                        qstring += '%s == "%s"' % (col, c)
                    else:
                        qstring += '%s == "%s" or ' % (col, c)
                sbs_sel = self.master_dsa_df.query(qstring).SB_UID.unique()
                self.selection_df['selConf'] = self.selection_df.apply(
                    lambda x: True if (x['SB_UID'] in sbs_sel) or
                    ((x['lenconf'] > 0) and
                     (x['minAR'] <= self.ar <= x['maxAR']))
                    else False,
                    axis=1
                )

                self.ar = CONFRES[conf[0]]

                self.master_dsa_df['bl_ratio'] = 1.
                self.master_dsa_df['array_ar_cond'] = self.master_dsa_df.apply(
                    lambda x: CONFRES[x['BestConf']] if x['BestConf'] in conf
                    else pd.np.NaN,
                    axis=1
                )
                self.master_dsa_df['num_bl_use'] = 630.

                if calc_blratio:
                    self._query_array()
                    array_id = self.arrays.iloc[0, 3]
                    array_ar, num_bl, num_ant, ruv = self._get_bl_prop(array_id)
                    self.master_dsa_df[['array_ar_cond', 'num_bl_use']] = (
                        self.master_dsa_df.apply(
                            lambda x: self._get_sbbased_bl_prop(
                                ruv, x['blmin'] * 0.9, x['blmax'] * 1.1,
                                x['array']),
                            axis=1)
                    )
                    self.master_dsa_df['bl_ratio'] = self.master_dsa_df.apply(
                        lambda x: 1. / calc_bl_ratio(
                            x['array'], x['CYCLE'], x['num_bl_use'],
                            self.selection_df.ix[x.name, 'selConf']),
                        axis=1
                    )

            else:
                self._query_array()
                try:
                    a = self._last_array_used
                except AttributeError:
                    self._last_array_used = ''

                if array_id == 'last':
                    array_id = self.arrays.iloc[0, 3]

                if self._last_array_used == array_id:
                    self.master_dsa_df['array_ar_cond'] = \
                        self.arr_cache['array_ar_cond']
                    self.master_dsa_df['num_bl_use'] = \
                        self.arr_cache['num_bl_use']
                else:
                    self.ar, numbl, numant, ruv = self._get_bl_prop(array_id)
                    self.master_dsa_df[['array_ar_cond', 'num_bl_use']] = (
                        self.master_dsa_df.apply(
                            lambda x: self._get_sbbased_bl_prop(
                                ruv, x['blmin'] * 0.9, x['blmax'] * 1.1,
                                x['array']),
                            axis=1)
                        )
                self.arr_cache = self.master_dsa_df[
                    ['array_ar_cond', 'num_bl_use']].copy()
                self._last_array_used = array_id

                self.selection_df['selConf'] = self.master_dsa_df.apply(
                    lambda x: True if
                    ((x['array_ar_cond'] > x['minAR']) and
                     (self.ar > 0.5 * x['ARcordec']) and
                     (x['array_ar_cond'] < (x['maxAR'] * 1.3)) and
                     (x['array_ar_cond'] < (x['ARcordec'] * 1.15))) or
                    ((x['lenconf'] > 2) and
                     (x['minAR'] <= self.ar) and
                     (self.ar <= x['maxAR']) and
                     (x['array'] == "TWELVE-M"))
                    else False, axis=1)

                self.master_dsa_df['bl_ratio'] = self.master_dsa_df.apply(
                    lambda x: 1. / calc_bl_ratio(
                        x['array'], x['CYCLE'], x['num_bl_use'],
                        self.selection_df.ix[x.name, 'selConf']),
                    axis=1
                )

        # Array Configuration Selection (7m or ACA)

        elif array_kind == "SEVEN-M":
            if numant is None:
                numant = 10.
            self.selection_df['selConf'] = self.master_dsa_df.apply(
                lambda x: True if x['array'] == "SEVEN-M" else
                False, axis=1
            )
            self.master_dsa_df['blmax'] = pd.np.NaN
            self.master_dsa_df['blmin'] = pd.np.NaN
            self.master_dsa_df['array_ar_cond'] = pd.np.NaN
            self.master_dsa_df['num_bl_use'] = pd.np.NaN
            self.master_dsa_df['bl_ratio'] = self.master_dsa_df.apply(
                lambda x: 1. / calc_bl_ratio(
                    x['array'], x['CYCLE'], x['num_bl_use'],
                    self.selection_df.ix[x.name, 'selConf'], numant=numant),
                axis=1
            )

        # Array Configuration selection (TP)
        else:
            if numant is None:
                numant = 2.

            # Until we have a clear idea on how to handle TP ampcals, removing
            # them from the DSA output

            # noinspection PyUnusedLocal
            selsb = self.master_dsa_df.query(
                    'array == "TP-Array"').SB_UID.unique()
            selsb1 = self.master_dsa_df[
                self.master_dsa_df.sbNote.str.contains('TP ampcal')
            ].SB_UID.unique()

            selsb2 = pd.merge(
                pd.merge(
                    self.data.orderedtar.query('SB_UID in @selsb'),
                    self.data.target,
                    on=['SB_UID', 'targetId']),
                self.data.fieldsource,
                on=['SB_UID', 'fieldRef']).query(
                    'name_y == "Amplitude"').SB_UID.unique()

            self.selection_df['selConf'] = self.master_dsa_df.apply(
                lambda x: True if (x['array'] == "TP-Array") and
                                  ((x['SB_UID'] not in selsb1) and
                                   (x['SB_UID'] not in selsb2)) else
                False, axis=1
            )
            self.master_dsa_df['blmax'] = pd.np.NaN
            self.master_dsa_df['blmin'] = pd.np.NaN
            self.master_dsa_df['array_ar_cond'] = pd.np.NaN
            self.master_dsa_df['num_bl_use'] = pd.np.NaN
            self.master_dsa_df['bl_ratio'] = 1.

        # select observable: elev, ha, moon & sun distance

        polarization = self.master_dsa_df.query('PolCalibrator != ""').copy()
        try:
            cpol = SkyCoord(
                ra=polarization.RA_pol * u.degree,
                dec=polarization.DEC_pol * u.degree,
                location=ALMA, obstime=self._time_astropy)
        except IndexError:
            cpol = polarization.copy()

        try:
            c = SkyCoord(
                ra=self.master_dsa_df.RA * u.degree,
                dec=self.master_dsa_df.DEC * u.degree,
                location=ALMA, obstime=self._time_astropy)
        except IndexError:
            print("Nothing to observe? %s" % len(self.master_dsa_df))
            self._availableobs = False
            return

        ha = self._time_astropy.sidereal_time('apparent') - c.ra
        self.master_dsa_df['HA'] = ha.wrap_at(180 * u.degree).value
        self.master_dsa_df['RAh'] = c.ra.hour
        self.master_dsa_df['elev'] = c.transform_to(
            AltAz(obstime=self._time_astropy, location=ALMA)).alt.value
        corr_el = ((self.master_dsa_df.ephem != 'N/A') &
                   (self.master_dsa_df.ephem != 'OK'))
        self.master_dsa_df.ix[corr_el, 'elev'] = -90.
        self.master_dsa_df.ix[corr_el, 'HA'] = -24.

        self.selection_df['selElev'] = (
            (self.master_dsa_df.elev >= horizon) &
            (self.master_dsa_df.RA != 0) &
            (self.master_dsa_df.DEC != 0)
        )

        self.selection_df.set_index('SB_UID', drop=False, inplace=True)

        if len(cpol) > 0:
            ha = self._time_astropy.sidereal_time('apparent') - cpol.ra
            polarization['HA_pol'] = ha.wrap_at(180 * u.degree).value
            polarization['RAh_pol'] = cpol.ra.hour
            polarization['elev_pol'] = cpol.transform_to(
                AltAz(obstime=self._time_astropy, location=ALMA)).alt.value
            parall = pd.np.arctan(
                    pd.np.sin(ha.radian) /
                    (pd.np.tan(ALMA.latitude.radian) *
                     pd.np.cos(cpol.dec.radian) -
                     pd.np.sin(cpol.dec.radian) * pd.np.cos(ha.radian)
                     ))
            polarization['parallactic'] = pd.np.degrees(parall)
            corr_el = ((polarization.ephem != 'N/A') &
                       (polarization.ephem != 'OK'))
            polarization.ix[corr_el, 'elev_pol'] = -90.
            polarization.ix[corr_el, 'HA_pol'] = -24.
            self.polarization = polarization
            self.selection_df.loc[
                polarization[polarization.elev_pol < 20].SB_UID.values,
                'selElev'] = False

        self.selection_df['selHA'] = (
            (self.master_dsa_df.set_index('SB_UID').HA >= minha) &
            (self.master_dsa_df.set_index('SB_UID').HA <= maxha)
        )

        # Sel Conditions, exec. frac

        ind1 = pd.np.around(self.master_dsa_df.repfreq, decimals=1)

        pwv_str = (str(int(pwv / 0.05) * 0.05 +
                   (0.05 if (int(pwv * 100) % 5) > 2 else 0.)))

        self.master_dsa_df['transmission'] = self.pwvdata.ix[
            ind1, pwv_str].values
        self.master_dsa_df['tau'] = self.tau.ix[ind1, pwv_str].values
        self.master_dsa_df['tsky'] = self.tsky.ix[ind1, pwv_str].values
        self.master_dsa_df['airmass'] = self.master_dsa_df.apply(
            lambda x: calc_airmass(x['elev'], transit=False), axis=1)
        self.master_dsa_df['tsys'] = (
            self.master_dsa_df.apply(
                lambda x: calc_tsys(x['band'], x['tsky'], x['tau'],
                                    x['airmass']), axis=1))
        self.master_dsa_df['tsys_ratio'] = self.master_dsa_df.apply(
            lambda x: 1. / (x['tsys'] / x['tsys_ot'])**2.
            if x['tsys'] <= 25000. else
            0., axis=1)

        self.master_dsa_df['Exec. Frac'] = self.master_dsa_df.apply(
            lambda x: (x['bl_ratio'] * x['tsys_ratio']) if
            (x['bl_ratio'] * x['tsys_ratio']) <= 100. else 0., axis=1)

        self.selection_df['selCond'] = self.master_dsa_df.set_index(
                'SB_UID').apply(
            lambda x: True if x['Exec. Frac'] >= 0.70 else False,
            axis=1
        )

        self.master_dsa_df.set_index('SB_UID', drop=False, inplace=True)
        self.selection_df.set_index('SB_UID', drop=False, inplace=True)

        savedate = ALMA1.date
        savehoriz = ALMA1.horizon
        ALMA1.horizon = 0.0
        lstdate = str(ALMA1.sidereal_time()).split(':')
        lstdate0 = dt.datetime.strptime(
            '2014-12-31 ' + str(lstdate[0]) + ':' +
            str(lstdate[1]), '%Y-%m-%d %H:%M')
        lstdate1 = dt.datetime.strptime(
            '2015-01-01 ' + str(lstdate[0]) + ':' +
            str(lstdate[1]), '%Y-%m-%d %H:%M')
        lstdate2 = dt.datetime.strptime(
            '2015-01-02 ' + str(lstdate[0]) + ':' +
            str(lstdate[1]), '%Y-%m-%d %H:%M')
        sunrisedate = ALMA1.previous_rising(ephem.Sun())
        ALMA1.date = sunrisedate
        sunriselst = str(ALMA1.sidereal_time()).split(':')
        sunriselst_h = dt.datetime.strptime(
            '2015-01-01 ' + str(sunriselst[0]) + ':' +
            str(sunriselst[1]), '%Y-%m-%d %H:%M')
        sunsetdate = ALMA1.next_setting(ephem.Sun())
        ALMA1.date = sunsetdate
        sunsetlst = str(ALMA1.sidereal_time()).split(':')
        sunsetlst_h = dt.datetime.strptime(
            '2015-01-01 ' + str(sunsetlst[0]) + ':' +
            str(sunriselst[1]), '%Y-%m-%d %H:%M')

        self.inputs = pd.DataFrame(
            pd.np.array([lstdate0, lstdate1, lstdate2,
                         sunsetlst_h, sunriselst_h,
                         sunsetlst_h + dt.timedelta(1),
                         sunriselst_h + dt.timedelta(1)]),
            index=['lst0', 'lst1', 'lst2', 'set1', 'rise1', 'set2', 'rise2'],
            columns=['2013.A']).transpose()
        self.inputs.ix['2013.1', :] = self.inputs.ix['2013.A', :]
        self.inputs.ix['2015.A', :] = self.inputs.ix['2013.A', :]
        self.inputs.ix['2015.1', :] = self.inputs.ix['2013.A', :]
        ALMA1.date = savedate
        ALMA1.horizon = savehoriz

    # noinspection PyTypeChecker,PyUnusedLocal
    def _aggregate_dfs(self, sim=False):

        """
        
        Args:
            sim: 
        """
        if sim:
            phase = ["I", "II"]
            hassb = 'hasSB == True or hasSB == False'
            how = 'right'
        else:
            phase = ["II"]
            hassb = 'hasSB == True'
            how = 'left'

        self.master_dsa_df = pd.merge(
            self.data.projects.query('phase in @phase')[
                ['OBSPROJECT_UID', 'CYCLE', 'CODE', 'DC_LETTER_GRADE',
                 'PRJ_SCIENTIFIC_RANK', 'PRJ_STATUS', 'EXEC']],
            self.data.sciencegoals.query(hassb)[
                ['OBSPROJECT_UID', 'SG_ID', 'OUS_ID', 'ARcor', 'LAScor',
                 'isTimeConstrained', 'isCalSpecial', 'isSpectralScan',
                 'sg_name', 'AR', 'LAS']],
            on='OBSPROJECT_UID', how='left')

        self.master_dsa_df = pd.merge(
            self.master_dsa_df,
            self.schedblocks[
                ['OBSPROJECT_UID', 'SB_UID', 'sbName', 'array',
                 'repfreq', 'band', 'RA', 'DEC',
                 'maxPWVC', 'minAR', 'maxAR', 'OT_BestConf', 'BestConf',
                 'two_12m', 'estimatedTime', 'isPolarization', 'ephem',
                 'airmass_ot', 'transmission_ot', 'tau_ot', 'tsky_ot',
                 'tsys_ot', 'sbNote', 'SG_ID', 'sgName', 'execount']],
            on=['OBSPROJECT_UID', 'SG_ID'], how=how)

        self.master_dsa_df = pd.merge(
            self.master_dsa_df,
            self.data.sblocks[
                ['OBSPROJECT_UID', 'OUS_ID', 'GOUS_ID', 'MOUS_ID',
                 'SB_UID']],
            on=['OBSPROJECT_UID', 'SB_UID'], how='left',
            suffixes=['_sg', '_sb'])

        self.master_dsa_df['ARcor'] = self.master_dsa_df.apply(
            lambda x: x['AR'] * x['repfreq'] / 100. if not
            str(x['sbName']).endswith('_TC') else
            x['minAR'] / 0.8, axis=1
        )

        self.master_dsa_df['ARcordec'] = self.master_dsa_df.apply(
                    lambda x: x['ARcor'] / (
                        0.4001 /
                        pd.np.cos(
                            pd.np.radians(-23.0262015) -
                            pd.np.radians(x['DEC'])) + 0.6103) if not
                    str(x['sbName']).endswith('_TC') else
                    x['ARcor'], axis=1)

        self.master_dsa_df['arcordec_original'] = \
            self.master_dsa_df.ARcordec.copy()

        self.master_dsa_df = pd.merge(
            self.master_dsa_df,
            self.data.sb_status[['SB_UID', 'SB_STATE', 'EXECOUNT']],
            on=['SB_UID'], how='left')

        self.master_dsa_df.EXECOUNT.fillna(-10, inplace=True)

        self.master_dsa_df['EXECOUNT'] = self.master_dsa_df.apply(
            lambda x: x['execount'] if x['EXECOUNT'] == -10 else
            x['EXECOUNT'], axis=1
        )

        self.master_dsa_df.drop('execount', axis=1, inplace=True)

        self.master_dsa_df = pd.merge(
            self.master_dsa_df,
            self.data.qastatus[
                ['Unset', 'Pass', 'Observed', 'ebTime',
                 'last_observed', 'last_qa0', 'last_status']],
            left_on='SB_UID', right_index=True, how='left')

        self.master_dsa_df.Unset.fillna(0, inplace=True)
        self.master_dsa_df.Pass.fillna(0, inplace=True)
        self.master_dsa_df.Observed.fillna(0, inplace=True)

        self.master_dsa_df = pd.merge(
            self.master_dsa_df,
            self.obs_param[
                ['SB_UID', 'rise', 'set', 'note', 'C36_1', 'C36_2', 'C36_3',
                 'C36_4', 'C36_5', 'C36_6', 'C36_7', 'C36_8', 'twelve_good']],
            on=['SB_UID'], how='left')

        self.master_dsa_df.ebTime.fillna(0, inplace=True)

        self.master_dsa_df['estTimeOr'] = \
            self.master_dsa_df.estimatedTime.copy()
        self.master_dsa_df['estimatedTime'] = self.master_dsa_df.apply(
            lambda x: x['estimatedTime'] if x['ebTime'] <= 0.1 else
            x['ebTime'] * x['EXECOUNT'], axis=1
        )

        step1 = pd.merge(self.data.polcalparam[['SB_UID', 'paramRef']],
                         self.data.target, on=['SB_UID', 'paramRef'])
        step2 = pd.merge(step1, self.data.orderedtar, on=['SB_UID', 'targetId'])
        step3 = pd.merge(
                step2, self.data.fieldsource, on=['SB_UID', 'fieldRef']
        ).drop_duplicates('SB_UID')[['SB_UID', 'RA', 'DEC', 'sourcename']]
        step3.columns = pd.Index(
                ['SB_UID', 'RA_pol', 'DEC_pol',
                 'PolCalibrator'], dtype='object')
        self.master_dsa_df = pd.merge(self.master_dsa_df, step3, on='SB_UID',
                                      how='left')
        self.master_dsa_df.RA_pol.fillna(0, inplace=True)
        self.master_dsa_df.DEC_pol.fillna(0, inplace=True)
        self.master_dsa_df.PolCalibrator.fillna('', inplace=True)

        self.calc_completion()
        self.master_dsa_df = pd.merge(
            self.master_dsa_df,
            self.grouped_ous[['OBSPROJECT_UID', 'GOUS_ID', 'GOUS_comp',
                              'proj_comp']],
            on=['OBSPROJECT_UID', 'GOUS_ID'],
            how='left'
        )

        self.exception_df = pd.merge(
            self.data._exceptions,
            self.master_dsa_df[['CODE', 'sbName', 'SB_UID', 'array']],
            on=['CODE', 'sbName'], how='left').set_index('SB_UID', drop=False)
        self.exception_df.forced_confs.fillna('None', inplace=True)
        self.exception_df['conflist'] = \
            self.exception_df.forced_confs.str.split(',')
        self.exception_df['CompConf'] = self.exception_df.apply(
            lambda x: x['conflist'][0], axis=1)
        self.exception_df['ExtConf'] = self.exception_df.apply(
            lambda x: x['conflist'][-1], axis=1)
        self.exception_df['lenconf'] = self.exception_df.apply(
            lambda x: len(x['conflist']) if
            'None' not in x['conflist'] and x['array'] == "TWELVE-M" else
            0, axis=1)

        self.master_dsa_df[
                ['SB_UID_ck', 'minAR', 'maxAR', 'ARcordec',
                 'LAScor', 'BestConf', 'lenconf']] = self.master_dsa_df.apply(
                lambda row: newimparam_excep(self.exception_df, row), axis=1
            )

    def _query_array(self, array_kind='TWELVE-M'):

        """

        Args:
            array_kind: 
        """
        if array_kind == 'TWELVE-M':
            sql = str(
                "select se.SE_TIMESTAMP ts1, sa.SLOG_ATTR_VALUE av1, "
                "se.SE_ARRAYNAME, se.SE_ID se1 from ALMA.SHIFTLOG_ENTRIES se, "
                "ALMA.SLOG_ENTRY_ATTR sa "
                "WHERE se.SE_TYPE=7 and se.SE_TIMESTAMP > SYSDATE - 1/1. "
                "and sa.SLOG_SE_ID = se.SE_ID and sa.SLOG_ATTR_TYPE = 31 "
                "and se.SE_LOCATION='OSF-AOS' and se.SE_CORRELATORTYPE = 'BL' "
                "and se.SE_ARRAYFAMILY = '12 [m]' "
                "and se.SE_ARRAYTYPE != 'Manual'")
        elif array_kind == 'SEVEN-M':
            sql = str(
                "select se.SE_TIMESTAMP ts1, sa.SLOG_ATTR_VALUE av1, "
                "se.SE_ARRAYNAME, se.SE_ID se1 from ALMA.SHIFTLOG_ENTRIES se, "
                "ALMA.SLOG_ENTRY_ATTR sa "
                "WHERE se.SE_TYPE=7 and se.SE_TIMESTAMP > SYSDATE - 1/1. "
                "and sa.SLOG_SE_ID = se.SE_ID and sa.SLOG_ATTR_TYPE = 31 "
                "and se.SE_LOCATION='OSF-AOS' and se.SE_CORRELATORTYPE = 'ACA' "
                "and se.SE_ARRAYFAMILY = '7 [m]' "
                "and se.SE_ARRAYTYPE != 'Manual'")
        elif array_kind == 'TP-Array':
            sql = str(
                "select se.SE_TIMESTAMP ts1, sa.SLOG_ATTR_VALUE av1, "
                "se.SE_ARRAYNAME, se.SE_ID se1 from ALMA.SHIFTLOG_ENTRIES se, "
                "ALMA.SLOG_ENTRY_ATTR sa "
                "WHERE se.SE_TYPE=7 and se.SE_TIMESTAMP > SYSDATE - 1/1. "
                "and sa.SLOG_SE_ID = se.SE_ID and sa.SLOG_ATTR_TYPE = 31 "
                "and se.SE_LOCATION='OSF-AOS' and se.SE_CORRELATORTYPE = 'ACA' "
                "and se.SE_ARRAYFAMILY = 'Total Power' "
                "and se.SE_ARRAYTYPE != 'Manual'")
        else:
            print("%s array kind is not valid. Use TWELVE-M, SEVEN-M, TP-Array")
            return

        con, cur = self.open_oracle_conn()
        try:
            cur.execute(sql)
            self._arrays_info = pd.DataFrame(
                cur.fetchall(),
                columns=[rec[0] for rec in cur.description]
            ).sort_values(by='TS1', ascending=False)
        finally:
            cur.close()
            con.close()

        if self._arrays_info.size == 0:
            self._arrays_info = pd.DataFrame(
                columns=pd.Index(
                    [u'TS1', u'AV1', u'SE_ARRAYNAME', u'SE1'], dtype='object'))
            print("No %s arrays have been created in the last 6 hours." %
                  array_kind)
            self._group_arrays = None
            self.arrays = None
            return

        if array_kind in ['TWELVE-M', 'TP-Array']:
            self._group_arrays = self._arrays_info[
                self._arrays_info.AV1.str.startswith('CM') == False].copy()
        else:
            self._group_arrays = self._arrays_info[
                self._arrays_info.AV1.str.startswith('CM') == True].copy()

        if array_kind == "TWELVE-M":
            self.arrays = self._group_arrays.groupby(
                'TS1').aggregate(
                {'SE_ARRAYNAME': max, 'SE1': max,
                 'AV1': pd.np.count_nonzero}).query(
                'AV1 > 28').reset_index().sort_values(by='TS1', ascending=False)

        elif array_kind == "SEVEN-M":
            self.arrays = self._group_arrays.groupby(
                'TS1').aggregate(
                {'SE_ARRAYNAME': max, 'SE1': max,
                 'AV1': pd.np.count_nonzero}).query(
                'AV1 > 5').reset_index().sort_values(by='TS1', ascending=False)

        else:
            self.arrays = self._group_arrays.groupby(
                'TS1').aggregate(
                {'SE_ARRAYNAME': max, 'SE1': max,
                 'AV1': pd.np.count_nonzero}).query(
                'AV1 >= 1').reset_index().sort_values(by='TS1', ascending=False)

        # get latest pad info

        b = str(
            "select se.SE_TIMESTAMP ts1, se.SE_SUBJECT, "
            "sa.SLOG_ATTR_VALUE av1, se.SE_ID se1, se.SE_SHIFTACTIVITY "
            "from alma.SHIFTLOG_ENTRIES se, alma.SLOG_ENTRY_ATTR sa "
            "WHERE se.SE_TYPE=1 and se.SE_TIMESTAMP > SYSDATE - 2. "
            "and sa.SLOG_SE_ID = se.SE_ID and sa.SLOG_ATTR_TYPE = 12 "
            "and se.SE_LOCATION='OSF-AOS'"
        )

        con, cur = self.open_oracle_conn()
        try:
            cur.execute(b)
            self._shifts = pd.DataFrame(
                cur.fetchall(),
                columns=[rec[0] for rec in cur.description]
            ).sort_values(by='TS1', ascending=False)
        except ValueError:
            self._shifts = pd.DataFrame(
                columns=pd.Index(
                    [u'TS1', u'AV1', u'SE_ARRAYNAME', u'SE1'], dtype='object'))
            print("No shiftlogs have been created in the last 6 hours.")
        finally:
            cur.close()
            con.close()

        last_shift = self._shifts[
            self._shifts.SE1 == self._shifts.iloc[0].SE1].copy(
            ).drop_duplicates('AV1')
        last_shift['AV1'] = last_shift.AV1.str.split(':')
        ante = last_shift.apply(lambda x: x['AV1'][0], axis=1)
        pads = last_shift.apply(lambda x: x['AV1'][1], axis=1)
        self._ante_pad = pd.DataFrame({'antenna': ante, 'pad': pads})

    def _get_bl_prop(self, array_name):

        # In case a bl_array is selected
        """

        Args:
            array_name: 

        Returns:
            object: 

        """
        if array_name not in CONF_LIM['minbase'].keys():
            id1 = self._arrays_info.query(
                'SE_ARRAYNAME == "%s"' % array_name).iloc[0].SE1
            ap = self._arrays_info.query(
                'SE_ARRAYNAME == "%s" and SE1 == %d' % (array_name, id1)
            )[['AV1']]

            ap.rename(columns={'AV1': 'antenna'}, inplace=True)
            ap = ap[ap.antenna.str.contains('CM') == False]
            if len(ap) == 0:
                ap = self._arrays_info.query(
                        'SE_ARRAYNAME == "%s" and SE1 == %d' %
                        (array_name, id1))[['AV1']]
                ap.rename(columns={'AV1': 'antenna'}, inplace=True)

            conf = pd.merge(ap, self._ante_pad,
                            left_on='antenna', right_on='antenna')[
                ['pad', 'antenna']]
            conf_file = self._dsa_path + 'conf/%s.txt' % array_name
            conf.to_csv(conf_file, header=False,
                        index=False, sep=' ')
            ac = rUV.ac.ArrayConfigurationCasaFile()
            ac.createCasaConfig(conf_file)
            ruv = rUV.compute_radialuv(conf_file + ".cfg")
            num_bl = len(ruv)
            num_ant = len(ap)
            array_ar = rUV.compute_array_ar(ruv)

        # If C36 is selected
        else:
            conf_file = (self._dsa_path +
                         'conf/%s.cfg' % array_name)
            ruv = rUV.compute_radialuv(conf_file)
            # noinspection PyTypeChecker
            array_ar = rUV.compute_array_ar(ruv)
            num_bl = len(ruv)
            if array_name.startswith('C40'):
                num_ant = 40
            else:
                num_ant = 36

        return array_ar, num_bl, num_ant, ruv

    @staticmethod
    def _get_sbbased_bl_prop(ruv, blmin, blmax, arrayfam):

        """

        Args:
            ruv: 
            blmin: 
            blmax: 
            arrayfam: 

        Returns:
            Pandas.Series: 

        """
        if arrayfam != "TWELVE-M":
            return pd.Series(
                [pd.np.NaN, 0],
                index=['array_ar_cond', 'num_bl_use'])

        ruv = ruv[(ruv >= blmin) & (ruv <= blmax)]
        if len(ruv) < 400.:
            return pd.Series(
                [pd.np.NaN, 0],
                index=['array_ar_cond', 'num_bl_use'])
        num_bl = len(ruv)
        array_ar = rUV.compute_array_ar(ruv)

        return pd.Series([array_ar, num_bl],
                         index=['array_ar_cond', 'num_bl_use'])

    def _observe_pol(self):

        pass

    def calc_completion(self):
        
        """

        """

        s1 = pd.merge(
                self.data.qastatus.reset_index(),
                self.data.sblocks[
                    ['OBSPROJECT_UID', 'SB_UID', 'OUS_ID', 'GOUS_ID',
                     'MOUS_ID', 'array']],
                on='SB_UID', how='right')
        s1.fillna(0, inplace=True)

        s2 = pd.merge(s1,
                      self.data.sb_status[
                          ['SB_UID', 'EXECOUNT', 'SB_STATE']],
                      on='SB_UID')
        s2['SB_Comp'] = s2.apply(
                lambda x: 1 if
                (x['SB_STATE'] == "FullyObserved" or
                 x['Observed'] >= x['EXECOUNT'])
                else 0, axis=1)

        self.grouped_ous = s2.groupby(
                ['OBSPROJECT_UID', 'GOUS_ID']).aggregate(
                {'SB_UID': pd.np.count_nonzero,
                 'SB_Comp': pd.np.sum,
                 'EXECOUNT': pd.np.sum,
                 'Observed': pd.np.sum}).reset_index()

        self.grouped_ous['GOUS_comp'] = (
            1. * self.grouped_ous.SB_Comp / self.grouped_ous.SB_UID)
        self.grouped_ous.columns = pd.Index(
                [u'OBSPROJECT_UID', u'GOUS_ID', u'TotalEBObserved_GOUS',
                 u'SB_Completed_GOUS', u'TotalExecount_GOUS', u'SB_Number_GOUS',
                 u'GOUS_comp'], dtype='object')
        gp = self.grouped_ous.groupby(
                'OBSPROJECT_UID').aggregate(
                {'GOUS_ID': pd.np.count_nonzero, 'GOUS_comp': pd.np.sum})
        gp['proj_comp'] = 1. * gp.GOUS_comp / gp.GOUS_ID
        self.grouped_ous = pd.merge(
            self.grouped_ous,
            gp.reset_index()[['OBSPROJECT_UID', 'proj_comp']],
            on='OBSPROJECT_UID', how='left'
        )

    def open_oracle_conn(self):

        """

        Returns:
            object:

        """
        connection = cx_Oracle.connect(self._conx_str, threaded=True)
        cursor = connection.cursor()
        return connection, cursor


def calc_bl_ratio(arrayk, cycle, numbl, selconf, numant=None):

    """

    Args:
        arrayk: 
        cycle: 
        numbl: 
        selconf: 
        numant: 

    Returns:
        float: 

    """
    if arrayk == "TWELVE-M" and selconf:
        bl_or = CYC_NA[cycle] * (CYC_NA[cycle] - 1.) / 2.
        try:
            bl_frac = bl_or / numbl
        except ZeroDivisionError:
            bl_frac = pd.np.Inf
        return bl_frac
    elif arrayk in ["ACA", "SEVEN-M"] and selconf:
        return 5 * 9. / (numant * (numant - 1) / 2.)
    elif arrayk in ["TP-Array"] and selconf:
        return 1.
    else:
        return pd.np.Inf


def calc_tsys(band, tsky, tau, airmass):

    """

    Args:
        band: 
        tsky: 
        tau: 
        airmass: 

    Returns:
        float: 

    """
    if airmass:

        g = RECEIVER['g'][band]
        trx = RECEIVER['trx'][band]

        tsys = ((1 + g) *
                (trx +
                 tsky * (
                     (1 - pd.np.exp(-1 * airmass * tau)) /
                     (1 - pd.np.exp(-1. * tau))
                 ) * 0.95 + 0.05 * 270.) /
                (0.95 * pd.np.exp(-1 * tau * airmass)))

    else:
        tsys = pd.np.Inf

    return tsys


# noinspection PyTypeChecker
def calc_airmass(dec_el, transit=True):

    """

    Args:
        dec_el: 
        transit: 

    Returns:
        float: 

    """
    if transit:
        airmass = 1 / pd.np.cos(pd.np.radians(-23.0262015 - dec_el))
    else:
        if dec_el > 0:
            airmass = 1 / pd.np.cos(pd.np.radians(90. - dec_el))
        else:
            airmass = None
    return airmass


def newimparam_excep(excdf, sbrow):

    """

    Args:
        excdf: 
        sbrow: 

    Returns:
        Pandas.Series: 

    """
    if sbrow['SB_UID'] not in excdf.SB_UID.unique():
        return pd.Series(
            [sbrow['SB_UID'], sbrow['minAR'], sbrow['maxAR'],
             sbrow['ARcordec'], sbrow['LAScor'],
             sbrow['BestConf'], 0],
            index=['SB_UID', 'minAR', 'maxAR', 'ARcordec', 'LAScor', 'BestConf',
                   'lenconf'])

    lenconf = excdf.ix[sbrow['SB_UID'], 'lenconf']
    compconf = excdf.ix[sbrow['SB_UID'], 'CompConf']
    extconf = excdf.ix[sbrow['SB_UID'], 'ExtConf']

    if lenconf == 0:
        return pd.Series(
            [sbrow['SB_UID'], sbrow['minAR'], sbrow['maxAR'],
             sbrow['ARcordec'], sbrow['LAScor'],
             sbrow['BestConf'], 0],
            index=['SB_UID', 'minAR', 'maxAR', 'ARcordec', 'LAScor', 'BestConf',
                   'lenconf'])

    elif lenconf == 1:
        minar = CONFRES[compconf] * 0.8
        maxar = CONFRES[compconf] * 1.2
        arcordec = CONFRES[compconf]
        lascor = CONFLAS[compconf]
        bestconf = compconf
        return pd.Series(
            [sbrow['SB_UID'], minar, maxar, arcordec, lascor, bestconf,
             lenconf],
            index=['SB_UID', 'minAR', 'maxAR', 'ARcordec', 'LAScor', 'BestConf',
                   'lenconf'])

    elif lenconf == 2:
        minar = CONFRES[extconf] * 0.8
        maxar = CONFRES[compconf] * 1.2
        arcordec = (CONFRES[extconf] + CONFRES[compconf]) / 2.
        lascor = CONFLAS[compconf]
        bestconf = extconf
        return pd.Series(
            [sbrow['SB_UID'], minar, maxar, arcordec, lascor, bestconf,
             lenconf],
            index=['SB_UID', 'minAR', 'maxAR', 'ARcordec', 'LAScor', 'BestConf',
                   'lenconf'])

    else:
        minar = CONFRES[extconf] * 0.8
        maxar = CONFRES[compconf] * 1.2
        arcordec = CONFRES[extconf]
        lascor = CONFLAS[compconf]
        bestconf = 'Any/Many'
        return pd.Series(
            [sbrow['SB_UID'], minar, maxar, arcordec, lascor, bestconf,
             lenconf],
            index=['SB_UID', 'minAR', 'maxAR', 'ARcordec', 'LAScor', 'BestConf',
                   'lenconf'])
