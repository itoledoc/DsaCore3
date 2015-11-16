import pandas as pd
import ephem
import os
import wto3_tools as wtool
import ruvTest as rUV

from WtoDataBase3 import WtoDatabase3
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
ALMA1.lat = '-23.0262015'
ALMA1.long = '-67.7551257'
ALMA1.elev = 5060
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
        'C36-8': 346.5
    },
    'maxbase': {
        'C36-1': 160.7,
        'C36-2': 376.9,
        'C36-3': 538.9,
        'C36-4': 969.4,
        'C36-5': 1396.4,
        'C36-6': 2299.6,
        'C36-7': 6074.2,
        'C36-8': 9743.7
    }
}

CONFRES = {
    'C36-1': 3.4,
    'C36-2': 1.8,
    'C36-3': 1.2,
    'C36-4': 0.7,
    'C36-5': 0.5,
    'C36-6': 0.27,
    'C36-7': 0.2,
    'C36-8': 0.075}

CYC_NA = {'2013.A': 34,
          '2013.1': 34,
          '2015.1': 36,
          '2015.A': 36}


# noinspection PyAttributeOutsideInit
class WtoAlgorithm3(WtoDatabase3):
    """
    Inherits from WtoDatabase, adds the methods for selection and scoring.
    It also sets the default parameters for these methods: pwv=1.2, date=now,
    array angular resolution, transmission=0.5, minha=-5, maxha=3, etc.

    :return: A WtoAlgorithm instance.

    Should run:
       .update_archive
       .crea_extrainfo
       .write_ephem
       .unmut_param
       .aggregate_dfs
       .selector
       .scorere

       (in this order)
    """
    def __init__(self, refresh_apdm=True, path=None, allc2=False, loadp1=False):

        """

        :type loadp1: bool
        :type allc2: bool
        :type path: str
        :type refresh_apdm: bool
        """
        super(WtoAlgorithm3, self).__init__(
            refresh_apdm=refresh_apdm, path=path, allc2=allc2, loadp1=loadp1)

        self.tau = pd.read_csv(
            self._wto_path + 'conf/tau.csv', sep=',', header=0).set_index(
            'freq')
        self.tsky = pd.read_csv(
            self._wto_path + 'conf/tskyR.csv', sep=',', header=0).set_index(
                'freq')
        self.pwvdata = pd.read_pickle(
            self._wto_path + 'conf/pwvdata.pandas').set_index(
                'freq')
        self.pwvdata.index = pd.Float64Index(
            pd.np.round(self.pwvdata.index.values, decimals=1), name=u'freq')

        self._pwv = None
        self._array_res = []
        self._date = ephem.now()
        self._availableobs = False
        self._time_astropy = TIME
        self._ALMA_ephem = ALMA1
        self._unmut = False

    def aggregate_dfs(self):

        self.master_wto_df = pd.merge(
            self.projects.query('phase == "II"')[
                ['OBSPROJECT_UID', 'CYCLE', 'CODE', 'DC_LETTER_GRADE',
                 'PRJ_SCIENTIFIC_RANK', 'PRJ_STATUS']],
            self.sciencegoals.query('hasSB == True')[
                ['OBSPROJECT_UID', 'SG_ID', 'OUS_ID', 'ARcor', 'LAScor',
                 'isTimeConstrained', 'isCalSpecial', 'isSpectralScan']],
            on='OBSPROJECT_UID', how='left')

        self.master_wto_df = pd.merge(
            self.master_wto_df,
            self.sblocks[
                ['OBSPROJECT_UID', 'OUS_ID', 'GOUS_ID', 'MOUS_ID', 'SB_UID']],
            on=['OBSPROJECT_UID', 'OUS_ID'], how='left')

        self.master_wto_df = pd.merge(
            self.master_wto_df,
            self.schedblocks[
                ['SB_UID', 'sbName', 'array', 'repfreq', 'band', 'RA', 'DEC',
                 'maxPWVC', 'minAR', 'maxAR', 'OT_BestConf', 'BestConf',
                 'two_12m', 'estimatedTime', 'isPolarization', 'ephem']],
            on=['SB_UID'], how='left')

        self.master_wto_df = pd.merge(
            self.master_wto_df,
            self.sb_status[['SB_UID', 'SB_STATE', 'EXECOUNT']],
            on=['SB_UID'], how='left')

        sbs_uid_s = self.master_wto_df.SB_UID.unique()

        qastatus = self.aqua_execblock.query(
            'SB_UID in @sbs_uid_s').groupby(
            ['SB_UID', 'QA0STATUS']).QA0STATUS.count().unstack().fillna(0)

        if 'Pass' not in qastatus.columns.values:
            qastatus['Pass'] = 0
        if 'Unset' not in qastatus.columns.values:
            qastatus['Unset'] = 0

        qastatus['Observed'] = qastatus.Unset + qastatus.Pass

        self.master_wto_df = pd.merge(
            self.master_wto_df,
            qastatus[
                ['Unset', 'Pass', 'Observed']],
            left_on='SB_UID', right_index=True, how='left')
        self.master_wto_df.Unset.fillna(0, inplace=True)
        self.master_wto_df.Pass.fillna(0, inplace=True)
        self.master_wto_df.Observed.fillna(0, inplace=True)

        self.master_wto_df = pd.merge(
            self.master_wto_df,
            self.obs_param[
                ['SB_UID', 'rise', 'set', 'note', 'C36_1', 'C36_2', 'C36_3',
                 'C36_4', 'C36_5', 'C36_6', 'C36_7', 'C36_8', 'twelve_good']],
            on=['SB_UID'], how='left')

    def set_time_now(self):
        self._time_astropy = Time.now()
        self._time_astropy.delta_ut1_utc = 0
        self._time_astropy.location = ALMA
        self._ALMA_ephem.date = ephem.now()

    def write_ephem_coords(self):

        """
        TODO: deal with multiple targets, which RA to take?
        """
        self.schedblocks['ephem'] = 'N/A'

        ephem_sb = pd.merge(
            self.schedblocks,
            self.target_tables.query(
                'solarSystem != "Unspecified" and isQuery == False and '
                'RA == 0'),
            on='SB_UID').drop_duplicates(['SB_UID', 'ephemeris']).set_index(
            'SB_UID', drop=False)

        results = ephem_sb.apply(
            lambda x: wtool.calc_ephem_coords(
                x['solarSystem'], x['ephemeris'], x['SB_UID'],
                alma=self._ALMA_ephem),
            axis=1)

        for r in results.iteritems():
            self.schedblocks.ix[r[0], 'RA'] = r[1][0]
            self.schedblocks.ix[r[0], 'DEC'] = r[1][1]
            self.schedblocks.ix[r[0], 'ephem'] = r[1][2]

    def unmut_param(self, horizon=20):

        if self._unmut:
            idx = self.target_tables.query(
                'solarSystem != "Unspecified" and isQuery == False and '
                'RA == 0').SB_UID.unique()

            self.obs_param.ix[idx] = self.schedblocks.ix[idx].apply(
                lambda r: wtool.observable(
                    r['RA'], r['DEC'], self._ALMA_ephem, r['RA'], r['minAR'],
                    r['maxAR'], r['array'], r['SB_UID'], horizon=horizon),
                axis=1
            )

        else:
            self.obs_param = self.schedblocks.apply(
                lambda r: wtool.observable(
                    r['RA'], r['DEC'], self._ALMA_ephem, r['RA'], r['minAR'],
                    r['maxAR'], r['array'], r['SB_UID'], horizon=horizon),
                axis=1
            )

        self._unmut = True

    def update_apdm(self, obsproject_uid):

        self._update_apdm(obsproject_uid)
        self._unmut = True

    def selector(self, array_kind='TWELVE-M',
                 prj_status=("Ready", "InProgress"),
                 sb_status=("Ready", "Suspended", "Running", "CalibratorCheck",
                            "Waiting"),
                 cycle=("2013.A", "2013.1", "2015.1", "2015.A"),
                 letterg=("A", "B", "C"),
                 bands=("ALMA_RB_03", "ALMA_RB_04", "ALMA_RB_06", "ALMA_RB_07",
                        "ALMA_RB_08", "ALMA_RB_09", "ALMA_RB_10"),
                 conf=None, numant=None, arrayar=None, ruv=None, horizon=20.,
                 minha=-3., maxha=3., site=ALMA, time=None, pwv=0,
                 check_count=True, mintrans=None):

        if time:
            time = Time(time, format='isot', scale='utc')
            time.delta_ut1_utc = 0
            time.location = ALMA
            self._ALMA_ephem.date = ephem.Date(time.iso)
        else:
            time = self._time_astropy
        print time

        self.aggregate_dfs()
        self.selection_df = self.master_wto_df[['SB_UID']].copy()
        # select array kind

        self.selection_df['selArray'] = (
            self.master_wto_df['array'] == array_kind)
        self.selection_df['selPrjState'] = (
            self.master_wto_df.apply(
                lambda x: True if x['PRJ_STATUS'] in prj_status else False,
                axis=1))
        self.selection_df['selSBState'] = (
            self.master_wto_df.apply(
                lambda x: True if x['SB_STATE'] in sb_status else False,
                axis=1))
        self.selection_df['selCount'] = True

        self.selection_df['selPrj'] = (
            self.master_wto_df.apply(
                lambda x: True if
                x['CYCLE'] in cycle and x['DC_LETTER_GRADE'] in letterg else
                False, axis=1)
        )

        self.selection_df['selBand'] = (
            self.master_wto_df.apply(
                lambda x: True if x['band'] in bands else False,
                axis=1
            )
        )

        if check_count:
            self.selection_df['selCount'] = (
                self.master_wto_df.EXECOUNT > self.master_wto_df.Observed)

        self.selection_df['selConf'] = True

        if conf:
            qstring = ''
            l = len(conf) - 1
            for i, c in enumerate(conf):
                col = c.replace('-', '_')
                if i == l:
                    qstring += '%s == "%s"' % (col, c)
                else:
                    qstring += '%s == "%s" or ' % (col, c)
            sbs_sel = self.master_wto_df.query(qstring).SB_UID.unique()
            self.selection_df['selConf'] = self.selection_df.apply(
                lambda x: True if x['SB_UID'] in sbs_sel else False,
                axis=1
            )

        # select observable: elev, ha, moon & sun distance

        try:
            c = SkyCoord(
                ra=self.master_wto_df.RA*u.degree,
                dec=self.master_wto_df.DEC*u.degree,
                location=site, obstime=time)
        except IndexError:
            print("Nothing to observe? %s" % len(self.master_wto_df))
            self._availableobs = False
            return

        ha = time.sidereal_time('apparent') - c.ra
        self.master_wto_df['HA'] = ha.wrap_at(180*u.degree).value
        self.master_wto_df['RAh'] = c.ra.hour
        self.master_wto_df['elev'] = c.transform_to(
            AltAz(obstime=time, location=site)).alt.value
        corr_el = ((self.master_wto_df.ephem != 'N/A') &
                   (self.master_wto_df.ephem != 'OK'))
        self.master_wto_df.ix[corr_el, 'elev'] = -90.
        self.master_wto_df.ix[corr_el, 'HA'] = -24.

        self.selection_df['selElev'] = (
            self.master_wto_df.elev >= horizon)

        self.selection_df['selHA'] = (
            (self.master_wto_df.HA >= minha) &
            (self.master_wto_df.HA <= maxha)
        )

        ind1 = pd.np.around(self.master_wto_df.repfreq, decimals=1)
        ind2 = self.master_wto_df.apply(
            lambda x: str(
                int(x['maxPWVC'] / 0.05) * 0.05 +
                (0.05 if (x['maxPWVC'] % 0.05) > 0.02 else 0.)),
            axis=1)

        pwv_str = (str(int(pwv / 0.05) * 0.05 +
                   (0.05 if (pwv % 0.05) > 0.02 else 0.)))

        self.master_wto_df['transmission_org'] = self.pwvdata.lookup(
            ind1, ind2)
        self.master_wto_df['tau_org'] = self.tau.lookup(ind1, ind2)
        self.master_wto_df['tsky_org'] = self.tsky.lookup(ind1, ind2)
        self.master_wto_df['airmass_org'] = self.master_wto_df.apply(
            lambda x: calc_airmass(x['DEC'], transit=True), axis=1)

        self.master_wto_df['transmission'] = self.pwvdata.ix[
            ind1, pwv_str].values
        self.master_wto_df['tau'] = self.tau.ix[ind1, pwv_str].values
        self.master_wto_df['tsky'] = self.tsky.ix[ind1, pwv_str].values
        self.master_wto_df['airmass'] = self.master_wto_df.apply(
            lambda x: calc_airmass(x['elev'], transit=False), axis=1)

        self.master_wto_df['tsys_org'] = (
            self.master_wto_df.apply(
                lambda x: calc_tsys(x['band'], x['tsky_org'], x['tau_org'],
                                    x['airmass_org']), axis=1))

        self.master_wto_df['tsys'] = (
            self.master_wto_df.apply(
                lambda x: calc_tsys(x['band'], x['tsky'], x['tau'],
                                    x['airmass']), axis=1))

        self.master_wto_df['tsysfrac'] = (
            self.master_wto_df.tsys / self.master_wto_df.tsys_org)**2

        self.master_wto_df['blmax'] = self.master_wto_df.apply(
            lambda row: rUV.computeBL(row['ARcor'], 100.), axis=1)
        self.master_wto_df['blmin'] = self.master_wto_df.apply(
            lambda row: rUV.computeBL(row['LAScor'], 100.), axis=1)

        # self.master_wto_df['blfrac'] = self.master_wto_df.apply(
        #     lambda x: calc_blfrac(
        #         x['array'], x['CYCLE'], x['blmin'], x['blmax'], ruv,
        #         self.selection_df.ix[x.name, 'selConf']),
        #     axis=1
        # )

        # select transmission

        # calculate frac

        # select frac

    def _query_array(self):
        a = str(
            "select se.SE_TIMESTAMP ts1, sa.SLOG_ATTR_VALUE av1, "
            "se.SE_ARRAYNAME, se.SE_ID se1 from ALMA.SHIFTLOG_ENTRIES se, "
            "ALMA.SLOG_ENTRY_ATTR sa "
            "WHERE se.SE_TYPE=7 and se.SE_TIMESTAMP > SYSDATE - 1/1. "
            "and sa.SLOG_SE_ID = se.SE_ID and sa.SLOG_ATTR_TYPE = 31 "
            "and se.SE_LOCATION='OSF-AOS' and se.SE_CORRELATORTYPE = 'BL'")
        try:
            self._cursor.execute(a)
            self.bl_arrays = pd.DataFrame(
                self._cursor.fetchall(),
                columns=[rec[0] for rec in self._cursor.description]
            ).sort('TS1', ascending=False)
        except ValueError:
            self._bl_arrays = pd.DataFrame(
                columns=pd.Index(
                    [u'TS1', u'AV1', u'SE_ARRAYNAME', u'SE1'], dtype='object'))
            print("No BL arrays have been created in the last 6 hours.")

        # get latest pad info

        b = str(
            "select se.SE_TIMESTAMP ts1, se.SE_SUBJECT, "
            "sa.SLOG_ATTR_VALUE av1, se.SE_ID se1, se.SE_SHIFTACTIVITY "
            "from alma.SHIFTLOG_ENTRIES se, alma.SLOG_ENTRY_ATTR sa "
            "WHERE se.SE_TYPE=1 and se.SE_TIMESTAMP > SYSDATE - 2. "
            "and sa.SLOG_SE_ID = se.SE_ID and sa.SLOG_ATTR_TYPE = 12 "
            "and se.SE_LOCATION='OSF-AOS'"
        )

        try:
            self._cursor.execute(b)
            self._shifts = pd.DataFrame(
                self._cursor.fetchall(),
                columns=[rec[0] for rec in self._cursor.description]
            ).sort('TS1', ascending=False)
        except ValueError:
            self._shifts = pd.DataFrame(
                columns=pd.Index(
                    [u'TS1', u'AV1', u'SE_ARRAYNAME', u'SE1'], dtype='object'))
            print("No shiftlogs have been created in the last 6 hours.")

        last_shift = self._shifts[
            self._shifts.SE1 == self._shifts.iloc[0].SE1].copy()
        last_shift['AV1'] = last_shift.AV1.str.split(':')
        ante = last_shift.apply(lambda x: x['AV1'][0], axis=1)
        pads = last_shift.apply(lambda x: x['AV1'][1], axis=1)
        self._ante_pad = pd.DataFrame({'antenna': ante, 'pad': pads})

    def _get_bl_prop(self, array_name):

        """

        :param array_name:
        """
        # In case a bl_array is selected
        if array_name not in CONF_LIM['minbase'].keys():
            id1 = self.bl_arrays.query(
                'SE_ARRAYNAME == "%s"' % array_name).iloc[0].SE1
            ap = self.bl_arrays.query(
                'SE_ARRAYNAME == "%s" and SE1 == %d' % (array_name, id1)
            )[['AV1']]

            ap.rename(columns={'AV1': 'antenna'}, inplace=True)
            ap = ap[ap.antenna.str.contains('CM') == False]
            conf = pd.merge(ap, self._ante_pad,
                            left_on='antenna', right_on='antenna')[
                ['pad', 'antenna']]
            conf_file = self._data_path + '%s.txt' % array_name
            conf.to_csv(conf_file, header=False,
                        index=False, sep=' ')
            ac = rUV.ac.ArrayConfigurationCasaFile()
            ac.createCasaConfig(conf_file)
            ruv = rUV.computeRuv(conf_file + ".cfg")
            num_bl = len(ruv)
            num_ant = len(ap)
            array_ar = rUV.compute_array_ar(ruv)

        # If C36 is selected
        else:
            conf_file = (self._wto_path +
                         'conf/%s.cfg' % array_name)
            ruv = rUV.computeRuv(conf_file)
            # noinspection PyTypeChecker
            array_ar = CONFRES[array_name]
            num_bl = 36 * 35. / 2.
            num_ant = 36

        return array_ar, num_bl, num_ant, ruv


def calc_blfrac(arrayk, cycle, minbl, maxbl, ruv, selconf):

    if arrayk == "TWELVE-M" and selconf:
        bl_or = CYC_NA[cycle] * (CYC_NA[cycle] - 1.) / 2.
        use_ruv = ruv[(ruv >= minbl) & (ruv <= maxbl)]
        try:
            bl_frac = bl_or / len(use_ruv)
        except ZeroDivisionError:
            bl_frac = pd.np.Inf
        return bl_frac
    else:
        return pd.np.Inf


def calc_tsys(band, tsky, tau, airmass):

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


def calc_airmass(dec_el, transit=True):
    if transit:
        airmass = 1 / pd.np.cos(pd.np.radians(-23.0262015 - dec_el))
    else:
        if dec_el > 0:
            airmass = 1 / pd.np.cos(pd.np.radians(90. - dec_el))
        else:
            airmass = None
    return airmass
