import os
import sys
import DsaArrayResolutionCy3 as ARes
import cx_Oracle
import DsaTools3 as DsaTool
# import logging

from DsaGetCycle3 import get_all_apdm, get_apdm
from collections import namedtuple
from DsaXmlParsers3 import *
from DsaConverter3 import *

prj = '{Alma/ObsPrep/ObsProject}'
val = '{Alma/ValueTypes}'
sbl = '{Alma/ObsPrep/SchedBlock}'

pd.options.display.width = 200
pd.options.display.max_columns = 100

Range = namedtuple('Range', ['start', 'end'])

PHASE_I_STATUS = ["Phase1Submitted", "Approved"]

CYCLES_SQL = {"2011": "1", "2012": "2", "2013": "3", "2015": "5", "2016": "6"}
CYCLES = {"2011": ["2011.1", "2011.A"],
          "2012": ["2012.1", "2012.A"],
          "2013": ["2013.1", "2013.A"],
          "2015": ["2015.1", "2015.A"],
          "2016": ["2016.1", "2016.A"]}


# noinspection PyAttributeOutsideInit
class DsaDatabase3(object):

    """

    """

    def __init__(self, path=None, refresh_apdm=True,
                 cycles=("2012", "2013", "2015"), loadp1=False,
                 status=("Canceled", "Rejected", "ObservingTimedOut",
                         "Completed")):

        """

        Args:
            path:
            refresh_apdm:
            cycles:
            loadp1:
            status:
        """

        # Set Initial Variables
        self._refresh_apdm = refresh_apdm
        self._loadp1 = loadp1
        self.status = status

        self._cycles_sql = '['
        self._cycles = []
        for c in cycles:
            self._cycles_sql += CYCLES_SQL[c]
            self._cycles.extend(CYCLES[c])
        self._cycles_sql += ']'

        # Default Paths and Preferences
        self._dsa_path = os.environ['DSA']
        if path:
            self._data_path = path
        else:
            self._data_path = os.environ['APDM_C3']

        # Load Exception File
        try:
            self._exceptions = pd.read_csv(
                self._dsa_path + '/conf/ExceptionsScheduling.txt',
                sep=' ', error_bad_lines=False, skip_blank_lines=True,
                comment='#', header=None,
                names=['CODE', 'sbName', 'forced_confs'])
        except IOError:
            print "No exception file found"
            self._exceptions = None

        # Create Initial DataFrame for Obsproject Info
        self.obsproject = pd.DataFrame()
        self._ares = ARes.ArrayRes(self._dsa_path + 'conf/')

        self._sql1 = str(
            "SELECT obs1.PRJ_ARCHIVE_UID as OBSPROJECT_UID, obs1.PI, "
            "obs1.PRJ_NAME,"
            "CODE,PRJ_SCIENTIFIC_RANK,PRJ_VERSION,"
            "PRJ_LETTER_GRADE,DOMAIN_ENTITY_STATE as PRJ_STATUS,"
            "obs3.ARCHIVE_UID as OBSPROPOSAL_UID, obs4.DC_LETTER_GRADE,"
            "obs3.CYCLE "
            "FROM ALMA.BMMV_OBSPROJECT obs1, ALMA.OBS_PROJECT_STATUS obs2,"
            " ALMA.BMMV_OBSPROPOSAL obs3, ALMA.PROPOSAL obs4 "
            "WHERE regexp_like (CODE, '^201%s\..*\.[AST]') "
            "AND obs2.OBS_PROJECT_ID = obs1.PRJ_ARCHIVE_UID AND "
            "obs1.PRJ_ARCHIVE_UID = obs3.PROJECTUID AND "
            "obs4.ARCHIVE_UID = obs3.ARCHIVE_UID AND "
            "obs4.DC_LETTER_GRADE IN ('A', 'B', 'C')" % self._cycles_sql)
        self._conx_str = os.environ['CON_STR']
        con, cur = self.open_oracle_conn()

        try:
            self._sql_sbstates = str(
                "SELECT bs.PRJ_REF as OBSPROJECT_UID, bs.STATUS as SB_STATE,"
                "SB_ARCHIVE_UID as SB_UID, EXECUTION_COUNT as EXECOUNT, "
                "sbs.DOMAIN_ENTITY_STATE as SB_STATE2 "
                "FROM ALMA.MV_SCHEDBLOCK bs, ALMA.SCHED_BLOCK_STATUS sbs,"
                "ALMA.BMMV_OBSPROJECT obs "
                "WHERE bs.SB_ARCHIVE_UID = sbs.DOMAIN_ENTITY_ID "
                "AND obs.PRJ_ARCHIVE_UID = bs.PRJ_REF "
                "AND regexp_like (obs.PRJ_CODE, '^201%s\..*\.[AST]')" %
                self._cycles_sql)
            cur.execute(self._sql_sbstates)
            self.sb_status = pd.DataFrame(
                cur.fetchall(),
                columns=[rec[0] for rec in cur.description]
            ).set_index('SB_UID', drop=False)
            self.sb_status['EXECOUNT'] = self.sb_status.EXECOUNT.astype(float)

            # self.qa0: QAO flags for observed SBs
            # Query QA0 flags from AQUA tables
            self._sqlqa0 = str(
                "SELECT aqua.SCHEDBLOCKUID as SB_UID, aqua.EXECBLOCKUID, "
                "aqua.STARTTIME, aqua.ENDTIME, "
                "aqua.QA0STATUS, shift.SE_STATUS, "
                "shift.SE_PROJECT_CODE, shift.SE_ARRAYENTRY_ID, "
                "aqua.FINALCOMMENTID "
                "FROM ALMA.AQUA_V_EXECBLOCK aqua, ALMA.SHIFTLOG_ENTRIES shift "
                "WHERE regexp_like "
                "(aqua.OBSPROJECTCODE, '^201%s\..*\.[AST]') "
                "AND aqua.EXECBLOCKUID = shift.SE_EB_UID" % self._cycles_sql)

            # noinspection SqlResolve
            self._sqlqa0com = str(
                "SELECT aqua.FINALCOMMENTID, "
                "DBMS_LOB.SUBSTR(acom.CCOMMENT, 200) as COMENT "
                "FROM ALMA.AQUA_V_EXECBLOCK aqua, ALMA.AQUA_COMMENT acom "
                "WHERE regexp_like "
                "(aqua.OBSPROJECTCODE, '^201%s\..*\.[AST]') "
                "AND aqua.FINALCOMMENTID = acom.COMMENTID" % self._cycles_sql
            )

            cur.execute(self._sqlqa0)
            self.aqua_execblock = pd.DataFrame(
                cur.fetchall(),
                columns=[rec[0] for rec in cur.description]
            ).set_index('SB_UID', drop=False)

            cur.execute(self._sqlqa0com)
            self._execblock_comm = pd.DataFrame(
                cur.fetchall(),
                columns=[rec[0] for rec in cur.description]
            ).set_index('FINALCOMMENTID', drop=False)

            self.aqdeb = self.aqua_execblock.copy()

            self.aqua_execblock = pd.merge(
                self.aqua_execblock, self._execblock_comm, on='FINALCOMMENTID',
                how='left').set_index('SB_UID', drop=False)

            self.aqua_execblock['delta'] = (self.aqua_execblock.ENDTIME -
                                            self.aqua_execblock.STARTTIME)
            self.aqua_execblock['delta'] = self.aqua_execblock.apply(
                lambda x: x['delta'].total_seconds() / 3600., axis=1
            )

            self.qastatus = self.aqua_execblock.query(
                'QA0STATUS in ["Unset", "Pass"]'
                ).groupby(
                ['SB_UID', 'QA0STATUS']).QA0STATUS.count().unstack().fillna(0)

            if 'Pass' not in self.qastatus.columns.values:
                self.qastatus['Pass'] = 0
            if 'Unset' not in self.qastatus.columns.values:
                self.qastatus['Unset'] = 0

            self.qastatus['Observed'] = self.qastatus.Unset + self.qastatus.Pass
            self.qastatus['ebTime'] = self.aqua_execblock.query(
                    'SE_STATUS == "SUCCESS"').groupby('SB_UID').delta.mean()

            last_info = self.aqua_execblock.query(
                'delta >= 0.2 and SE_STATUS == "SUCCESS" and '
                'QA0STATUS in ["Pass", "Unset"]').sort_values(
                by='STARTTIME', ascending=True
            ).drop_duplicates('SB_UID',  keep='last').copy()

            self.qastatus['last_observed'] = last_info['STARTTIME']
            self.qastatus['last_qa0'] = last_info['QA0STATUS']
            self.qastatus['last_status'] = last_info['SE_STATUS']

            # Query for Executives
            self._sql_executive = str(
                "SELECT PROJECTUID as OBSPROJECT_UID, ASSOCIATEDEXEC "
                "FROM ALMA.BMMV_OBSPROPOSAL "
                "WHERE regexp_like (CYCLE, '^201%s.[1A]')" % self._cycles_sql)
            cur.execute(self._sql_executive)
            self.executive = pd.DataFrame(
                cur.fetchall(), columns=['OBSPROJECT_UID', 'EXEC'])

            self._sql_obstatus_exec = str(
                    "SELECT obs1.ARCHIVE_UID as SB_UID,"
                    "obs1.PRJ_REF as OBSPROJECT_UID, obs1.SB_NAME, "
                    "obs1.STATUS as SB_STATE, obs1.EXECUTION_COUNT "
                    "FROM ALMA.BMMV_SCHEDBLOCK obs1, ALMA.BMMV_OBSPROJECT obs2 "
                    "WHERE obs1.PRJ_REF = obs2.PRJ_ARCHIVE_UID "
                    "AND regexp_like (obs2.PRJ_CODE, '^201%s\..*\.[AST]')" %
                    self._cycles_sql
                )

            self._c1 = np.sqrt(self._ares.data[0][1] * self._ares.data[0][2])
            self._c2 = np.sqrt(self._ares.data[1][1] * self._ares.data[1][2])
            self._c3 = np.sqrt(self._ares.data[2][1] * self._ares.data[2][2])
            self._c4 = np.sqrt(self._ares.data[3][1] * self._ares.data[3][2])
            self._c5 = np.sqrt(self._ares.data[4][1] * self._ares.data[4][2])
            self._c6 = np.sqrt(self._ares.data[5][1] * self._ares.data[5][2])
            self._c7 = np.sqrt(self._ares.data[6][1] * self._ares.data[6][2])
            self._c8 = np.sqrt(self._ares.data[7][1] * self._ares.data[7][2])
            self._listconf = [
                self._c1, self._c2, self._c3, self._c4, self._c5, self._c6,
                self._c7, self._c8]
            self.start_apa()
        finally:
            cur.close()
            con.close()

    def open_oracle_conn(self):

        """

        Returns:
            object:

        """
        connection = cx_Oracle.connect(self._conx_str, threaded=True)
        cursor = connection.cursor()
        return connection, cursor

    def start_apa(self, update_arch=False):

        """

        Args:
            update_arch:

        """
        if update_arch:
            self.update_from_archive()

        # noinspection PyUnusedLocal
        status = self.status
        # noinspection PyUnusedLocal
        cycles = self._cycles
        con, cur = self.open_oracle_conn()

        try:
            # Query for Projects, from BMMV.
            cur.execute(self._sql1)
            self._df1 = pd.DataFrame(
                cur.fetchall(),
                columns=[rec[0] for rec in cur.description])

            self._df1 = self._df1.query(
                'CYCLE in @cycles and '
                'DC_LETTER_GRADE == ["A", "B", "C"]').copy()

            self.projects = pd.merge(
                self._df1.query('PRJ_STATUS not in @status'), self.executive,
                on='OBSPROJECT_UID'
            ).set_index('CODE', drop=False)

            self.projects['xmlfile'] = self.projects.apply(
                lambda r: r['OBSPROJECT_UID'].replace('://', '___').replace(
                    '/', '_') + '.xml', axis=1
            )
            self.projects['phase'] = self.projects.apply(
                lambda r: 'I' if r['PRJ_STATUS'] in PHASE_I_STATUS else 'II',
                axis=1
            )

            if not self._loadp1:
                self.projects = self.projects.query('phase == "II"').copy()

            if self._refresh_apdm:
                print("Downloading APDM data for %d projects...\n" %
                      len(self.projects))
                phase1uids = self.projects.query(
                    'phase == "I"').OBSPROJECT_UID.unique()
                get_all_apdm(cur, self._data_path,
                             self.projects.OBSPROJECT_UID.unique(),
                             phase1uids)

            self._load_obsprojects(path=self._data_path + 'obsproject/')
            self._load_sciencegoals()
            self._load_sblocks_meta()
            self._load_schedblocks()
            self._add_imaging_param()
            self.correct_specscan_times()
            self._create_extrainfo()
        finally:
            cur.close()
            con.close()

    def _add_imaging_param(self):

        """

        """
        self._schedblocks_temp['assumedconf_ar_ot'] = (
            self._schedblocks_temp.minAR_ot / 0.9) * \
            self._schedblocks_temp.repfreq / 100.
        self._schedblocks_temp['OT_BestConf'] = self._schedblocks_temp.apply(
            lambda x: self._ares.array[
                DsaTool.find_array(x['assumedconf_ar_ot'], self._listconf)] if
            x['array'] == "TWELVE-M" else "N/A",
            axis=1)
        ar = self._schedblocks_temp.apply(lambda x: self._get_ar_lim(x), axis=1)
        # noinspection PyUnresolvedReferences
        self.schedblocks = pd.concat(
            [self._schedblocks_temp, ar], axis=1).set_index(
            'SB_UID', drop=False)
        self.schedblocks['SG_ID'] = self.schedblocks.SG_ID.astype(
            'str', inplace=True)

    def _create_extrainfo(self):

        """

        """
        target_tables_temp = pd.merge(
            self.orderedtar.query('name != "Calibrators"'),
            self.target, on=['SB_UID', 'targetId'])
        target_tables_temp2 = pd.merge(
            target_tables_temp, self.scienceparam, on=['SB_UID', 'paramRef'])

        target_tables_temp3 = pd.merge(
            target_tables_temp2, self.fieldsource[
                ['SB_UID', 'fieldRef', 'name', 'RA', 'DEC', 'isQuery', 'use',
                 'solarSystem', 'isMosaic', 'pointings', 'ephemeris']
            ], on=['SB_UID', 'fieldRef'],
            suffixes=['_target', '_so'])

        self.target_tables = target_tables_temp3.copy().set_index(
            'targetId', drop=False)

        sb_target_num = self.target_tables.groupby('SB_UID').agg(
            {'fieldRef': pd.Series.nunique, 'pointings': pd.Series.max,
             'targetId': pd.Series.nunique, 'paramRef': pd.Series.nunique,
             'specRef': pd.Series.nunique}).reset_index()
        self.multi_point_su = sb_target_num.query(
            'pointings > 1').SB_UID.unique()
        self.multi_field_su = sb_target_num.query(
            'fieldRef > 1').SB_UID.unique()
        self.ephem_su = self.target_tables.query(
            'solarSystem != "Unspecified"').SB_UID.unique()

    def _load_obsprojects(self, path):

        """

        Args:
            path:
        """
        projt = []

        for r in self.projects.iterrows():
            xml = r[1].xmlfile
            proj = self._read_obsproject(xml, path)
            projt.append(proj)

        projt_arr = np.array(projt, dtype=object)
        self.obsproject = pd.DataFrame(
            projt_arr,
            columns=['CODE', 'OBSPROJECT_UID', 'OBSPROPOSAL_UID',
                     'OBSREVIEW_UID', 'VERSION',
                     'NOTE', 'IS_CALIBRATION', 'IS_DDT']
        ).set_index('OBSPROJECT_UID', drop=False)

    @staticmethod
    def _read_obsproject(xml, path):

        """

        Args:
            xml:
            path:

        Returns:

        """
        try:
            obsparse = ObsProject(xml, path)
        except KeyError:
            print("Something went wrong while trying to parse %s" % xml)
            return 0

        return obsparse.get_info()

    def _load_sciencegoals(self):

        """

        """
        sgt = []
        tart = []
        visitt = []
        temp_paramt = []
        sgspwt = []
        sgspsct = []

        for r in self.obsproject.iterrows():
            obsproject_uid = r[1].OBSPROJECT_UID
            obsproposal_uid = r[1].OBSPROPOSAL_UID
            if obsproject_uid is None:
                continue
            xml = obsproposal_uid.replace('://', '___').replace('/', '_')
            xml += '.xml'
            code = r[1].CODE
            obspropparse = self._read_sciencegoal(code, xml, obsproject_uid)

            if obspropparse == 0:
                continue

            sgt.extend(obspropparse.sciencegoals)
            tart.extend(obspropparse.sg_targets)
            if len(obspropparse.sg_specscan) > 0:
                sgspsct.extend(obspropparse.sg_specscan)
            if len(obspropparse.sg_specwindows) > 0:
                sgspwt.extend(obspropparse.sg_specwindows)
            if len(obspropparse.visits) > 0:
                visitt.extend(obspropparse.visits)
            if len(obspropparse.temp_param) > 0:
                temp_paramt.extend(obspropparse.temp_param)

        sgt_arr = np.array(sgt, dtype=object)
        tart_arr = np.array(tart, dtype=object)
        visitt_arr = np.array(visitt, dtype=object)
        temp_paramt_arr = np.array(temp_paramt, dtype=object)
        sgspwt_arr = np.array(sgspwt, dtype=object)
        sgspsct_arr = np.array(sgspsct, dtype=object)

        self.sciencegoals = pd.DataFrame(
            sgt_arr,
            columns=['SG_ID', 'OBSPROJECT_UID', 'OUS_ID', 'sg_name', 'band',
                     'estimatedTime', 'est12Time', 'estACATime',
                     'est7Time', 'eTPTime',
                     'AR', 'LAS', 'ARcor', 'LAScor', 'sensitivity',
                     'sensitivityFrequencyWidthRef', 'sensitivityMeasure',
                     'useACA', 'useTP', 'isTimeConstrained', 'repFreq',
                     'repFreq_spec', 'singleContFreq', 'isCalSpecial',
                     'isPointSource', 'polarization', 'isSpectralScan', 'type',
                     'hasSB', 'dummy', 'num_targets', 'mode']
        ).set_index('SG_ID', drop=False)

        self.sg_targets = pd.DataFrame(
            tart_arr,
            columns=['TARG_ID', 'OBSPROJECT_UID', 'SG_ID', 'tarType',
                     'solarSystem', 'sourceName', 'RA', 'DEC',
                     'offra', 'offdec', 'isMosaic',
                     'isRect', 'PA', 'rect_width',
                     'rect_height', 'rect_spacing', 'mos_refsys',
                     'centerVel', 'centerVel_units', 'centerVel_refsys',
                     'centerVel_doppler', 'lineWidth']
        ).set_index('TARG_ID', drop=False)

        self.visits = pd.DataFrame(
            visitt_arr,
            columns=['SG_ID', 'sgName', 'OBSPROJECT_UID', 'startTime', 'margin',
                     'margin_unit', 'note', 'avoidConstraint', 'priority',
                     'visit_id', 'prev_visit_id', 'requiredDelay',
                     'requiredDelay_unit', 'fixedStart']
        )

        self.temp_param = pd.DataFrame(
            temp_paramt_arr,
            columns=['SG_ID', 'sgName', 'OBSPROJECT_UID', 'startTime',
                     'endTime', 'margin', 'margin_unit', 'repeats', 'LSTmin',
                     'LSTmax', 'note', 'avoidConstraint', 'priority',
                     'fixedStart']
        )

        self.sg_spw = pd.DataFrame(
            sgspwt_arr,
            columns=['SG_ID', 'SPW_ID', 'transitionName', 'centerFrequency',
                     'bandwidth', 'spectralRes', 'isRepSPW', 'isSkyFreq',
                     'group_index', 'corr_ord', 'smoothingFunct',
                     'smoothingFact']
        )
        self.sg_specscan = pd.DataFrame(
            sgspsct_arr,
            columns=['SG_ID', 'SSCAN_ID', 'startFrequency', 'endFrequency',
                     'bandwidth', 'spectralRes', 'isSkyFreq', 'smoothingFact']
        )

    def _read_sciencegoal(self, code, xml, obsproject_uid):

        """

        Args:
            code:
            xml:
            obsproject_uid:

        Returns:
            obsproparse:

        """
        try:
            if self.projects.ix[code, 'phase'] == 'I':
                obspropparse = ObsProposal(
                    xml, obsproject_uid, self._data_path + 'obsproposal/')
                obspropparse.get_sg()
            else:
                # print "Processing Phase II %s" % r[1].CODE
                xml = obsproject_uid.replace('://', '___').replace(
                    '/', '_')
                xml += '.xml'
                obspropparse = ObsProject(
                    xml, self._data_path + 'obsproject/')
                obspropparse.get_sg()
            return obspropparse
        except IOError:
            print("Something went wrong while trying to parse %s" % xml)
            return 0

    def _load_sblocks_meta(self):

        """

        """

        sbt = []
        for r in self.obsproject.iterrows():

            code = r[1].CODE
            phase = self.projects.ix[code, 'phase']
            parse = self._read_sblock_meta(phase, r)

            if parse == 0:
                continue
            sbt.extend(parse.sg_sb)

        sbt_arr = np.array(sbt, dtype=object)

        self.sblocks = pd.DataFrame(
            sbt_arr,
            columns=['SB_UID', 'OBSPROJECT_UID', 'ous_name', 'OUS_ID',
                     'GOUS_ID',
                     'gous_name', 'MOUS_ID', 'mous_name',
                     'array', 'execount']
        ).set_index('SB_UID', drop=False)

        self.sblocks['sg_name'] = self.sblocks.ous_name.str.replace(
            "SG OUS \(", "")
        self.sblocks['sg_name'] = self.sblocks.sg_name.str.slice(0, -1)

    def _read_sblock_meta(self, phase, r):

        """

        Args:
            phase:
            r:

        Returns:
            obsparse:

        """
        if phase == 'I':
            obsreview_uid = r[1].OBSREVIEW_UID
            if obsreview_uid is None:
                return 0
            xml = obsreview_uid.replace('://', '___').replace('/', '_')
            xml += '.xml'
            try:
                parse = ObsReview(xml, self._data_path + 'obsreview/')
                parse.get_sg_sb()
            except IOError:
                print("Something went wrong while trying to parse %s" % xml)
                return 0
        else:
            obsproject_uid = r[1].OBSPROJECT_UID
            xml = obsproject_uid.replace('://', '___').replace('/', '_')
            xml += '.xml'
            try:
                parse = ObsProject(xml, self._data_path + 'obsproject/')
                parse.get_sg_sb()
            except IOError:
                print("Something went wrong while trying to parse %s" % xml)
                return 0

        return parse

    def _load_schedblocks(self, sb_path='schedblock/'):

        """

        Args:
            sb_path:

        """

        path = self._data_path + sb_path

        rst = []
        rft = []
        tart = []
        spwt = []
        bbt = []
        spct = []
        scpart = []
        acpart = []
        bcpart = []
        pcpart = []
        ordtart = []
        polcpart = []
        delaycpart = []
        checkcpart = []
        sys.stdout.write("Processing Phase II SBs ")
        sys.stdout.flush()

        c = 10
        i = 0
        n = len(self.sblocks)
        for sg_sb in self.sblocks.iterrows():
            i += 1
            if (100. * i / n) > c:
                sys.stdout.write('.')
                sys.stdout.flush()
                c += 10

            xmlf = sg_sb[1].SB_UID.replace('://', '___')
            xmlf = xmlf.replace('/', '_') + '.xml'

            sb1 = SchedBlock(
                xmlf, sg_sb[1].SB_UID, sg_sb[1].OBSPROJECT_UID, sg_sb[1].OUS_ID,
                sg_sb[1].sg_name, path)

            rs, rf, tar, spc, bb, spw, scpar, acpar, bcpar, pcpar, ordtar, \
                polcpar, delaycpar, checkcpar = \
                sb1.read_schedblocks()

            rst.append(rs)
            rft.extend(rf)
            tart.extend(tar)
            spct.extend(spc)
            bbt.extend(bb)
            spwt.extend(spw)
            scpart.extend(scpar)
            acpart.extend(acpar)
            bcpart.extend(bcpar)
            pcpart.extend(pcpar)
            ordtart.extend(ordtar)
            polcpart.extend(polcpar)
            delaycpart.extend(delaycpar)
            checkcpart.extend(checkcpar)

        sys.stdout.write("\nDone!\n")
        sys.stdout.flush()

        rst_arr = np.array(rst, dtype=object)
        rft_arr = np.array(rft, dtype=object)
        tart_arr = np.array(tart, dtype=object)
        spct_arr = np.array(spct, dtype=object)
        bbt_arr = np.array(bbt, dtype=object)
        spwt_arr = np.array(spwt, dtype=object)
        scpart_arr = np.array(scpart, dtype=object)
        acpart_arr = np.array(acpart, dtype=object)
        bcpart_arr = np.array(bcpart, dtype=object)
        pcpart_arr = np.array(pcpart, dtype=object)
        ordtart_arr = np.array(ordtart, dtype=object)
        polcpart_arr = np.array(polcpart, dtype=object)
        delaycpart_arr = np.array(delaycpart, dtype=object)
        checkcpart_arr = np.array(checkcpart, dtype=object)

        self._schedblocks_temp = pd.DataFrame(
            rst_arr,
            columns=['SB_UID', 'OBSPROJECT_UID', 'sgName', 'OUS_ID',
                     'sbName', 'sbNote', 'sbStatusXml', 'repfreq', 'band',
                     'array',
                     'RA', 'DEC', 'minAR_ot', 'maxAR_ot', 'execount',
                     'isPolarization', 'maxPWVC', 'array12mType',
                     'estimatedTime', 'maximumTime'],
        ).set_index('SB_UID', drop=False)

        tof = ['repfreq', 'RA', 'DEC', 'minAR_ot', 'maxAR_ot', 'maxPWVC']
        self._schedblocks_temp[tof] = self._schedblocks_temp[tof].astype(float)
        self._schedblocks_temp[['execount']] = self._schedblocks_temp[
            ['execount']].astype(int)

        self.scienceparam = pd.DataFrame(
            scpart_arr,
            columns=['paramRef', 'SB_UID', 'parName', 'representative_bw',
                     'sensitivy', 'sensUnit', 'intTime', 'subScanDur']
        ).set_index('paramRef', drop=False)

        self.ampcalparam = pd.DataFrame(
            acpart_arr,
            columns=['paramRef', 'SB_UID', 'parName', 'intTime',
                     'subScanDur']
        ).set_index('paramRef', drop=False)

        self.bbandcalparam = pd.DataFrame(
            bcpart_arr,
            columns=['paramRef', 'SB_UID', 'parName', 'intTime',
                     'subScanDur']
        ).set_index('paramRef', drop=False)

        self.phasecalparam = pd.DataFrame(
            pcpart_arr,
            columns=['paramRef', 'SB_UID', 'parName', 'intTime',
                     'subScanDur']
        ).set_index('paramRef', drop=False)

        self.orderedtar = pd.DataFrame(
            ordtart_arr,
            columns=['targetId', 'SB_UID', 'indObs', 'name']
        ).set_index('targetId', drop=False)

        self.polcalparam = pd.DataFrame(
            polcpart_arr,
            columns=['paramRef', 'SB_UID', 'parName', 'intTime',
                     'subScanDur']
        ).set_index('paramRef', drop=False)

        self.delaycalparam = pd.DataFrame(
            delaycpart_arr,
            columns=['paramRef', 'SB_UID', 'parName', 'intTime',
                     'subScanDur']
        ).set_index('paramRef', drop=False)

        self.checkcalparam = pd.DataFrame(
            checkcpart_arr,
            columns=['paramRef', 'SB_UID', 'parName', 'intTime',
                     'subScanDur']
        ).set_index('paramRef', drop=False)

        self.fieldsource = pd.DataFrame(
            rft_arr,
            columns=['fieldRef', 'SB_UID', 'solarSystem', 'sourcename',
                     'name', 'RA', 'DEC', 'isQuery', 'intendedUse', 'qRA',
                     'qDEC', 'use', 'search_radius', 'rad_unit',
                     'ephemeris', 'pointings', 'isMosaic', 'arraySB']
        ).set_index('fieldRef', drop=False)

        self.target = pd.DataFrame(
            tart_arr,
            columns=['targetId', 'SB_UID', 'specRef', 'fieldRef',
                     'paramRef']).set_index('targetId', drop=False)

        self.spectralconf = pd.DataFrame(
            spct_arr,
            columns=['specRef', 'SB_UID', 'Name', 'BaseBands', 'SPWs']
        ).set_index('specRef', drop=False)

        self.spectralconf[['BaseBands', 'SPWs']] = self.spectralconf[
            ['BaseBands', 'SPWs']].astype(int)

        self.baseband = pd.DataFrame(
            bbt_arr,
            columns=['basebandRef', 'specRef', 'SB_UID', 'Name',
                     'CenterFreq', 'FreqSwitching', 'l02Freq',
                     'Weighting', 'useUDB']
        ).set_index('basebandRef', drop=False)

        tof = ['CenterFreq', 'l02Freq', 'Weighting']
        tob = ['FreqSwitching', 'useUDB']

        self.baseband[tof] = self.baseband[tof].astype(float)
        self.baseband[tob] = self.baseband[tob].astype(bool)

        tof = ['CenterFreq', 'EffectiveBandwidth', 'lineRestFreq']
        toi = ['AveragingFactor', 'EffectiveChannels']
        tob = ['Use']

        self.spectralwindow = pd.DataFrame(
            spwt_arr,
            columns=['basebandRef', 'SB_UID', 'Name',
                     'SideBand', 'WindowsFunction',
                     'CenterFreq', 'AveragingFactor',
                     'EffectiveBandwidth', 'EffectiveChannels', 'lineRestFreq',
                     'lineName', 'Use'],
        ).set_index('basebandRef', drop=False)

        self.spectralwindow[tof] = self.spectralwindow[tof].astype(float)
        self.spectralwindow[toi] = self.spectralwindow[toi].astype(int)
        self.spectralwindow[tob] = self.spectralwindow[tob].astype(bool)

    def _update_apdm(self, obsproject_uid):

        """

        Args:
            obsproject_uid:
        """
        con, cur = self.open_oracle_conn()
        try:
            proj_xmlfile = get_apdm(cur, self._data_path, obsproject_uid)
            proj = [self._read_obsproject(
                proj_xmlfile, self._data_path + 'obsproject/')]
            projt_arr = np.array(proj, dtype=object)
            obsproject = pd.DataFrame(
                projt_arr,
                columns=['CODE', 'OBSPROJECT_UID', 'OBSPROPOSAL_UID',
                         'OBSREVIEW_UID', 'VERSION',
                         'NOTE', 'IS_CALIBRATION', 'IS_DDT']
            ).set_index('OBSPROJECT_UID', drop=False)

            self.obsproject.update(obsproject)
            self.update_from_archive()
            self._update_sciencegoal(obsproject_uid)
            self._update_sblock_meta(obsproject_uid)
            self._update_schedblock(obsproject_uid)
            self._add_imaging_param()
            self.correct_specscan_times()
            self._create_extrainfo()
        finally:
            cur.close()
            con.close()

    def _update_sciencegoal(self, obsproject_uid):

        """

        Args:
            obsproject_uid:
        """
        obsproposal_uid = self.obsproject.ix[obsproject_uid, 'OBSPROPOSAL_UID']
        prop_xmlfile = obsproposal_uid.replace('://', '___').replace('/', '_')
        prop_xmlfile += '.xml'
        code = self.obsproject.ix[obsproject_uid, 'CODE']
        obspropparse = self._read_sciencegoal(
            code, prop_xmlfile, obsproject_uid)
        for sg in obspropparse.sciencegoals:
            self.sciencegoals.ix[sg[0]] = np.array(sg, dtype=object)

        # noinspection PyUnusedLocal
        sg_ids = self.sciencegoals.query(
            'OBSPROJECT_UID in @obsproject_uid').SG_ID.unique()

        for tar in obspropparse.sg_targets:
            self.sg_targets.ix[tar[0]] = np.array(tar, dtype=object)

        self.sg_spw.drop(self.sg_spw.query('SG_ID in @sg_ids').index.values,
                         inplace=True, errors='ignore')
        spw = pd.DataFrame(
            np.array(obspropparse.sg_specwindows, dtype=object),
            columns=['SG_ID', 'SPW_ID', 'transitionName', 'centerFrequency',
                     'bandwidth', 'spectralRes', 'isRepSPW', 'isSkyFreq',
                     'group_index']
        )
        self.sg_spw = self.sg_spw.append(spw, ignore_index=True)

        if len(obspropparse.sg_specscan) > 0:
            self.sg_specscan.drop(
                self.sg_specscan.query('SG_ID in @sg_ids').index.values,
                inplace=True, errors='ignore')
            specscan = pd.DataFrame(
                np.array(obspropparse.sg_specscan, dtype=object),
                columns=['SG_ID', 'SSCAN_ID', 'startFrequency', 'endFrequency',
                         'bandwidth', 'spectralRes', 'isSkyFreq']
            )
            self.sg_specscan = self.sg_specscan.append(
                specscan, ignore_index=True)

        if len(obspropparse.visits) > 0:
            self.visits.drop(
                self.visits.query('SG_ID in @sg_ids').index.values,
                inplace=True, errors='ignore')
            visit = pd.DataFrame(
                np.array(obspropparse.visits, dtype=object),
                columns=[
                    'SG_ID', 'sgName', 'OBSPROJECT_UID', 'startTime', 'margin',
                    'margin_unit', 'note', 'avoidConstraint', 'priority',
                    'visit_id', 'prev_visit_id', 'requiredDelay',
                    'requiredDelay_unit', 'fixedStart']
            )
            self.visits = self.visits.append(visit, ignore_index=True)

        if len(obspropparse.temp_param) > 0:
            self.temp_param.drop(
                self.temp_param.query('SG_ID in @sg_ids').index.values,
                inplace=True, errors='ignore')
            temppar = pd.DataFrame(
                np.array(obspropparse.temp_param, dtype=object),
                columns=[
                    'SG_ID', 'sgName', 'OBSPROJECT_UID', 'startTime',
                    'endTime', 'margin', 'margin_unit', 'repeats', 'LSTmin',
                    'LSTmax', 'note', 'avoidConstraint', 'priority',
                    'fixedStart']
            )
            self.temp_param = self.temp_param.append(temppar, ignore_index=True)

    def _update_sblock_meta(self, obsproject_uid):
        """

        Args:
            obsproject_uid:
        """
        r = [0, None]
        r[1] = self.obsproject.ix[obsproject_uid]
        parse = self._read_sblock_meta('II', r)
        sbt = []
        sbt.extend(parse.sg_sb)
        sbt_arr = np.array(sbt, dtype=object)
        # print sbt_arr, sbt_arr.shape
        sblocks = pd.DataFrame(
            sbt_arr,
            columns=['SB_UID', 'OBSPROJECT_UID', 'ous_name', 'OUS_ID',
                     'GOUS_ID',
                     'gous_name', 'MOUS_ID', 'mous_name',
                     'array', 'execount']
        ).set_index('SB_UID', drop=False)
        sblocks['sg_name'] = sblocks.ous_name.str.replace(
            "SG OUS \(", "")
        sblocks['sg_name'] = sblocks.sg_name.str.slice(0, -1)
        self.sblocks.update(sblocks)

    def _update_schedblock(self, obsproject_uid, sb_path='schedblock/'):
        """

        Args:
            obsproject_uid:
            sb_path:
        """
        path = self._data_path + sb_path
        rst = []
        rft = []
        tart = []
        spwt = []
        bbt = []
        spct = []
        scpart = []
        acpart = []
        bcpart = []
        pcpart = []
        ordtart = []
        polcpart = []
        delaycpart = []
        checkcpart = []

        print "Updating SBs of %s." % obsproject_uid
        sb_uids = []

        for sg_sb in self.sblocks.query('OBSPROJECT_UID == @obsproject_uid'
                                        ).iterrows():
            sb_uids.append(sg_sb[1].SB_UID)
            xmlf = sg_sb[1].SB_UID.replace('://', '___')
            xmlf = xmlf.replace('/', '_') + '.xml'
            sb1 = SchedBlock(
                xmlf, sg_sb[1].SB_UID, sg_sb[1].OBSPROJECT_UID, sg_sb[1].OUS_ID,
                sg_sb[1].sg_name, path)
            rs, rf, tar, spc, bb, spw, scpar, acpar, bcpar, pcpar, ordtar,\
                polcpar, delaycpar, checkcpar = \
                sb1.read_schedblocks()
            rst.append(rs)
            rft.extend(rf)
            tart.extend(tar)
            spct.extend(spc)
            bbt.extend(bb)
            spwt.extend(spw)
            scpart.extend(scpar)
            acpart.extend(acpar)
            bcpart.extend(bcpar)
            pcpart.extend(pcpar)
            ordtart.extend(ordtar)
            polcpart.extend(polcpar)
            delaycpart.extend(delaycpar)
            checkcpart.extend(checkcpar)

        rst_arr = np.array(rst, dtype=object)
        rft_arr = np.array(rft, dtype=object)
        tart_arr = np.array(tart, dtype=object)
        spct_arr = np.array(spct, dtype=object)
        bbt_arr = np.array(bbt, dtype=object)
        spwt_arr = np.array(spwt, dtype=object)
        scpart_arr = np.array(scpart, dtype=object)
        acpart_arr = np.array(acpart, dtype=object)
        bcpart_arr = np.array(bcpart, dtype=object)
        pcpart_arr = np.array(pcpart, dtype=object)
        ordtart_arr = np.array(ordtart, dtype=object)
        polcpart_arr = np.array(polcpart, dtype=object)
        delaycpart_arr = np.array(delaycpart, dtype=object)
        checkcpart_arr = np.array(checkcpart, dtype=object)

        self._schedblocks_temp.drop(
            self._schedblocks_temp.query('SB_UID in @sb_uids').index.values,
            inplace=True, errors='ignore')
        rst_df = pd.DataFrame(
            rst_arr,
            columns=['SB_UID', 'OBSPROJECT_UID', 'sgName', 'OUS_ID',
                     'sbName', 'sbNote', 'sbStatusXml', 'repfreq', 'band',
                     'array',
                     'RA', 'DEC', 'minAR_ot', 'maxAR_ot', 'execount',
                     'isPolarization', 'maxPWVC', 'array12mType',
                     'estimatedTime', 'maximumTime'],
        ).set_index('SB_UID', drop=False)
        self._schedblocks_temp = self._schedblocks_temp.append(rst_df)
        tof = ['repfreq', 'RA', 'DEC', 'minAR_ot', 'maxAR_ot', 'maxPWVC']
        self._schedblocks_temp[tof] = self._schedblocks_temp[tof].astype(float)
        self._schedblocks_temp[['execount']] = self._schedblocks_temp[
            ['execount']].astype(int)

        self.scienceparam.drop(
            self.scienceparam.query('SB_UID in @sb_uids').index.values,
            inplace=True, errors='ignore')
        scienceparam = pd.DataFrame(
            scpart_arr,
            columns=['paramRef', 'SB_UID', 'parName', 'representative_bw',
                     'sensitivy', 'sensUnit', 'intTime', 'subScanDur']
        ).set_index('paramRef', drop=False)
        self.scienceparam = self.scienceparam.append(scienceparam)

        self.ampcalparam.drop(
            self.ampcalparam.query('SB_UID in @sb_uids').index.values,
            inplace=True, errors='ignore')
        ampcalparam = pd.DataFrame(
            acpart_arr,
            columns=['paramRef', 'SB_UID', 'parName', 'intTime',
                     'subScanDur']
        ).set_index('paramRef', drop=False)
        self.ampcalparam = self.ampcalparam.append(ampcalparam)

        self.bbandcalparam.drop(
            self.bbandcalparam.query('SB_UID in @sb_uids').index.values,
            inplace=True, errors='ignore')
        bbandcalparam = pd.DataFrame(
            bcpart_arr,
            columns=['paramRef', 'SB_UID', 'parName', 'intTime',
                     'subScanDur']
        ).set_index('paramRef', drop=False)
        self.bbandcalparam = self.bbandcalparam.append(bbandcalparam)

        self.phasecalparam.drop(
            self.phasecalparam.query('SB_UID in @sb_uids').index.values,
            inplace=True, errors='ignore')
        phasecalparam = pd.DataFrame(
            pcpart_arr,
            columns=['paramRef', 'SB_UID', 'parName', 'intTime',
                     'subScanDur']
        ).set_index('paramRef', drop=False)
        self.phasecalparam = self.phasecalparam.append(phasecalparam)

        self.delaycalparam = pd.DataFrame(
            delaycpart_arr,
            columns=['paramRef', 'SB_UID', 'parName', 'intTime',
                     'subScanDur']
        ).set_index('paramRef', drop=False)

        self.checkcalparam = pd.DataFrame(
            checkcpart_arr,
            columns=['paramRef', 'SB_UID', 'parName', 'intTime',
                     'subScanDur']
        ).set_index('paramRef', drop=False)

        self.orderedtar.drop(
            self.orderedtar.query('SB_UID in @sb_uids').index.values,
            inplace=True, errors='ignore')
        orderedtar = pd.DataFrame(
            ordtart_arr,
            columns=['targetId', 'SB_UID', 'indObs', 'name']
        ).set_index('targetId', drop=False)
        self.orderedtar = self.orderedtar.append(orderedtar)

        self.polcalparam.drop(
            self.polcalparam.query('SB_UID in @sb_uids').index.values,
            inplace=True, errors='ignore')
        polcalparam = pd.DataFrame(
            polcpart_arr,
            columns=['paramRef', 'SB_UID', 'parName', 'intTime',
                     'subScanDur']
        ).set_index('paramRef', drop=False)
        self.polcalparam = self.polcalparam.append(polcalparam)

        self.fieldsource.drop(
            self.fieldsource.query('SB_UID in @sb_uids').index.values,
            inplace=True, errors='ignore')
        fieldsource = pd.DataFrame(
            rft_arr,
            columns=['fieldRef', 'SB_UID', 'solarSystem', 'sourcename',
                     'name', 'RA', 'DEC', 'isQuery', 'intendedUse', 'qRA',
                     'qDEC', 'use', 'search_radius', 'rad_unit',
                     'ephemeris', 'pointings', 'isMosaic', 'arraySB']
        ).set_index('fieldRef', drop=False)
        self.fieldsource = self.fieldsource.append(fieldsource)

        self.target.drop(
            self.target.query('SB_UID in @sb_uids').index.values,
            inplace=True, errors='ignore')
        target = pd.DataFrame(
            tart_arr,
            columns=['targetId', 'SB_UID', 'specRef', 'fieldRef',
                     'paramRef']).set_index('targetId', drop=False)
        self.target = self.target.append(target)

        self.spectralconf.drop(
            self.spectralconf.query('SB_UID in @sb_uids').index.values,
            inplace=True, errors='ignore')
        spectralconf = pd.DataFrame(
            spct_arr,
            columns=['specRef', 'SB_UID', 'Name', 'BaseBands', 'SPWs']
        ).set_index('specRef', drop=False)
        self.spectralconf = self.spectralconf.append(spectralconf)
        self.spectralconf[['BaseBands', 'SPWs']] = self.spectralconf[
            ['BaseBands', 'SPWs']].astype(int)

        self.baseband.drop(
            self.baseband.query('SB_UID in @sb_uids').index.values,
            inplace=True, errors='ignore')
        baseband = pd.DataFrame(
            bbt_arr,
            columns=['basebandRef', 'specRef', 'SB_UID', 'Name',
                     'CenterFreq', 'FreqSwitching', 'l02Freq',
                     'Weighting', 'useUDB']
        ).set_index('basebandRef', drop=False)
        self.baseband = self.baseband.append(baseband)
        tof = ['CenterFreq', 'l02Freq', 'Weighting']
        tob = ['FreqSwitching', 'useUDB']
        self.baseband[tof] = self.baseband[tof].astype(float)
        self.baseband[tob] = self.baseband[tob].astype(bool)

        self.spectralwindow.drop(
            self.spectralwindow.query('SB_UID in @sb_uids').index.values,
            inplace=True, errors='ignore')
        spectralwindow = pd.DataFrame(
            spwt_arr,
            columns=['basebandRef', 'SB_UID', 'Name',
                     'SideBand', 'WindowsFunction',
                     'CenterFreq', 'AveragingFactor',
                     'EffectiveBandwidth', 'EffectiveChannels', 'lineRestFreq',
                     'lineName', 'Use'],
        ).set_index('basebandRef', drop=False)
        self.spectralwindow = self.spectralwindow.append(spectralwindow)
        tof = ['CenterFreq', 'EffectiveBandwidth', 'lineRestFreq']
        toi = ['AveragingFactor', 'EffectiveChannels']
        tob = ['Use']
        self.spectralwindow[tof] = self.spectralwindow[tof].astype(float)
        self.spectralwindow[toi] = self.spectralwindow[toi].astype(int)
        self.spectralwindow[tob] = self.spectralwindow[tob].astype(bool)

    def update_from_archive(self):

        """

        """
        con, cur = self.open_oracle_conn()
        try:
            cur.execute(self._sql1)
            self._df1 = pd.DataFrame(
                cur.fetchall(),
                columns=[rec[0] for rec in cur.description])

            cur.execute(self._sql_executive)
            self.executive = pd.DataFrame(
               cur.fetchall(), columns=['OBSPROJECT_UID', 'EXEC'])

            self._old_projects = self.projects.copy()
        finally:
            cur.close()
            con.close()

        # noinspection PyUnusedLocal
        status = self.status
        # noinspection PyUnusedLocal
        cycles = self._cycles
        self._df1 = self._df1.query(
                'CYCLE in @cycles and '
                'DC_LETTER_GRADE == ["A", "B", "C"]').copy()

        self.projects = pd.merge(
            self._df1.query('PRJ_STATUS not in @status'), self.executive,
            on='OBSPROJECT_UID'
        ).set_index('CODE', drop=False)

        self.projects['xmlfile'] = self.projects.apply(
            lambda r: r['OBSPROJECT_UID'].replace('://', '___').replace(
                '/', '_') + '.xml', axis=1
        )
        self.projects['phase'] = self.projects.apply(
            lambda r: 'I' if r['PRJ_STATUS'] in PHASE_I_STATUS else 'II',
            axis=1
        )
        if not self._loadp1:
            self.projects = self.projects.query('phase == "II"').copy()

    def update_status(self):

        """

        """
        con, cur = self.open_oracle_conn()
        try:
            cur.execute(self._sqlqa0)
            self.aqua_execblock = pd.DataFrame(
                cur.fetchall(),
                columns=[rec[0] for rec in cur.description]).set_index(
                'SB_UID', drop=False)

            cur.execute(self._sqlqa0com)
            self._execblock_comm = pd.DataFrame(
                cur.fetchall(),
                columns=[rec[0] for rec in cur.description]
            ).set_index('FINALCOMMENTID', drop=False)

            self.aqua_execblock = pd.merge(
                self.aqua_execblock, self._execblock_comm, on='FINALCOMMENTID',
                how='left').set_index('SB_UID', drop=False)

            self.aqua_execblock['delta'] = (self.aqua_execblock.ENDTIME -
                                            self.aqua_execblock.STARTTIME)
            self.aqua_execblock['delta'] = self.aqua_execblock.apply(
                lambda x: x['delta'].total_seconds() / 3600., axis=1
            )

            self.qastatus = self.aqua_execblock.query(
                'QA0STATUS in ["Unset", "Pass"]'
                ).groupby(
                ['SB_UID', 'QA0STATUS']).QA0STATUS.count().unstack().fillna(0)

            if 'Pass' not in self.qastatus.columns.values:
                self.qastatus['Pass'] = 0
            if 'Unset' not in self.qastatus.columns.values:
                self.qastatus['Unset'] = 0

            self.qastatus['Observed'] = self.qastatus.Unset + self.qastatus.Pass
            self.qastatus['ebTime'] = self.aqua_execblock.query(
                'SE_STATUS == "SUCCESS" and QA0STATUS != ["Fail", "SemiPass"]'
            ).groupby('SB_UID').delta.mean()

            last_info = self.aqua_execblock.query(
                'delta >= 0.2 and SE_STATUS == "SUCCESS" and '
                'QA0STATUS in ["Pass", "Unset"]').sort_values(
                by='STARTTIME', ascending=True
            ).drop_duplicates('SB_UID',  keep='last').copy()

            self.qastatus['last_observed'] = last_info['STARTTIME']
            self.qastatus['last_qa0'] = last_info['QA0STATUS']
            self.qastatus['last_status'] = last_info['SE_STATUS']

            cur.execute(self._sql_sbstates)
            self.sb_status = pd.DataFrame(
                cur.fetchall(),
                columns=[rec[0] for rec in cur.description]
            ).set_index('SB_UID', drop=False)
            self.sb_status['EXECOUNT'] = self.sb_status.EXECOUNT.astype(float)
        finally:
            cur.close()
            con.close()

    # noinspection PyUnusedLocal
    def _get_ar_lim(self, sbrow):

        """

        Args:
            sbrow:

        Returns:
            Pandas.Series:

        """
        ouid = sbrow['OBSPROJECT_UID']
        sgn = sbrow['sgName']
        uid = sbrow['SB_UID']
        ousid = sbrow['OUS_ID']
        sgrow = self.sciencegoals.query('OBSPROJECT_UID == @ouid and '
                                        'OUS_ID == @ousid')
        if len(sgrow) == 0:
            sgn = sbrow['sgName'].rstrip()
            sgn = sgn.lstrip()
            sgrow = self.sciencegoals.query(
                'OBSPROJECT_UID == @ouid and sg_name == @sgn')

        if len(sgrow) == 0:
            sgn = sbrow['sgName']
            sgrow = self.sciencegoals.query(
                'OBSPROJECT_UID == @ouid and sg_name == @sgn')

        if len(sgrow) == 0:
            if sbrow['sgName'] == 'CO(4-3), [CI] 1-0 setup':
                sgn = 'CO(4-3), [CI]1-0 setup'
            elif sbrow['sgName'] == 'CO(7-6), [CI] 2-1 setup':
                sgn = 'CO(7-6), [CI]2-1 setup'
            elif (sbrow['sgName'] ==
                    'CRL618: HNC & HCO+ 3-2 + H29a + HC3N 28-27'):
                sgn = 'CRL618: HNC &HCO+ 3-2 + H29a + HC3N 28-27'
            elif sbrow['sgName'] == 'HCN, H13CN & HC15N J=8-7':
                sgn = 'HCN, H13CN &HC15N J=8-7'
            else:
                sgn = sgn
            sgrow = self.sciencegoals.query(
                'OBSPROJECT_UID == @ouid and sg_name == @sgn')

        sbs = self._schedblocks_temp.query(
            'OBSPROJECT_UID == @ouid and sgName == @sgn and '
            'array == "TWELVE-M"')
        isextended = True
        sb_bl_num = len(sbs)
        sb_7m_num = len(self._schedblocks_temp.query(
            'OBSPROJECT_UID == @ouid and sgName == @sgn and '
            'array == "SEVEN-M"'))
        sb_tp_num = len(self._schedblocks_temp.query(
            'OBSPROJECT_UID == @ouid and sgName == @sgn and '
            'array == "TP-Array"'))

        if sbrow['array'] != "TWELVE-M":
            return pd.Series(
                [None, None, 'N/A', 0, sb_bl_num, sb_7m_num, sb_tp_num,
                 sgrow['SG_ID'].values[0]],
                index=["minAR", "maxAR", "BestConf", "two_12m", "SB_BL_num",
                       "SB_7m_num", "SB_TP_num", "SG_ID"])
        if len(sgrow) == 0:
            print "What? %s" % uid
            return pd.Series(
                [0, 0, 'E', 0, sb_bl_num, sb_7m_num, sb_tp_num, sgrow['SG_ID']],
                index=["minAR", "maxAR", "BestConf", "two_12m", "SB_BL_num",
                       "SB_7m_num", "SB_TP_num", "SG_ID"])
        else:
            sgrow = sgrow.iloc[0]

        num12 = 1

        if len(sbs) > 1:
            two = sbs[sbs.array12mType.str.contains('Comp')]
            if len(two) > 0:
                num12 = 2
                isextended = True
                if sbrow['sbName'].endswith('_TC'):
                    isextended = False

        # noinspection PyBroadException
        try:
            if sgrow['ARcor'] == 0:
                sgrow['ARcor'] = sgrow['AR'] * sbrow['repfreq'] / 100.
            minar, maxar, conf1, conf2 = self._ares.run(
                sgrow['ARcor'], sgrow['LAScor'], sbrow['DEC'], sgrow['useACA'],
                num12, sbrow['OT_BestConf'], uid)
        except:
            print "Exception, %s" % uid
            print sgrow['ARcor'], sgrow['LAScor'], sbrow['DEC'], sgrow['useACA']
            return pd.Series(
                [0, 0, 'C', num12, sb_bl_num, sb_7m_num, sb_tp_num,
                 sgrow['SG_ID']],
                index=["minAR", "maxAR", "BestConf", "two_12m", "SB_BL_num",
                       "SB_7m_num", "SB_TP_num", "SG_ID"])

        if not isextended:

            return pd.Series(
                [minar[1], maxar[1], conf2, num12, sb_bl_num, sb_7m_num,
                 sb_tp_num, sgrow['SG_ID']],
                index=["minAR", "maxAR", "BestConf", "two_12m", "SB_BL_num",
                       "SB_7m_num", "SB_TP_num", "SG_ID"])

        return pd.Series(
            [minar[0], maxar[0], conf1, num12, sb_bl_num, sb_7m_num, sb_tp_num,
             sgrow['SG_ID']],
            index=["minAR", "maxAR", "BestConf", "two_12m", "SB_BL_num",
                   "SB_7m_num", "SB_TP_num", "SG_ID"])

    def get_ousstatus(self):

        """

        Returns:
            Pandas.Frame:

        """

        con, cur = self.open_oracle_conn()
        try:
            sql = str(
                'SELECT STATUS_ENTITY_ID, DOMAIN_ENTITY_ID, '
                'DOMAIN_ENTITY_STATE, PARENT_OBS_UNIT_SET_STATUS_ID, '
                'OBS_PROJECT_STATUS_ID, OBS_PROJECT_ID, TOTAL_USED_TIME_IN_SEC '
                'FROM ALMA.OBS_UNIT_SET_STATUS')
            cur.execute(sql)
            ous = pd.DataFrame(
                cur.fetchall(),
                columns=[rec[0] for rec in cur.description])
            return ous
        finally:
            cur.close()
            con.close()

    def correct_specscan_times(self):

        """

        """

        # noinspection PyUnusedLocal
        sg_ous = self.sciencegoals.query(
                'isSpectralScan == True').OUS_ID.unique()

        times_1 = self.schedblocks.query(
                'OUS_ID in @sg_ous and array == "TWELVE-M"')[
            ['OBSPROJECT_UID', 'OUS_ID', 'estimatedTime', 'SB_UID']
        ].groupby(['OBSPROJECT_UID', 'OUS_ID']).aggregate(
                {'SB_UID': pd.np.count_nonzero, 'estimatedTime': pd.np.max}
        ).reset_index()

        times_1['estimatedTime'] = 1.3 * times_1.estimatedTime / times_1.SB_UID

        if len(times_1) == 0:
            return

        aux = pd.merge(
                self.schedblocks, times_1,
                on=['OBSPROJECT_UID', 'OUS_ID'],
                suffixes=['', '_t']).set_index('SB_UID', drop=False)
        self.schedblocks.ix[aux.SB_UID.values, 'estimatedTime'] = \
            aux.ix[aux.SB_UID.values, 'estimatedTime_t']
