.. WTO3 documentation master file, created by
   sphinx-quickstart on Fri Oct 30 21:03:31 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.


*************
DSA Selectors
*************

Inputs
======

Array Family (*array_kind*)
---------------------------

Can be any of the following:

   * TWELVE-M
   * SEVEN-M
   * TP-Array

Project States (*prj_status*)
-----------------------------

By default, only projects in State *Ready* or *InProgress*

SB States (*sb_status*)
-----------------------

By default, select only projects with statuses *Ready*, *Suspended*, *Waiting*,
*Phase2Submitted* and *CalibratorCheck*.


Cycles (*cycle*)
----------------

By default are 2013.A,, 2013.1, 2015.1 and 2015.A. For Cycle 2, only grade A
projects are selected.


Priorities (*letterg*)
----------------------

By default projects with grades A, B (high priority) and C (fillers) are
selected

Bands (*bands*)
---------------

List with the bands to be used by the selector. By defaul all bands are allowed.


Std. Configuration or Array ID (*conf* or *array_id*)
-----------------------------------------------------

Is is possible to use either an Array that has been created in the last 24
hours in the AOS-STE, or standard configurations. By default, for 12m, it will
use the last automatic Array created with more than 28 12m antennas.

Elevation Limit (*horizon*)
---------------------------

Minimum source elevation to be selected. By default 20 degrees.

Hour Angle Limits (*minha* and *maxha*)
---------------------------------------

Minimum and maximum hour angles a source must be to be considered. By defult
they are -3 and +3 hours.

Precipitable Water Vapor (*pwv*)
--------------------------------

Precipitable water vapor to be use by the selector. In the online mode, is by
default the latest ALMA measurement calculated by the `plot_pwvs.py` script.


Selectors
=========

Project State Selector
----------------------

Project Grade Selector
----------------------

Scheduling Block State Selector
-------------------------------

Band Selector
-------------

Remaining Executions Selector
-----------------------------

Array Family Selector
---------------------

Array Configuration Selector
----------------------------

Elevation Selector
------------------

Hour Angle Selector
-------------------

Conditions Selector (Pred. Exec. Frac)
--------------------------------------

