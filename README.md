# DSACore

The ALMA Dynamic Scheduler Algorithm Core [python].

The package is composed by the Core algorithm plus the definition of the two
basic XMLRPC services: online and simulation.

The Core is bases in 2 main classes:

* DsaDataBase3
* DsaAlgorithm3

export PATH="/home/itoledo/anaconda/bin:$PATH"
export DSA="/home/itoledo/Work/SCHEDULING/DSACore/"
export CON_STR="almasu/alma4dba@ALMA_ONLINE.SCO.CL"
export APDM_C3="/home/itoledo/Documents/apdm_c3/"
export APDM_PREFIX="/home/itoledo/Documents/apdm"
export PYTHONPATH="$PYTHONPATH:/home/itoledo/Work/SCHEDULING/DSACore"