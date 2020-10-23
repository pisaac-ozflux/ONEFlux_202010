'''
runoneflux.py

For license information:
see LICENSE file or headers in oneflux.__init__.py

Execution controller module for running tools/functions in the oneflux library

@author: Gilberto Pastorello
@contact: gzpastorello@lbl.gov
@date: 2017-01-31
'''
import os
import sys
import logging
import argparse
import traceback
# PRI 2020/10/23 - imports to support control files
from configobj import ConfigObj
import datetime

from oneflux import ONEFluxError, log_config, log_trace, VERSION_PROCESSING, VERSION_METADATA
from oneflux.tools.partition_nt import run_partition_nt, PROD_TO_COMPARE, PERC_TO_COMPARE
from oneflux.tools.partition_dt import run_partition_dt
from oneflux.tools.pipeline import run_pipeline, NOW_TS

#log = logging.getLogger(__name__)
log = logging.getLogger("oneflux_log")

DEFAULT_LOGGING_FILENAME = 'oneflux.log'
# PRI 2020/10/22 - add gap_fill to command list
COMMAND_LIST = ['partition_nt', 'partition_dt', 'all', 'gap_fill']

# main function
if __name__ == '__main__':

    # PRI 2020/10/23 - add ability to read options from control file
    if len(sys.argv) == 2:
        if os.path.isfile(sys.argv[1]):
            cfg = ConfigObj(sys.argv[1])
            args = {"timestamp": datetime.datetime.now().strftime("%Y%m%dT%H%M%S"),
                    "versiond": str(VERSION_METADATA), "versionp": str(VERSION_PROCESSING)}
            args["command"] = cfg["Run"]["command"]
            args["datadir"] = cfg["Files"]["data_dir"]
            args["siteid"] = cfg["Site"]["site_id"]
            args["sitedir"] = cfg["Files"]["site_dir"]
            args["firstyear"] = int(cfg["Run"]["first_year"])
            args["lastyear"] = int(cfg["Run"]["last_year"])
            if cfg["Options"]["percentiles"].lower() == "default":
                args["perc"] = None
            else:
                args["perc"] = cfg["Options"]["percentiles"]
            if cfg["Options"]["products"].lower() == "default":
                args["prod"] = None
            else:
                args["prod"] = cfg["Options"]["products"]
            args["logfile"] = cfg["Files"]["log_file"]
            args["forcepy"] = True
            if cfg["Options"]["force_py"].lower() == "no":
                args["forcepy"] = False
            args["mcr_directory"] = cfg["Files"]["mcr_dir"]
            args["recint"] = cfg["Options"]["recint"]
            args["logging_level"] = cfg["Options"]["logging_level"]
    else:
        # cli arguments
        parser = argparse.ArgumentParser()
        parser.add_argument('command', metavar="COMMAND", help="ONEFlux command to be run", type=str, choices=COMMAND_LIST)
        parser.add_argument('datadir', metavar="DATA-DIR", help="Absolute path to general data directory", type=str)
        parser.add_argument('siteid', metavar="SITE-ID", help="Site Flux ID in the form CC-XXX", type=str)
        parser.add_argument('sitedir', metavar="SITE-DIR", help="Relative path to site data directory (within data-dir)", type=str)
        parser.add_argument('firstyear', metavar="FIRST-YEAR", help="First year of data to be processed", type=int)
        parser.add_argument('lastyear', metavar="LAST-YEAR", help="Last year of data to be processed", type=int)
        parser.add_argument('--perc', metavar="PERC", help="List of percentiles to be processed", dest='perc', type=str, choices=PERC_TO_COMPARE, action='append', nargs='*')
        parser.add_argument('--prod', metavar="PROD", help="List of products to be processed", dest='prod', type=str, choices=PROD_TO_COMPARE, action='append', nargs='*')
        parser.add_argument('-l', '--logfile', help="Logging file path", type=str, dest='logfile', default=DEFAULT_LOGGING_FILENAME)
        parser.add_argument('--force-py', help="Force execution of PY partitioning (saves original output, generates new)", action='store_true', dest='forcepy', default=False)
        parser.add_argument('--mcr', help="Path to MCR directory", type=str, dest='mcr_directory', default=None)
        parser.add_argument('--ts', help="Timestamp to be used in processing IDs", type=str, dest='timestamp', default=NOW_TS)
        parser.add_argument('--recint', help="Record interval for site", type=str, choices=['hh', 'hr'], dest='recint', default='hh')
        parser.add_argument('--versionp', help="Version of processing (hardcoded default)", type=str, dest='versionp', default=str(VERSION_PROCESSING))
        parser.add_argument('--versiond', help="Version of data (hardcoded default)", type=str, dest='versiond', default=str(VERSION_METADATA))
        args = parser.parse_args()
        # PRI 2020/10/23 - convert to dictionary to be compatible with use of ConfigObj
        args = vars(args)

    # setup logging file and stdout
    # PRI 2020/10/23 - logging level to stdout set in control file
    loglevel = logging.DEBUG
    if "logging_level" in list(args.keys()):
        if args["logging_level"].lower() in ["debug", "info", "warning", "error"]:
            loglevels = {"debug": logging.DEBUG, "info": logging.INFO,
                         "warning": logging.WARNING, "error": logging.ERROR}
            loglevel = loglevels[args["logging_level"].lower()]
    log_config(level=logging.DEBUG, filename=args["logfile"], std=True, std_level=loglevel)

    # set defaults if no perc or prod
    perc = (PERC_TO_COMPARE if args["perc"] is None else args["perc[0]"])
    prod = (PROD_TO_COMPARE if args["prod"] is None else args["prod[0]"])

    firstyear = args["firstyear"]
    lastyear = args["lastyear"]

    msg = "Using:"
    msg += "command ({c})".format(c=args["command"])
    msg += ", data-dir ({i})".format(i=args["datadir"])
    msg += ", site-id ({i})".format(i=args["siteid"])
    msg += ", site-dir ({d})".format(d=args["sitedir"])
    msg += ", first-year ({y})".format(y=firstyear)
    msg += ", last-year ({y})".format(y=lastyear)
    msg += ", perc ({i})".format(i=perc)
    msg += ", prod ({i})".format(i=prod)
    msg += ", log-file ({f})".format(f=args["logfile"])
    msg += ", force-py ({i})".format(i=args["forcepy"])
    log.debug(msg)

    # start execution
    try:
        # check arguments
        # PRI 2020/10/23 - changed use of args to dictionary syntax
        print os.path.join(args["datadir"], args["sitedir"])
        if not os.path.isdir(os.path.join(args["datadir"], args["sitedir"])):
            raise ONEFluxError("Site dir not found: {d}".format(d=args["sitedir"]))

        # run command
        # PRI 2020/10/23 - changed use of args to dictionary syntax
        log.info("Starting execution: {c}".format(c=args["command"]))
        if args["command"] == 'all':
            # PRI 2020/10/22
            # dictionary of logicals to control which pipeline steps will be executed
            pipeline_steps = {"qc_auto_execute": True, "ustar_mp_execute": True,
                              "ustar_cp_execute": False, "meteo_proc_execute": True,
                              "nee_proc_execute": True, "energy_proc_execute": True,
                              "nee_partition_nt_execute": True, "nee_partition_dt_execute": True,
                              "prepare_ure_execute": True, "ure_execute": True,
                              "fluxnet2015_execute": True, "fluxnet2015_site_plots": True,
                              "simulation": False}
            # PRI 2020/10/23 - changed use of args to dictionary syntax
            run_pipeline(datadir=args["datadir"], siteid=args["siteid"], sitedir=args["sitedir"],
                         firstyear=firstyear, lastyear=lastyear, prod_to_compare=prod,
                         perc_to_compare=perc, mcr_directory=args["mcr_directory"],
                         timestamp=args["timestamp"], record_interval=args["recint"],
                         version_data=args["versiond"], version_proc=args["versionp"],
                         pipeline_steps=pipeline_steps)
        elif args["command"] == 'gap_fill':
            # PRI 2020/10/22
            # dictionary of logicals to control which pipeline steps will be executed
            pipeline_steps = {"qc_auto_execute": True, "ustar_mp_execute": True,
                              "ustar_cp_execute": False, "meteo_proc_execute": True,
                              "nee_proc_execute": True, "energy_proc_execute": True,
                              "nee_partition_nt_execute": False, "nee_partition_dt_execute": False,
                              "prepare_ure_execute": False, "ure_execute": False,
                              "fluxnet2015_execute": False, "fluxnet2015_site_plots": False,
                              "simulation": False}
            run_pipeline(datadir=args["datadir"], siteid=args["siteid"], sitedir=args["sitedir"],
                         firstyear=firstyear, lastyear=lastyear, prod_to_compare=prod,
                         perc_to_compare=perc, mcr_directory=args["mcr_directory"],
                         timestamp=args["timestamp"], record_interval=args["recint"],
                         version_data=args["versiond"], version_proc=args["versionp"],
                         pipeline_steps=pipeline_steps)
        elif args["command"] == 'partition_nt':
            run_partition_nt(datadir=args["datadir"], siteid=args["siteid"], sitedir=args["sitedir"],
                             years_to_compare=range(firstyear, lastyear + 1),
                             py_remove_old=args["forcepy"], prod_to_compare=prod, perc_to_compare=perc)
        elif args["command"] == 'partition_dt':
            run_partition_dt(datadir=args["datadir"], siteid=args["siteid"], sitedir=args["sitedir"],
                             years_to_compare=range(firstyear, lastyear + 1),
                             py_remove_old=args["forcepy"], prod_to_compare=prod, perc_to_compare=perc)
        else:
            raise ONEFluxError("Unknown command: {c}".format(c=args["command"]))
        log.info("Finished execution: {c}".format(c=args["command"]))

    except Exception as e:
        msg = log_trace(exception=e, level=logging.CRITICAL, log=log)
        log.critical("***Problem during execution*** {e}".format(e=str(e)))
        tb = traceback.format_exc()
        log.critical("***Problem traceback*** {s}".format(s=str(tb)))
        sys.exit(msg)

    sys.exit(0)
