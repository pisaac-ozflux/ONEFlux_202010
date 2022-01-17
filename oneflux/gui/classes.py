# standard modules
import copy
import datetime
import logging
import os
import sys
import traceback
# 3rd party modules
from configobj import ConfigObj
from PyQt5 import QtCore, QtGui, QtWidgets
# ONEFlux modules
from oneflux import ONEFluxError, log_config, log_trace, VERSION_PROCESSING, VERSION_METADATA
from oneflux.tools.partition_nt import run_partition_nt, PROD_TO_COMPARE, PERC_TO_COMPARE
from oneflux.tools.partition_dt import run_partition_dt
from oneflux.tools.pipeline import run_pipeline

log = logging.getLogger("oneflux_log")

def do_run_oneflux(cfg):
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

    # setup logging file and stdout
    # PRI 2020/10/23 - logging level to stdout set in control file
    #loglevel = logging.DEBUG
    #if "logging_level" in list(args.keys()):
        #if args["logging_level"].lower() in ["debug", "info", "warning", "error"]:
            #loglevels = {"debug": logging.DEBUG, "info": logging.INFO,
                         #"warning": logging.WARNING, "error": logging.ERROR}
            #loglevel = loglevels[args["logging_level"].lower()]
    #log_config(level=logging.DEBUG, filename=args["logfile"], std=True, std_level=loglevel)

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
                              "ustar_cp_execute": True, "meteo_proc_execute": True,
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
                              "ustar_cp_execute": True, "meteo_proc_execute": True,
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
        #sys.exit(msg)

    return

class edit_control_file(QtWidgets.QWidget):
    def __init__(self, oneflux_main_gui):

        super(edit_control_file, self).__init__()

        self.cfg = copy.deepcopy(oneflux_main_gui.cfg)

        self.cfg_changed = False
        self.tabs = oneflux_main_gui.tabs

        self.edit_oneflux_gui()

    def edit_oneflux_gui(self):
        """ Edit the ONEFlux control file GUI."""
        # get a QTreeView
        self.view = myTreeView()
        # get a QStandardItemModel
        self.model = QtGui.QStandardItemModel()
        # add the model to the view
        self.view.setModel(self.model)
        # set the context menu policy
        self.view.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        # connect the context menu requested signal to appropriate slot
        self.view.customContextMenuRequested.connect(self.context_menu)
        vbox = QtWidgets.QVBoxLayout()
        vbox.addWidget(self.view)
        self.setLayout(vbox)
        self.setGeometry(300, 300, 600, 400)
        # build the model
        self.get_model_from_data()
        # set the default width for the first column
        self.view.setColumnWidth(0, 200)
        # expand the top level of the sections
        for row in range(self.model.rowCount()):
            idx = self.model.index(row, 0)
            self.view.expand(idx)

    def get_model_from_data(self):
        """ Build the data model."""
        self.model.setHorizontalHeaderLabels(['Parameter', 'Value'])
        self.model.itemChanged.connect(self.handleItemChanged)
        # there must be someway outa here, said the Joker to the Thief ...
        self.sections = {}
        for key1 in self.cfg:
            if not self.cfg[key1]:
                continue
            if key1 in ["Files", "Site", "Run", "Options"]:
                # sections with only 1 level
                self.sections[key1] = QtGui.QStandardItem(key1)
                for key2 in self.cfg[key1]:
                    val = self.cfg[key1][key2]
                    child0 = QtGui.QStandardItem(key2)
                    child1 = QtGui.QStandardItem(val)
                    self.sections[key1].appendRow([child0, child1])
                self.model.appendRow(self.sections[key1])
            else:
                # should trap unrecognised control file sections
                pass

    def get_data_from_model(self):
        """ Iterate over the model and get the data."""
        cfg = ConfigObj(indent_type="    ", list_values=False)
        cfg["level"] = "oneflux"
        model = self.model
        # there must be a way to do this recursively
        for i in range(model.rowCount()):
            section = model.item(i)
            key1 = str(section.text())
            cfg[key1] = {}
            if key1 in ["Files", "Site", "Run", "Options"]:
                # sections with only 1 level
                for j in range(section.rowCount()):
                    key2 = str(section.child(j, 0).text())
                    val2 = str(section.child(j, 1).text())
                    cfg[key1][key2] = val2
        return cfg

    def context_menu(self, position):
        """ Right click context menu."""
        # get a menu
        self.context_menu = QtWidgets.QMenu()
        # get the index of the selected item
        idx = self.view.selectedIndexes()[0]
        # get the selected item text
        selected_text = str(idx.data())
        # get the selected item
        selected_item = idx.model().itemFromIndex(idx)
        # get the level of the selected item
        level = self.get_level_selected_item()
        if level == 0:
            if selected_text in ["Files", "Site", "Run", "Options"]:
                # add code for adding sections here
                pass
            else:
                # add handler for unrecognised sections here
                pass
        elif level == 1:
            # sections with 2 levels
            # get the parent of the selected item
            parent = selected_item.parent()
            if (str(parent.text()) == "Files") and (selected_item.column() == 1):
                key = str(parent.child(selected_item.row(),0).text())
                if key in ["data_dir"]:
                    self.context_menu.actionBrowseDataDir = QtWidgets.QAction(self)
                    self.context_menu.actionBrowseDataDir.setText("Browse...")
                    self.context_menu.addAction(self.context_menu.actionBrowseDataDir)
                    self.context_menu.actionBrowseDataDir.triggered.connect(self.browse_data_dir)
                elif key in ["site_dir"]:
                    self.context_menu.actionBrowseSiteDir = QtWidgets.QAction(self)
                    self.context_menu.actionBrowseSiteDir.setText("Browse...")
                    self.context_menu.addAction(self.context_menu.actionBrowseSiteDir)
                    self.context_menu.actionBrowseSiteDir.triggered.connect(self.browse_site_dir)
                elif key in ["mcr_dir"]:
                    self.context_menu.actionBrowseMCRDir = QtWidgets.QAction(self)
                    self.context_menu.actionBrowseMCRDir.setText("Browse...")
                    self.context_menu.addAction(self.context_menu.actionBrowseMCRDir)
                    self.context_menu.actionBrowseMCRDir.triggered.connect(self.browse_mcr_dir)
                elif key in ["log_file"]:
                    self.context_menu.actionBrowseLogFile = QtWidgets.QAction(self)
                    self.context_menu.actionBrowseLogFile.setText("Browse...")
                    self.context_menu.addAction(self.context_menu.actionBrowseLogFile)
                    self.context_menu.actionBrowseLogFile.triggered.connect(self.browse_log_file)
                else:
                    pass
            elif (str(parent.text()) == "Run") and (selected_item.column() == 1):
                key = str(parent.child(selected_item.row(),0).text())
                if key == "command":
                    existing_entry = str(parent.child(selected_item.row(),1).text())
                    if existing_entry != "gap_fill":
                        self.context_menu.actionSetCommandGapFill = QtWidgets.QAction(self)
                        self.context_menu.actionSetCommandGapFill.setText("gap_fill")
                        self.context_menu.addAction(self.context_menu.actionSetCommandGapFill)
                        self.context_menu.actionSetCommandGapFill.triggered.connect(self.set_command_gap_fill)
                    if existing_entry != "all":
                        self.context_menu.actionSetCommandAll = QtWidgets.QAction(self)
                        self.context_menu.actionSetCommandAll.setText("all")
                        self.context_menu.addAction(self.context_menu.actionSetCommandAll)
                        self.context_menu.actionSetCommandAll.triggered.connect(self.set_command_all)
                    if existing_entry != "partition_nt":
                        self.context_menu.actionSetCommandPartitionNT = QtWidgets.QAction(self)
                        self.context_menu.actionSetCommandPartitionNT.setText("partition_nt")
                        self.context_menu.addAction(self.context_menu.actionSetCommandPartitionNT)
                        self.context_menu.actionSetCommandPartitionNT.triggered.connect(self.set_command_partition_nt)
                    if existing_entry != "partition_dt":
                        self.context_menu.actionSetCommandPartitionDT = QtWidgets.QAction(self)
                        self.context_menu.actionSetCommandPartitionDT.setText("partition_dt")
                        self.context_menu.addAction(self.context_menu.actionSetCommandPartitionDT)
                        self.context_menu.actionSetCommandPartitionDT.triggered.connect(self.set_command_partition_dt)
            else:
                pass
        else:
            pass
        self.context_menu.exec_(self.view.viewport().mapToGlobal(position))
        return

    def browse_data_dir(self):
        """ Browse for the data directory path."""
        # get the index of the selected item
        idx = self.view.selectedIndexes()[0]
        # get the selected item from the index
        selected_item = idx.model().itemFromIndex(idx)
        # get the parent of the selected item
        parent = selected_item.parent()
        # get the selected entry text
        file_path = str(idx.data())
        # dialog for new directory
        new_dir = QtWidgets.QFileDialog.getExistingDirectory(self, "Choose the data directory",
                                                             file_path, QtWidgets.QFileDialog.ShowDirsOnly)
        # quit if cancel button pressed
        if len(str(new_dir)) > 0:
            # make sure the string ends with a path delimiter
            tmp_dir = QtCore.QDir.toNativeSeparators(str(new_dir))
            new_dir = os.path.join(tmp_dir, "")
            # update the model
            parent.child(selected_item.row(), 1).setText(new_dir)
        return

    def browse_site_dir(self):
        """ Browse for the site data file path."""
        # get the index of the selected item
        idx = self.view.selectedIndexes()[0]
        # get the selected item from the index
        selected_item = idx.model().itemFromIndex(idx)
        # get the parent of the selected item
        parent = selected_item.parent()
        # get the file_path so it can be used as a default directory
        key, file_path, found, j = self.get_keyval_by_key_name(parent, "data_dir")
        # dialog for open file
        new_file_path = QtWidgets.QFileDialog.getExistingDirectory(self, "Choose the site directory",
                                                              file_path, QtWidgets.QFileDialog.ShowDirsOnly)
        # update the model
        if len(str(new_file_path)) > 0:
            new_file_parts = os.path.split(str(new_file_path))
            parent.child(selected_item.row(), 1).setText(new_file_parts[1])
        return

    def browse_mcr_dir(self):
        """ Browse for the MCR file path."""
        # get the index of the selected item
        idx = self.view.selectedIndexes()[0]
        # get the selected item from the index
        selected_item = idx.model().itemFromIndex(idx)
        # get the parent of the selected item
        parent = selected_item.parent()
        # get the selected entry text
        file_path = str(idx.data())
        # dialog for new directory
        new_dir = QtWidgets.QFileDialog.getExistingDirectory(self, "Choose the MCR directory",
                                                             file_path, QtWidgets.QFileDialog.ShowDirsOnly)
        # quit if cancel button pressed
        if len(str(new_dir)) > 0:
            # make sure the string ends with a path delimiter
            tmp_dir = QtCore.QDir.toNativeSeparators(str(new_dir))
            new_dir = os.path.join(tmp_dir, "")
            # update the model
            parent.child(selected_item.row(), 1).setText(new_dir)
        return

    def browse_log_file(self):
        """ Browse for the input data file path."""
        # get the index of the selected item
        idx = self.view.selectedIndexes()[0]
        # get the selected item from the index
        selected_item = idx.model().itemFromIndex(idx)
        # get the parent of the selected item
        parent = selected_item.parent()
        # get the file_path so it can be used as a default directory
        key, file_path, found, j = self.get_keyval_by_key_name(parent, "data_dir")
        # dialog for open file
        new_file_path = QtWidgets.QFileDialog.getOpenFileName(caption="Choose a log file ...",
                                                          directory=file_path)[0]
        # update the model
        if len(str(new_file_path)) > 0:
            new_file_parts = os.path.split(str(new_file_path))
            parent.child(selected_item.row(), 1).setText(new_file_parts[1])

    def get_keyval_by_key_name(self, section, key):
        """ Get the value from a section based on the key name."""
        found = False
        val_child = ""
        key_child = ""
        for i in range(section.rowCount()):
            if str(section.child(i, 0).text()) == str(key):
                found = True
                key_child = str(section.child(i, 0).text())
                val_child = str(section.child(i, 1).text())
                break
        return key_child, val_child, found, i

    def get_level_selected_item(self):
        """ Get the level of the selected item."""
        indexes = self.view.selectedIndexes()
        level = -1
        if len(indexes) > 0:
            level = 0
            index = indexes[0]
            while index.parent().isValid():
                index = index.parent()
                level += 1
        return level

    def handleItemChanged(self, item):
        """ Handler for when view items are edited."""
        # update the control file contents
        self.cfg = self.get_data_from_model()
        # add an asterisk to the tab text to indicate the tab contents have changed
        self.update_tab_text()

    def remove_item(self):
        """ Remove an item from the view."""
        # loop over selected items in the tree
        for idx in self.view.selectedIndexes():
            # get the selected item from the index
            selected_item = idx.model().itemFromIndex(idx)
            # get the parent of the selected item
            parent = selected_item.parent()
            # remove the row
            parent.removeRow(selected_item.row())
        self.update_tab_text()

    def set_command_all(self):
        """ Set the run command to 'all'."""
        idx = self.view.selectedIndexes()[0]
        selected_item = idx.model().itemFromIndex(idx)
        parent = selected_item.parent()
        parent.child(selected_item.row(), 1).setText("all")

    def set_command_gap_fill(self):
        """ Set the run command to 'gap_fill'."""
        idx = self.view.selectedIndexes()[0]
        selected_item = idx.model().itemFromIndex(idx)
        parent = selected_item.parent()
        parent.child(selected_item.row(), 1).setText("gap_fill")

    def set_command_partition_nt(self):
        """ Set the run command to 'partition_nt'."""
        idx = self.view.selectedIndexes()[0]
        selected_item = idx.model().itemFromIndex(idx)
        parent = selected_item.parent()
        parent.child(selected_item.row(), 1).setText("partition_nt")

    def set_command_partition_dt(self):
        """ Set the run command to 'partition_dt'."""
        idx = self.view.selectedIndexes()[0]
        selected_item = idx.model().itemFromIndex(idx)
        parent = selected_item.parent()
        parent.child(selected_item.row(), 1).setText("partition_dt")

    def update_tab_text(self):
        """ Add an asterisk to the tab title text to indicate tab contents have changed."""
        # add an asterisk to the tab text to indicate the tab contents have changed
        tab_text = str(self.tabs.tabText(self.tabs.tab_index_current))
        if "*" not in tab_text:
            self.tabs.setTabText(self.tabs.tab_index_current, tab_text+"*")

class myMessageBox(QtWidgets.QMessageBox):
    def __init__(self, msg, title="Information", parent=None):
        super(myMessageBox, self).__init__(parent)
        if title == "Critical":
            self.setIcon(QtWidgets.QMessageBox.Critical)
        elif title == "Warning":
            self.setIcon(QtWidgets.QMessageBox.Warning)
        else:
            self.setIcon(QtWidgets.QMessageBox.Information)
        self.setText(msg)
        self.setWindowTitle(title)
        self.setStandardButtons(QtWidgets.QMessageBox.Ok)
        self.exec_()

class myPlainTextEditLogger(logging.Handler):
    def __init__(self, parent):
        super(myPlainTextEditLogger, self).__init__()
        self.textBox = QtWidgets.QPlainTextEdit(parent)
        self.textBox.setReadOnly(True)
        logfmt = logging.Formatter('%(asctime)s %(levelname)s %(message)s','%H:%M:%S')
        self.setFormatter(logfmt)

    def emit(self, record):
        msg = self.format(record)
        self.textBox.appendPlainText(msg)
        QtWidgets.QApplication.processEvents()

class myTreeView(QtWidgets.QTreeView):
    """
    Purpose:
     Subclass of QTreeView with the dragEnterEvent() and dropEvent() methods overloaded
     to constrain drag and drop moves within the control file. The following drag
     and drop rules are implemented:
     1) items can only be dropped within the section from which they originate.
     2) items can't be dropped on top of other items.
    Usage:
     view = myTreeView()
    Author: PRI
    Date: August 2020
    """
    def __init__(self):
        QtWidgets.QTreeView.__init__(self)
        # disable multiple selections
        self.setSelectionMode(self.SingleSelection)
        # enable selction of single cells
        self.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectItems)
        # enable drag and drop as internal move only
        self.setDragDropMode(QtWidgets.QAbstractItemView.InternalMove)
        # enable drag and drop
        self.setDragEnabled(True)
        self.setAcceptDrops(True)
        self.setDropIndicatorShown(True)
        # rows have alternating colours and headers
        self.setAlternatingRowColors(True)
        self.setHeaderHidden(False)
        # create info dictionary
        self.info = {"one_line_sections": ["Files", "Site", "Run", "Options"]}

    def dragEnterEvent(self, event):
        """
        Purpose:
         Re-implement the standard dragEnterEvent to get the behaviour we want.
        Usage:
        Author: PRI
        Date: August 2020
        """
        # wrap in a try ... except to trap unforseen events (quick but dirty)
        try:
            self.setDropIndicatorShown(True)
            # index of selected item
            idxs = self.selectedIndexes()[0]
            # only enable event if user has clicked in first column
            if idxs.column() == 0:
                # save some stuff needed for the drop event
                self.info["source_index"] = idxs
                self.info["source_item"] = idxs.model().itemFromIndex(idxs)
                self.info["source_parent"] = self.info["source_item"].parent()
                source_parent = self.info["source_parent"]
                self.info["source_key"] = QtGui.QStandardItem(source_parent.child(idxs.row(),0).text())
                # second column only available if section in "one_line_sections"
                if self.info["source_parent"].text() in self.info["one_line_sections"]:
                    self.info["source_value"] = QtGui.QStandardItem(source_parent.child(idxs.row(),1).text())
                else:
                    self.info["source_value"] = QtGui.QStandardItem("")
                # accept this event
                event.accept()
            else:
                # ignore everything else
                event.ignore()
        except:
            event.ignore()

    def dropEvent(self, event):
        """
        Purpose:
         Re-implement the standard dropEvent to get the behaviour we want.
        Usage:
        Author: PRI
        Date: August 2020
        """
        # wrap in a try ... except to trap unforseen events (dirty coding)
        try:
            # index of the item under the drop
            idxd = self.indexAt(event.pos())
            # save so useful stuff
            self.info["destination_index"] = idxd
            self.info["destination_item"] = idxd.model().itemFromIndex(idxd)
            self.info["destination_parent"] = self.info["destination_item"].parent()
            destination_parent_text = self.info["destination_parent"].text()
            source_parent_text = self.info["source_parent"].text()
            # only allow drag and drop within the same section
            if (destination_parent_text == source_parent_text):
                # don't allow drop on another item
                if (self.dropIndicatorPosition() != QtWidgets.QAbstractItemView.OnItem):
                    # use special drop event code for one line sections
                    if self.info["source_parent"].text() in self.info["one_line_sections"]:
                        idxs = self.info["source_index"]
                        key = self.info["source_key"]
                        value = self.info["source_value"]
                        self.info["source_parent"].removeRow(idxs.row())
                        self.info["source_parent"].insertRow(idxd.row(), [key, value])
                        event.accept()
                    else:
                        # use standard drop event code for everything else
                        QtWidgets.QTreeView.dropEvent(self, event)
                else:
                    # ignore everything else
                    event.ignore()
            else:
                event.ignore()
        except:
            event.ignore()
        # refresh the GUI
        self.model().layoutChanged.emit()

def init_logger(logger_name, log_file_name, to_file=True, to_screen=False):
    """
    Purpose:
     Returns a logger object.
    Usage:
     logger = pfp_log.init_logger()
    Author: PRI with acknowledgement to James Cleverly
    Date: September 2016
    """
    # create formatter
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s', '%H:%M:%S')
    # create the logger and set the level
    logger = logging.getLogger(name=logger_name)
    logger.setLevel(logging.INFO)
    if to_file:
        # create file handler for all messages
        fh1 = logging.FileHandler(log_file_name)
        fh1.setLevel(logging.DEBUG)
        fh1.setFormatter(formatter)
        # add the file handler to the logger
        logger.addHandler(fh1)
        # set up a separate file for errors
        ext = os.path.splitext(log_file_name)[1]
        error_file_name = log_file_name.replace(ext, ".errors")
        fh2 = logging.FileHandler(error_file_name)
        fh2.setLevel(logging.ERROR)
        fh2.setFormatter(formatter)
        logger.addHandler(fh2)
    if to_screen:
        console = logging.StreamHandler()
        console.setFormatter(formatter)
        console.setLevel(logging.INFO)
        logger.addHandler(console)
    return logger

