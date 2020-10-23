# standard modules
import datetime
import logging
import os
import platform
import sys
import traceback
import warnings
# 3rd party modules
from configobj import ConfigObj
from PyQt5 import QtWidgets

from oneflux.gui.classes import myPlainTextEditLogger, init_logger, edit_control_file, myMessageBox
from oneflux.gui.classes import do_run_oneflux

warnings.filterwarnings("ignore", category=Warning)

# now check the required directories are present
dir_list = ["./logfiles/"]
for item in dir_list:
    if not os.path.exists(item):
        os.makedirs(item)

now = datetime.datetime.now()
log_file_name = "oneflux_" + now.strftime("%Y%m%d%H%M") + ".log"
log_file_name = os.path.join("./logfiles", log_file_name)
log = init_logger("oneflux_log", log_file_name, to_file=True, to_screen=False)

class oneflux_main_ui(QtWidgets.QWidget):
    def __init__(self, oneflux_version):
        super(oneflux_main_ui, self).__init__()

        logTextBox = myPlainTextEditLogger(self)
        log.addHandler(logTextBox)

        # menu bar
        self.menubar = QtWidgets.QMenuBar(self)
        # File menu
        self.menuFile = QtWidgets.QMenu(self.menubar)
        self.menuFile.setTitle("File")
        # Edit menu
        self.menuEdit = QtWidgets.QMenu(self.menubar)
        self.menuEdit.setTitle("Edit")
        # Run menu
        self.menuRun = QtWidgets.QMenu(self.menubar)
        self.menuRun.setTitle("Run")
        # Help menu
        self.menuHelp = QtWidgets.QMenu(self.menubar)
        self.menuHelp.setTitle("Help")
        # File menu items: menu actions for control files
        self.actionFileOpen = QtWidgets.QAction(self)
        self.actionFileOpen.setText("Open")
        self.actionFileOpen.setShortcut('Ctrl+O')
        self.actionFileSave = QtWidgets.QAction(self)
        self.actionFileSave.setText("Save")
        self.actionFileSave.setShortcut('Ctrl+S')
        self.actionFileSaveAs = QtWidgets.QAction(self)
        self.actionFileSaveAs.setText("Save As...")
        self.actionFileSaveAs.setShortcut('Shift+Ctrl+S')
        # File menu item: Quit
        self.actionFileQuit = QtWidgets.QAction(self)
        self.actionFileQuit.setText("Quit")
        self.actionFileQuit.setShortcut('Ctrl+Z')
        # Edit menu items
        self.actionEditPreferences = QtWidgets.QAction(self)
        self.actionEditPreferences.setText("Preferences...")
        # Run menu items
        self.actionRunCurrent = QtWidgets.QAction(self)
        self.actionRunCurrent.setText("Current...")
        self.actionRunCurrent.setShortcut('Ctrl+R')
        # File menu
        self.menuFile.addAction(self.actionFileOpen)
        self.menuFile.addAction(self.actionFileSave)
        self.menuFile.addAction(self.actionFileSaveAs)
        self.menuFile.addSeparator()
        self.menuFile.addAction(self.actionFileQuit)
        # Edit menu
        self.menuEdit.addAction(self.actionEditPreferences)
        # Run menu
        self.menuRun.addAction(self.actionRunCurrent)
        # add individual menus to menu bar
        self.menubar.addAction(self.menuFile.menuAction())
        self.menubar.addAction(self.menuEdit.menuAction())
        self.menubar.addAction(self.menuRun.menuAction())
        self.menubar.addAction(self.menuHelp.menuAction())

        # create a tab bar
        self.tabs = QtWidgets.QTabWidget(self)
        self.tabs.tab_index_all = 0
        self.tabs.tab_index_current = 0
        self.tabs.tab_dict = {}
        self.tabs.cfg_dict = {}
        # make the tabs closeable
        self.tabs.setTabsClosable(True)
        self.tabs.tabCloseRequested.connect(self.closeTab)
        # add the text editor to the first tab
        self.tabs.addTab(logTextBox.textBox, "Log")
        self.tabs.tab_index_all = self.tabs.tab_index_all + 1
        # hide the tab close icon for the console tab
        self.tabs.tabBar().setTabButton(0, QtWidgets.QTabBar.RightSide, None)
        # connect the tab-in-focus signal to the appropriate slot
        self.tabs.currentChanged[int].connect(self.tabSelected)

        # use VBoxLayout to position widgets so they resize with main window
        layout = QtWidgets.QVBoxLayout()
        # add widgets to the layout
        layout.addWidget(self.menubar)
        layout.addWidget(self.tabs)
        self.setLayout(layout)
        self.setGeometry(50,50,800, 600)
        self.setWindowTitle(oneflux_version)

        # Connect signals to slots
        # File menu actions
        self.actionFileOpen.triggered.connect(self.open_controlfile)
        self.actionFileSave.triggered.connect(self.save_controlfile)
        self.actionFileSaveAs.triggered.connect(self.saveas_controlfile)
        self.actionFileQuit.triggered.connect(QtWidgets.QApplication.quit)
        # Edit menu actions
        self.actionEditPreferences.triggered.connect(self.edit_preferences)
        # Run menu actions
        self.actionRunCurrent.triggered.connect(self.run_current)
        return

    def open_controlfile(self):
        # get the control file path
        cfgpath = QtWidgets.QFileDialog.getOpenFileName(caption="Choose a control file ...")[0]
        cfgpath = str(cfgpath)
        # check to see if file open was cancelled
        if len(cfgpath) == 0:
            return
        # read the contents of the control file
        log.info(" Opening " + cfgpath)
        try:
            self.cfg = ConfigObj(cfgpath, indent_type="    ", list_values=False,
                                 write_empty_values=True)
        except Exception:
            msg = "Syntax error in control file, see below for line number"
            log.error(msg)
            error_message = traceback.format_exc()
            log.error(error_message)
            return
        # create a QtTreeView to edit the control file
        if self.cfg["level"] == "oneflux":
            # put the GUI for editing the L1 control file in a new tab
            self.tabs.tab_dict[self.tabs.tab_index_all] = edit_control_file(self)
            # get the control file data from the L1 edit GUI
            self.tabs.cfg_dict[self.tabs.tab_index_all] = self.tabs.tab_dict[self.tabs.tab_index_all].get_data_from_model()
            # put the control file path into the cfg dictionary
            self.tabs.cfg_dict[self.tabs.tab_index_all]["controlfile_name"] = cfgpath
        else:
            log.error(" Unrecognised control file type: " + self.cfg["level"])
            return
        # add a tab for the control file
        self.tabs.addTab(self.tabs.tab_dict[self.tabs.tab_index_all], os.path.basename(str(cfgpath)))
        self.tabs.setCurrentIndex(self.tabs.tab_index_all)
        if self.tabs.tab_dict[self.tabs.tab_index_all].cfg_changed:
            self.update_tab_text()
        self.tabs.tab_index_all = self.tabs.tab_index_all + 1
        return

    def save_controlfile(self):
        """ Save the current tab as a control file."""
        # get the current tab index
        tab_index_current = self.tabs.tab_index_current
        # get the control file name
        cfg_filename = self.tabs.cfg_dict[tab_index_current]["controlfile_name"]
        # get the updated control file data
        cfg = self.tabs.tab_dict[tab_index_current].get_data_from_model()
        # check to make sure we are not overwriting the template version
        if "template" not in cfg_filename:
            # set the control file name
            cfg.filename = cfg_filename
        else:
            msg = " You are trying to write to the template folder.\n"
            msg = msg + "Please save this control file to a different location."
            msgbox = myMessageBox(msg)
            # put up a "Save as ..." dialog
            cfg_filename = QtWidgets.QFileDialog.getSaveFileName(self, "Save as ...")[0]
            # return without doing anything if cancel used
            if len(str(cfg_filename)) == 0:
                return
            # set the control file name
            cfg.filename = str(cfg_filename)
        # write the control file
        log.info(" Saving "+cfg.filename)
        cfg.write()
        # remove the asterisk in the tab text
        tab_text = str(self.tabs.tabText(tab_index_current))
        self.tabs.setTabText(self.tabs.tab_index_current, tab_text.replace("*",""))
        # reset the cfg changed logical to false
        self.tabs.tab_dict[tab_index_current].cfg_changed = False
        return

    def saveas_controlfile(self):
        """ Save the current tab with a different name."""
        # get the current tab index
        tab_index_current = self.tabs.tab_index_current
        # get the updated control file data
        cfg = self.tabs.tab_dict[tab_index_current].get_data_from_model()
        # put up a "Save as ..." dialog
        cfgpath = QtWidgets.QFileDialog.getSaveFileName(self, "Save as ...")[0]
        # return without doing anything if cancel used
        if len(str(cfgpath)) == 0:
            return
        # set the control file name
        cfg.filename = str(cfgpath)
        # write the control file
        log.info(" Saving "+cfg.filename)
        cfg.write()
        # update the control file name
        self.tabs.cfg_dict[tab_index_current]["controlfile_name"] = cfg.filename
        # update the tab text
        self.tabs.setTabText(tab_index_current, os.path.basename(str(cfgpath)))
        # reset the cfg changed logical to false
        self.tabs.tab_dict[tab_index_current].cfg_changed = False
        return

    def edit_preferences(self):
        log.warning("Edit/Preferences not implemented yet")
        return

    def tabSelected(self, arg=None):
        self.tabs.tab_index_current = arg

    def run_current(self):
        # save the current tab index
        tab_index_current = self.tabs.tab_index_current
        if tab_index_current == 0:
            msg = " No control file selected ..."
            log.warning(msg)
            return
        # get the updated control file data
        cfg = self.tabs.tab_dict[tab_index_current].get_data_from_model()
        # set the focus back to the log tab
        self.tabs.setCurrentIndex(0)
        # call the appropriate processing routine depending on the level
        self.tabs.tab_index_running = tab_index_current
        if self.tabs.cfg_dict[tab_index_current]["level"] == "oneflux":
            do_run_oneflux(cfg)
        else:
            log.error("Option not implemented yet ...")

    def closeTab (self, currentIndex):
        """ Close the selected tab."""
        # check to see if the tab contents have been saved
        tab_text = str(self.tabs.tabText(currentIndex))
        if "*" in tab_text:
            msg = "Save control file?"
            reply = QtWidgets.QMessageBox.question(self, 'Message', msg,
                                               QtWidgets.QMessageBox.Yes, QtWidgets.QMessageBox.No)
            if reply == QtWidgets.QMessageBox.Yes:
                self.save_controlfile()
        # get the current tab from its index
        currentQWidget = self.tabs.widget(currentIndex)
        # delete the tab
        currentQWidget.deleteLater()
        self.tabs.removeTab(currentIndex)
        # remove the corresponding entry in cfg_dict
        self.tabs.cfg_dict.pop(currentIndex)
        # and renumber the keys
        for n in self.tabs.cfg_dict.keys():
            if n > currentIndex:
                self.tabs.cfg_dict[n-1] = self.tabs.cfg_dict.pop(n)
        # remove the corresponding entry in tab_dict
        self.tabs.tab_dict.pop(currentIndex)
        # and renumber the keys
        for n in self.tabs.tab_dict.keys():
            if n > currentIndex:
                self.tabs.tab_dict[n-1] = self.tabs.tab_dict.pop(n)
        # decrement the tab index
        self.tabs.tab_index_all = self.tabs.tab_index_all - 1
        return

    def update_tab_text(self):
        """ Add an asterisk to the tab title text to indicate tab contents have changed."""
        # add an asterisk to the tab text to indicate the tab contents have changed
        tab_text = str(self.tabs.tabText(self.tabs.tab_index_current))
        if "*" not in tab_text:
            self.tabs.setTabText(self.tabs.tab_index_current, tab_text+"*")

if (__name__ == '__main__'):
    # get the application name and version
    app = QtWidgets.QApplication(["ONEFlux"])
    ui = oneflux_main_ui("ONEFlux V0.4.1")
    ui.show()
    app.exec_()
    del ui
