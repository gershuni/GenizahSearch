from PyQt6.QtWidgets import (QDialog, QVBoxLayout, QLabel, QRadioButton, QButtonGroup,
                             QTreeWidget, QTreeWidgetItem, QTreeWidgetItemIterator, QPushButton, QHBoxLayout, QWidget)
from PyQt6.QtCore import Qt
from genizah_core import tr

class ListFilterDialog(QDialog):
    """
    Dialog to configure list-based filtering for search results.
    Allows selecting inclusion/exclusion mode and specific lists/projects.
    """
    def __init__(self, parent, lists_mgr, current_state=None):
        super().__init__(parent)
        self.setWindowTitle(tr("Filter by List"))
        self.resize(400, 500)
        self.lists_mgr = lists_mgr

        state = current_state or {}
        # Defaults
        self.selected_mode = state.get('mode', 'in') # 'in' or 'not_in'
        # current_selection: set of list_ids or "all"
        self.selected_lists = state.get('lists') if state.get('lists') is not None else "all"

        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # --- 1. Mode Selection ---
        mode_group_box = QWidget()
        mode_layout = QVBoxLayout(mode_group_box)
        mode_layout.setContentsMargins(0, 0, 0, 0)

        self.lbl_instruction = QLabel(tr("Show entries:"))
        mode_layout.addWidget(self.lbl_instruction)

        self.radio_in = QRadioButton(tr("In selected lists"))
        self.radio_not_in = QRadioButton(tr("Not in selected lists"))

        self.mode_group = QButtonGroup(self)
        self.mode_group.addButton(self.radio_in, 1)
        self.mode_group.addButton(self.radio_not_in, 2)

        if self.selected_mode == "not_in":
            self.radio_not_in.setChecked(True)
        else:
            self.radio_in.setChecked(True)

        mode_layout.addWidget(self.radio_in)
        mode_layout.addWidget(self.radio_not_in)
        layout.addWidget(mode_group_box)

        # --- 2. List Selection Tree ---
        self.tree = QTreeWidget()
        self.tree.setHeaderHidden(True)
        self.tree.setIndentation(20)
        layout.addWidget(self.tree)

        # Populate Tree
        self._populate_tree()

        # Connect signal for checkbox propagation
        self.tree.itemChanged.connect(self.on_item_changed)

        # --- 3. Buttons ---
        btn_box = QHBoxLayout()
        btn_box.addStretch()

        self.btn_cancel = QPushButton(tr("Cancel"))
        self.btn_cancel.clicked.connect(self.reject)

        self.btn_ok = QPushButton(tr("OK"))
        self.btn_ok.clicked.connect(self.on_accept)
        self.btn_ok.setDefault(True)
        self.btn_ok.setAutoDefault(True)
        self.btn_cancel.setAutoDefault(False)

        btn_box.addWidget(self.btn_cancel)
        btn_box.addWidget(self.btn_ok)
        layout.addLayout(btn_box)

    def _populate_tree(self):
        self.tree.clear()

        # Root Item: "All Lists"
        self.root_all = QTreeWidgetItem(self.tree)
        self.root_all.setText(0, tr("All Lists"))
        self.root_all.setFlags(self.root_all.flags() | Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsAutoTristate)
        self.root_all.setCheckState(0, Qt.CheckState.Checked) # Default
        self.root_all.setData(0, Qt.ItemDataRole.UserRole, "all")

        # Get data structure
        projects = self.lists_mgr.get_projects() # [{'id', 'name', ...}]
        all_lists = self.lists_mgr.get_all_lists(include_recent=False) # [{'id', 'name', 'project_id'}]

        # Map lists to projects
        project_map = {p['id']: [] for p in projects}
        unassigned_lists = []

        for lst in all_lists:
            pid = lst.get('project_id')
            if pid and pid in project_map:
                project_map[pid].append(lst)
            else:
                unassigned_lists.append(lst)

        # 1. Add Projects and their lists
        for proj in projects:
            proj_node = QTreeWidgetItem(self.root_all)
            proj_node.setText(0, proj.get('name', 'Project'))
            proj_node.setFlags(proj_node.flags() | Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsAutoTristate)
            proj_node.setCheckState(0, Qt.CheckState.Checked)
            proj_node.setData(0, Qt.ItemDataRole.UserRole, f"proj:{proj['id']}")

            p_lists = project_map.get(proj['id'], [])
            for lst in p_lists:
                self._add_list_node(proj_node, lst)

        # 2. Add Unassigned Lists
        for lst in unassigned_lists:
            self._add_list_node(self.root_all, lst)

        self.tree.expandAll()

        # Apply current selection if not "all"
        if self.selected_lists != "all":
            # First uncheck everything
            self.root_all.setCheckState(0, Qt.CheckState.Unchecked)

            # Now traverse and check items in set
            iterator = QTreeWidgetItemIterator(self.tree)
            while iterator.value():
                item = iterator.value()
                data = item.data(0, Qt.ItemDataRole.UserRole)
                if data and not str(data).startswith("proj:") and data != "all":
                    # It's a list ID
                    if data in self.selected_lists:
                        item.setCheckState(0, Qt.CheckState.Checked)
                    else:
                        item.setCheckState(0, Qt.CheckState.Unchecked)
                iterator += 1

    def _add_list_node(self, parent, list_data):
        node = QTreeWidgetItem(parent)
        name = list_data.get('name', '')
        # Translate default list name
        if list_data.get('is_default'):
            name = tr("General")

        node.setText(0, name)
        node.setFlags(node.flags() | Qt.ItemFlag.ItemIsUserCheckable)
        node.setCheckState(0, Qt.CheckState.Checked)
        node.setData(0, Qt.ItemDataRole.UserRole, list_data['id'])

    def on_item_changed(self, item, column):
        """Handle checkbox clicks to propagate state to children."""
        if column == 0:
            state = item.checkState(0)

            # Ignore intermediate states (don't propagate Partial)
            if state == Qt.CheckState.PartiallyChecked:
                return

            if item.flags() & Qt.ItemFlag.ItemIsAutoTristate:
                # If it's a parent/project node, push state to children
                # Block signals to prevent recursive loops from child updates bubbling back up
                self.tree.blockSignals(True)
                for i in range(item.childCount()):
                    child = item.child(i)
                    if child.checkState(0) != state:
                        child.setCheckState(0, state)
                        # Recurse if child also has children
                        if child.childCount() > 0:
                            self._propagate_state(child, state)
                self.tree.blockSignals(False)

    def _propagate_state(self, item, state):
        for i in range(item.childCount()):
            child = item.child(i)
            child.setCheckState(0, state)
            if child.childCount() > 0:
                self._propagate_state(child, state)

    def on_accept(self):
        # 1. Get Mode
        self.selected_mode = "not_in" if self.radio_not_in.isChecked() else "in"

        # 2. Get Selection
        if self.root_all.checkState(0) == Qt.CheckState.Checked:
            self.selected_lists = "all"
        else:
            selected_ids = set()
            iterator = QTreeWidgetItemIterator(self.tree)
            while iterator.value():
                item = iterator.value()
                data = item.data(0, Qt.ItemDataRole.UserRole)
                # We only care about leaf lists, not projects or root container
                if data and not str(data).startswith("proj:") and data != "all":
                    if item.checkState(0) == Qt.CheckState.Checked:
                        selected_ids.add(data)
                iterator += 1

            self.selected_lists = selected_ids

        self.accept()

    def get_selection(self):
        return self.selected_mode, self.selected_lists
