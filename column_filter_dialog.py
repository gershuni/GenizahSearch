from PyQt6.QtWidgets import QDialog, QVBoxLayout, QLabel, QLineEdit, QCheckBox, QHBoxLayout, QPushButton
from genizah_core import tr


class ColumnFilterDialog(QDialog):
    """Dialog to filter a column by include/exclude text."""
    def __init__(self, parent, column_label, current_text="", exclude=False):
        super().__init__(parent)
        self.setWindowTitle(tr("Filter {}").format(column_label))
        self.resize(420, 160)

        layout = QVBoxLayout()
        layout.addWidget(QLabel(tr("Enter text to filter this column:")))

        self.text_input = QLineEdit()
        self.text_input.setPlaceholderText(tr("Filter text..."))
        self.text_input.setText(current_text)
        layout.addWidget(self.text_input)

        self.exclude_checkbox = QCheckBox(tr("Exclude matches (must NOT contain the text)"))
        self.exclude_checkbox.setChecked(exclude)
        layout.addWidget(self.exclude_checkbox)

        btn_row = QHBoxLayout()
        btn_row.addStretch()
        btn_cancel = QPushButton(tr("Cancel"))
        btn_cancel.clicked.connect(self.reject)
        btn_ok = QPushButton(tr("OK"))
        btn_ok.clicked.connect(self.accept)
        btn_row.addWidget(btn_cancel)
        btn_row.addWidget(btn_ok)
        layout.addLayout(btn_row)

        self.setLayout(layout)

    def get_text(self):
        return self.text_input.text()

    def is_exclude(self):
        return self.exclude_checkbox.isChecked()
