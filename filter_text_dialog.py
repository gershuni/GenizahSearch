from PyQt6.QtWidgets import QDialog, QVBoxLayout, QLabel, QPlainTextEdit, QHBoxLayout, QPushButton, QFileDialog
from genizah_core import tr

class FilterTextDialog(QDialog):
    """Dialog to input or load text for filtering composition results."""
    def __init__(self, parent, current_text=""):
        super().__init__(parent)
        self.setWindowTitle(tr("Filter Text"))
        self.resize(500, 400)
        self.result_text = current_text

        layout = QVBoxLayout()

        layout.addWidget(QLabel(tr("Enter text to filter results (results found in this text will be moved to a separate list):")))

        self.text_area = QPlainTextEdit()
        self.text_area.setPlaceholderText(tr("Paste text here..."))
        self.text_area.setPlainText(current_text)
        layout.addWidget(self.text_area)

        btn_row = QHBoxLayout()
        btn_load = QPushButton(tr("Load from File"))
        btn_load.clicked.connect(self.load_file)
        btn_row.addWidget(btn_load)

        btn_row.addStretch()

        btn_ok = QPushButton(tr("OK"))
        btn_ok.clicked.connect(self.accept)
        btn_cancel = QPushButton(tr("Cancel"))
        btn_cancel.clicked.connect(self.reject)

        btn_row.addWidget(btn_cancel)
        btn_row.addWidget(btn_ok)
        layout.addLayout(btn_row)

        self.setLayout(layout)

    def load_file(self):
        path, _ = QFileDialog.getOpenFileName(self, tr("Load Text"), "", "Text Files (*.txt);;All Files (*)")
        if path:
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    self.text_area.setPlainText(f.read())
            except Exception as e:
                # In a real app we might show an error message, but simplicity for now
                pass

    def get_text(self):
        return self.text_area.toPlainText()
