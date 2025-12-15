import sys
import requests
import urllib3
from PyQt6.QtWidgets import QApplication, QMainWindow, QPushButton, QLabel, QVBoxLayout, QWidget
from PyQt6.QtCore import QThread, pyqtSignal, Qt
from PyQt6.QtGui import QImage, QPixmap, QImageReader

# Silence SSL warnings for test run
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Known-good test URL
TEST_URL = "https://iiif.nli.org.il/IIIFv21/FL160999962/full/400,/0/default.jpg"

class DebugThread(QThread):
    image_loaded = pyqtSignal(QImage)
    load_failed = pyqtSignal(str)

    def run(self):
        print(f"[THREAD] Starting download from: {TEST_URL}")
        
        headers = {"User-Agent": "GenizahPro/2.1 Debugger"}
        
        try:
            print("[THREAD] Sending GET request (verify=False)...")
            resp = requests.get(TEST_URL, headers=headers, timeout=30, stream=True, verify=False)
            
            print(f"[THREAD] Response received. Status Code: {resp.status_code}")
            print(f"[THREAD] Content-Type header: {resp.headers.get('Content-Type')}")
            
            if resp.status_code != 200:
                self.load_failed.emit(f"HTTP Error {resp.status_code}")
                return

            data = resp.content
            data_len = len(data)
            print(f"[THREAD] Data downloaded. Size: {data_len} bytes")
            
            if data_len == 0:
                self.load_failed.emit("Empty response body")
                return

            # Print first bytes to confirm image vs. HTML error page
            first_bytes = data[:20]
            print(f"[THREAD] First 20 bytes: {first_bytes}")

            print("[THREAD] Attempting QImage.fromData()...")
            img = QImage.fromData(data)
            
            if img.isNull():
                print("[THREAD] FAIL: QImage.fromData returned Null image!")
                self.load_failed.emit("QImage decoding failed (isNull=True)")
            else:
                print(f"[THREAD] SUCCESS: Image created. Size: {img.width()}x{img.height()}")
                self.image_loaded.emit(img)

        except Exception as e:
            print(f"[THREAD] EXCEPTION: {e}")
            self.load_failed.emit(str(e))

class DebugWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Genizah Image Debugger")
        self.resize(400, 400)
        
        layout = QVBoxLayout()
        
        self.lbl_status = QLabel("Ready to test.")
        layout.addWidget(self.lbl_status)
        
        self.btn_start = QPushButton("Start Test")
        self.btn_start.clicked.connect(self.start_test)
        layout.addWidget(self.btn_start)
        
        self.lbl_img = QLabel("Image area")
        self.lbl_img.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.lbl_img.setStyleSheet("border: 2px solid red; background: #eee;")
        self.lbl_img.setFixedSize(300, 300)
        layout.addWidget(self.lbl_img)
        
        container = QWidget()
        container.setLayout(layout)
        self.setCentralWidget(container)
        
        # Report supported image formats
        formats = [fmt.data().decode("ascii") for fmt in QImageReader.supportedImageFormats()]
        print(f"[MAIN] Supported Image Formats on this system: {formats}")

    def start_test(self):
        self.lbl_status.setText("Downloading...")
        self.btn_start.setEnabled(False)
        
        # Keep reference to avoid premature garbage collection
        self.worker = DebugThread()
        self.worker.image_loaded.connect(self.on_success)
        self.worker.load_failed.connect(self.on_fail)
        self.worker.start()

    def on_success(self, img):
        print("[MAIN] Signal received: Image Loaded!")
        self.lbl_status.setText("Success!")
        self.lbl_img.setPixmap(QPixmap.fromImage(img))
        self.btn_start.setEnabled(True)

    def on_fail(self, err):
        print(f"[MAIN] Signal received: FAILED - {err}")
        self.lbl_status.setText(f"Error: {err}")
        self.btn_start.setEnabled(True)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = DebugWindow()
    win.show()
    sys.exit(app.exec())
