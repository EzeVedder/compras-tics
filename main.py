# main.py

import sys
import time
from datetime import date as dt_date

from PySide6.QtCore import Qt, QDate, QThread, Signal, QObject
from PySide6.QtWidgets import (
    QApplication,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QComboBox,
    QDateEdit,
    QLineEdit,
    QPushButton,
    QFileDialog,
    QProgressBar,
    QMessageBox,
)

from scrapers import get_scraper


class ScraperWorker(QObject):
    progressChanged = Signal(int)       # 0..100
    finished = Signal(int, bool)        # (cantidad_registros, cancelado)
    error = Signal(str)

    def __init__(self, site_key: str, date_from: QDate, date_to: QDate, output_dir: str):
        super().__init__()
        self.site_key = site_key
        self.start_date = dt_date(date_from.year(), date_from.month(), date_from.day())
        self.end_date = dt_date(date_to.year(), date_to.month(), date_to.day())
        self.output_dir = output_dir
        self._cancelled = False

    def request_cancel(self):
        """Se llama desde el hilo principal para pedir cancelación."""
        self._cancelled = True

    def is_cancelled(self) -> bool:
        return self._cancelled

    def run(self):
        try:
            scraper_func = get_scraper(self.site_key)
            count = scraper_func(
                self.start_date,
                self.end_date,
                self.output_dir,
                progress_callback=self.progressChanged.emit,
                is_cancelled=self.is_cancelled,
            )
            cancelled = self._cancelled
            if not cancelled and count > 0:
                self.progressChanged.emit(100)
            self.finished.emit(count, cancelled)
        except Exception as e:
            self.error.emit(str(e))


class ScraperWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Scraper de procesos públicos")
        self.resize(700, 280)

        self._thread = None
        self._worker = None
        self._start_time: float | None = None

        self._init_ui()
        self._connect_signals()

    # ---------------- UI ----------------
    def _init_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(12, 12, 12, 12)
        main_layout.setSpacing(10)

        title = QLabel("Scraping de procesos de compra")
        title.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        title.setStyleSheet("font-size: 18px; font-weight: bold;")
        main_layout.addWidget(title)

        # --- Selección de web ---
        row_site = QHBoxLayout()
        lbl_site = QLabel("Web a scrapear:")
        self.site_combo = QComboBox()
        # Boletín Oficial - Tercera Sección
        self.site_combo.addItem(
            "Boletín Oficial - Contrataciones (Tercera sección)",
            "boletin_tercera",
        )
        # COMPR.AR - Procesos TICs
        self.site_combo.addItem("COMPR.AR TICs (HTTP)", "comprar_tics")
        self.site_combo.addItem("COMPR.AR TICs (Robot Selenium)", "comprar_tics_robot")


        row_site.addWidget(lbl_site)
        row_site.addWidget(self.site_combo, stretch=1)
        main_layout.addLayout(row_site)

        # --- Período ---
        row_dates = QHBoxLayout()
        lbl_from = QLabel("Desde:")
        self.date_from_edit = QDateEdit(calendarPopup=True)
        self.date_from_edit.setDisplayFormat("dd/MM/yyyy")
        today = QDate.currentDate()
        self.date_from_edit.setDate(today.addDays(-7))

        lbl_to = QLabel("Hasta:")
        self.date_to_edit = QDateEdit(calendarPopup=True)
        self.date_to_edit.setDisplayFormat("dd/MM/yyyy")
        self.date_to_edit.setDate(today)

        row_dates.addWidget(lbl_from)
        row_dates.addWidget(self.date_from_edit)
        row_dates.addSpacing(20)
        row_dates.addWidget(lbl_to)
        row_dates.addWidget(self.date_to_edit)
        row_dates.addStretch()
        main_layout.addLayout(row_dates)

        # --- Directorio exportación ---
        row_dir = QHBoxLayout()
        lbl_dir = QLabel("Directorio de exportación:")
        self.output_dir_edit = QLineEdit()
        self.output_dir_edit.setPlaceholderText("Elegí una carpeta donde guardar el Excel...")
        self.output_dir_edit.setReadOnly(True)
        self.btn_browse = QPushButton("Seleccionar...")

        row_dir.addWidget(lbl_dir)
        row_dir.addWidget(self.output_dir_edit, stretch=1)
        row_dir.addWidget(self.btn_browse)
        main_layout.addLayout(row_dir)

        # --- Indicador + barra + botones ---
        row_bottom = QHBoxLayout()

        left_bottom = QVBoxLayout()
        self.lbl_count = QLabel("Registros exportados: -")
        self.lbl_count.setStyleSheet("font-size: 14px;")

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)

        self.lbl_eta = QLabel("Tiempo restante estimado: -")
        self.lbl_eta.setStyleSheet("color: gray;")

        left_bottom.addWidget(self.lbl_count)
        left_bottom.addWidget(self.progress_bar)
        left_bottom.addWidget(self.lbl_eta)

        row_bottom.addLayout(left_bottom, stretch=1)
        row_bottom.addSpacing(20)

        # Botones iniciar/cancelar
        btns_layout = QVBoxLayout()
        self.btn_start = QPushButton("Iniciar scraping")
        self.btn_start.setMinimumWidth(150)

        self.btn_cancel = QPushButton("Cancelar")
        self.btn_cancel.setMinimumWidth(150)
        self.btn_cancel.setEnabled(False)

        btns_layout.addWidget(self.btn_start)
        btns_layout.addWidget(self.btn_cancel)

        row_bottom.addLayout(btns_layout)

        main_layout.addLayout(row_bottom)

        self.status_label = QLabel("Listo.")
        self.status_label.setStyleSheet("color: gray;")
        main_layout.addWidget(self.status_label)

    # ---------------- Señales ----------------
    def _connect_signals(self):
        self.btn_browse.clicked.connect(self._select_output_dir)
        self.btn_start.clicked.connect(self._on_start_clicked)
        self.btn_cancel.clicked.connect(self._on_cancel_clicked)

    # ---------------- Helpers ----------------
    @staticmethod
    def _format_seconds(seconds: float) -> str:
        seconds = int(seconds)
        h, rem = divmod(seconds, 3600)
        m, s = divmod(rem, 60)
        if h:
            return f"{h:d}h {m:02d}m {s:02d}s"
        else:
            return f"{m:d}m {s:02d}s"

    # ---------------- Lógica UI ----------------
    def _select_output_dir(self):
        directory = QFileDialog.getExistingDirectory(
            self,
            "Seleccionar directorio de exportación",
            "",
        )
        if directory:
            self.output_dir_edit.setText(directory)

    def _on_start_clicked(self):
        site_key = self.site_combo.currentData()
        if not site_key:
            QMessageBox.warning(self, "Scraper", "Seleccioná una web a scrapear.")
            return

        date_from = self.date_from_edit.date()
        date_to = self.date_to_edit.date()
        if date_from > date_to:
            QMessageBox.warning(
                self,
                "Scraper",
                "La fecha 'Desde' no puede ser mayor que la fecha 'Hasta'.",
            )
            return

        output_dir = self.output_dir_edit.text().strip()
        if not output_dir:
            QMessageBox.warning(
                self,
                "Scraper",
                "Seleccioná un directorio de exportación.",
            )
            return

        self.btn_start.setEnabled(False)
        self.btn_cancel.setEnabled(True)
        self.progress_bar.setValue(0)
        self.lbl_count.setText("Registros exportados: -")
        self.lbl_eta.setText("Tiempo restante estimado: calculando…")
        self.status_label.setText("Ejecutando scraping...")

        self._start_time = time.time()

        self._thread = QThread(self)
        self._worker = ScraperWorker(site_key, date_from, date_to, output_dir)
        self._worker.moveToThread(self._thread)

        self._thread.started.connect(self._worker.run)
        self._worker.progressChanged.connect(self._on_progress_changed)
        self._worker.finished.connect(self._on_finished)
        self._worker.error.connect(self._on_error)

        self._worker.finished.connect(self._thread.quit)
        self._worker.error.connect(self._thread.quit)
        self._thread.finished.connect(self._worker.deleteLater)
        self._thread.finished.connect(self._thread.deleteLater)

        self._thread.start()

    def _on_cancel_clicked(self):
        if self._worker is not None:
            self.status_label.setText("Cancelando... puede demorar unos segundos.")
            self._worker.request_cancel()
            self.btn_cancel.setEnabled(False)

    def _on_progress_changed(self, value: int):
        self.progress_bar.setValue(value)

        if self._start_time is not None and value > 0:
            elapsed = time.time() - self._start_time
            total_est = elapsed * 100.0 / value
            remaining = max(0.0, total_est - elapsed)
            self.lbl_eta.setText(
                f"Tiempo restante estimado: {self._format_seconds(remaining)}"
            )

    def _on_finished(self, count: int, cancelled: bool):
        self.btn_start.setEnabled(True)
        self.btn_cancel.setEnabled(False)

        elapsed = None
        if self._start_time is not None:
            elapsed = time.time() - self._start_time
        self._start_time = None

        self.lbl_count.setText(f"Registros exportados: {count}")
        if elapsed is not None:
            self.lbl_eta.setText(f"Tiempo total: {self._format_seconds(elapsed)}")
        else:
            self.lbl_eta.setText("Tiempo total: -")

        if cancelled:
            self.status_label.setText("Scraping cancelado por el usuario.")
            QMessageBox.information(
                self,
                "Scraper",
                f"Scraping cancelado.\n\nRegistros exportados (parcial): {count}",
            )
        else:
            self.status_label.setText("Scraping finalizado.")
            QMessageBox.information(
                self,
                "Scraper",
                f"Scraping finalizado.\n\nRegistros exportados: {count}",
            )

    def _on_error(self, message: str):
        self.btn_start.setEnabled(True)
        self.btn_cancel.setEnabled(False)
        self._start_time = None
        self.status_label.setText("Error en el scraping.")
        self.lbl_eta.setText("Tiempo restante estimado: -")
        QMessageBox.critical(self, "Error en el scraper", message)


def main():
    app = QApplication(sys.argv)
    win = ScraperWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
