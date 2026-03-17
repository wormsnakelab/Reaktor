import csv
import queue
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import serial
import serial.tools.list_ports
import tkinter as tk
from tkinter import ttk, messagebox

from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure


BAUD = 115200
CSV_FILE = Path(f"Reaktor_log_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv")


@dataclass
class FrameData:
    frame_index: int
    pc_time: str
    frame_type: str
    mode_num: Optional[int]
    mode_name: str
    input_mv: Optional[int]
    battery_mv: Optional[int]
    current_cA: Optional[int]
    cell1_mv: Optional[int]
    cell2_mv: Optional[int]
    cell3_mv: Optional[int]
    cell4_mv: Optional[int]
    cell5_mv: Optional[int]
    cell6_mv: Optional[int]
    temp_raw: Optional[int]
    status: Optional[int]
    mah: Optional[int]
    unknown_value: Optional[int]
    raw_line: str

    @property
    def input_v(self) -> Optional[float]:
        return self.input_mv / 1000 if self.input_mv is not None else None

    @property
    def battery_v(self) -> Optional[float]:
        return self.battery_mv / 1000 if self.battery_mv is not None else None

    @property
    def current_a(self) -> Optional[float]:
        return self.current_cA / 100 if self.current_cA is not None else None

    @property
    def temp_c(self) -> Optional[float]:
        return self.temp_raw / 10 if self.temp_raw is not None else None

    @property
    def cells_mv(self) -> list[int]:
        vals = [
            self.cell1_mv,
            self.cell2_mv,
            self.cell3_mv,
            self.cell4_mv,
            self.cell5_mv,
            self.cell6_mv,
        ]
        return [v for v in vals if v is not None and v > 0]

    @property
    def cell_delta_mv(self) -> Optional[int]:
        vals = self.cells_mv
        if not vals:
            return None
        return max(vals) - min(vals)

    @property
    def cell_delta_v(self) -> Optional[float]:
        return self.cell_delta_mv / 1000 if self.cell_delta_mv is not None else None

    @property
    def status_bin(self) -> str:
        return f"{self.status:08b}" if self.status is not None else "-"

    @property
    def unknown_value_bin(self) -> str:
        return f"{self.unknown_value:08b}" if self.unknown_value is not None else "-"


class SerialWorker(threading.Thread):
    def __init__(self, port: str, out_queue: queue.Queue):
        super().__init__(daemon=True)
        self.port = port
        self.out_queue = out_queue
        self._stop_event = threading.Event()
        self.ser: Optional[serial.Serial] = None
        self.buffer = bytearray()
        self.frame_index = 0

    def stop(self) -> None:
        self._stop_event.set()
        try:
            if self.ser and self.ser.is_open:
                self.ser.close()
        except Exception:
            pass

    @staticmethod
    def decode_frame(payload: bytes) -> str:
        return "".join(
            chr(b - 0x60) if 32 <= (b - 0x60) <= 126 else "."
            for b in payload
        )

    @staticmethod
    def to_int(value: str) -> Optional[int]:
        return int(value) if value.isdigit() else None

    @staticmethod
    def mode_name(mode: Optional[int]) -> str:
        mapping = {
            1: "charge",
            2: "discharge",
        }
        if mode is None:
            return "unknown"
        return mapping.get(mode, f"mode_{mode}")

    def parse_line(self, decoded: str) -> Optional[FrameData]:
        parts = decoded.split(";")
        if len(parts) < 16:
            return None

        self.frame_index += 1

        return FrameData(
            frame_index=self.frame_index,
            pc_time=time.strftime("%H:%M:%S"),
            frame_type=parts[0],
            mode_num=self.to_int(parts[1]),
            mode_name=self.mode_name(self.to_int(parts[1])),
            input_mv=self.to_int(parts[3]),
            battery_mv=self.to_int(parts[4]),
            current_cA=self.to_int(parts[5]),
            cell1_mv=self.to_int(parts[6]),
            cell2_mv=self.to_int(parts[7]),
            cell3_mv=self.to_int(parts[8]),
            cell4_mv=self.to_int(parts[9]),
            cell5_mv=self.to_int(parts[10]),
            cell6_mv=self.to_int(parts[11]),
            temp_raw=self.to_int(parts[12]),
            status=self.to_int(parts[13]),
            mah=self.to_int(parts[14]),
            unknown_value=self.to_int(parts[15]),
            raw_line=decoded,
        )

    def run(self) -> None:
        try:
            self.ser = serial.Serial(self.port, BAUD, timeout=0.2)
            self.out_queue.put(("info", f"Open {self.port} @ {BAUD}"))

            while not self._stop_event.is_set():
                data = self.ser.read(self.ser.in_waiting or 1)
                if not data:
                    continue

                self.buffer.extend(data)

                while b"" in self.buffer:
                    pos = self.buffer.index(0x8A)
                    frame = bytes(self.buffer[:pos + 1])
                    del self.buffer[:pos + 1]

                    payload = frame[:-2] if frame.endswith(b"") else frame[:-1]
                    if not payload:
                        continue

                    decoded = self.decode_frame(payload)
                    parsed = self.parse_line(decoded)
                    if parsed:
                        self.out_queue.put(("frame", parsed))
                    else:
                        self.out_queue.put(("info", f"Short frame: {decoded}"))
        except Exception as exc:
            self.out_queue.put(("error", str(exc)))


class App:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Reaktor Logger")
        self.root.geometry("1680x980")

        self.msg_queue: queue.Queue = queue.Queue()
        self.worker: Optional[SerialWorker] = None
        self.last_frame: Optional[FrameData] = None

        self.history_x: list[int] = []
        self.history_input_v: list[float] = []
        self.history_battery_v: list[float] = []
        self.history_current_a: list[float] = []
        self.history_cells: dict[str, list[float]] = {
            "cell1": [],
            "cell2": [],
            "cell3": [],
            "cell4": [],
            "cell5": [],
            "cell6": [],
        }
        self.plot_dirty = False

        self._build_ui()
        self._ensure_csv_header()
        self.refresh_ports()
        self.root.after(100, self.poll_queue)
        self.root.after(400, self.redraw_plot_if_needed)
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    def _build_ui(self) -> None:
        top = ttk.Frame(self.root, padding=10)
        top.pack(fill="x")

        ttk.Label(top, text="COM port:").pack(side="left")
        self.port_var = tk.StringVar()
        self.port_combo = ttk.Combobox(top, textvariable=self.port_var, width=12, state="readonly")
        self.port_combo.pack(side="left", padx=(6, 10))

        ttk.Button(top, text="Refresh", command=self.refresh_ports).pack(side="left")
        ttk.Button(top, text="Connect", command=self.connect).pack(side="left", padx=(10, 4))
        ttk.Button(top, text="Disconnect", command=self.disconnect).pack(side="left")
        ttk.Button(top, text="Clear history", command=self.clear_history).pack(side="left", padx=(10, 4))

        self.status_var = tk.StringVar(value="Disconnected")
        ttk.Label(top, textvariable=self.status_var).pack(side="left", padx=(14, 0))

        grid = ttk.LabelFrame(self.root, text="Live values", padding=10)
        grid.pack(fill="x", padx=10, pady=6)

        self.value_vars = {
            "frame_index": tk.StringVar(value="-"),
            "mode": tk.StringVar(value="-"),
            "input": tk.StringVar(value="-"),
            "battery": tk.StringVar(value="-"),
            "current": tk.StringVar(value="-"),
            "capacity": tk.StringVar(value="-"),
            "unknown_value": tk.StringVar(value="-"),
            "unknown_value_bin": tk.StringVar(value="-"),
            "temp": tk.StringVar(value="-"),
            "status": tk.StringVar(value="-"),
            "status_bin": tk.StringVar(value="-"),
            "delta": tk.StringVar(value="-"),
            "cell1": tk.StringVar(value="-"),
            "cell2": tk.StringVar(value="-"),
            "cell3": tk.StringVar(value="-"),
            "cell4": tk.StringVar(value="-"),
            "cell5": tk.StringVar(value="-"),
            "cell6": tk.StringVar(value="-"),
        }

        pairs = [
            ("Frame #", "frame_index"),
            ("Mode", "mode"),
            ("Input", "input"),
            ("Battery", "battery"),
            ("Current", "current"),
            ("Capacity", "capacity"),
            ("Unknown value", "unknown_value"),
            ("Unknown bin", "unknown_value_bin"),
            ("Temp", "temp"),
            ("Status", "status"),
            ("Status bin", "status_bin"),
            ("Cell delta", "delta"),
            ("Cell1", "cell1"),
            ("Cell2", "cell2"),
            ("Cell3", "cell3"),
            ("Cell4", "cell4"),
            ("Cell5", "cell5"),
            ("Cell6", "cell6"),
        ]

        for i, (label, key) in enumerate(pairs):
            r = i // 3
            c = (i % 3) * 2
            ttk.Label(grid, text=label + ":").grid(row=r, column=c, sticky="w", padx=(0, 8), pady=2)
            ttk.Label(grid, textvariable=self.value_vars[key], width=24).grid(row=r, column=c + 1, sticky="w", pady=2)

        plot_frame = ttk.LabelFrame(self.root, text="Live history plot", padding=6)
        plot_frame.pack(fill="both", expand=False, padx=10, pady=6)

        self.figure = Figure(figsize=(14, 4.2), dpi=100)
        self.ax_voltage = self.figure.add_subplot(111)
        self.ax_current = self.ax_voltage.twinx()
        self.ax_voltage.set_xlabel("Frame #")
        self.ax_voltage.set_ylabel("Voltage [V]")
        self.ax_current.set_ylabel("Current [A]")
        self.ax_voltage.grid(True)
        self.figure.tight_layout()

        self.plot_canvas = FigureCanvasTkAgg(self.figure, master=plot_frame)
        self.plot_canvas.get_tk_widget().pack(fill="both", expand=True)

        mid = ttk.Panedwindow(self.root, orient="vertical")
        mid.pack(fill="both", expand=True, padx=10, pady=6)

        frame_top = ttk.LabelFrame(mid, text="Recent frames", padding=6)
        frame_bottom = ttk.LabelFrame(mid, text="Raw log", padding=6)
        mid.add(frame_top, weight=3)
        mid.add(frame_bottom, weight=2)

        columns = (
            "pc_time",
            "frame_index",
            "input_v",
            "mode",
            "battery_v",
            "current_a",
            "mah",
            "cell1",
            "cell2",
            "cell3",
            "cell4",
            "cell5",
            "cell6",
            "delta_v",
            "status",
            "status_bin",
            "unknown_value",
            "unknown_bin",
        )

        self.tree = ttk.Treeview(frame_top, columns=columns, show="headings", height=14)

        for col, title, width in [
            ("pc_time", "PC time", 90),
            ("frame_index", "Frame #", 80),
            ("input_v", "Input V", 90),
            ("mode", "Mode", 100),
            ("battery_v", "Bat V", 90),
            ("current_a", "Bat I", 90),
            ("mah", "mAh", 80),
            ("cell1", "Cell1", 80),
            ("cell2", "Cell2", 80),
            ("cell3", "Cell3", 80),
            ("cell4", "Cell4", 80),
            ("cell5", "Cell5", 80),
            ("cell6", "Cell6", 80),
            ("delta_v", "Delta V", 90),
            ("status", "Status", 70),
            ("status_bin", "Status bin", 110),
            ("unknown_value", "Unknown", 80),
            ("unknown_bin", "Unknown bin", 110),
        ]:
            self.tree.heading(col, text=title)
            self.tree.column(col, width=width, anchor="center")

        self.tree.pack(fill="both", expand=True)

        self.log = tk.Text(frame_bottom, height=10, wrap="none")
        self.log.pack(fill="both", expand=True)

    def refresh_ports(self) -> None:
        ports = [p.device for p in serial.tools.list_ports.comports()]
        self.port_combo["values"] = ports
        if ports and not self.port_var.get():
            self.port_var.set(ports[0])

    def connect(self) -> None:
        port = self.port_var.get().strip()
        if not port:
            messagebox.showwarning("Reaktor Logger", "Select a COM port.")
            return
        if self.worker is not None:
            messagebox.showinfo("Reaktor Logger", "Port is already open.")
            return

        self.worker = SerialWorker(port, self.msg_queue)
        self.worker.start()
        self.status_var.set(f"Connecting {port}...")

    def disconnect(self) -> None:
        if self.worker:
            self.worker.stop()
            self.worker = None
        self.status_var.set("Disconnected")

    def clear_history(self) -> None:
        self.history_x.clear()
        self.history_input_v.clear()
        self.history_battery_v.clear()
        self.history_current_a.clear()
        for key in self.history_cells:
            self.history_cells[key].clear()
        for item in self.tree.get_children():
            self.tree.delete(item)
        self.plot_dirty = True
        self.redraw_plot()

    def _ensure_csv_header(self) -> None:
        if CSV_FILE.exists() and CSV_FILE.stat().st_size > 0:
            return

        with CSV_FILE.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "pc_time",
                "frame_index",
                "frame_type",
                "mode_num",
                "mode_name",
                "input_mv",
                "input_v",
                "battery_mv",
                "battery_v",
                "current_cA",
                "current_a",
                "mah",
                "cell1_mv",
                "cell1_v",
                "cell2_mv",
                "cell2_v",
                "cell3_mv",
                "cell3_v",
                "cell4_mv",
                "cell4_v",
                "cell5_mv",
                "cell5_v",
                "cell6_mv",
                "cell6_v",
                "cell_delta_mv",
                "cell_delta_v",
                "temp_raw",
                "temp_c",
                "status",
                "status_bin",
                "unknown_value",
                "unknown_value_bin",
                "raw_line",
            ])

    def append_csv(self, d: FrameData) -> None:
        def cell_v(mv: Optional[int]) -> Optional[float]:
            return mv / 1000 if mv is not None else None

        with CSV_FILE.open("a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                d.pc_time,
                d.frame_index,
                d.frame_type,
                d.mode_num,
                d.mode_name,
                d.input_mv,
                d.input_v,
                d.battery_mv,
                d.battery_v,
                d.current_cA,
                d.current_a,
                d.mah,
                d.cell1_mv,
                cell_v(d.cell1_mv),
                d.cell2_mv,
                cell_v(d.cell2_mv),
                d.cell3_mv,
                cell_v(d.cell3_mv),
                d.cell4_mv,
                cell_v(d.cell4_mv),
                d.cell5_mv,
                cell_v(d.cell5_mv),
                d.cell6_mv,
                cell_v(d.cell6_mv),
                d.cell_delta_mv,
                d.cell_delta_v,
                d.temp_raw,
                d.temp_c,
                d.status,
                d.status_bin,
                d.unknown_value,
                d.unknown_value_bin,
                d.raw_line,
            ])

    def add_history(self, d: FrameData) -> None:
        self.history_x.append(d.frame_index)
        self.history_input_v.append(d.input_v if d.input_v is not None else float("nan"))
        self.history_battery_v.append(d.battery_v if d.battery_v is not None else float("nan"))
        self.history_current_a.append(d.current_a if d.current_a is not None else float("nan"))
        self.history_cells["cell1"].append(d.cell1_mv / 1000 if d.cell1_mv is not None and d.cell1_mv > 0 else float("nan"))
        self.history_cells["cell2"].append(d.cell2_mv / 1000 if d.cell2_mv is not None and d.cell2_mv > 0 else float("nan"))
        self.history_cells["cell3"].append(d.cell3_mv / 1000 if d.cell3_mv is not None and d.cell3_mv > 0 else float("nan"))
        self.history_cells["cell4"].append(d.cell4_mv / 1000 if d.cell4_mv is not None and d.cell4_mv > 0 else float("nan"))
        self.history_cells["cell5"].append(d.cell5_mv / 1000 if d.cell5_mv is not None and d.cell5_mv > 0 else float("nan"))
        self.history_cells["cell6"].append(d.cell6_mv / 1000 if d.cell6_mv is not None and d.cell6_mv > 0 else float("nan"))
        self.plot_dirty = True

    def redraw_plot(self) -> None:
        self.ax_voltage.clear()
        self.ax_current.clear()

        self.ax_voltage.set_xlabel("Frame #")
        self.ax_voltage.set_ylabel("Voltage [V]")
        self.ax_current.set_ylabel("Current [A]")
        self.ax_voltage.grid(True)

        if self.history_x:
            self.ax_voltage.plot(self.history_x, self.history_input_v, label="Input V")
            self.ax_voltage.plot(self.history_x, self.history_battery_v, label="Battery V")
            for key, label in [
                ("cell1", "Cell1"),
                ("cell2", "Cell2"),
                ("cell3", "Cell3"),
                ("cell4", "Cell4"),
                ("cell5", "Cell5"),
                ("cell6", "Cell6"),
            ]:
                if any(v == v for v in self.history_cells[key]):
                    self.ax_voltage.plot(self.history_x, self.history_cells[key], label=label)
            self.ax_current.plot(self.history_x, self.history_current_a, label="Current A")

            handles_v, labels_v = self.ax_voltage.get_legend_handles_labels()
            handles_c, labels_c = self.ax_current.get_legend_handles_labels()
            self.ax_voltage.legend(handles_v + handles_c, labels_v + labels_c, loc="upper right", ncol=4, fontsize=8)

        self.figure.tight_layout()
        self.plot_canvas.draw()
        self.plot_dirty = False

    def redraw_plot_if_needed(self) -> None:
        if self.plot_dirty:
            self.redraw_plot()
        self.root.after(400, self.redraw_plot_if_needed)

    def show_frame(self, d: FrameData) -> None:
        self.last_frame = d
        self.status_var.set("Connected")

        def fmt_v(mv: Optional[int]) -> str:
            return f"{mv / 1000:.3f} V" if mv is not None else "-"

        self.value_vars["frame_index"].set(str(d.frame_index))
        self.value_vars["mode"].set(f"{d.mode_num} ({d.mode_name})")
        self.value_vars["input"].set(fmt_v(d.input_mv))
        self.value_vars["battery"].set(fmt_v(d.battery_mv))
        self.value_vars["current"].set(f"{d.current_a:.2f} A" if d.current_a is not None else "-")
        self.value_vars["capacity"].set(f"{d.mah} mAh" if d.mah is not None else "-")
        self.value_vars["unknown_value"].set(str(d.unknown_value) if d.unknown_value is not None else "-")
        self.value_vars["unknown_value_bin"].set(d.unknown_value_bin)
        self.value_vars["temp"].set(f"{d.temp_c:.1f} °C ?" if d.temp_c is not None else "-")
        self.value_vars["status"].set(str(d.status) if d.status is not None else "-")
        self.value_vars["status_bin"].set(d.status_bin)
        self.value_vars["delta"].set(f"{d.cell_delta_v:.3f} V" if d.cell_delta_v is not None else "-")
        self.value_vars["cell1"].set(fmt_v(d.cell1_mv))
        self.value_vars["cell2"].set(fmt_v(d.cell2_mv))
        self.value_vars["cell3"].set(fmt_v(d.cell3_mv))
        self.value_vars["cell4"].set(fmt_v(d.cell4_mv))
        self.value_vars["cell5"].set(fmt_v(d.cell5_mv))
        self.value_vars["cell6"].set(fmt_v(d.cell6_mv))

        self.tree.insert("", 0, values=(
            d.pc_time,
            d.frame_index,
            f"{d.input_v:.3f}" if d.input_v is not None else "-",
            d.mode_name,
            f"{d.battery_v:.3f}" if d.battery_v is not None else "-",
            f"{d.current_a:.2f}" if d.current_a is not None else "-",
            d.mah if d.mah is not None else "-",
            f"{d.cell1_mv / 1000:.3f}" if d.cell1_mv is not None else "-",
            f"{d.cell2_mv / 1000:.3f}" if d.cell2_mv is not None else "-",
            f"{d.cell3_mv / 1000:.3f}" if d.cell3_mv is not None else "-",
            f"{d.cell4_mv / 1000:.3f}" if d.cell4_mv is not None else "-",
            f"{d.cell5_mv / 1000:.3f}" if d.cell5_mv is not None else "-",
            f"{d.cell6_mv / 1000:.3f}" if d.cell6_mv is not None else "-",
            f"{d.cell_delta_v:.3f}" if d.cell_delta_v is not None else "-",
            d.status if d.status is not None else "-",
            d.status_bin,
            d.unknown_value if d.unknown_value is not None else "-",
            d.unknown_value_bin,
        ))

        children = self.tree.get_children()
        if len(children) > 200:
            self.tree.delete(children[-1])

        self.log.insert("end", f"{d.pc_time} | #{d.frame_index} | {d.raw_line}
")
        self.log.see("end")

        if float(self.log.index("end-1c").split(".")[0]) > 500:
            self.log.delete("1.0", "50.0")

        self.add_history(d)
        self.append_csv(d)

    def poll_queue(self) -> None:
        try:
            while True:
                kind, payload = self.msg_queue.get_nowait()
                if kind == "info":
                    self.log.insert("end", f"INFO: {payload}
")
                    self.log.see("end")
                elif kind == "error":
                    self.status_var.set("Error")
                    self.log.insert("end", f"ERROR: {payload}
")
                    self.log.see("end")
                    self.worker = None
                elif kind == "frame":
                    self.show_frame(payload)
        except queue.Empty:
            pass

        self.root.after(100, self.poll_queue)

    def on_close(self) -> None:
        self.disconnect()
        self.root.destroy()


if __name__ == "__main__":
    root = tk.Tk()
    app = App(root)
    root.mainloop()
