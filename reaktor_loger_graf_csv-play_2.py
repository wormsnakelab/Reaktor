import csv
import math
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
from tkinter import filedialog, messagebox, ttk

from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure


BAUD = 115200
LF = bytes([0x8A])
CRLF = bytes([0x8D, 0x8A])


def make_csv_filename() -> Path:
    return Path(f"Reaktor_log_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv")


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


class CellGauge(ttk.Frame):
    SCALE_MIN_V = 3.0
    SCALE_MAX_V = 4.3

    def __init__(self, master: tk.Misc, title: str):
        super().__init__(master, padding=(4, 2))
        self.title = title
        self.width = 92
        self.height = 92
        self.center_x = self.width / 2
        self.center_y = self.height - 12
        self.radius = 34

        self.label_var = tk.StringVar(value=f"{title}  -")
        ttk.Label(self, textvariable=self.label_var, justify="center", width=12).pack()

        self.canvas = tk.Canvas(
            self,
            width=self.width,
            height=self.height,
            highlightthickness=0,
            bg="#f7f7f5",
        )
        self.canvas.pack()

        self._draw_static()
        self.set_value(None)

    def _draw_static(self) -> None:
        arc_box = (
            self.center_x - self.radius,
            self.center_y - self.radius,
            self.center_x + self.radius,
            self.center_y + self.radius,
        )
        self.canvas.create_arc(arc_box, start=150, extent=240, style="arc", width=2, outline="#4a4a4a")

        for voltage in (3.0, 3.5, 4.0, 4.2, 4.3):
            angle = self._angle_from_value(voltage)
            inner = self._point_from_angle(angle, self.radius - 7)
            outer = self._point_from_angle(angle, self.radius + 2)
            label = self._point_from_angle(angle, self.radius + 11)
            self.canvas.create_line(*inner, *outer, fill="#6f7782", width=2)
            self.canvas.create_text(label[0], label[1], text=f"{voltage:.1f}", fill="#6f7782", font=("TkDefaultFont", 7))

        self.canvas.create_oval(
            self.center_x - 4,
            self.center_y - 4,
            self.center_x + 4,
            self.center_y + 4,
            fill="#303030",
            outline="",
        )

    def _angle_from_value(self, value_v: float) -> float:
        value_v = min(max(value_v, self.SCALE_MIN_V), self.SCALE_MAX_V)
        ratio = (value_v - self.SCALE_MIN_V) / (self.SCALE_MAX_V - self.SCALE_MIN_V)
        return 150 - ratio * 240

    def _point_from_angle(self, angle_deg: float, radius: float) -> tuple[float, float]:
        angle = math.radians(angle_deg)
        return (
            self.center_x + math.cos(angle) * radius,
            self.center_y - math.sin(angle) * radius,
        )

    def set_value(self, value_v: Optional[float]) -> None:
        self.canvas.delete("dynamic")

        if value_v is None or value_v <= 0:
            self.canvas.create_text(
                self.center_x,
                self.center_y - 26,
                text="OFF",
                fill="#6b7280",
                font=("TkDefaultFont", 9, "bold"),
                tags="dynamic",
            )
            self.canvas.create_line(
                self.center_x,
                self.center_y,
                self.center_x - self.radius + 12,
                self.center_y - 8,
                fill="#b0b6bf",
                width=3,
                tags="dynamic",
            )
            self.label_var.set(f"{self.title}  -")
            return

        angle = self._angle_from_value(value_v)
        needle = self._point_from_angle(angle, self.radius - 8)
        color = "#c97a12"
        if value_v >= 4.15:
            color = "#2d9c5a"
        elif value_v <= 3.3:
            color = "#c23b22"

        self.canvas.create_arc(
            self.center_x - self.radius,
            self.center_y - self.radius,
            self.center_x + self.radius,
            self.center_y + self.radius,
            start=150,
            extent=240,
            style="arc",
            width=4,
            outline="#e5e7eb",
            tags="dynamic",
        )
        self.canvas.create_arc(
            self.center_x - self.radius,
            self.center_y - self.radius,
            self.center_x + self.radius,
            self.center_y + self.radius,
            start=150,
            extent=150 if value_v < 4.0 else 210 if value_v < 4.15 else 240,
            style="arc",
            width=4,
            outline=color,
            tags="dynamic",
        )
        self.canvas.create_line(
            self.center_x,
            self.center_y,
            needle[0],
            needle[1],
            fill="#202020",
            width=3,
            tags="dynamic",
        )
        self.canvas.create_text(
            self.center_x,
            self.center_y - 26,
            text=f"{value_v:.3f}V",
            fill="#202020",
            font=("TkDefaultFont", 8, "bold"),
            tags="dynamic",
        )
        self.label_var.set(f"{self.title}  {value_v:.3f} V")


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
            3: "monitor",
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

                while LF in self.buffer:
                    pos = self.buffer.index(LF)
                    frame = bytes(self.buffer[:pos + 1])
                    del self.buffer[:pos + 1]

                    if frame.endswith(CRLF):
                        payload = frame[:-2]
                    else:
                        payload = frame[:-1]

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


class PlaybackWorker(threading.Thread):
    def __init__(self, frames: list[FrameData], out_queue: queue.Queue, speed: float):
        super().__init__(daemon=True)
        self.frames = frames
        self.out_queue = out_queue
        self.speed = speed
        self._stop_event = threading.Event()
        self._pause_event = threading.Event()
        self._pause_event.set()

    def stop(self) -> None:
        self._stop_event.set()
        self._pause_event.set()

    def pause(self) -> None:
        self._pause_event.clear()

    def resume(self) -> None:
        self._pause_event.set()

    def run(self) -> None:
        self.out_queue.put(("info", f"Playback started at {self.speed}x"))
        delay = max(0.001, 1.0 / self.speed)
        for frame in self.frames:
            if self._stop_event.is_set():
                self.out_queue.put(("info", "Playback stopped"))
                return
            while not self._pause_event.is_set():
                if self._stop_event.is_set():
                    self.out_queue.put(("info", "Playback stopped"))
                    return
                time.sleep(0.05)
            self.out_queue.put(("playback_frame", frame))
            time.sleep(delay)
        self.out_queue.put(("info", "Playback finished"))


class App:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Reaktor Logger")
        self.root.geometry("1720x980")

        self.msg_queue: queue.Queue = queue.Queue()
        self.worker: Optional[SerialWorker] = None
        self.playback_worker: Optional[PlaybackWorker] = None
        self.last_frame: Optional[FrameData] = None

        self.csv_file = make_csv_filename()
        self.loaded_frames: list[FrameData] = []
        self.loaded_csv_path: Optional[Path] = None
        self.playback_cursor = 0
        self.queue_poll_ms = 50

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
        self.root.after(self.queue_poll_ms, self.poll_queue)
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
        ttk.Button(top, text="Start new session", command=self.start_new_session).pack(side="left", padx=(10, 4))
        ttk.Button(top, text="Load CSV", command=self.load_csv).pack(side="left", padx=(10, 4))
        ttk.Button(top, text="Play", command=self.play_loaded_csv).pack(side="left", padx=(10, 4))
        ttk.Button(top, text="Pause", command=self.pause_playback).pack(side="left")
        ttk.Button(top, text="Stop", command=self.stop_playback).pack(side="left", padx=(4, 4))

        ttk.Label(top, text="Speed:").pack(side="left", padx=(10, 4))
        self.playback_speed_var = tk.StringVar(value="1x")
        self.playback_speed_combo = ttk.Combobox(
            top,
            textvariable=self.playback_speed_var,
            width=6,
            state="readonly",
            values=["1x", "10x", "100x", "1000x"],
        )
        self.playback_speed_combo.pack(side="left")

        self.status_var = tk.StringVar(value="Disconnected")
        ttk.Label(top, textvariable=self.status_var).pack(side="left", padx=(14, 0))

        grid = ttk.LabelFrame(self.root, text="Live values", padding=10)
        grid.pack(fill="x", padx=10, pady=6)
        grid.columnconfigure(0, weight=1)
        grid.columnconfigure(1, weight=0)

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

        values_frame = ttk.Frame(grid)
        values_frame.grid(row=0, column=0, sticky="nw")

        for i, (label, key) in enumerate(pairs):
            r = i // 3
            c = (i % 3) * 2
            ttk.Label(values_frame, text=label + ":").grid(row=r, column=c, sticky="w", padx=(0, 8), pady=2)
            ttk.Label(values_frame, textvariable=self.value_vars[key], width=24).grid(row=r, column=c + 1, sticky="w", pady=2)

        gauges_frame = ttk.LabelFrame(grid, text="Cell gauges", padding=6)
        gauges_frame.grid(row=0, column=1, sticky="ne", padx=(12, 0))

        self.cell_gauges: dict[str, CellGauge] = {}
        for index in range(1, 7):
            key = f"cell{index}"
            gauge = CellGauge(gauges_frame, f"C{index}")
            gauge.grid(row=0, column=index - 1, padx=1, pady=2, sticky="n")
            self.cell_gauges[key] = gauge

        plot_frame = ttk.LabelFrame(self.root, text="Live history plot", padding=6)
        plot_frame.pack(fill="both", expand=False, padx=10, pady=6)

        self.figure = Figure(figsize=(14, 4.6), dpi=100)

        self.ax_main = self.figure.add_subplot(111)
        self.ax_cells = self.ax_main.twinx()
        self.ax_current = self.ax_main.twinx()
        self.ax_current.spines["right"].set_position(("outward", 55))

        self.ax_main.set_xlabel("Frame #")
        self.ax_main.set_ylabel("Input / Battery [V]")
        self.ax_cells.set_ylabel("Cells [V]")
        self.ax_current.set_ylabel("Current [A]")
        self.ax_main.grid(True)

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

        self.stop_playback()
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

    def clear_gui_logs(self) -> None:
        self.log.delete("1.0", "end")
        for key in self.value_vars:
            self.value_vars[key].set("-")
        for gauge in self.cell_gauges.values():
            gauge.set_value(None)

    def start_new_session(self) -> None:
        self.stop_playback()
        self.clear_history()
        self.clear_gui_logs()
        self.csv_file = make_csv_filename()
        self._ensure_csv_header()
        self.status_var.set(f"New session: {self.csv_file.name}")
        self.log.insert("end", f"INFO: Started new session -> {self.csv_file.name}\n")
        self.log.see("end")

    def load_csv(self) -> None:
        path = filedialog.askopenfilename(
            title="Load Reaktor CSV log",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        if not path:
            return

        try:
            frames = self.read_frames_from_csv(Path(path))
        except Exception as exc:
            messagebox.showerror("Reaktor Logger", f"Failed to load CSV:\n{exc}")
            return

        self.stop_playback()
        self.loaded_csv_path = Path(path)
        self.loaded_frames = frames
        self.clear_history()
        self.clear_gui_logs()

        for frame in frames:
            self.add_history(frame)
        self.redraw_plot()

        if frames:
            self.show_frame_readonly(frames[-1])

        self.status_var.set(f"Loaded CSV: {self.loaded_csv_path.name} ({len(frames)} frames)")
        self.log.insert("end", f"INFO: Loaded CSV -> {self.loaded_csv_path.name} ({len(frames)} frames)\n")
        self.log.see("end")

    def read_frames_from_csv(self, path: Path) -> list[FrameData]:
        frames: list[FrameData] = []
        with path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row_number, row in enumerate(reader, start=1):
                frame_index = self._csv_int(row.get("frame_index"))
                if frame_index is None:
                    frame_index = row_number
                frames.append(
                    FrameData(
                        frame_index=frame_index,
                        pc_time=row.get("pc_time", "") or "",
                        frame_type=row.get("frame_type", "") or "",
                        mode_num=self._csv_int(row.get("mode_num")),
                        mode_name=row.get("mode_name", "") or "unknown",
                        input_mv=self._csv_int(row.get("input_mv")),
                        battery_mv=self._csv_int(row.get("battery_mv")),
                        current_cA=self._csv_int(row.get("current_cA")),
                        cell1_mv=self._csv_int(row.get("cell1_mv")),
                        cell2_mv=self._csv_int(row.get("cell2_mv")),
                        cell3_mv=self._csv_int(row.get("cell3_mv")),
                        cell4_mv=self._csv_int(row.get("cell4_mv")),
                        cell5_mv=self._csv_int(row.get("cell5_mv")),
                        cell6_mv=self._csv_int(row.get("cell6_mv")),
                        temp_raw=self._csv_int(row.get("temp_raw")),
                        status=self._csv_int(row.get("status")),
                        mah=self._csv_int(row.get("mah")),
                        unknown_value=self._csv_int(row.get("unknown_value")),
                        raw_line=row.get("raw_line", "") or "",
                    )
                )
        return frames

    @staticmethod
    def _csv_int(value: Optional[str]) -> Optional[int]:
        if value is None:
            return None
        value = str(value).strip()
        if value == "" or value.lower() == "none":
            return None
        return int(float(value))

    def playback_speed(self) -> float:
        text = self.playback_speed_var.get().strip().lower().replace("x", "")
        try:
            speed = float(text)
        except ValueError:
            return 1.0
        return speed if speed > 0 else 1.0

    def play_loaded_csv(self) -> None:
        if not self.loaded_frames:
            messagebox.showinfo("Reaktor Logger", "Load a CSV file first.")
            return

        if self.playback_worker:
            self.playback_worker.resume()
            self.status_var.set(f"Playback running ({self.playback_speed_var.get()})")
            return

        self.stop_playback()
        self.clear_history()
        self.clear_gui_logs()

        frames_copy = [
            FrameData(
                frame_index=f.frame_index,
                pc_time=f.pc_time,
                frame_type=f.frame_type,
                mode_num=f.mode_num,
                mode_name=f.mode_name,
                input_mv=f.input_mv,
                battery_mv=f.battery_mv,
                current_cA=f.current_cA,
                cell1_mv=f.cell1_mv,
                cell2_mv=f.cell2_mv,
                cell3_mv=f.cell3_mv,
                cell4_mv=f.cell4_mv,
                cell5_mv=f.cell5_mv,
                cell6_mv=f.cell6_mv,
                temp_raw=f.temp_raw,
                status=f.status,
                mah=f.mah,
                unknown_value=f.unknown_value,
                raw_line=f.raw_line,
            )
            for f in self.loaded_frames
        ]
        self.playback_worker = PlaybackWorker(frames_copy, self.msg_queue, self.playback_speed())
        self.playback_worker.start()
        self.status_var.set(f"Playback running ({self.playback_speed_var.get()})")

    def pause_playback(self) -> None:
        if self.playback_worker:
            self.playback_worker.pause()
            self.status_var.set("Playback paused")

    def stop_playback(self) -> None:
        if self.playback_worker:
            self.playback_worker.stop()
            self.playback_worker = None
            self.status_var.set("Playback stopped")

    def _ensure_csv_header(self) -> None:
        if self.csv_file.exists() and self.csv_file.stat().st_size > 0:
            return

        with self.csv_file.open("w", newline="", encoding="utf-8") as f:
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

        with self.csv_file.open("a", newline="", encoding="utf-8") as f:
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
        self.ax_main.clear()
        self.ax_cells.clear()
        self.ax_current.clear()

        self.ax_main.set_xlabel("Frame #")
        self.ax_main.set_ylabel("Input / Battery [V]")
        self.ax_cells.set_ylabel("Cells [V]")
        self.ax_current.set_ylabel("Current [A]")
        self.ax_current.spines["right"].set_position(("outward", 55))
        self.ax_main.grid(True)

        if self.history_x:
            self.ax_main.plot(self.history_x, self.history_input_v, label="Input V")
            self.ax_main.plot(self.history_x, self.history_battery_v, label="Battery V")

            for key, label in [
                ("cell1", "Cell1"),
                ("cell2", "Cell2"),
                ("cell3", "Cell3"),
                ("cell4", "Cell4"),
                ("cell5", "Cell5"),
                ("cell6", "Cell6"),
            ]:
                series = self.history_cells[key]
                if any(v == v for v in series):
                    self.ax_cells.plot(self.history_x, series, label=label)

            self.ax_current.plot(self.history_x, self.history_current_a, label="Current A")

            handles_a, labels_a = self.ax_main.get_legend_handles_labels()
            handles_b, labels_b = self.ax_cells.get_legend_handles_labels()
            handles_c, labels_c = self.ax_current.get_legend_handles_labels()
            self.ax_main.legend(
                handles_a + handles_b + handles_c,
                labels_a + labels_b + labels_c,
                loc="upper right",
                ncol=4,
                fontsize=8,
            )

        self.figure.tight_layout()
        self.plot_canvas.draw()
        self.plot_dirty = False

    def redraw_plot_if_needed(self) -> None:
        if self.plot_dirty:
            self.redraw_plot()
        self.root.after(400, self.redraw_plot_if_needed)

    def update_live_values(self, d: FrameData) -> None:
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
        self.value_vars["temp"].set(f"{d.temp_c:.1f} degC ?" if d.temp_c is not None else "-")
        self.value_vars["status"].set(str(d.status) if d.status is not None else "-")
        self.value_vars["status_bin"].set(d.status_bin)
        self.value_vars["delta"].set(f"{d.cell_delta_v:.3f} V" if d.cell_delta_v is not None else "-")
        self.value_vars["cell1"].set(fmt_v(d.cell1_mv))
        self.value_vars["cell2"].set(fmt_v(d.cell2_mv))
        self.value_vars["cell3"].set(fmt_v(d.cell3_mv))
        self.value_vars["cell4"].set(fmt_v(d.cell4_mv))
        self.value_vars["cell5"].set(fmt_v(d.cell5_mv))
        self.value_vars["cell6"].set(fmt_v(d.cell6_mv))
        for index in range(1, 7):
            cell_mv = getattr(d, f"cell{index}_mv")
            cell_v = cell_mv / 1000 if cell_mv is not None and cell_mv > 0 else None
            self.cell_gauges[f"cell{index}"].set_value(cell_v)

    def add_tree_row(self, d: FrameData) -> None:
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

    def show_frame(self, d: FrameData, write_csv: bool = True, log_raw: bool = True) -> None:
        self.last_frame = d
        self.update_live_values(d)
        self.add_tree_row(d)
        if log_raw:
            self.log.insert("end", f"{d.pc_time} | #{d.frame_index} | {d.raw_line}\n")
            self.log.see("end")
            if float(self.log.index("end-1c").split(".")[0]) > 500:
                self.log.delete("1.0", "50.0")
        self.add_history(d)
        if write_csv:
            self.append_csv(d)

    def show_frame_readonly(self, d: FrameData) -> None:
        self.last_frame = d
        self.update_live_values(d)
        self.log.insert("end", f"INFO: Showing last frame from loaded CSV -> #{d.frame_index}\n")
        self.log.see("end")

    def playback_frame_summary(self, d: FrameData) -> None:
        self.last_frame = d
        self.update_live_values(d)
        self.add_tree_row(d)
        self.add_history(d)

    def handle_playback_frames(self, frames: list[FrameData]) -> None:
        if not frames:
            return

        for frame in frames[:-1]:
            self.add_history(frame)

        self.playback_frame_summary(frames[-1])

    def poll_queue(self) -> None:
        playback_frames: list[FrameData] = []
        try:
            while True:
                kind, payload = self.msg_queue.get_nowait()
                if kind == "info":
                    self.log.insert("end", f"INFO: {payload}\n")
                    self.log.see("end")
                elif kind == "error":
                    self.status_var.set("Error")
                    self.log.insert("end", f"ERROR: {payload}\n")
                    self.log.see("end")
                    self.worker = None
                    self.playback_worker = None
                elif kind == "frame":
                    self.status_var.set("Connected")
                    self.show_frame(payload, write_csv=True, log_raw=True)
                elif kind == "playback_frame":
                    playback_frames.append(payload)
        except queue.Empty:
            pass

        if playback_frames:
            self.status_var.set(f"Playback running ({self.playback_speed_var.get()})")
            self.handle_playback_frames(playback_frames)

        self.root.after(self.queue_poll_ms, self.poll_queue)

    def on_close(self) -> None:
        self.disconnect()
        self.stop_playback()
        self.root.destroy()


if __name__ == "__main__":
    root = tk.Tk()
    app = App(root)
    root.mainloop()
