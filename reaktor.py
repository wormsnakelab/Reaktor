import csv
import serial
import time
from typing import Optional

PORT = "COM6"
BAUD = 115200
CSV_FILE = "reaktor_log_v2.csv"


def decode_frame(payload: bytes) -> str:
    return "".join(
        chr(b - 0x60) if 32 <= (b - 0x60) <= 126 else "."
        for b in payload
    )


def to_int(value: str) -> Optional[int]:
    return int(value) if value.isdigit() else None


def mode_name(mode: Optional[int]) -> str:
    mapping = {
        0: "unknown",
        1: "charge?",
        2: "discharge",
        3: "storage?",
        4: "balance?",
    }
    if mode is None:
        return "unknown"
    return mapping.get(mode, f"mode_{mode}")


ser = serial.Serial(PORT, BAUD, timeout=0.2)
print(f"Open {PORT} @ {BAUD}")

buffer = bytearray()

with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)

    if f.tell() == 0:
        writer.writerow([
            "pc_time",
            "frame_type",
            "mode_num",
            "mode_name",
            "reserved",
            "input_mv",
            "input_v",
            "battery_mv",
            "battery_v",
            "current_cA",
            "current_a",
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
            "temp_raw",
            "temp_c",
            "status",
            "mah",
            "time_s",
            "cell_min_mv",
            "cell_max_mv",
            "cell_delta_mv",
            "raw_line",
        ])

    while True:
        data = ser.read(ser.in_waiting or 1)
        if not data:
            continue

        buffer.extend(data)

        while b"\x8A" in buffer:
            pos = buffer.index(0x8A)
            frame = bytes(buffer[:pos + 1])
            del buffer[:pos + 1]

            payload = frame[:-2] if frame.endswith(b"\x8D\x8A") else frame[:-1]
            if not payload:
                continue

            decoded = decode_frame(payload)
            parts = decoded.split(";")

            # očekávaný formát:
            # $1;2;;13263;15684;6;3915;3911;3922;3917;0;0;287;1;0;31
            if len(parts) < 16:
                print("=" * 72)
                print(time.strftime("%H:%M:%S"), "| RAW (short):", decoded)
                print("=" * 72)
                continue

            frame_type = parts[0]
            mode_num = to_int(parts[1])
            reserved = parts[2]
            input_mv = to_int(parts[3])
            battery_mv = to_int(parts[4])
            current_cA = to_int(parts[5])
            cell1_mv = to_int(parts[6])
            cell2_mv = to_int(parts[7])
            cell3_mv = to_int(parts[8])
            cell4_mv = to_int(parts[9])
            cell5_mv = to_int(parts[10])
            cell6_mv = to_int(parts[11])
            temp_raw = to_int(parts[12])
            status = to_int(parts[13])
            mah = to_int(parts[14])
            time_s = to_int(parts[15])

            input_v = input_mv / 1000 if input_mv is not None else None
            battery_v = battery_mv / 1000 if battery_mv is not None else None
            current_a = current_cA / 100 if current_cA is not None else None
            temp_c = temp_raw / 10 if temp_raw is not None else None

            cells_mv = [c for c in [cell1_mv, cell2_mv, cell3_mv, cell4_mv, cell5_mv, cell6_mv] if c is not None and c > 0]
            cell_min_mv = min(cells_mv) if cells_mv else None
            cell_max_mv = max(cells_mv) if cells_mv else None
            cell_delta_mv = (cell_max_mv - cell_min_mv) if cells_mv else None

            def v(cell_mv: Optional[int]) -> Optional[float]:
                return cell_mv / 1000 if cell_mv is not None else None

            ts = time.strftime("%H:%M:%S")

            print("=" * 72)
            print(f"{ts} | frame_type : {frame_type}")
            print(f"{ts} | mode       : {mode_num} ({mode_name(mode_num)})")
            print(f"{ts} | input      : {input_v:.3f} V" if input_v is not None else f"{ts} | input      : ?")
            print(f"{ts} | battery    : {battery_v:.3f} V" if battery_v is not None else f"{ts} | battery    : ?")
            print(f"{ts} | current    : {current_a:.2f} A   [{current_cA} cA]" if current_a is not None else f"{ts} | current    : ?")
            print(f"{ts} | cell1      : {v(cell1_mv):.3f} V" if cell1_mv is not None else f"{ts} | cell1      : ?")
            print(f"{ts} | cell2      : {v(cell2_mv):.3f} V" if cell2_mv is not None else f"{ts} | cell2      : ?")
            print(f"{ts} | cell3      : {v(cell3_mv):.3f} V" if cell3_mv is not None else f"{ts} | cell3      : ?")
            print(f"{ts} | cell4      : {v(cell4_mv):.3f} V" if cell4_mv is not None else f"{ts} | cell4      : ?")
            print(f"{ts} | cell5      : {v(cell5_mv):.3f} V" if cell5_mv is not None else f"{ts} | cell5      : ?")
            print(f"{ts} | cell6      : {v(cell6_mv):.3f} V" if cell6_mv is not None else f"{ts} | cell6      : ?")
            print(f"{ts} | temp_raw   : {temp_raw}   (~{temp_c:.1f} °C?)" if temp_raw is not None else f"{ts} | temp_raw   : ?")
            print(f"{ts} | status     : {status}")
            print(f"{ts} | capacity   : {mah} mAh")
            print(f"{ts} | time       : {time_s} s")
            print(f"{ts} | cell delta : {cell_delta_mv} mV" if cell_delta_mv is not None else f"{ts} | cell delta : ?")
            print(f"{ts} | raw        : {decoded}")

            writer.writerow([
                ts,
                frame_type,
                mode_num,
                mode_name(mode_num),
                reserved,
                input_mv,
                input_v,
                battery_mv,
                battery_v,
                current_cA,
                current_a,
                cell1_mv,
                v(cell1_mv),
                cell2_mv,
                v(cell2_mv),
                cell3_mv,
                v(cell3_mv),
                cell4_mv,
                v(cell4_mv),
                cell5_mv,
                v(cell5_mv),
                cell6_mv,
                v(cell6_mv),
                temp_raw,
                temp_c,
                status,
                mah,
                time_s,
                cell_min_mv,
                cell_max_mv,
                cell_delta_mv,
                decoded,
            ])
            f.flush()
