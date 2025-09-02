#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
motor_console.py  (class-based single file)
---------------------------------------------------------------
- Server / Client / Spawner 클래스로 구조화
- Command DTO + CommandParser로 I/O(문자열)와 제어 로직 분리
- FullStateController는 '숫자 명령'만 처리 (단일 책임)
- FIFO(cmd.fifo)로 별도 터미널 간 입력 전달 (spawn에서 두 창)
- SoftRealtimeLoop가 없으면 soft_loop로 대체
- spawn 시 sys.executable(현재 파이썬/venv) 사용 + PYTHONPATH 주입
- 인용/경로/로그 안정화, CSV/print 안전 출력

사용 예:
  # 두 터미널 팝업(출력/서버 + 입력/클라이언트)
  python3 motor_console.py --mode spawn --type AK70-10 --id 1 --hz 200 --csv session.csv

  # 서버/클라이언트 직접 실행
  python3 motor_console.py --mode server --type AK70-10 --id 1 --hz 200 --csv session.csv
  python3 motor_console.py --mode client

CAN 준비(예):
  sudo ip link set can0 type can bitrate 1000000
  sudo ip link set up can0
"""

import os
import sys
import csv
import shlex
import time
import stat
import math
import argparse
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, Tuple, Literal

# ---------- 경로(절대) 고정 & sys.path 보강 ----------
BASE_DIR = Path(__file__).resolve().parent
FIFO_PATH = str(BASE_DIR / "cmd.fifo")  # 상대경로 문제 방지

if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
if str(BASE_DIR.parent) not in sys.path:
    sys.path.insert(0, str(BASE_DIR.parent))

# ---- 외부 라이브러리 ----
try:
    from NeuroLocoMiddleware.SoftRealtimeLoop import SoftRealtimeLoop
    HAVE_SRL = True
except Exception:
    HAVE_SRL = False

try:
    from TMotorCANControl import TMotorManager_mit_can, MIT_Params
except ModuleNotFoundError:
    print(
        "[import] Could not import TMotorCANControl.\n"
        f"python={sys.executable}\n"
        f"sys.path[0]={sys.path[0]}\n"
        f"BASE_DIR={BASE_DIR}\n"
        f"BASE_DIR.parent={BASE_DIR.parent}",
        file=sys.stderr,
    )
    raise

# numpy가 있으면 CSV 안전 변환에 활용
try:
    import numpy as np
    _HAVE_NP = True
except Exception:
    _HAVE_NP = False


# ---------------------- 유틸 ----------------------
def ensure_fifo(path: str):
    """지정 경로에 FIFO(네임드 파이프) 보장: 일반 파일이면 제거 후 FIFO 생성"""
    p = Path(path)
    if p.exists():
        st = p.stat()
        if not stat.S_ISFIFO(st.st_mode):
            try:
                p.unlink()
            except Exception:
                pass
            os.mkfifo(path)
    else:
        os.mkfifo(path)


def soft_loop(dt: float):
    """SoftRealtimeLoop 대체(미설치시) — 간단 고정주기 제너레이터"""
    t0 = time.perf_counter()
    n = 0
    try:
        while True:
            t = time.perf_counter() - t0
            yield t
            n += 1
            next_t = t0 + n * dt
            time.sleep(max(0.0, next_t - time.perf_counter()))
    except GeneratorExit:
        return


def find_terminal():
    """라즈비안에서 흔한 터미널 탐색"""
    import shutil

    for term in ("lxterminal", "xterm", "gnome-terminal", "xfce4-terminal", "mate-terminal"):
        if shutil.which(term):
            return term
    return None


def wrap_in_shell(cmd: str) -> str:
    """터미널 -e/-- 인자에 안전하게 전달 (내부에 쌍따옴표가 있어도 OK)"""
    return f"bash -lc {shlex.quote(cmd)}"


def _csv_safe(x):
    """CSV에 안전하게 넣기: None/NaN/넘파이 스칼라 대응"""
    if x is None:
        return ""
    if _HAVE_NP:
        try:
            if isinstance(x, (np.floating, np.integer)):
                x = x.item()
            elif isinstance(x, np.ndarray) and x.size == 1:
                x = x.item()
        except Exception:
            pass
    if isinstance(x, float) and not math.isfinite(x):
        return ""
    return x


def _fmt_float(x, fmt: str = "{:+.3f}") -> str:
    """숫자면 지정 포맷, 아니면 그대로 문자열화"""
    try:
        return fmt.format(float(x))
    except Exception:
        return str(x)


# ---------------------- Command / Parser ----------------------
Action = Literal["NONE", "ZERO", "QUIT"]


@dataclass(frozen=True)
class Command:
    """도메인 명령(숫자 파라미터). 텍스트/프로토콜과 분리된 DTO."""
    K: Optional[float] = None
    B: Optional[float] = None
    pos: Optional[float] = None
    vel: Optional[float] = None
    iq: Optional[float] = None
    torque: Optional[float] = None
    full_state: Optional[bool] = None


class CommandParser:
    """문자열 한 줄 → (Command, Action) 변환. I/O 포맷 책임 분리."""
    def parse(self, line: str) -> Tuple[Command, Action]:
        s = line.strip()
        if not s:
            return Command(), "NONE"

        lo = s.lower()
        if lo in ("quit", "exit"):
            return Command(), "QUIT"
        if lo in ("zero", "origin"):
            return Command(), "ZERO"

        kv = {}
        try:
            tokens = shlex.split(s)
        except ValueError:
            tokens = s.split()

        full_flag = None
        for tok in tokens:
            if "=" not in tok:
                continue
            k, v = tok.split("=", 1)
            k = k.strip().lower()
            v = v.strip()
            if k in ("k", "kp"):
                kv["K"] = float(v)
            elif k in ("b", "kd"):
                kv["B"] = float(v)
            elif k in ("pos", "position"):
                kv["pos"] = float(v)
            elif k in ("vel", "velocity"):
                kv["vel"] = float(v)
            elif k == "iq":
                kv["iq"] = float(v)
            elif k in ("tor", "torque"):
                kv["torque"] = float(v)
            elif k == "full":
                full_flag = bool(int(v))

        return Command(**kv, full_state=full_flag), "NONE"


# ---------------------- 컨트롤러 ----------------------
class FullStateController:
    """
    TMotorManager_mit_can을 감싼 원자적 갱신 컨트롤러.
    - update_full_state(): K/B/pos/vel/iq/torque를 하나의 락으로 동시 갱신(원자성)
    - tick(): manager.update() 호출(전송+상태 동기화)
    - apply(): Command DTO를 받아 update_full_state 호출
    """
    def __init__(self, manager: TMotorManager_mit_can):
        self.m = manager
        self._lock = threading.Lock()
        self._K = 5.0
        self._B = 0.1
        self._full_state = True

    def update_full_state(
        self,
        K: Optional[float] = None,
        B: Optional[float] = None,
        pos: Optional[float] = None,
        vel: Optional[float] = None,
        iq: Optional[float] = None,
        torque: Optional[float] = None,
        *,
        full_state: Optional[bool] = None,
        clamp: bool = True,
    ):
        with self._lock:
            if full_state is None:
                full_state = self._full_state

            # 현재값 유지/반영
            K_eff = self._K if K is None else float(K)
            B_eff = self._B if B is None else float(B)

            # 허용 범위로 클램프
            if clamp:
                kp_min = MIT_Params[self.m.type].get("Kp_min", None)
                kp_max = MIT_Params[self.m.type].get("Kp_max", None)
                kd_min = MIT_Params[self.m.type].get("Kd_min", None)
                kd_max = MIT_Params[self.m.type].get("Kd_max", None)
                if kp_min is not None:
                    K_eff = max(kp_min, K_eff)
                if kp_max is not None:
                    K_eff = min(K_eff, kp_max)
                if kd_min is not None:
                    B_eff = max(kd_min, B_eff)
                if kd_max is not None:
                    B_eff = min(B_eff, kd_max)

            # 게인/모드 적용
            if full_state:
                self.m.set_impedance_gains_real_unit_full_state_feedback(K=K_eff, B=B_eff)
            else:
                self.m.set_impedance_gains_real_unit(K=K_eff, B=B_eff)
            self._K, self._B, self._full_state = K_eff, B_eff, full_state

            # 명령 저장(전송은 update()에서 수행)
            if pos is not None:
                self.m.set_output_angle_radians(float(pos))
            if vel is not None:
                self.m.set_output_velocity_radians_per_second(float(vel))
            if torque is not None:
                self.m.set_output_torque_newton_meters(float(torque))
            if iq is not None:
                self.m.set_motor_current_qaxis_amps(float(iq))

    def apply(self, cmd: Command):
        """문자열 파서와 분리된 도메인 명령 적용"""
        self.update_full_state(
            K=cmd.K,
            B=cmd.B,
            pos=cmd.pos,
            vel=cmd.vel,
            iq=cmd.iq,
            torque=cmd.torque,
            full_state=cmd.full_state,
        )

    def tick(self):
        with self._lock:
            self.m.update()

    def read_state(self) -> Dict[str, float]:
        return {
            "pos": self.m.position,
            "vel": self.m.velocity,
            "acc": self.m.acceleration,
            "iq": self.m.current_qaxis,
            "torque": self.m.torque,
            "temp": self.m.temperature,
            "err": self.m.error,
        }


# ---------------------- 서버 ----------------------
class MotorConsoleServer:
    def __init__(self, motor_type: str, motor_id: int, hz: int, csv_path: str, fifo_path: str = FIFO_PATH):
        self.motor_type = motor_type
        self.motor_id = motor_id
        self.hz = hz
        self.csv_path = Path(csv_path)
        if not self.csv_path.is_absolute():
            self.csv_path = BASE_DIR / self.csv_path
        self.fifo_path = fifo_path

        self._stop_flag = threading.Event()
        self._fifo_thread: Optional[threading.Thread] = None

        self._ctrl: Optional[FullStateController] = None
        self._parser = CommandParser()

    def _fifo_reader(self):
        # server는 FIFO를 읽고, 명령을 실시간 반영
        try:
            with open(self.fifo_path, "r") as fr:
                while not self._stop_flag.is_set():
                    line = fr.readline()
                    if line == "":
                        time.sleep(0.05)
                        continue
                    cmd, act = self._parser.parse(line)
                    if act == "QUIT":
                        self._stop_flag.set()
                        break
                    if act == "ZERO":
                        self._ctrl.m.set_zero_position()
                        continue
                    # 숫자 명령 적용
                    self._ctrl.apply(cmd)
        except Exception:
            self._stop_flag.set()

    def run(self):
        ensure_fifo(self.fifo_path)

        m = TMotorManager_mit_can(motor_type=self.motor_type, motor_ID=self.motor_id, max_mosfett_temp=60)
        with m:
            self._ctrl = FullStateController(m)
            # 초기 안전값
            self._ctrl.update_full_state(K=5.0, B=0.1, pos=0.0, vel=0.0, iq=0.0, full_state=True)

            # CSV 로깅 준비
            csv_fh = open(self.csv_path, "w", newline="")
            csv_w = csv.writer(csv_fh)
            csv_w.writerow(["time", "pos", "vel", "acc", "iq", "torque", "temp", "err"])
            t0 = time.perf_counter()
            last_flush = 0.0
            last_print = 0.0

            # FIFO 리더 스레드 시작
            self._fifo_thread = threading.Thread(target=self._fifo_reader, daemon=True)
            self._fifo_thread.start()

            # 루프 선택: SoftRealtimeLoop or fallback
            loop_iter = (
                SoftRealtimeLoop(dt=1.0 / float(self.hz), report=True, fade=0.0)
                if HAVE_SRL
                else soft_loop(1.0 / float(self.hz))
            )

            print("[server] running. type commands in the client window (or `quit`).")
            for t in loop_iter:
                if self._stop_flag.is_set():
                    break

                # 전송 + 상태 동기화
                self._ctrl.tick()

                # 상태 기록
                st = self._ctrl.read_state()
                csv_w.writerow(
                    [
                        f"{time.perf_counter()-t0:.6f}",
                        _csv_safe(st.get("pos")),
                        _csv_safe(st.get("vel")),
                        _csv_safe(st.get("acc")),
                        _csv_safe(st.get("iq")),
                        _csv_safe(st.get("torque")),
                        _csv_safe(st.get("temp")),
                        _csv_safe(st.get("err")),
                    ]
                )

                # 출력/flush: 10Hz
                if (t - last_print) >= 0.1:
                    print(
                        f"\rpos={_fmt_float(st.get('pos'))} "
                        f"vel={_fmt_float(st.get('vel'))} "
                        f"iq={_fmt_float(st.get('iq'))} "
                        f"tor={_fmt_float(st.get('torque'))} "
                        f"temp={_fmt_float(st.get('temp'), fmt='{:.1f}')}C "
                        f"err={_csv_safe(st.get('err'))}  ",
                        end="",
                        flush=True,
                    )
                    last_print = t
                if (t - last_flush) >= 0.1:
                    csv_fh.flush()
                    last_flush = t

            # 안전 정지
            try:
                self._ctrl.update_full_state(K=5.0, B=0.1, pos=0.0, vel=0.0, iq=0.0)
                for _ in range(5):
                    self._ctrl.tick()
                    time.sleep(0.01)
            except Exception:
                pass

            csv_fh.flush()
            csv_fh.close()

        print("\n[server] exit.")


# ---------------------- 클라이언트 ----------------------
class MotorConsoleClient:
    def __init__(self, fifo_path: str = FIFO_PATH):
        self.fifo_path = fifo_path

    def send_command(self, line: str) -> bool:
        """프로그램적으로 한 줄 명령 전송. 성공 시 True"""
        if not Path(self.fifo_path).exists():
            return False
        try:
            with open(self.fifo_path, "w") as fw:
                fw.write(line.strip() + "\n")
                fw.flush()
            return True
        except Exception:
            return False

    def run(self):
        print("Commands: K=.. B=.. pos=.. vel=.. iq=.. tor=.. full=0|1 | zero | quit")
        waited = 0.0
        # FIFO가 생길 때까지 대기
        while not Path(self.fifo_path).exists():
            if waited == 0.0:
                print("[client] waiting for FIFO to appear... (start server or use --mode spawn)")
            time.sleep(0.2)
            waited += 0.2
            if waited >= 10.0:
                print("[client] still waiting... creating FIFO locally.")
                try:
                    ensure_fifo(self.fifo_path)
                except Exception as e:
                    print(f"[client] ensure_fifo error: {e}")
                    waited = 0.0

        try:
            print("[client] opening FIFO for writing... (may wait until server opens reader)")
            with open(self.fifo_path, "w") as fw:
                while True:
                    try:
                        line = input("CMD> ").strip()
                    except (EOFError, KeyboardInterrupt):
                        line = "quit"
                    if not line:
                        continue
                    fw.write(line + "\n")
                    fw.flush()
                    if line.lower() in ("quit", "exit"):
                        print("bye")
                        break
        except Exception as e:
            print(f"[client] error: {e}")
            input("Press Enter to close...")


# ---------------------- 스포너 ----------------------
class MotorConsoleSpawner:
    def __init__(self, args):
        self.args = args

    def run(self):
        term = find_terminal()
        if not term:
            print("No terminal emulator found. Install lxterminal or xterm or gnome-terminal.")
            sys.exit(1)

        # 1) FIFO를 먼저 만들어 레이스 방지 (절대경로)
        try:
            ensure_fifo(FIFO_PATH)
        except Exception as e:
            print(f"[spawn] ensure_fifo error: {e}")

        # 2) 절대 경로들 준비 + 현재 파이썬(venv) 해석기 사용
        script_path = str((BASE_DIR / Path(__file__).name).resolve())
        py = f"{shlex.quote(sys.executable)} -u {shlex.quote(script_path)}"
        server_log = shlex.quote(str(BASE_DIR / "server.log"))
        client_log = shlex.quote(str(BASE_DIR / "client.log"))

        # 3) PYTHONPATH 주입 (BASE_DIR, 부모 폴더 우선)
        py_path_parts = []
        env_pp = os.environ.get("PYTHONPATH")
        if env_pp:
            py_path_parts.append(env_pp)
        py_path_parts.append(str(BASE_DIR))
        py_path_parts.append(str(BASE_DIR.parent))
        env_prefix = "PYTHONPATH=" + shlex.quote(":".join(py_path_parts)) + " "

        # 4) 명령 구성 (값만 quote)
        csv_out = (BASE_DIR / self.args.csv) if not Path(self.args.csv).is_absolute() else Path(self.args.csv)
        server_cmd = (
            env_prefix
            + f"{py} --mode server "
            + f"--type {shlex.quote(self.args.type)} "
            + f"--id {int(self.args.id)} "
            + f"--hz {int(self.args.hz)} "
            + f"--csv {shlex.quote(str(csv_out))} "
            + f"2>&1 | tee -a {server_log}"
        )
        client_cmd = env_prefix + f"{py} --mode client 2>&1 | tee -a {client_log}"

        # 5) 창이 바로 닫히지 않도록 hold 동작 추가(hold 미지원 터미널 대비)
        hold_tail = '; echo; echo "[press Enter to close]"; read -r _'

        if term == "lxterminal":
            os.system(f'lxterminal -t "Motor Output" -e {wrap_in_shell(server_cmd + hold_tail)} &')
            time.sleep(0.8)
            os.system(f'lxterminal -t "Motor Input"  -e {wrap_in_shell(client_cmd + hold_tail)} &')

        elif term == "xterm":
            os.system(f'xterm -T "Motor Output" -hold -e {wrap_in_shell(server_cmd)} &')
            time.sleep(0.8)
            os.system(f'xterm -T "Motor Input"  -hold -e {wrap_in_shell(client_cmd)} &')

        elif term in ("xfce4-terminal", "mate-terminal"):
            os.system(f'{term} --title="Motor Output" -- bash -lc {shlex.quote(server_cmd + hold_tail)} &')
            time.sleep(0.8)
            os.system(f'{term} --title="Motor Input"  -- bash -lc {shlex.quote(client_cmd + hold_tail)} &')

        else:  # gnome-terminal
            os.system(f'gnome-terminal --title="Motor Output" -- bash -lc {shlex.quote(server_cmd + hold_tail)} &')
            time.sleep(0.8)
            os.system(f'gnome-terminal --title="Motor Input"  -- bash -lc {shlex.quote(client_cmd + hold_tail)} &')


# ---------------------- main ----------------------
def build_argparser():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=("server", "client", "spawn"), default="server")
    p.add_argument("--type", default="AK70-10")
    p.add_argument("--id", type=int, default=1)
    p.add_argument("--hz", type=int, default=200)
    p.add_argument("--csv", default="session.csv")
    return p


def main():
    args = build_argparser().parse_args()

    if args.mode == "server":
        MotorConsoleServer(args.type, args.id, args.hz, args.csv, fifo_path=FIFO_PATH).run()
    elif args.mode == "client":
        MotorConsoleClient(fifo_path=FIFO_PATH).run()
    else:
        MotorConsoleSpawner(args).run()


if __name__ == "__main__":
    main()
