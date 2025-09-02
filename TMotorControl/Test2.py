#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
motor_console.py  (single-file, dual-window console for TMotorCANControl)
---------------------------------------------------------------
- 하나의 파일로 "출력/제어 서버"와 "명령 입력 클라이언트"를 모두 포함
- SoftRealtimeLoop로 주기 루프, python-can Notifier 수신(라이브러리 내부)
- FIFO(cmd.fifo)로 입력을 전달 → 서버가 실시간 반영
- 옵션으로 두 개의 터미널 창을 자동 팝업(spawn)

사용 예:
  # 두 터미널 팝업(출력/서버 + 입력/클라이언트)
  python3 motor_console.py --mode spawn --type AK70-10 --id 1 --hz 200 --csv session.csv

  # 출력/서버만 직접 실행 (다른 터미널에서 client 실행)
  python3 motor_console.py --mode server --type AK70-10 --id 1 --hz 200 --csv session.csv
  python3 motor_console.py --mode client

명령 입력 형식(클라이언트 창에서):
  K=20 B=0.5 pos=0.0
  vel=0.5
  iq=0.8
  tor=1.2
  full=1
  zero
  quit
"""

import os
import sys
import csv
import shlex
import time
import stat
import argparse
import threading
from pathlib import Path
from typing import Optional, Dict

# ---- 외부 라이브러리 ----
try:
    from NeuroLocoMiddleware.SoftRealtimeLoop import SoftRealtimeLoop
    HAVE_SRL = True
except Exception:
    HAVE_SRL = False

from TMotorCANControl import TMotorManager_mit_can, MIT_Params

# ---- 경로(절대) 고정 ----
BASE_DIR = Path(__file__).resolve().parent
FIFO_PATH = str(BASE_DIR / "cmd.fifo")  # 상대경로로 인한 spawn/작업폴더 불일치 방지


# ---------------------- 유틸 ----------------------
def ensure_fifo(path: str):
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
            next_t = t0 + n*dt
            time.sleep(max(0.0, next_t - time.perf_counter()))
    except GeneratorExit:
        return


def find_terminal():
    import shutil
    # 라즈비안 보편 + 대체 후보
    for term in ("lxterminal", "xterm", "gnome-terminal", "xfce4-terminal", "mate-terminal"):
        if shutil.which(term):
            return term
    return None


def wrap_in_shell(cmd: str) -> str:
    """
    터미널 -e/-- command 인자에 안전하게 전달.
    내부에 쌍따옴표가 있어도 전체를 단따옴표로 안전 인용.
    """
    return f"bash -lc {shlex.quote(cmd)}"


# ---------------------- 컨트롤러 ----------------------
class FullStateController:
    """
    TMotorManager_mit_can을 감싼 원자적 갱신 컨트롤러.
    - update_full_state(): K/B/pos/vel/iq/torque를 하나의 락으로 동시 갱신(원자성)
    - tick(): manager.update() 호출(전송+상태 동기화)
    """
    def __init__(self, manager: TMotorManager_mit_can):
        self.m = manager
        self._lock = threading.Lock()
        self._K = 5.0
        self._B = 0.1
        self._full_state = True

    def update_full_state(self, K: Optional[float] = None, B: Optional[float] = None,
                          pos: Optional[float] = None, vel: Optional[float] = None,
                          iq: Optional[float] = None, torque: Optional[float] = None,
                          *, full_state: Optional[bool] = None, clamp: bool = True):
        with self._lock:
            if full_state is None:
                full_state = self._full_state

            # 게인 효과값
            K_eff = self._K if K is None else float(K)
            B_eff = self._B if B is None else float(B)

            # 허용 범위로 클램프
            if clamp:
                kp_min = MIT_Params[self.m.type]['Kp_min']
                kp_max = MIT_Params[self.m.type]['Kp_max']
                kd_min = MIT_Params[self.m.type]['Kd_min']
                kd_max = MIT_Params[self.m.type]['Kd_max']
                K_eff = max(kp_min, min(K_eff, kp_max))
                B_eff = max(kd_min, min(B_eff, kd_max))

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


# ---------------------- 파서 ----------------------
def parse_and_apply(ctrl: FullStateController, line: str):
    """
    'K=.. B=.. pos=.. vel=.. iq=.. tor=.. full=0|1' 혹은 'zero'/'quit' 형식 파싱
    """
    line = line.strip()
    if not line:
        return None

    lo = line.lower()
    if lo in ("quit", "exit"):
        return "quit"
    if lo in ("zero", "origin"):
        ctrl.m.set_zero_position()
        return "zero"

    kv = {}
    full_flag = None
    try:
        tokens = shlex.split(line)
    except ValueError:
        tokens = line.split()

    for tok in tokens:
        if "=" not in tok:
            continue
        k, v = tok.split("=", 1)
        kl = k.strip().lower()
        v = v.strip()
        if kl in ("k", "kp"):
            kv["K"] = float(v)
        elif kl in ("b", "kd"):
            kv["B"] = float(v)
        elif kl in ("pos", "position"):
            kv["pos"] = float(v)
        elif kl in ("vel", "velocity"):
            kv["vel"] = float(v)
        elif kl == "iq":
            kv["iq"] = float(v)
        elif kl in ("tor", "torque"):
            kv["torque"] = float(v)
        elif kl == "full":
            full_flag = bool(int(v))

    ctrl.update_full_state(full_state=full_flag, **kv)
    return kv


# ---------------------- 서버/클라이언트/스폰 ----------------------
def run_server(args):
    ensure_fifo(FIFO_PATH)

    m = TMotorManager_mit_can(motor_type=args.type,
                              motor_ID=args.id,
                              max_mosfett_temp=60)

    stop_flag = threading.Event()

    def fifo_reader():
        # server는 FIFO를 읽고, 명령을 실시간 반영
        try:
            with open(FIFO_PATH, "r") as fr:
                while not stop_flag.is_set():
                    line = fr.readline()
                    if line == "":
                        time.sleep(0.05)
                        continue
                    res = parse_and_apply(ctrl, line)
                    if res == "quit":
                        stop_flag.set()
                        break
        except Exception:
            stop_flag.set()

    with m:
        ctrl = FullStateController(m)
        # 초기 안전값
        ctrl.update_full_state(K=5.0, B=0.1, pos=0.0, vel=0.0, iq=0.0, full_state=True)

        # CSV 로깅 준비
        csv_path = Path(args.csv)
        if not csv_path.is_absolute():
            csv_path = BASE_DIR / csv_path
        csv_fh = open(csv_path, "w", newline="")
        csv_w = csv.writer(csv_fh)
        csv_w.writerow(["time","pos","vel","acc","iq","torque","temp","err"])
        t0 = time.perf_counter()
        last_flush = 0.0
        last_print = 0.0

        # FIFO 리더 스레드 시작
        th = threading.Thread(target=fifo_reader, daemon=True)
        th.start()

        # 루프 선택: SoftRealtimeLoop or fallback
        loop_iter = SoftRealtimeLoop(dt=1.0/float(args.hz), report=True, fade=0.0) if HAVE_SRL else soft_loop(1.0/float(args.hz))

        print("[server] running. type commands in the client window (or `quit`).")
        for t in loop_iter:
            if stop_flag.is_set():
                break

            # 전송 + 상태 동기화
            ctrl.tick()

            # 상태 기록
            st = ctrl.read_state()
            csv_w.writerow([f"{time.perf_counter()-t0:.6f}", st["pos"], st["vel"], st["acc"],
                            st["iq"], st["torque"], st["temp"], int(st["err"])"])

            # 출력/flush: 10Hz
            if (t - last_print) >= 0.1:
                print(f"\rpos={st['pos']:+.3f} vel={st['vel']:+.3f} iq={st['iq']:+.3f} "
                      f"tor={st['torque']:+.3f} temp={st['temp']:.1f}C err={int(st['err'])}  ",
                      end="", flush=True)
                last_print = t
            if (t - last_flush) >= 0.1:
                csv_fh.flush()
                last_flush = t

        # 안전 정지
        ctrl.update_full_state(K=5.0, B=0.1, pos=0.0, vel=0.0, iq=0.0)
        for _ in range(5):
            ctrl.tick()
            time.sleep(0.01)

        csv_fh.flush()
        csv_fh.close()

    print("\n[server] exit.")


def run_client(args):
    print("Commands: K=.. B=.. pos=.. vel=.. iq=.. tor=.. full=0|1 | zero | quit")
    waited = 0.0
    # FIFO가 생길 때까지 대기
    while not Path(FIFO_PATH).exists():
        if waited == 0.0:
            print("[client] waiting for FIFO to appear... (start server or use --mode spawn)")
        time.sleep(0.2)
        waited += 0.2
        if waited >= 10.0:
            print("[client] still waiting... creating FIFO locally.")
            try:
                ensure_fifo(FIFO_PATH)
            except Exception as e:
                print(f"[client] ensure_fifo error: {e}")
                waited = 0.0

    try:
        print("[client] opening FIFO for writing... (may wait until server opens reader)")
        with open(FIFO_PATH, "w") as fw:
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


def run_spawn(args):
    term = find_terminal()
    if not term:
        print("No terminal emulator found. Install lxterminal or xterm or gnome-terminal.")
        sys.exit(1)

    # 1) FIFO를 먼저 만들어 레이스 방지 (절대경로)
    try:
        ensure_fifo(FIFO_PATH)
    except Exception as e:
        print(f"[spawn] ensure_fifo error: {e}")

    # 2) 절대 경로들 준비
    script_path = str((BASE_DIR / Path(__file__).name).resolve())
    py = f"python3 -u {shlex.quote(script_path)}"
    server_log = shlex.quote(str(BASE_DIR / "server.log"))
    client_log = shlex.quote(str(BASE_DIR / "client.log"))

    # 3) 명령 구성 (값만 quote)
    server_cmd = (
        f"{py} --mode server "
        f"--type {shlex.quote(args.type)} "
        f"--id {int(args.id)} "
        f"--hz {int(args.hz)} "
        f"--csv {shlex.quote(str((BASE_DIR / args.csv) if not Path(args.csv).is_absolute() else args.csv))} "
        f"2>&1 | tee -a {server_log}"
    )
    client_cmd = f"{py} --mode client 2>&1 | tee -a {client_log}"

    # 4) 창이 바로 닫히지 않도록 hold 동작 추가(hold 미지원 터미널 대비)
    hold_tail = '; echo; echo "[press Enter to close]"; read -r _'

    if term == "lxterminal":
        os.system(f'lxterminal -t "Motor Output" -e {wrap_in_shell(server_cmd + hold_tail)} &')
        time.sleep(0.8)  # 서버가 FIFO 읽기로 열 시간 확보
        os.system(f'lxterminal -t "Motor Input"  -e {wrap_in_shell(client_cmd + hold_tail)} &')

    elif term == "xterm":
        # xterm은 -hold 지원: 서버/클라 모두 -hold 로 유지
        os.system(f'xterm -T "Motor Output" -hold -e {wrap_in_shell(server_cmd)} &')
        time.sleep(0.8)
        os.system(f'xterm -T "Motor Input"  -hold -e {wrap_in_shell(client_cmd)} &')

    elif term in ("xfce4-terminal", "mate-terminal"):
        # 이 둘은 gnome-terminal과 유사하게 --command 대신 쉘로 전달
        os.system(f'{term} --title="Motor Output" -- bash -lc {shlex.quote(server_cmd + hold_tail)} &')
        time.sleep(0.8)
        os.system(f'{term} --title="Motor Input"  -- bash -lc {shlex.quote(client_cmd + hold_tail)} &')

    else:  # gnome-terminal
        os.system(f'gnome-terminal --title="Motor Output" -- bash -lc {shlex.quote(server_cmd + hold_tail)} &')
        time.sleep(0.8)
        os.system(f'gnome-terminal --title="Motor Input"  -- bash -lc {shlex.quote(client_cmd + hold_tail)} &')


# ---------------------- main ----------------------
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=("server","client","spawn"), default="server")
    p.add_argument("--type", default="AK70-10")
    p.add_argument("--id", type=int, default=1)
    p.add_argument("--hz", type=int, default=200)
    p.add_argument("--csv", default="session.csv")
    args = p.parse_args()

    if args.mode == "server":
        run_server(args)
    elif args.mode == "client":
        run_client(args)
    else:
        run_spawn(args)


if __name__ == "__main__":
    main()
