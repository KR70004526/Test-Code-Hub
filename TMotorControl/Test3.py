#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
multi_motor_console_setup.py
- 모터별 타입(예: ID1=AK70-10, ID2=AK80-64) 설정 지원
- 단일/다중 채널 spawn 가능
- setup 모드: 한 콘솔에서 버스/ID/타입을 입력 후 자동 스폰
- FIFO는 버스별 분리: cmd_{bus}.fifo (절대경로)
- 클라이언트 입력: id=3 K=.. B=.. pos=.. vel=.. iq=.. tor=.. full=0|1 | zero | start | end | stop | quit | help
- MIT_Params는 TMotorCANControl.mit_can에서 import
- TMotorManager_mit_can 초기화 시 max_mosfett_temp=90
- CSV는 **start~end 사이**에만 기록, **모터 ID별 별도 파일**로 저장
"""

import os, sys, csv, shlex, time, stat, argparse, threading, math
from pathlib import Path
from typing import Optional, Dict, List, Tuple

# ---------- 경로/로그 ----------
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

def fifo_path_for_bus(bus: str) -> str:
    return str(BASE_DIR / f"cmd_{bus}.fifo")  # 절대경로

def csv_path_for_motor(bus: str, mid: int) -> Path:
    return LOG_DIR / f"session_{bus}_id{mid}.csv"

def cmdlog_path_for_motor(bus: str, mid: int) -> Path:
    return LOG_DIR / f"cmd_{bus}_id{mid}.log"

# ---------- sys.path 보강 ----------
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
if str(BASE_DIR.parent) not in sys.path:
    sys.path.insert(0, str(BASE_DIR.parent))

# ---------- SoftRealtimeLoop ----------
try:
    from NeuroLocoMiddleware.SoftRealtimeLoop import SoftRealtimeLoop
    HAVE_SRL = True
except Exception:
    HAVE_SRL = False

def soft_loop(dt: float):
    t0 = time.perf_counter(); n = 0
    try:
        while True:
            t = time.perf_counter() - t0
            yield t
            n += 1
            next_t = t0 + n*dt
            time.sleep(max(0.0, next_t - time.perf_counter()))
    except GeneratorExit:
        return

# ---------- TMotor ----------
TMOTOR_OK = True
TMOTOR_IMPORT_ERR = ""
try:
    from TMotorCANControl import TMotorManager_mit_can
    from TMotorCANControl.mit_can import MIT_Params   # 요구사항 반영
except Exception as e:
    TMOTOR_OK = False
    TMOTOR_IMPORT_ERR = str(e)

# ---------- 유틸 ----------
def ensure_fifo(path: str):
    p = Path(path)
    if p.exists():
        st = p.stat()
        if not stat.S_ISFIFO(st.st_mode):
            try: p.unlink()
            except Exception: pass
            os.mkfifo(path)
    else:
        os.mkfifo(path)

def find_terminal():
    import shutil
    for term in ("lxterminal","xterm","gnome-terminal","xfce4-terminal","mate-terminal"):
        if shutil.which(term):
            return term
    return None

def wrap_in_shell(cmd: str) -> str:
    return f"bash -lc {shlex.quote(cmd)}"

def _fmtf(x, fmt="{:+.3f}"):
    try: return fmt.format(float(x))
    except Exception: return str(x)

# ---------- python-can 채널 고정(한 프로세스=한 버스) ----------
def force_socketcan_channel(channel: str):
    try:
        import can
    except Exception:
        return
    orig_bus = can.interface.Bus
    def Bus_patched(*args, **kwargs):
        bustype = kwargs.get("bustype", None)
        if bustype is None and len(args)>0:
            bustype = args[0]
        if isinstance(bustype, str) and bustype.startswith("socketcan"):
            kwargs["channel"] = channel
        return orig_bus(*args, **kwargs)
    can.interface.Bus = Bus_patched

# ---------- HELP 텍스트 ----------
def help_text(bus_hint: Optional[str]=None) -> str:
    bus_str = f"[bus={bus_hint}] " if bus_hint else ""
    return (
f"""\
=== Motor Console Help {bus_str}===

명령 형식 (한 줄에 여러 키=값 조합 가능):
  id=<번호|all>  K=<float>  B=<float>  pos=<rad>  vel=<rad/s>  iq=<A>  tor=<Nm>  full=<0|1>

제어/유틸:
  start           - 지정한 id(또는 id=all)의 CSV 기록 시작 (파일: logs/session_<bus>_id<ID>.csv)
  end             - 지정한 id(또는 id=all)의 CSV 기록 종료
  stop            - 지정한 id(또는 id=all)의 파라미터를 모두 0으로 설정하고, 현재 위치를 0으로 재설정
  zero            - 지정한 id(또는 id=all)의 현재 위치를 0으로 재설정(영점)
  help | h | ?    - 이 도움말 출력
  quit | exit     - 서버 종료 (client에서 보내면 서버도 함께 종료)

예시:
  id=1 K=20 B=0.5 pos=0.0          # 1번 모터 위치 명령
  id=2 vel=0.8                     # 2번 모터 속도 명령
  id=all iq=1.2                    # 모든 모터 q축 전류 명령
  id=1 start                       # 1번 모터 CSV 기록 시작
  id=1 end                         # 1번 모터 CSV 기록 종료
  id=all stop                      # 모든 모터 파라미터 0 및 현재 위치 0(주의)
  help                             # 도움말

참고:
- pos 명령은 하드웨어 프로토콜 특성상 [-12.5, +12.5] rad 범위 내에서 적용됩니다(포화).
- 속도/전류/토크 모드에서는 여러 바퀴 회전 가능하며 피드백 position은 드라이버가 멀티턴으로 누적될 수 있습니다.
- CSV 기록은 start~end 구간에만 저장되며, **모터 ID별 파일**로 분리됩니다.
"""
    )

# ---------- 파서 ----------
def parse_and_apply(targets: Dict[int, "FullStateController"], line: str):
    """
    id=3 K=.. B=.. pos=.. vel=.. iq=.. tor=.. full=0|1
    start | end | stop | zero | quit | help
    반환:
      - "quit"
      - ("start", [ids])
      - ("end", [ids])
      - ("stop", [ids])
      - dict(...)  # 일반 수치 명령/zero 적용됨 (ids 포함)
      - None
    """
    s = line.strip()
    if not s: return None
    lo = s.lower()
    if lo in ("help","h","?"):
        print("\n" + help_text() + "\n")
        return None
    if lo in ("quit","exit"):
        return "quit"

    try: tokens = shlex.split(s)
    except ValueError: tokens = s.split()

    target_ids: List[int] = []
    kv = {}; full_flag = None
    zero_flag = False
    start_flag = False
    end_flag = False
    stop_flag = False

    for tok in tokens:
        tl = tok.lower()
        if tl in ("start",): start_flag = True;  continue
        if tl in ("end",):   end_flag   = True;  continue
        if tl in ("stop",):  stop_flag  = True;  continue
        if tl in ("zero","origin"): zero_flag = True; continue

        if "=" not in tok:
            continue
        k,v = tok.split("=",1); k=k.strip().lower(); v=v.strip()
        if k=="id":
            if v.lower()=="all": target_ids = sorted(targets.keys())
            else: target_ids = [int(v)]
        elif k in ("k","kp"): kv["K"] = float(v)
        elif k in ("b","kd"): kv["B"] = float(v)
        elif k in ("pos","position"): kv["pos"] = float(v)
        elif k in ("vel","velocity"): kv["vel"] = float(v)
        elif k=="iq": kv["iq"] = float(v)
        elif k in ("tor","torque"): kv["torque"] = float(v)
        elif k=="full": full_flag = bool(int(v))

    if not target_ids:
        print("[server] 반드시 id=.. 또는 id=all 을 지정하세요. (help로 도움말)")
        return None

    # start/end/stop는 서버에서 처리하도록 토스
    if start_flag and not (end_flag or stop_flag or zero_flag or kv):
        return ("start", target_ids)
    if end_flag and not (start_flag or stop_flag or zero_flag or kv):
        return ("end", target_ids)
    if stop_flag and not (start_flag or end_flag or zero_flag or kv):
        return ("stop", target_ids)

    # zero/수치명령은 즉시 적용 (기존 방식 유지)
    for mid in target_ids:
        ctrl = targets.get(mid)
        if not ctrl:
            print(f"[server] 알 수 없는 모터 id={mid}")
            continue
        if zero_flag:
            ctrl.m.set_zero_position()
        if kv or (full_flag is not None):
            ctrl.update_full_state(full_state=full_flag, **kv)

    return {"ids": target_ids, **kv, **({"zero": True} if zero_flag else {}), **({"full": int(full_flag)} if full_flag is not None else {})}

# ---------- 컨트롤러 ----------
class FullStateController:
    def __init__(self, manager, motor_type: str):
        self.m = manager
        self.type = motor_type
        self._lock = threading.Lock()
        self._K = 5.0; self._B = 0.1; self._full_state = True

    def update_full_state(self, K: Optional[float]=None, B: Optional[float]=None,
                          pos: Optional[float]=None, vel: Optional[float]=None,
                          iq: Optional[float]=None, torque: Optional[float]=None,
                          *, full_state: Optional[bool]=None, clamp: bool=True):
        with self._lock:
            if full_state is None: full_state = self._full_state
            K_eff = self._K if K is None else float(K)
            B_eff = self._B if B is None else float(B)
            if clamp:
                kp_min = MIT_Params[self.type].get("Kp_min", None)
                kp_max = MIT_Params[self.type].get("Kp_max", None)
                kd_min = MIT_Params[self.type].get("Kd_min", None)
                kd_max = MIT_Params[self.type].get("Kd_max", None)
                if kp_min is not None: K_eff = max(kp_min, K_eff)
                if kp_max is not None: K_eff = min(K_eff, kp_max)
                if kd_min is not None: B_eff = max(kd_min, B_eff)
                if kd_max is not None: B_eff = min(B_eff, kd_max)

            if full_state:
                self.m.set_impedance_gains_real_unit_full_state_feedback(K=K_eff, B=B_eff)
            else:
                self.m.set_impedance_gains_real_unit(K=K_eff, B=B_eff)
            self._K, self._B, self._full_state = K_eff, B_eff, full_state

            if pos is not None:    self.m.set_output_angle_radians(float(pos))
            if vel is not None:    self.m.set_output_velocity_radians_per_second(float(vel))
            if torque is not None: self.m.set_output_torque_newton_meters(float(torque))
            if iq is not None:     self.m.set_motor_current_qaxis_amps(float(iq))

    def tick(self):
        with self._lock:
            self.m.update()

    def read_state(self) -> Dict[str, float]:
        return {
            "pos": self.m.position, "vel": self.m.velocity, "acc": self.m.acceleration,
            "iq": self.m.current_qaxis, "torque": self.m.torque, "temp": self.m.temperature,
            "err": self.m.error,
        }

# ---------- 매핑 파서(문자열 → 버스: {id: type}) ----------
def parse_idmap_onebus(idmap: str, default_type: str) -> Dict[int, str]:
    """
    "1:AK70-10,2:AK80-64" → {1:"AK70-10", 2:"AK80-64"}
    "1,2" → {1:default_type, 2:default_type}
    """
    out: Dict[int,str] = {}
    if not idmap: return out
    for tok in idmap.split(","):
        tok = tok.strip()
        if not tok: continue
        if ":" in tok:
            i, t = tok.split(":",1)
            out[int(i.strip())] = t.strip()
        else:
            out[int(tok)] = default_type
    return out

def parse_map_multibus(map_str: str, default_type: str) -> Dict[str, Dict[int, str]]:
    """
    "can0:1:AK70-10,2:AK80-64;can1:3:AK80-64,4:AK80-9"
      → {"can0":{1:"AK70-10",2:"AK80-64"}, "can1":{3:"AK80-64",4:"AK80-9"}}
    기존 형식 "can0:1,2;can1:3,4"도 허용 (기본 타입 적용)
    """
    out: Dict[str, Dict[int,str]] = {}
    if not map_str: return out
    for part in map_str.split(";"):
        part = part.strip()
        if not part: continue
        if ":" not in part: raise ValueError(f"--map 형식 오류: {part}")
        bus, ids = part.split(":",1)
        out[bus.strip()] = parse_idmap_onebus(ids, default_type)
    return out

# ---------- 서버(단일 버스, 모터별 타입/녹화 지원) ----------
def run_server_singlebus(bus: str, id2type: Dict[int, str], hz: int):
    if not TMOTOR_OK:
        print("[server] TMotorCANControl import 실패:", TMOTOR_IMPORT_ERR)
        sys.exit(1)

    force_socketcan_channel(bus)
    fifo = fifo_path_for_bus(bus)
    ensure_fifo(fifo)

    # 모터 매니저/컨트롤러 준비 (온도 90C 설정)
    motors: Dict[int, Tuple[TMotorManager_mit_can, FullStateController]] = {}
    try:
        for mid, mtype in id2type.items():
            m = TMotorManager_mit_can(motor_type=mtype, motor_ID=mid, max_mosfett_temp=90)
            motors[mid] = (m, FullStateController(m, mtype))
    except Exception as e:
        print(f"[server] 모터 초기화 실패: {e}")
        sys.exit(1)

    # ID별 녹화 상태 / CSV 핸들/작성기 / 명령로그 핸들
    recording: Dict[int, bool] = {mid: False for mid in motors.keys()}
    csv_fh: Dict[int, Optional[any]] = {mid: None for mid in motors.keys()}
    csv_w : Dict[int, Optional[csv.writer]] = {mid: None for mid in motors.keys()}
    cmd_fh: Dict[int, any] = {}

    # 명령 로그 파일(항상 append)
    for mid in motors.keys():
        fh = open(cmdlog_path_for_motor(bus, mid), "a", buffering=1)
        fh.write(f"# cmd log start {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        cmd_fh[mid] = fh

    def _open_csv(mid: int):
        if csv_fh[mid] is not None:
            return
        path = csv_path_for_motor(bus, mid)
        exists = path.exists() and path.stat().st_size > 0
        fh = open(path, "a", newline="")
        w = csv.writer(fh)
        if not exists:
            w.writerow(["time","bus","id","type","pos","vel","acc","iq","torque","temp","err"])
        csv_fh[mid] = fh
        csv_w[mid] = w

    def _close_csv(mid: int):
        if csv_fh[mid] is not None:
            try: csv_fh[mid].flush(); csv_fh[mid].close()
            except Exception: pass
            csv_fh[mid] = None
            csv_w[mid] = None

    stop_flag = threading.Event()

    def _log_cmd(ids: List[int], line: str):
        t = time.time()
        for mid in ids:
            fh = cmd_fh.get(mid)
            if fh:
                fh.write(f"{t:.6f} {line.strip()}\n")

    def fifo_reader():
        try:
            with open(fifo, "r") as fr:
                while not stop_flag.is_set():
                    line = fr.readline()
                    if line == "":
                        time.sleep(0.05); continue
                    res = parse_and_apply({mid: ctrl for mid,(_,ctrl) in motors.items()}, line)
                    if res == "quit":
                        stop_flag.set(); break
                    # start / end / stop 처리
                    if isinstance(res, tuple):
                        action, ids = res
                        if action == "start":
                            for mid in ids:
                                _open_csv(mid); recording[mid] = True
                            print(f"\n[server:{bus}] start recording ids={ids}")
                            _log_cmd(ids, "## START")
                        elif action == "end":
                            for mid in ids:
                                recording[mid] = False; _close_csv(mid)
                            print(f"\n[server:{bus}] end recording ids={ids}")
                            _log_cmd(ids, "## END")
                        elif action == "stop":
                            for mid in ids:
                                # 모든 파라미터 0, 위치 0
                                _, ctrl = motors[mid]
                                try:
                                    ctrl.update_full_state(K=0.0, B=0.0, pos=0.0, vel=0.0, iq=0.0, torque=0.0, full_state=ctrl._full_state)
                                    ctrl.m.set_zero_position()
                                except Exception as e:
                                    print(f"\n[server:{bus}] stop 적용 실패 id={mid}: {e}")
                            print(f"\n[server:{bus}] stop applied ids={ids} (all params=0, zero pos)")
                            _log_cmd(ids, "## STOP")
                        continue
                    # 일반 수치명령/zero → 명령 로그만 남김
                    if isinstance(res, dict) and "ids" in res:
                        _log_cmd(res["ids"], line)
        except Exception:
            stop_flag.set()

    # with 컨텍스트로 안전 관리
    ctx = [m for m,_ in motors.values()]
    for m in ctx: m.__enter__()
    try:
        # 초기 안전값
        for _, ctrl in motors.values():
            ctrl.update_full_state(K=5.0, B=0.1, pos=0.0, vel=0.0, iq=0.0, full_state=True)

        th = threading.Thread(target=fifo_reader, daemon=True)
        th.start()

        loop_iter = SoftRealtimeLoop(dt=1.0/float(hz), report=True, fade=0.0) if HAVE_SRL else soft_loop(1.0/float(hz))
        print(f"[server:{bus}] running. ids={list(id2type.keys())}  (`help`로 도움말, `quit` 종료)")
        t0 = time.perf_counter(); last_print=0.0

        while not stop_flag.is_set():
            t = next(loop_iter)
            for mid, (mgr, ctrl) in motors.items():
                ctrl.tick()
                st = ctrl.read_state()

                # 녹화 중인 ID만 CSV 저장
                if recording[mid] and csv_w[mid] is not None:
                    csv_w[mid].writerow([
                        f"{time.perf_counter()-t0:.6f}", bus, mid, ctrl.type,
                        st["pos"], st["vel"], st["acc"], st["iq"],
                        st["torque"], st["temp"], st["err"]
                    ])

            if (t - last_print) >= 0.1:
                status=[]
                for mid,(mgr,ctrl) in motors.items():
                    st = ctrl.read_state()
                    rec = "R" if recording[mid] else "-"
                    status.append(f"[{rec}] id{mid}({ctrl.type}): pos={_fmtf(st['pos'])} vel={_fmtf(st['vel'])} iq={_fmtf(st['iq'],'{:+.2f}')} tor={_fmtf(st['torque'],'{:+.2f}')} T={_fmtf(st['temp'],'{:.1f}')} err={st['err']}")
                print("\r" + " | ".join(status) + "   ", end="", flush=True)
                last_print = t

        # 안전 정지
        for _, ctrl in motors.values():
            ctrl.update_full_state(K=5.0, B=0.1, pos=0.0, vel=0.0, iq=0.0)
        for _ in range(5):
            for _, ctrl in motors.values(): ctrl.tick()
            time.sleep(0.01)

    finally:
        # CSV/로그 닫기
        for mid in motors.keys():
            try:
                if csv_fh[mid]: csv_fh[mid].flush(); csv_fh[mid].close()
            except Exception: pass
            try:
                if cmd_fh.get(mid): cmd_fh[mid].write(f"# cmd log end {time.strftime('%Y-%m-%d %H:%M:%S')}\n"); cmd_fh[mid].close()
            except Exception: pass
        # 컨텍스트 종료
        for m in ctx: m.__exit__(None,None,None)
    print(f"\n[server:{bus}] exit.")

# ---------- 클라이언트 ----------
def run_client(bus: str):
    fifo = fifo_path_for_bus(bus)
    print(f"[client:{bus}] Commands: id=.. K=.. B=.. pos=.. vel=.. iq=.. tor=.. full=0|1 | start | end | stop | zero | quit | help")
    waited=0.0
    while not Path(fifo).exists():
        if waited==0.0: print(f"[client:{bus}] waiting for FIFO({fifo}) ... (start server or use --mode spawn)")
        time.sleep(0.2); waited+=0.2
        if waited>=10.0:
            print(f"[client:{bus}] still waiting... creating FIFO locally.")
            try: ensure_fifo(fifo)
            except Exception as e: print(f"[client:{bus}] ensure_fifo error: {e}"); waited=0.0
    try:
        print(f"[client:{bus}] opening FIFO... (may wait until server opens reader)")
        print("  * 언제든 'help' 입력으로 사용법을 볼 수 있습니다.")
        with open(fifo,"w") as fw:
            while True:
                try: line = input(f"CMD[{bus}]> ").strip()
                except (EOFError,KeyboardInterrupt): line="quit"
                if not line: continue

                # 로컬 help 처리 (서버로 보내지 않음)
                if line.lower() in ("help","h","?"):
                    print(help_text(bus_hint=bus))
                    continue

                fw.write(line+"\n"); fw.flush()
                if line.lower() in ("quit","exit"):
                    print("bye"); break
    except Exception as e:
        print(f"[client:{bus}] error: {e}")
        input("Press Enter to close...")

# ---------- spawn ----------
def run_spawn(mapping: Dict[str, Dict[int,str]], hz: int):
    term = find_terminal()
    if not term:
        print("No terminal emulator found. Install lxterminal/xterm/gnome-terminal.")
        sys.exit(1)

    script_path = str((BASE_DIR / Path(__file__).name).resolve())
    py = f"{shlex.quote(sys.executable)} -u {shlex.quote(script_path)}"

    for bus, id2type in mapping.items():
        idmap_str = ",".join(f"{i}:{t}" for i,t in id2type.items())
        server_cmd = (f"{py} --mode server --bus {bus} --idmap {shlex.quote(idmap_str)} "
                      f"--hz {hz} 2>&1 | tee -a {shlex.quote(str(LOG_DIR / f'server_{bus}.log'))}")
        client_cmd = (f"{py} --mode client --bus {bus} "
                      f"2>&1 | tee -a {shlex.quote(str(LOG_DIR / f'client_{bus}.log'))}")
        hold_tail = '; echo; echo "[press Enter to close]"; read -r _'

        if term=="lxterminal":
            os.system(f'lxterminal -t "Motor Output {bus}" -e {wrap_in_shell(server_cmd + hold_tail)} &')
            time.sleep(0.6)
            os.system(f'lxterminal -t "Motor Input {bus}"  -e {wrap_in_shell(client_cmd + hold_tail)} &')
        elif term=="xterm":
            os.system(f'xterm -T "Motor Output {bus}" -hold -e {wrap_in_shell(server_cmd)} &')
            time.sleep(0.6)
            os.system(f'xterm -T "Motor Input {bus}"  -hold -e {wrap_in_shell(client_cmd)} &')
        else:
            os.system(f'gnome-terminal --title="Motor Output {bus}" -- bash -lc {shlex.quote(server_cmd + hold_tail)} &')
            time.sleep(0.6)
            os.system(f'gnome-terminal --title="Motor Input {bus}"  -- bash -lc {shlex.quote(client_cmd + hold_tail)} &')

# ---------- setup(설정 마법사: 설명 강화) ----------
def run_setup(default_hz: int):
    print("# Setup Wizard")
    print("이 마법사는 아래 순서로 도와줍니다.")
    print("  1) 사용할 CAN 버스 이름(can0/can1/...)을 정합니다.")
    print("  2) 각 버스에 연결된 모터의 ID와 타입을 입력합니다. (예: 1 AK70-10, 2 AK80-64)")
    print("  3) 제어 주파수(Hz)를 정합니다. (기본 200)")
    print("  4) 설정을 확인한 뒤, 버스별 서버/클라이언트를 자동으로 띄웁니다(spawn).")
    print("")
    print("입력 형식 예시:")
    print("  can0: 1 AK70-10, 2 AK80-64")
    print("  can1: 3 AK80-64")
    print("입력을 마치려면 빈 줄을 입력하세요.")
    print("")
    print("실행 후 사용:")
    print("  - 각 버스마다 두 창이 뜹니다(서버/클라이언트).")
    print("  - 클라이언트 창에서 'help'를 입력하면 명령 설명이 나옵니다.")
    print("  - 기록은 'id=<번호> start' 로 시작하고 'id=<번호> end' 로 끝냅니다.")
    print("  - 기록 파일과 명령 로그는 logs/ 폴더에 모터 ID별로 저장됩니다.")
    print("")

    mapping: Dict[str, Dict[int,str]] = {}
    while True:
        line = input("버스 입력 (예: can0: 1 AK70-10, 2 AK80-64) > ").strip()
        if not line: break
        if ":" not in line:
            print("형식: canX: ID TYPE[, ID TYPE ...]"); continue
        bus, rest = line.split(":",1)
        bus = bus.strip()
        pairs = {}
        # "1 AK70-10, 2 AK80-64"
        for seg in rest.split(","):
            seg = seg.strip()
            if not seg: continue
            parts = seg.split()
            if len(parts)<2:
                print(f"  항목 형식 오류: {seg}  (ID TYPE)"); continue
            try:
                mid = int(parts[0]); mtype = parts[1]
            except Exception:
                print(f"  항목 형식 오류: {seg}"); continue
            pairs[mid] = mtype
        if not pairs:
            print("  유효한 ID TYPE 쌍이 없습니다."); continue
        mapping[bus] = pairs

    if not mapping:
        print("설정이 비어 있습니다. 종료.")
        return

    print("\n## 설정 요약")
    for bus, mp in mapping.items():
        print(f"  {bus}: " + ", ".join(f"{i}:{t}" for i,t in mp.items()))
    try:
        hz = int(input(f"제어 주기 Hz 입력 (기본 {default_hz}) > ").strip() or str(default_hz))
    except Exception:
        hz = default_hz

    print("\n[안내] 이제 버스별 서버/클라이언트 창을 자동으로 띄웁니다.")
    print("  - 서버 창: 실시간 상태 출력, start~end 구간만 CSV 기록")
    print("  - 클라 창: id=.. 으로 대상 지정 후 명령 입력 (help로 도움말)")
    yn = input("이 설정으로 spawn 할까요? [Y/n] ").strip().lower()
    if yn in ("", "y", "yes"):
        run_spawn(mapping, hz)
    else:
        print("취소됨.")

# ---------- main ----------
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=("server","client","spawn","setup"), default="setup",
                   help="setup: 설정 마법사 / spawn: 버스별 서버/클라 자동 스폰 / server|client: 단일 버스")
    p.add_argument("--hz", type=int, default=200)
    p.add_argument("--bus", default="can0", help="server/client 단일 버스 지정")
    p.add_argument("--idmap", default="", help='server 단일 버스: "1:AK70-10,2:AK80-64" (타입 지정)')
    p.add_argument("--map", default="", help='spawn 다중 버스: "can0:1:AK70-10,2:AK80-64;can1:3:AK80-64"')
    args = p.parse_args()

    if args.mode == "setup":
        run_setup(args.hz); return

    if args.mode == "spawn":
        mapping = parse_map_multibus(args.map, default_type="AK70-10")
        if not mapping:
            # 단일 버스로도 spawn 허용: --bus + --idmap
            id2type = parse_idmap_onebus(args.idmap, default_type="AK70-10")
            if not id2type:
                print('spawn 모드: --map 또는 (--bus 와 --idmap "1:AK70-10,2:AK80-64") 필요'); sys.exit(2)
            mapping = {args.bus: id2type}
        run_spawn(mapping, args.hz); return

    if args.mode == "client":
        run_client(args.bus); return

    # server
    id2type = parse_idmap_onebus(args.idmap, default_type="AK70-10")
    if not id2type:
        print('server 모드: --idmap "1:AK70-10,2:AK80-64" 필요'); sys.exit(2)
    run_server_singlebus(args.bus, id2type, args.hz)

if __name__ == "__main__":
    main()
