#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
multi_motor_console_setup.py (refined)
- 클래스화(Controller/Server/Parser/Recorder/Scheduler)
- 토크-only (iq 완전 제거)
- FIFO는 버스별 분리: cmd_{bus}.fifo (절대경로)
- 명령: id=3 K=.. B=.. pos=.. vel=.. tor=.. full=0|1 | zero | start | end | stop | quit | help
- MIT_Params는 TMotorCANControl.mit_can에서 import (누락 시 1회 경고)
- TMotorManager_mit_can 초기화 시 max_mosfett_temp=90
- CSV는 start~end 구간만 저장, **모터 ID별 별도 파일**
- 개선점 포함:
  * help 중복 제거(서버 파서는 출력하지 않음, 클라만 안내)
  * state() 락 + acceleration 방어(getattr)
  * stop 순서: K/B=0 → vel/torque=0 → zero() (pos 명령 제거)
  * FIFO non-blocking + EOF 재오픈 처리
  * 액션과 파라미터 혼합 시 경고 출력(액션만 수행)
"""

import os, sys, csv, shlex, time, stat, argparse, threading, math, queue, select
from dataclasses import dataclass, field
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

# ---------- TMotor ----------
TMOTOR_OK = True
TMOTOR_IMPORT_ERR = ""
try:
    from TMotorCANControl import TMotorManager_mit_can
    from TMotorCANControl.mit_can import MIT_Params
except Exception as e:
    TMOTOR_OK = False
    TMOTOR_IMPORT_ERR = str(e)

# ---------- HELP ----------
def help_text(bus_hint: Optional[str]=None) -> str:
    bus_str = f"[bus={bus_hint}] " if bus_hint else ""
    return (
f"""\
=== Motor Console Help {bus_str}===

명령 형식 (한 줄에 여러 key=value 조합 가능):
  id=<번호|all>  K=<float>  B=<float>  pos=<rad>  vel=<rad/s>  tor=<Nm>  full=<0|1>

제어/유틸:
  start           - 지정한 id(또는 id=all)의 CSV 기록 시작 (logs/session_<bus>_id<ID>.csv)
  end             - 지정한 id(또는 id=all)의 CSV 기록 종료
  stop            - 안전 정지(K=B=0, vel/tor=0, zero)
  zero            - 지정한 id(또는 id=all)의 현재 위치를 0으로 재설정(영점)
  help | h | ?    - 이 도움말 출력(클라이언트에서 표시)
  quit | exit     - 서버 종료 (client에서 보내면 서버도 함께 종료)

예시:
  id=1 K=20 B=0.5 pos=0.0          # 1번 모터 위치 명령
  id=2 vel=0.8                     # 2번 모터 속도 명령
  id=all tor=1.2                   # 모든 모터 토크 명령
  id=1 start                       # 1번 모터 CSV 기록 시작
  id=1 end                         # 1번 모터 CSV 기록 종료
  id=all stop                      # 안전 정지
  help                             # 도움말

참고:
- pos 명령은 [-12.5, +12.5] rad 제한(드라이버 프로토콜 상 포화).
- 속도/토크 모드에서는 멀티턴 누적될 수 있습니다.
- CSV 기록은 start~end 구간만 저장되며, 모터 ID별 파일로 분리됩니다.
"""
    )

# ---------- 스케줄러 ----------
class RealtimeScheduler:
    def __init__(self, hz: int):
        self.dt = 1.0/float(hz)
        self._use_srl = False
        if HAVE_SRL:
            try:
                self._srl = SoftRealtimeLoop(dt=self.dt, report=True, fade=0.0)
                self._iter = iter(self._srl)
                self._use_srl = True
            except Exception:
                self._use_srl = False
        if not self._use_srl:
            self._t0 = time.perf_counter()
            self._n = 0
    def tick(self):
        if self._use_srl:
            return next(self._iter)
        self._n += 1
        tgt = self._t0 + self._n * self.dt
        now = time.perf_counter()
        delay = max(0.0, tgt - now)
        if delay > 0: time.sleep(delay)
        return tgt - self._t0

# ---------- 디바이스 컨트롤러 (토크-only) ----------
class MotorController:
    def __init__(self, manager: "TMotorManager_mit_can", motor_type: str,
                 K_init: float=5.0, B_init: float=0.1, full_state: bool=True):
        self.m = manager
        self.type = motor_type
        self._K = float(K_init)
        self._B = float(B_init)
        self._full = bool(full_state)
        self._lock = threading.Lock()
        self._warned_missing_type = False
        self.apply_gains()  # 초기 적용

    def _clamp(self, K: float, B: float) -> Tuple[float,float]:
        p = MIT_Params.get(self.type) if TMOTOR_OK else None
        if p is None:
            if not self._warned_missing_type:
                print(f"[warn] MIT_Params missing for type '{self.type}'. Gains are not clamped.")
                self._warned_missing_type = True
            return K, B
        Kp_min, Kp_max = p.get("Kp_min", None), p.get("Kp_max", None)
        Kd_min, Kd_max = p.get("Kd_min", None), p.get("Kd_max", None)
        if Kp_min is not None: K = max(K, Kp_min)
        if Kp_max is not None: K = min(K, Kp_max)
        if Kd_min is not None: B = max(B, Kd_min)
        if Kd_max is not None: B = min(B, Kd_max)
        return K, B

    def apply_gains(self, K: Optional[float]=None, B: Optional[float]=None,
                    full_state: Optional[bool]=None):
        with self._lock:
            if K is not None: self._K = float(K)
            if B is not None: self._B = float(B)
            if full_state is not None: self._full = bool(full_state)
            Kc, Bc = self._clamp(self._K, self._B)
            if self._full:
                self.m.set_impedance_gains_real_unit_full_state_feedback(K=Kc, B=Bc)
            else:
                self.m.set_impedance_gains_real_unit(K=Kc, B=Bc)

    def command(self, *, pos=None, vel=None, torque=None):
        with self._lock:
            if pos is not None:    self.m.set_output_angle_radians(float(pos))
            if vel is not None:    self.m.set_output_velocity_radians_per_second(float(vel))
            if torque is not None: self.m.set_output_torque_newton_meters(float(torque))

    def zero(self):
        with self._lock:
            self.m.set_zero_position()

    def tick(self):
        with self._lock:
            self.m.update()

    def state(self) -> Dict[str, float]:
        # 락 적용 + acceleration 방어
        with self._lock:
            return {
                "pos": self.m.position,
                "vel": self.m.velocity,
                "acc": getattr(self.m, "acceleration", 0.0),
                "torque": self.m.torque,
                "temp": self.m.temperature,
                "err": self.m.error,
            }

# ---------- 파서 ----------
@dataclass
class Command:
    ids: List[int] = field(default_factory=list)
    gains: Dict[str, float] = field(default_factory=dict)      # K,B
    setp : Dict[str, float] = field(default_factory=dict)      # pos,vel,torque
    full : Optional[bool] = None
    action: Optional[str] = None                               # start|end|stop|zero|quit|help
    mixed: bool = False                                        # action + params 혼합 여부

class CommandParser:
    def parse(self, line: str, id_pool: List[int]) -> Optional[Command]:
        s = line.strip()
        if not s: return None
        lo = s.lower()
        if lo in ("quit","exit"):
            return Command(action="quit")
        if lo in ("help","h","?"):
            # 서버는 help를 출력하지 않음(클라이언트가 출력)
            return Command(action="help")

        try: tokens = shlex.split(s)
        except ValueError: tokens = s.split()

        cmd = Command()
        for tok in tokens:
            tl = tok.lower()
            if tl in ("start",): cmd.action="start"; continue
            if tl in ("end",):   cmd.action="end";   continue
            if tl in ("stop",):  cmd.action="stop";  continue
            if tl in ("zero","origin"): cmd.action="zero"; continue
            if "=" not in tok:
                continue
            k,v = tok.split("=",1); k=k.strip().lower(); v=v.strip()
            if k=="id":
                if v.lower()=="all": cmd.ids = sorted(id_pool)
                else:
                    try: cmd.ids = [int(v)]
                    except: pass
            elif k in ("k","kp"):
                cmd.gains["K"] = float(v)
            elif k in ("b","kd"):
                cmd.gains["B"] = float(v)
            elif k in ("pos","position"):
                cmd.setp["pos"] = float(v)
            elif k in ("vel","velocity"):
                cmd.setp["vel"] = float(v)
            elif k in ("tor","torque"):
                cmd.setp["torque"] = float(v)
            elif k=="full":
                cmd.full = bool(int(v))

        if not cmd.ids:
            print("[server] 반드시 id=.. 또는 id=all 을 지정하세요. (help로 도움말)")
            return None

        # 액션과 파라미터 혼합 여부 기록(서버에서 경고 출력)
        if cmd.action in ("start","end","stop","zero") and (cmd.gains or cmd.setp or cmd.full is not None):
            cmd.mixed = True

        return cmd

# ---------- 기록기 ----------
class CsvRecorder:
    def __init__(self, path: Path):
        self.path = path
        self._fh = None
        self._w = None

    def open(self):
        exists = self.path.exists() and self.path.stat().st_size > 0
        self._fh = open(self.path, "a", newline="")
        self._w = csv.writer(self._fh)
        if not exists:
            # iq 제거됨
            self._w.writerow(["time","bus","id","type","pos","vel","acc","torque","temp","err"])

    def write(self, row: List):
        if self._w: self._w.writerow(row)

    def close(self):
        try:
            if self._fh: self._fh.flush(); self._fh.close()
        finally:
            self._fh = self._w = None

class CmdLogger:
    def __init__(self, path: Path):
        self._fh = open(path, "a", buffering=1)
        self._fh.write(f"# cmd log start {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    def log(self, s: str):
        t = time.time()
        try:
            self._fh.write(f"{t:.6f} {s.strip()}\n")
        except Exception:
            pass
    def close(self):
        try:
            self._fh.write(f"# cmd log end {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            self._fh.close()
        except Exception:
            pass

# ---------- FIFO ----------
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

class FifoCommandServer:
    """
    Non-blocking reader using os.open(O_RDONLY|O_NONBLOCK) + select.select().
    EOF(=writers all closed) 발생 시 FD 재오픈하여 지속 수신.
    """
    def __init__(self, path: str):
        self.path = path
        ensure_fifo(self.path)

    def reader_thread(self, q: "queue.Queue[str]", stop_evt: threading.Event):
        fd = None
        fobj = None
        try:
            while not stop_evt.is_set():
                try:
                    if fobj is None:
                        fd = os.open(self.path, os.O_RDONLY | os.O_NONBLOCK)
                        fobj = os.fdopen(fd, "r", buffering=1)
                    rlist, _, _ = select.select([fobj], [], [], 0.1)
                    if fobj in rlist:
                        line = fobj.readline()
                        if line == "":
                            # EOF: no writers; reopen
                            try: fobj.close()
                            except Exception: pass
                            fobj = None
                            time.sleep(0.05)
                            continue
                        q.put(line)
                except FileNotFoundError:
                    # fifo removed & recreated? ensure and retry
                    ensure_fifo(self.path)
                    time.sleep(0.1)
                except Exception:
                    time.sleep(0.05)
        finally:
            try:
                if fobj: fobj.close()
            except Exception:
                pass

class FifoCommandClient:
    def __init__(self, path: str):
        self.path = path

    def run(self, bus: str):
        print(f"[client:{bus}] Commands: id=.. K=.. B=.. pos=.. vel=.. tor=.. full=0|1 | start | end | stop | zero | quit | help")
        waited=0.0
        while not Path(self.path).exists():
            if waited==0.0: print(f"[client:{bus}] waiting for FIFO({self.path}) ... (start server or use --mode spawn)")
            time.sleep(0.2); waited+=0.2
            if waited>=10.0:
                print(f"[client:{bus}] still waiting... creating FIFO locally.")
                try: ensure_fifo(self.path)
                except Exception as e: print(f"[client:{bus}] ensure_fifo error: {e}"); waited=0.0
        try:
            print(f"[client:{bus}] opening FIFO... (may wait until server opens reader)")
            print("  * 언제든 'help' 입력으로 사용법을 볼 수 있습니다.")
            with open(self.path,"w") as fw:
                while True:
                    try: line = input(f"CMD[{bus}]> ").strip()
                    except (EOFError,KeyboardInterrupt): line="quit"
                    if not line: continue
                    if line.lower() in ("help","h","?"):
                        print(help_text(bus_hint=bus))
                        continue
                    fw.write(line+"\n"); fw.flush()
                    if line.lower() in ("quit","exit"):
                        print("bye"); break
        except Exception as e:
            print(f"[client:{bus}] error: {e}")
            input("Press Enter to close...")

# ---------- 매핑 파서(문자열 → 버스: {id: type}) ----------
def parse_idmap_onebus(idmap: str, default_type: str) -> Dict[int, str]:
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
    out: Dict[str, Dict[int,str]] = {}
    if not map_str: return out
    for part in map_str.split(";"):
        part = part.strip()
        if not part: continue
        if ":" not in part: raise ValueError(f"--map 형식 오류: {part}")
        bus, ids = part.split(":",1)
        out[bus.strip()] = parse_idmap_onebus(ids, default_type)
    return out

# ---------- 버스 서버 ----------
@dataclass
class _MotorRuntime:
    mgr: "TMotorManager_mit_can"
    ctrl: MotorController
    rec_on: bool = False
    csv: Optional[CsvRecorder] = None
    cmdlog: Optional[CmdLogger] = None
    mtype: str = ""

class MotorBusServer:
    def __init__(self, bus: str, id2type: Dict[int, str], hz: int):
        if not TMOTOR_OK:
            raise RuntimeError(f"TMotorCANControl import 실패: {TMOTOR_IMPORT_ERR}")
        self.bus = bus
        self.id2type = id2type
        self.hz = hz
        self.scheduler = RealtimeScheduler(hz=self.hz)
        self.stop_evt = threading.Event()
        self.q: "queue.Queue[str]" = queue.Queue()
        self.fifo = FifoCommandServer(fifo_path_for_bus(self.bus))
        self.parser = CommandParser()
        self.motors: Dict[int, _MotorRuntime] = {}
        self._status_last = 0.0

    def start(self):
        force_socketcan_channel(self.bus)
        # 디바이스 매니저/컨트롤러 준비
        for mid, mtype in self.id2type.items():
            m = TMotorManager_mit_can(motor_type=mtype, motor_ID=mid, max_mosfett_temp=90)
            m.__enter__()
            ctrl = MotorController(m, mtype, K_init=5.0, B_init=0.1, full_state=True)
            # 안전초기화: vel/tor=0 (pos는 명령하지 않음)
            ctrl.command(vel=0.0, torque=0.0)
            rt = _MotorRuntime(mgr=m, ctrl=ctrl, rec_on=False, csv=None,
                               cmdlog=CmdLogger(cmdlog_path_for_motor(self.bus, mid)),
                               mtype=mtype)
            self.motors[mid] = rt

        # FIFO reader thread
        th = threading.Thread(target=self.fifo.reader_thread, args=(self.q, self.stop_evt), daemon=True)
        th.start()
        print(f"[server:{self.bus}] running. ids={list(self.id2type.keys())}  (`help`는 클라이언트에서, `quit` 종료)")

    def shutdown(self):
        # 안전 정지
        for mid, rt in self.motors.items():
            try:
                rt.ctrl.apply_gains(K=0.0, B=0.0)
                rt.ctrl.command(vel=0.0, torque=0.0)
                rt.ctrl.zero()
                if rt.csv: rt.csv.close()
                if rt.cmdlog: rt.cmdlog.close()
            except Exception:
                pass
        for mid, rt in self.motors.items():
            try: rt.mgr.__exit__(None,None,None)
            except Exception: pass
        print(f"\n[server:{self.bus}] exit.")

    def _handle_action(self, cmd: Command):
        if cmd.mixed:
            print("[server] 경고: action과 파라미터를 한 줄에 섞을 수 없습니다. (action만 수행)")
        ids = cmd.ids
        if cmd.action == "start":
            for mid in ids:
                rt = self.motors.get(mid); 
                if not rt: continue
                if not rt.csv:
                    rec = CsvRecorder(csv_path_for_motor(self.bus, mid)); rec.open()
                    rt.csv = rec; rt.rec_on = True
                if rt.cmdlog: rt.cmdlog.log("## START")
            print(f"\n[server:{self.bus}] start recording ids={ids}")
            return
        if cmd.action == "end":
            for mid in ids:
                rt = self.motors.get(mid); 
                if not rt: continue
                rt.rec_on = False
                if rt.csv: rt.csv.close(); rt.csv=None
                if rt.cmdlog: rt.cmdlog.log("## END")
            print(f"\n[server:{self.bus}] end recording ids={ids}")
            return
        if cmd.action == "stop":
            for mid in ids:
                rt = self.motors.get(mid); 
                if not rt: continue
                try:
                    rt.ctrl.apply_gains(K=0.0, B=0.0)
                    rt.ctrl.command(vel=0.0, torque=0.0)   # pos 명령 없음
                    rt.ctrl.zero()
                    if rt.cmdlog: rt.cmdlog.log("## STOP")
                except Exception as e:
                    print(f"\n[server:{self.bus}] stop 적용 실패 id={mid}: {e}")
            print(f"\n[server:{self.bus}] stop applied ids={ids}")
            return
        if cmd.action == "zero":
            for mid in ids:
                rt = self.motors.get(mid); 
                if not rt: continue
                rt.ctrl.zero()
                if rt.cmdlog: rt.cmdlog.log("## ZERO")
            return

    def _handle_non_action(self, cmd: Command, raw_line: str):
        # gains/full
        if cmd.gains or (cmd.full is not None):
            for mid in cmd.ids:
                rt = self.motors.get(mid); 
                if not rt: continue
                rt.ctrl.apply_gains(cmd.gains.get("K"), cmd.gains.get("B"), cmd.full)
        # setpoints
        if cmd.setp:
            for mid in cmd.ids:
                rt = self.motors.get(mid); 
                if not rt: continue
                rt.ctrl.command(**cmd.setp)
        # cmd log
        for mid in cmd.ids:
            rt = self.motors.get(mid)
            if rt and rt.cmdlog: rt.cmdlog.log(raw_line)

    def _print_status(self, t_now: float):
        if (t_now - self._status_last) < 0.1:
            return
        status = []
        for mid, rt in self.motors.items():
            st = rt.ctrl.state()
            rec = "R" if rt.rec_on and rt.csv else "-"
            status.append(
                f"[{rec}] id{mid}({rt.mtype}): "
                f"pos={_fmtf(st['pos'])} vel={_fmtf(st['vel'])} tor={_fmtf(st['torque'],'{:+.2f}')} "
                f"T={_fmtf(st['temp'],'{:.1f}')} err={st['err']}"
            )
        print("\r" + " | ".join(status) + "   ", end="", flush=True)
        self._status_last = t_now

    def run(self):
        self.start()
        t0 = time.perf_counter()
        try:
            while not self.stop_evt.is_set():
                _ = self.scheduler.tick()
                # 제어 업데이트 + CSV 기록
                for mid, rt in self.motors.items():
                    rt.ctrl.tick()
                    if rt.rec_on and rt.csv:
                        st = rt.ctrl.state()
                        rt.csv.write([
                            f"{time.perf_counter()-t0:.6f}", self.bus, mid, rt.mtype,
                            st["pos"], st["vel"], st["acc"], st["torque"], st["temp"], st["err"]
                        ])
                # 커맨드 처리(가능한 만큼 비우기)
                try:
                    while True:
                        line = self.q.get_nowait()
                        cmd = self.parser.parse(line, id_pool=list(self.motors.keys()))
                        if cmd is None: 
                            continue
                        if cmd.action == "quit":
                            self.stop_evt.set(); break
                        if cmd.action == "help":
                            # 서버에서는 help 출력 안 함(클라에서 표시)
                            continue
                        if cmd.action in ("start","end","stop","zero"):
                            self._handle_action(cmd)
                        else:
                            self._handle_non_action(cmd, raw_line=line)
                except queue.Empty:
                    pass
                # 상태 출력
                self._print_status(time.perf_counter())
        finally:
            self.shutdown()

# ---------- spawn/setup/cli ----------
def find_terminal():
    import shutil
    for term in ("lxterminal","xterm","gnome-terminal","xfce4-terminal","mate-terminal"):
        if shutil.which(term):
            return term
    return None

def wrap_in_shell(cmd: str) -> str:
    return f"bash -lc {shlex.quote(cmd)}"

def run_client(bus: str):
    fifo = fifo_path_for_bus(bus)
    FifoCommandClient(fifo).run(bus)

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
    try:
        server = MotorBusServer(args.bus, id2type, args.hz)
        server.run()
    except KeyboardInterrupt:
        print("\n[server] KeyboardInterrupt")
    except Exception as e:
        print("[server] fatal:", e)
        sys.exit(1)

if __name__ == "__main__":
    main()
