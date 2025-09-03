#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
multi_motor_console_setup.py (events-in-CSV)
- CSV에 명령 이벤트 컬럼 추가: cmd_text, cmd_kind, cmd_recv_epoch, cmd_latency_ms
- 각 ID CSV에는 해당 ID에 적용된 명령만 표시
- 이벤트는 명령 적용 직후 "다음 샘플 행"에 기록 (END는 즉시 한 줄 기록)
- 나머지 구조: 클래스화 / 토크-only / FIFO non-blocking / lazy SRL
"""

import os, sys, csv, shlex, time, stat, argparse, threading, queue, select
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Dict, List, Tuple

# ---------- 경로/로그 ----------
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

def fifo_path_for_bus(bus: str) -> str:
    return str(BASE_DIR / f"cmd_{bus}.fifo")

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

# ---------- python-can 채널 고정 ----------
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

명령 형식:
  id=<번호|all>  K=<float>  B=<float>  pos=<rad>  vel=<rad/s>  tor=<Nm>  full=<0|1>
제어/유틸:
  start | end | stop | zero | help | quit

CSV 추가 컬럼:
  cmd_text, cmd_kind(cmd|start|end|stop|zero), cmd_recv_epoch(sec), cmd_latency_ms
"""
    )

# ---------- 스케줄러 (lazy SRL) ----------
class RealtimeScheduler:
    def __init__(self, hz: int):
        self.dt = 1.0 / float(hz)
        self._use_srl = HAVE_SRL
        self._srl = None
        self._iter = None
        self._t0 = None
        self._n = 0

    def tick(self):
        if self._use_srl and self._iter is None:
            try:
                self._srl = SoftRealtimeLoop(dt=self.dt, report=True, fade=0.0)
                self._iter = iter(self._srl)
            except Exception:
                self._use_srl = False

        if self._use_srl:
            return next(self._iter)

        if self._t0 is None:
            self._t0 = time.perf_counter()
            self._n = 0

        self._n += 1
        tgt = self._t0 + self._n * self.dt
        now = time.perf_counter()
        delay = max(0.0, tgt - now)
        if delay > 0:
            time.sleep(delay)
        return tgt - self._t0

# ---------- 디바이스 컨트롤러 ----------
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
        self.apply_gains()

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
    mixed: bool = False

class CommandParser:
    def parse(self, line: str, id_pool: List[int]) -> Optional[Command]:
        s = line.strip()
        if not s: return None
        lo = s.lower()
        if lo in ("quit","exit"):  return Command(action="quit")
        if lo in ("help","h","?"): return Command(action="help")
        try: tokens = shlex.split(s)
        except ValueError: tokens = s.split()
        cmd = Command()
        for tok in tokens:
            tl = tok.lower()
            if tl in ("start",): cmd.action="start"; continue
            if tl in ("end",):   cmd.action="end";   continue
            if tl in ("stop",):  cmd.action="stop";  continue
            if tl in ("zero","origin"): cmd.action="zero"; continue
            if "=" not in tok: continue
            k,v = tok.split("=",1); k=k.strip().lower(); v=v.strip()
            if k=="id":
                if v.lower()=="all": cmd.ids = sorted(id_pool)
                else:
                    try: cmd.ids = [int(v)]
                    except: pass
            elif k in ("k","kp"):   cmd.gains["K"] = float(v)
            elif k in ("b","kd"):   cmd.gains["B"] = float(v)
            elif k in ("pos","position"): cmd.setp["pos"] = float(v)
            elif k in ("vel","velocity"): cmd.setp["vel"] = float(v)
            elif k in ("tor","torque"):   cmd.setp["torque"] = float(v)
            elif k=="full": cmd.full = bool(int(v))
        if not cmd.ids:
            print("[server] 반드시 id=.. 또는 id=all 을 지정하세요. (help로 도움말)")
            return None
        if cmd.action in ("start","end","stop","zero") and (cmd.gains or cmd.setp or cmd.full is not None):
            cmd.mixed = True
        return cmd

# ---------- 이벤트/기록 ----------
class CsvRecorder:
    """CSV: time,bus,id,type,pos,vel,acc,torque,temp,err,cmd_text,cmd_kind,cmd_recv_epoch,cmd_latency_ms"""
    HEADER = ["time","bus","id","type","pos","vel","acc","torque","temp","err",
              "cmd_text","cmd_kind","cmd_recv_epoch","cmd_latency_ms"]
    def __init__(self, path: Path):
        self.path = path
        self._fh = None
        self._w = None
    def open(self):
        exists = self.path.exists() and self.path.stat().st_size > 0
        self._fh = open(self.path, "a", newline="")
        self._w = csv.writer(self._fh)
        if not exists:
            self._w.writerow(self.HEADER)
            # 세션 기준시각 주석(절대시각 복원용)
            self._fh.write(f"# session_epoch={time.time():.6f}\n")
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
        self._fh.write(f"# cmd log start {time.strftime('%Y-%m-%d %H:%M:%S')} epoch={time.time():.6f}\n")
    def log(self, s: str):
        t = time.time()
        try:
            self._fh.write(f"{t:.6f} {s.strip()}\n")
        except Exception:
            pass
    def close(self):
        try:
            self._fh.write(f"# cmd log end {time.strftime('%Y-%m-%d %H:%M:%S')} epoch={time.time():.6f}\n")
            self._fh.close()
        except Exception:
            pass

@dataclass
class PendingCmd:
    text: str
    kind: str
    recv_epoch: float
    recv_rel: float

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

@dataclass
class RawCmd:
    text: str
    recv_epoch: float
    recv_rel: float

class FifoCommandServer:
    """Non-blocking reader; enqueues RawCmd(text, recv_epoch, recv_rel)."""
    def __init__(self, path: str):
        self.path = path
        ensure_fifo(self.path)
    def reader_thread(self, q: "queue.Queue[RawCmd]", stop_evt: threading.Event, t0_perf: float):
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
                            try: fobj.close()
                            except Exception: pass
                            fobj = None
                            time.sleep(0.05)
                            continue
                        q.put(RawCmd(text=line.rstrip("\n"),
                                     recv_epoch=time.time(),
                                     recv_rel=time.perf_counter() - t0_perf))
                except FileNotFoundError:
                    ensure_fifo(self.path); time.sleep(0.1)
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
                        print(help_text(bus_hint=bus)); continue
                    fw.write(line+"\n"); fw.flush()
                    if line.lower() in ("quit","exit"):
                        print("bye"); break
        except Exception as e:
            print(f"[client:{bus}] error: {e}")
            input("Press Enter to close...")

# ---------- 매핑 파서 ----------
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
    pending: Optional[PendingCmd] = None

class MotorBusServer:
    def __init__(self, bus: str, id2type: Dict[int, str], hz: int):
        if not TMOTOR_OK:
            raise RuntimeError(f"TMotorCANControl import 실패: {TMOTOR_IMPORT_ERR}")
        self.bus = bus
        self.id2type = id2type
        self.hz = hz
        self.scheduler = RealtimeScheduler(hz=self.hz)
        self.stop_evt = threading.Event()
        self.q: "queue.Queue[RawCmd]" = queue.Queue()
        self.fifo = FifoCommandServer(fifo_path_for_bus(self.bus))
        self.parser = CommandParser()
        self.motors: Dict[int, _MotorRuntime] = {}
        self._status_last = 0.0
        self._t0 = None  # perf base

    def start(self):
        force_socketcan_channel(self.bus)
        failed = []
        for mid, mtype in self.id2type.items():
            try:
                m = TMotorManager_mit_can(motor_type=mtype, motor_ID=mid, max_mosfett_temp=90)
                m.__enter__()
                ctrl = MotorController(m, mtype, K_init=5.0, B_init=0.1, full_state=True)
                ctrl.command(vel=0.0, torque=0.0)
                rt = _MotorRuntime(mgr=m, ctrl=ctrl, rec_on=False, csv=None,
                                   cmdlog=CmdLogger(cmdlog_path_for_motor(self.bus, mid)),
                                   mtype=mtype)
                self.motors[mid] = rt
            except Exception as e:
                msg = str(e); failed.append((mid, mtype, msg))
                print(f"[server:{self.bus}] init failed for id{mid}({mtype}): {msg}")
                if "Device not connected" in msg:
                    print("  → 전원/공통 GND/CAN-H/L/종단저항/ID/bitrate(1Mbps) 확인 및 라즈베리파이 저전압 경고 해결 필요")
        if not self.motors:
            reason = failed[0][2] if failed else "unknown"
            raise RuntimeError(f"No motors started on {self.bus}. First error: {reason}")
        if failed:
            human = ", ".join([f"id{mid}({t})" for mid, t, _ in failed])
            print(f"[server:{self.bus}] WARNING: some motors failed to init: {human}")
        print(f"[server:{self.bus}] ready. ids={list(self.motors.keys())}  (`help`는 클라이언트에서, `quit` 종료)")

    # 이벤트 스케줄: 다음 샘플 행에 기록
    def _schedule_event(self, mid: int, kind: str, text: str, recv_epoch: float, recv_rel: float):
        rt = self.motors.get(mid)
        if not rt or not rt.rec_on or not rt.csv:
            return  # 녹화 중이 아닐 때는 CSV에 남기지 않음
        if rt.pending is None:
            rt.pending = PendingCmd(text=text, kind=kind, recv_epoch=recv_epoch, recv_rel=recv_rel)
        else:
            # 텍스트 합치고, kind가 다르면 'cmd'로 포괄
            rt.pending.text = f"{rt.pending.text} ; {text}"
            if rt.pending.kind != kind:
                rt.pending.kind = "cmd"
            # 수신시각은 가장 이른 것으로 유지
            rt.pending.recv_epoch = min(rt.pending.recv_epoch, recv_epoch)
            rt.pending.recv_rel   = min(rt.pending.recv_rel, recv_rel)

    def _handle_action(self, cmd: Command, raw: RawCmd):
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
                # 다음 샘플에 'start' 표시
                self._schedule_event(mid, "start", "start", raw.recv_epoch, raw.recv_rel)
            print(f"\n[server:{self.bus}] start recording ids={ids}")
            return

        if cmd.action == "end":
            # 종료 직전에 'end' 이벤트를 즉시 1줄 기록
            now_rel = time.perf_counter() - self._t0
            for mid in ids:
                rt = self.motors.get(mid)
                if not rt or not rt.csv: continue
                st = rt.ctrl.state()
                latency_ms = (now_rel - raw.recv_rel) * 1000.0
                rt.csv.write([
                    f"{now_rel:.6f}", self.bus, mid, rt.mtype,
                    st["pos"], st["vel"], st["acc"], st["torque"], st["temp"], st["err"],
                    "end", "end", f"{raw.recv_epoch:.6f}", f"{latency_ms:.3f}"
                ])
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
                    rt.ctrl.command(vel=0.0, torque=0.0)
                    rt.ctrl.zero()
                    if rt.cmdlog: rt.cmdlog.log("## STOP")
                    self._schedule_event(mid, "stop", "stop", raw.recv_epoch, raw.recv_rel)
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
                self._schedule_event(mid, "zero", "zero", raw.recv_epoch, raw.recv_rel)
            return

    def _handle_non_action(self, cmd: Command, raw: RawCmd):
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

        # 명령 이벤트 예약 (원문 전체를 기록)
        for mid in cmd.ids:
            self._schedule_event(mid, "cmd", raw.text, raw.recv_epoch, raw.recv_rel)

        # cmd log
        for mid in cmd.ids:
            rt = self.motors.get(mid)
            if rt and rt.cmdlog: rt.cmdlog.log(raw.text)

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
        self._t0 = time.perf_counter()
        # FIFO reader thread (수신시각에 self._t0 사용)
        th = threading.Thread(target=self.fifo.reader_thread, args=(self.q, self.stop_evt, self._t0), daemon=True)
        th.start()
        try:
            while not self.stop_evt.is_set():
                _ = self.scheduler.tick()
                # 제어 업데이트 + CSV 기록
                now_rel = time.perf_counter() - self._t0
                for mid, rt in self.motors.items():
                    rt.ctrl.tick()
                    if rt.rec_on and rt.csv:
                        st = rt.ctrl.state()
                        # 이벤트가 있으면 채우고 비움
                        if rt.pending:
                            latency_ms = (now_rel - rt.pending.recv_rel) * 1000.0
                            row = [
                                f"{now_rel:.6f}", self.bus, mid, rt.mtype,
                                st["pos"], st["vel"], st["acc"], st["torque"], st["temp"], st["err"],
                                rt.pending.text, rt.pending.kind, f"{rt.pending.recv_epoch:.6f}", f"{latency_ms:.3f}"
                            ]
                            rt.pending = None
                        else:
                            row = [
                                f"{now_rel:.6f}", self.bus, mid, rt.mtype,
                                st["pos"], st["vel"], st["acc"], st["torque"], st["temp"], st["err"],
                                "", "", "", ""
                            ]
                        rt.csv.write(row)

                # 커맨드 처리
                try:
                    while True:
                        raw = self.q.get_nowait()
                        cmd = self.parser.parse(raw.text, id_pool=list(self.motors.keys()))
                        if cmd is None: 
                            continue
                        if cmd.action == "quit":
                            self.stop_evt.set(); break
                        if cmd.action == "help":
                            continue
                        if cmd.action in ("start","end","stop","zero"):
                            self._handle_action(cmd, raw)
                        else:
                            self._handle_non_action(cmd, raw)
                except queue.Empty:
                    pass

                self._print_status(time.perf_counter())
        finally:
            self.shutdown()

    def shutdown(self):
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
    print("버스/ID/타입 입력 → 주파수 설정 → spawn 자동 실행")
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
        print("설정이 비어 있습니다. 종료."); return

    print("\n## 설정 요약")
    for bus, mp in mapping.items():
        print(f"  {bus}: " + ", ".join(f"{i}:{t}" for i,t in mp.items()))
    try:
        hz = int(input(f"제어 주기 Hz 입력 (기본 {default_hz}) > ").strip() or str(default_hz))
    except Exception:
        hz = default_hz

    print("\n[안내] 버스별 서버/클라이언트 창을 자동으로 띄웁니다.")
    yn = input("이 설정으로 spawn 할까요? [Y/n] ").strip().lower()
    if yn in ("", "y", "yes"):
        run_spawn(mapping, hz)
    else:
        print("취소됨.")

# ---------- main ----------
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=("server","client","spawn","setup"), default="setup")
    p.add_argument("--hz", type=int, default=200)
    p.add_argument("--bus", default="can0")
    p.add_argument("--idmap", default="", help='server: "1:AK70-10,2:AK80-64"')
    p.add_argument("--map", default="",  help='spawn: "can0:1:AK70-10,2:AK80-64;can1:3:AK80-64"')
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
