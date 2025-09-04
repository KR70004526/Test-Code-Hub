#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test5.py  (Ready->Go handshake + Scenario runner)
- 서버가 루프 안정화 후 ready_event를 set → 러너는 이를 기다린 뒤 자동 오프셋을 두고 실행
- cmd_* 이벤트 컬럼을 CSV에 기록
- 세션 폴더 logs/<YYYYmmdd-HHMMSS_xxxxxx>/ 에 모든 산출물 저장
"""

import os, sys, csv, shlex, time, stat, argparse, threading, queue, select, math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Any
from datetime import datetime

# ---------------- Paths & Session ----------------
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

SESSION_TAG: Optional[str] = None
SESSION_DIR: Path = LOG_DIR

def set_session_tag(tag: Optional[str] = None):
    """세션 태그/폴더 생성 (최초 1회)"""
    global SESSION_TAG, SESSION_DIR
    if SESSION_TAG is not None:
        return
    if tag is None:
        tag = datetime.now().strftime("%Y%m%d-%H%M%S_%f")
    SESSION_TAG = tag
    SESSION_DIR = LOG_DIR / SESSION_TAG
    SESSION_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[session] tag={SESSION_TAG} dir={SESSION_DIR}")

def fifo_path_for_bus(bus: str) -> str:
    return str(BASE_DIR / f"cmd_{bus}.fifo")

def csv_path_for_motor(bus: str, mid: int) -> Path:
    return SESSION_DIR / f"session_{bus}_id{mid}.csv"

def cmdlog_path_for_motor(bus: str, mid: int) -> Path:
    return SESSION_DIR / f"cmd_{bus}_id{mid}.log"

# ---------------- Optional deps ----------------
try:
    from NeuroLocoMiddleware.SoftRealtimeLoop import SoftRealtimeLoop
    HAVE_SRL = True
except Exception:
    HAVE_SRL = False

TMOTOR_OK = True
TMOTOR_IMPORT_ERR = ""
try:
    from TMotorCANControl import TMotorManager_mit_can
    from TMotorCANControl.mit_can import MIT_Params
except Exception as e:
    TMOTOR_OK = False
    TMOTOR_IMPORT_ERR = str(e)

# ---------------- Utilities ----------------
def _fmtf(x, fmt="{:+.3f}"):
    try: return fmt.format(float(x))
    except Exception: return str(x)

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

# ---------------- Realtime Scheduler ----------------
class RealtimeScheduler:
    def __init__(self, hz: int):
        self.dt = 1.0 / float(hz)
        self._use_srl = HAVE_SRL
        self._iter = None
        self._t0 = None
        self._n = 0

    def tick(self) -> float:
        # SoftRealtimeLoop가 가능하면 사용
        if self._use_srl and self._iter is None:
            try:
                self._iter = iter(SoftRealtimeLoop(dt=self.dt, report=False, fade=0.0))
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

# ---------------- Motor Controller ----------------
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
                print(f"[warn] MIT_Params missing for '{self.type}'. Gains not clamped.")
                self._warned_missing_type = True
            return K, B
        Kp_min, Kp_max = p.get("Kp_min"), p.get("Kp_max")
        Kd_min, Kd_max = p.get("Kd_min"), p.get("Kd_max")
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
            if pos is not None:
                self.m.set_output_angle_radians(float(pos))
            if vel is not None:
                self.m.set_output_velocity_radians_per_second(float(vel))
            if torque is not None:
                self.m.set_output_torque_newton_meters(float(torque))

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

# ---------------- Command Parser ----------------
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
            if tl=="start": cmd.action="start"; continue
            if tl=="end":   cmd.action="end";   continue
            if tl=="stop":  cmd.action="stop";  continue
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

# ---------------- Logging ----------------
class CsvRecorder:
    HEADER = ["time","bus","id","type","pos","vel","acc","torque","temp","err",
              "cmd_text","cmd_kind","cmd_recv_epoch","cmd_latency_ms"]
    def __init__(self, path: Path):
        self.path = path
        self._fh = None
        self._w = None
    def open(self):
        self._fh = open(self.path, "w", newline="")
        self._w = csv.writer(self._fh)
        self._fh.write(f"# session_epoch={time.time():.6f}\n")
        self._w.writerow(self.HEADER)
    def write(self, row: List[Any]):
        if self._w: self._w.writerow(row)
    def close(self):
        try:
            if self._fh: self._fh.flush(); self._fh.close()
        finally:
            self._fh = self._w = None

class CmdLogger:
    def __init__(self, path: Path):
        self._fh = open(path, "w", buffering=1)
        self._fh.write(f"# cmd log start {time.strftime('%Y-%m-%d %H:%M:%S')} epoch={time.time():.6f}\n")
    def log(self, s: str):
        t = time.time()
        try: self._fh.write(f"{t:.6f} {s.strip()}\n")
        except Exception: pass
    def close(self):
        try:
            self._fh.write(f"# cmd log end {time.strftime('%Y-%m-%d %H:%M:%S')} epoch={time.time():.6f}\n")
            self._fh.close()
        except Exception: pass

@dataclass
class PendingCmd:
    text: str
    kind: str
    recv_epoch: float
    recv_rel: float

# ---------------- FIFO ----------------
@dataclass
class RawCmd:
    text: str
    recv_epoch: float
    recv_rel: float

class FifoCommandServer:
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
            if waited==0.0: print(f"[client:{bus}] waiting for FIFO({self.path}) ... (start server)")
            time.sleep(0.2); waited+=0.2
            if waited>=10.0:
                print(f"[client:{bus}] still waiting... creating FIFO locally.")
                try: ensure_fifo(self.path)
                except Exception as e: print(f"[client:{bus}] ensure_fifo error: {e}"); waited=0.0
        try:
            print(f"[client:{bus}] opening FIFO... (may wait until server opens reader)")
            print(f"  * 세션 폴더: {SESSION_DIR}")
            with open(self.path,"w") as fw:
                while True:
                    try: line = input(f"CMD[{bus}]> ").strip()
                    except (EOFError,KeyboardInterrupt): line="quit"
                    if not line: continue
                    if line.lower() in ("help","h","?"):
                        print("id=.. K=.. B=.. pos=.. vel=.. tor=.. full=0|1 | start | end | stop | zero | quit | help")
                        continue
                    fw.write(line+"\n"); fw.flush()
                    if line.lower() in ("quit","exit"):
                        print("bye"); break
        except Exception as e:
            print(f"[client:{bus}] error: {e}")
            input("Press Enter to close...")

# ---------------- MotorBusServer ----------------
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
    def __init__(self, bus: str, id2type: Dict[int, str], hz: int, warmup_ticks: int = 20):
        if not TMOTOR_OK:
            raise RuntimeError(f"TMotorCANControl import 실패: {TMOTOR_IMPORT_ERR}")
        self.bus = bus
        self.id2type = id2type
        self.scheduler = RealtimeScheduler(hz=hz)
        self.warmup_ticks = int(warmup_ticks)
        self.stop_evt = threading.Event()
        self.q: "queue.Queue[RawCmd]" = queue.Queue()
        self.fifo = FifoCommandServer(fifo_path_for_bus(self.bus))
        self.parser = CommandParser()
        self.motors: Dict[int, _MotorRuntime] = {}
        self._status_last = 0.0
        self._t0 = None               # perf base
        # Ready->Go handshake
        self.ready_event = threading.Event()
        self.ready_at_perf: Optional[float] = None

    def start(self):
        force_socketcan_channel(self.bus)
        # init motors
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
                failed.append((mid, mtype, str(e)))
                print(f"[server:{self.bus}] init failed for id{mid}({mtype}): {e}")
        if not self.motors:
            reason = failed[0][2] if failed else "unknown"
            raise RuntimeError(f"No motors started on {self.bus}. First error: {reason}")
        if failed:
            human = ", ".join([f"id{mid}({t})" for mid, t, _ in failed])
            print(f"[server:{self.bus}] WARNING: some motors failed: {human}")
        print(f"[server:{self.bus}] ready to run. ids={list(self.motors.keys())}")

    def _schedule_event(self, mid: int, kind: str, text: str, recv_epoch: float, recv_rel: float):
        rt = self.motors.get(mid)
        if not rt or not rt.rec_on or not rt.csv: return
        if rt.pending is None:
            rt.pending = PendingCmd(text=text, kind=kind, recv_epoch=recv_epoch, recv_rel=recv_rel)
        else:
            rt.pending.text = f"{rt.pending.text} ; {text}"
            if rt.pending.kind != kind:
                rt.pending.kind = "cmd"
            rt.pending.recv_epoch = min(rt.pending.recv_epoch, recv_epoch)
            rt.pending.recv_rel   = min(rt.pending.recv_rel,  recv_rel)

    def _handle_action(self, cmd: Command, raw: 'RawCmd'):
        if cmd.mixed:
            print("[server] 경고: action과 파라미터를 한 줄에 섞지 마세요. (action만 수행)")
        ids = cmd.ids
        if cmd.action == "start":
            for mid in ids:
                rt = self.motors.get(mid)
                if not rt: continue
                if not rt.csv:
                    rec = CsvRecorder(csv_path_for_motor(self.bus, mid)); rec.open()
                    rt.csv = rec; rt.rec_on = True
                if rt.cmdlog: rt.cmdlog.log("## START")
                self._schedule_event(mid, "start", "start", raw.recv_epoch, raw.recv_rel)
            print(f"\n[server:{self.bus}] start recording ids={ids}")
            return

        if cmd.action == "end":
            now_rel = time.perf_counter() - self._t0
            for mid in ids:
                rt = self.motors.get(mid)
                if not rt or not rt.csv: continue
                st = rt.ctrl.state()
                latency_ms = (now_rel - raw.recv_rel) * 1000.0
                rt.csv.write([f"{now_rel:.6f}", self.bus, mid, rt.mtype,
                              st["pos"], st["vel"], st["acc"], st["torque"], st["temp"], st["err"],
                              "end", "end", f"{raw.recv_epoch:.6f}", f"{latency_ms:.3f}"])
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

    def _handle_non_action(self, cmd: Command, raw: 'RawCmd'):
        if cmd.gains or (cmd.full is not None):
            for mid in cmd.ids:
                rt = self.motors.get(mid); 
                if not rt: continue
                rt.ctrl.apply_gains(cmd.gains.get("K"), cmd.gains.get("B"), cmd.full)
        if cmd.setp:
            for mid in cmd.ids:
                rt = self.motors.get(mid); 
                if not rt: continue
                rt.ctrl.command(**cmd.setp)
        for mid in cmd.ids:
            self._schedule_event(mid, "cmd", raw.text, raw.recv_epoch, raw.recv_rel)
        for mid in cmd.ids:
            rt = self.motors.get(mid)
            if rt and rt.cmdlog: rt.cmdlog.log(raw.text)

    def _print_status(self, t_now: float):
        if (t_now - self._status_last) < 0.1: return
        status=[]
        for mid, rt in self.motors.items():
            st = rt.ctrl.state()
            rec = "R" if rt.rec_on and rt.csv else "-"
            status.append(f"[{rec}] id{mid}({rt.mtype}): pos={_fmtf(st['pos'])} vel={_fmtf(st['vel'])} "
                          f"tor={_fmtf(st['torque'],'{:+.2f}')} T={_fmtf(st['temp'],'{:.1f}')} err={st['err']}")
        print("\r" + " | ".join(status) + "   ", end="", flush=True)
        self._status_last = t_now

    def run(self):
        self.start()
        self._t0 = time.perf_counter()
        # FIFO reader thread
        th = threading.Thread(target=self.fifo.reader_thread, args=(self.q, self.stop_evt, self._t0), daemon=True)
        th.start()

        ticks = 0
        try:
            while not self.stop_evt.is_set():
                _ = self.scheduler.tick()
                now_rel = time.perf_counter() - self._t0

                # Warm-up (READY) -------------------------------------------------
                if not self.ready_event.is_set():
                    ticks += 1
                    if ticks >= self.warmup_ticks:
                        self.ready_at_perf = time.perf_counter()
                        self.ready_event.set()
                        print(f"\n[server:{self.bus}] READY after {ticks} ticks "
                              f"(~{ticks*self.scheduler.dt:.3f}s).")

                # Tick + CSV write -----------------------------------------------
                for mid, rt in self.motors.items():
                    rt.ctrl.tick()
                    if rt.rec_on and rt.csv:
                        st = rt.ctrl.state()
                        if rt.pending:
                            latency_ms = (now_rel - rt.pending.recv_rel) * 1000.0
                            row = [f"{now_rel:.6f}", self.bus, mid, rt.mtype,
                                   st["pos"], st["vel"], st["acc"], st["torque"], st["temp"], st["err"],
                                   rt.pending.text, rt.pending.kind,
                                   f"{rt.pending.recv_epoch:.6f}", f"{latency_ms:.3f}"]
                            rt.pending = None
                        else:
                            row = [f"{now_rel:.6f}", self.bus, mid, rt.mtype,
                                   st["pos"], st["vel"], st["acc"], st["torque"], st["temp"], st["err"],
                                   "", "", "", ""]
                        rt.csv.write(row)

                # Command queue ---------------------------------------------------
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
            except Exception: pass
        for mid, rt in self.motors.items():
            try: rt.mgr.__exit__(None,None,None)
            except Exception: pass
        print(f"\n[server:{self.bus}] exit.")

# ---------------- Scenario Loader & Runner ----------------
# YAML/JSON 로더
def _load_yaml_or_json(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() in (".yaml",".yml"):
        try:
            import yaml
        except Exception:
            raise RuntimeError("PyYAML이 필요합니다: pip install pyyaml")
        return yaml.safe_load(text)
    import json
    return json.loads(text)

@dataclass
class ScheduledAction:
    t_abs: float
    action: str
    ids: List[int]
    params: Dict[str, Any]
    note: str = ""

class ScenarioLoader:
    def __init__(self, path: str, id_pool: List[int]):
        # 경로 보정: 절대가 아니면 BASE_DIR 기준
        p = Path(path)
        if not p.is_absolute():
            p2 = (BASE_DIR / path).resolve()
            p = p2 if p2.exists() else p
        if not p.exists():
            raise FileNotFoundError(f"scenario not found: {p}")
        self.raw = _load_yaml_or_json(p)
        self.id_pool = id_pool
        self.steps: List[ScheduledAction] = []
        self.sources: List[dict] = []

    def _expand_targets(self, spec_targets: Any, groups: Dict[str, List[int]]) -> List[int]:
        if spec_targets is None:
            return []
        if isinstance(spec_targets, (list,tuple)):
            ids = []
            for x in spec_targets:
                if isinstance(x, int):
                    ids.append(x)
                elif isinstance(x, str) and x in groups:
                    ids += groups[x]
            return sorted([i for i in ids if i in self.id_pool])
        if isinstance(spec_targets, str) and spec_targets in groups:
            return [i for i in groups[spec_targets] if i in self.id_pool]
        if isinstance(spec_targets, int):
            return [spec_targets] if spec_targets in self.id_pool else []
        return []

    def normalize(self) -> List[ScheduledAction]:
        name = self.raw.get("name","scenario")
        defaults = self.raw.get("defaults", {})
        def_gains = defaults.get("gains", {})
        groups = self.raw.get("groups", {})
        # groups의 값이 문자열 키면 숫자 리스트로 강제
        groups2 = {}
        for k,v in groups.items():
            if isinstance(v, (list,tuple)):
                groups2[k] = [int(x) for x in v if isinstance(x,int) or str(x).isdigit()]
        steps_raw = self.raw.get("steps", [])
        t_cursor = 0.0
        out: List[ScheduledAction] = []
        for i, st in enumerate(steps_raw):
            # 블록/플로우 모두 지원
            at   = st.get("at", None)
            after= st.get("after", None)
            if at is not None and after is not None:
                raise ValueError(f"step#{i}: 'at'와 'after'를 동시에 쓸 수 없음")
            if at is not None:
                t_abs = float(at)
                t_cursor = t_abs
            else:
                dt = float(after or 0.0)
                t_cursor += dt
                t_abs = t_cursor
            action = st.get("action")
            targets = self._expand_targets(st.get("targets"), groups2)
            params  = dict(st.get("params", {}))
            if action == "apply_gains":
                # defaults 병합
                for k in ("K","B","full_state"):
                    if k not in params and k in def_gains:
                        params[k] = def_gains[k]
            out.append(ScheduledAction(t_abs=t_abs, action=action, ids=targets, params=params))
        self.steps = out
        self.sources = self.raw.get("sources", [])
        print(f"[scenario] loaded '{name}', steps={len(out)}, sources={len(self.sources)}")
        return out

class ScenarioRunner(threading.Thread):
    """
    - 서버 ready_event.wait() 후 실행
    - 첫 스텝은 start_offset_s 만큼 밀어줌
    - 히스테리시스(= dt/2) 적용: now + dt/2 < t_abs 일 때만 대기
    """
    def __init__(self, server: MotorBusServer, loader: ScenarioLoader,
                 start_offset_s: float = 0.20, debug: bool=False):
        super().__init__(daemon=True)
        self.server = server
        self.loader = loader
        self.start_offset_s = float(start_offset_s)
        self.debug = debug
        self._compiled: List[Tuple[float, List[str]]] = []  # (t_abs, [cmdline...])

    def _emit_for_ids(self, t: float, ids: List[int], text: str):
        self._compiled.append((t, [f"id={i} {text}" for i in ids]))

    def compile(self):
        self._compiled.clear()
        steps = self.loader.normalize()
        # steps -> cmd text
        for s in steps:
            if s.action == "start_record":
                self._emit_for_ids(s.t_abs, s.ids, "start")
            elif s.action == "end_record":
                self._emit_for_ids(s.t_abs, s.ids, "end")
            elif s.action == "apply_gains":
                K = s.params.get("K"); B = s.params.get("B")
                full = s.params.get("full_state", True)
                parts = []
                if K is not None: parts.append(f"K={float(K)}")
                if B is not None: parts.append(f"B={float(B)}")
                parts.append(f"full={1 if bool(full) else 0}")
                self._emit_for_ids(s.t_abs, s.ids, " ".join(parts))
            elif s.action in ("move","vel","torque"):
                parts=[]
                if s.action=="move" and "pos" in s.params:
                    parts.append(f"pos={float(s.params['pos'])}")
                if s.action=="vel" and "vel" in s.params:
                    parts.append(f"vel={float(s.params['vel'])}")
                if s.action=="torque" and "torque" in s.params:
                    parts.append(f"tor={float(s.params['torque'])}")
                if parts:
                    self._emit_for_ids(s.t_abs, s.ids, " ".join(parts))
            elif s.action == "safe_zero":
                dwell = float(s.params.get("dwell_s", 0.05))
                # 1) K/B=0 + vel/tor=0
                self._emit_for_ids(s.t_abs, s.ids, "K=0 B=0")
                self._emit_for_ids(s.t_abs, s.ids, "vel=0.0 tor=0.0")
                # 2) zero
                self._emit_for_ids(s.t_abs + dwell, s.ids, "zero")
            elif s.action == "stop":
                self._emit_for_ids(s.t_abs, s.ids, "stop")
            elif s.action == "hold":
                pass  # 시간만 전진
            else:
                print(f"[scenario] WARN: unsupported action '{s.action}' (ignored)")

        # 정렬 & 병합(같은 시각 끼리는 그대로 순서 유지)
        self._compiled.sort(key=lambda x: x[0])

    def run(self):
        # 서버 준비 대기
        self.server.ready_event.wait()
        ready_perf = self.server.ready_at_perf or time.perf_counter()
        # 시작 오프셋
        offset = self.start_offset_s
        dt = self.server.scheduler.dt
        hysteresis = 0.5 * dt

        # 시점 보정
        timeline = [(t + offset, cmds) for (t,cmds) in self._compiled]

        if self.debug:
            print("\n[scenario] --- COMPILED SCHEDULE ---")
            for t,cmds in timeline:
                for c in cmds:
                    print(f"  t={t:.3f}s : {c}")
            print("[scenario] --------------------------")

        # 재생
        while timeline and not self.server.stop_evt.is_set():
            now = time.perf_counter() - (self.server._t0 or ready_perf)
            t_abs, cmds = timeline[0]
            if now + hysteresis < t_abs:
                time.sleep(min(0.01, max(0.0, t_abs - now - hysteresis)))
                continue
            # enqueue
            recv_epoch = time.time()
            recv_rel   = time.perf_counter() - (self.server._t0 or ready_perf)
            for text in cmds:
                self.server.q.put(RawCmd(text=text, recv_epoch=recv_epoch, recv_rel=recv_rel))
                if self.debug:
                    print(f"[scenario] enqueue @ {now:.3f}s (target {t_abs:.3f}s): {text}")
            timeline.pop(0)

# ---------------- Parsing helpers ----------------
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

# ---------------- CLI ----------------
def run_server(bus: str, id2type: Dict[int, str], hz: int,
               scenario_path: Optional[str], scenario_debug: bool,
               start_offset_s: float):
    set_session_tag(None)
    server = MotorBusServer(bus, id2type, hz)
    # 시나리오 지정 시: 로더/러너 준비(스레드)
    runner = None
    if scenario_path:
        loader = ScenarioLoader(scenario_path, id_pool=list(id2type.keys()))
        runner = ScenarioRunner(server, loader, start_offset_s=start_offset_s, debug=scenario_debug)
        runner.compile()
        runner.start()  # 내부에서 ready_event.wait()

    try:
        server.run()
    finally:
        if runner and runner.is_alive():
            server.stop_evt.set()
            runner.join(timeout=1.0)

def run_client(bus: str):
    set_session_tag(None)
    FifoCommandClient(fifo_path_for_bus(bus)).run(bus)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=("server","client"), default="server")
    p.add_argument("--bus", default="can0")
    p.add_argument("--idmap", default="", help='예: "1:AK80-64,2:AK70-10"')
    p.add_argument("--hz", type=int, default=200)
    p.add_argument("--scenario", default=None, help="시나리오 파일(.yaml/.yml/.json)")
    p.add_argument("--scenario-debug", action="store_true")
    p.add_argument("--start-offset", type=float, default=0.20, help="시나리오 자동 시작 오프셋(초)")
    args = p.parse_args()

    if args.mode == "client":
        run_client(args.bus); return

    id2type = parse_idmap_onebus(args.idmap, default_type="AK70-10")
    if not id2type:
        print('server 모드: --idmap "1:AK70-10,2:AK80-64" 필요'); sys.exit(2)

    run_server(args.bus, id2type, args.hz, args.scenario, args.scenario_debug, args.start_offset)

if __name__ == "__main__":
    main()
