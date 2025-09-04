#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
multi_motor_console_setup.py (session-folder + events-in-CSV + Scenario Loader/Runner)
- 세션 폴더 방식 유지: logs/<SESSION_TAG>/ 아래에 CSV/명령로그/서버·클라 로그 저장
- CSV 이벤트 컬럼: cmd_text, cmd_kind, cmd_recv_epoch, cmd_latency_ms
- FIFO non-blocking, lazy SoftRealtimeLoop
- 시나리오 실행 지원:
  * YAML/JSON 파일로 steps를 정의 (apply_gains/move/vel/torque/start_record/end_record/hold/safe_zero/stop 등)
  * YAML에서 CSV trajectory를 sources로 참조 → play_sources 스텝에서 재생
  * 서버 내부 큐(RawCmd)를 통해 텍스트 명령 주입 → 기존 파서/로깅 그대로 사용
"""

import os, sys, csv as csvmod, shlex, time, stat, argparse, threading, queue, select, json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Any, Union
from datetime import datetime

# ---------- 경로/세션 ----------
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

SESSION_TAG: Optional[str] = None
SESSION_DIR: Path = LOG_DIR  # main()에서 set_session_tag 호출 시 실제 세션 폴더로 교체

def set_session_tag(tag: Optional[str] = None):
    """세션 태그/폴더 설정 (main에서 처음 생성)"""
    global SESSION_TAG, SESSION_DIR
    if tag is None:
        tag = datetime.now().strftime("%Y%m%d-%H%M%S_%f")
    SESSION_TAG = tag
    SESSION_DIR = LOG_DIR / SESSION_TAG
    SESSION_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[session] tag={SESSION_TAG} dir={SESSION_DIR}")

def fifo_path_for_bus(bus: str) -> str:
    return str(BASE_DIR / f"cmd_{bus}.fifo")  # 고정 경로(FIFO는 세션과 무관)

def csv_path_for_motor(bus: str, mid: int) -> Path:
    return SESSION_DIR / f"session_{bus}_id{mid}.csv"

def cmdlog_path_for_motor(bus: str, mid: int) -> Path:
    return SESSION_DIR / f"cmd_{bus}_id{mid}.log"

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

세션 폴더:
  {SESSION_DIR}

시나리오 실행(서버 모드에서 CLI):
  --scenario path.yaml   # 또는 .json (PyYAML 없으면 JSON만)
  YAML 예시: steps에 apply_gains/move/vel/torque/start_record/end_record/hold/safe_zero/stop 등
  CSV 재생: YAML 'sources' 정의 후 steps에 'play_sources' 추가 (아래 주석 참고)
"""
    )

# ---------- 스케줄러 ----------
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

        # fallback
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
        self._fh = open(self.path, "w", newline="")
        self._w = csvmod.writer(self._fh)
        self._fh.write(f"# session_epoch={time.time():.6f}\n")
        self._w.writerow(self.HEADER)
    def write(self, row: List):
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
    """Non-blocking FIFO reader; enqueues RawCmd(text, recv_epoch, recv_rel)."""
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
            print(f"  * 세션 폴더: {SESSION_DIR}")
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
            return
        if rt.pending is None:
            rt.pending = PendingCmd(text=text, kind=kind, recv_epoch=recv_epoch, recv_rel=recv_rel)
        else:
            rt.pending.text = f"{rt.pending.text} ; {text}"
            if rt.pending.kind != kind:
                rt.pending.kind = "cmd"
            rt.pending.recv_epoch = min(rt.pending.recv_epoch, recv_epoch)
            rt.pending.recv_rel   = min(rt.pending.recv_rel, recv_rel)

    def _handle_action(self, cmd: Command, raw: 'RawCmd'):
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

    def _handle_non_action(self, cmd: Command, raw: 'RawCmd'):
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

        # 이벤트 예약 (원문)
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

# ---------- 시나리오: 데이터 구조 ----------
@dataclass
class ScheduledAction:
    t_abs: float
    action: str                       # 'apply_gains' | 'move' | 'vel' | 'torque' | 'hold' | 'start_record' | 'end_record' | 'safe_zero' | 'stop' | 'play_sources'
    ids: List[int]
    params: Dict[str, Any]
    raw_note: Optional[str] = None

@dataclass
class CsvSourceSpec:
    file: str
    id: int
    col_t: str = "time_s"             # CSV 헤더명: 시간 컬럼
    col_pos: Optional[str] = None     # CSV 헤더명: 위치
    col_vel: Optional[str] = None     # CSV 헤더명: 속도
    col_tor: Optional[str] = None     # CSV 헤더명: 토크
    time_unit: str = "s"              # "s" | "ms"
    downsample_hz: Optional[float] = None
    offset_s: float = 0.0

@dataclass
class ScenarioSpec:
    name: str
    rate_hz: int
    actions: List[ScheduledAction]
    sources: List[CsvSourceSpec] = field(default_factory=list)

# ---------- 시나리오: 로더 ----------
try:
    import yaml  # PyYAML
    HAVE_YAML = True
except Exception:
    HAVE_YAML = False

class ScenarioLoader:
    ALLOWED_ACTIONS = {
        "apply_gains", "move", "vel", "torque",
        "hold", "start_record", "end_record", "safe_zero", "stop",
        "play_sources"
    }

    def load(self, path: str, id_pool: List[int]) -> ScenarioSpec:
        doc = self._read_any(path)
        name = str(doc.get("name", "unnamed"))
        rate_hz = int(doc.get("rate_hz", 200))

        groups = doc.get("groups", {}) or {}
        defaults = doc.get("defaults", {}) or {}
        steps = doc.get("steps", []) or []
        sources_doc = doc.get("sources", []) or []

        # 그룹 확장
        def expand_targets(tgt: Union[str, int, List[Union[str,int]]]) -> List[int]:
            if isinstance(tgt, (int, str)):
                tgt = [tgt]
            out: List[int] = []
            for item in tgt:
                if isinstance(item, int):
                    out.append(item)
                elif isinstance(item, str):
                    if item in groups:
                        out.extend(int(x) for x in groups[item])
                    else:
                        out.append(int(item))
                else:
                    raise ValueError(f"Unsupported target entry: {item}")
            out = [i for i in out if i in id_pool]
            if not out:
                raise ValueError(f"targets '{tgt}' expand to empty set (unknown ids?)")
            return sorted(set(out))

        # 시간 정규화
        actions: List[ScheduledAction] = []
        t_cursor = 0.0
        for s in steps:
            s = dict(s)
            if "action" not in s:
                raise ValueError(f"step missing 'action': {s}")
            act = str(s["action"]).strip()
            if act not in self.ALLOWED_ACTIONS:
                raise ValueError(f"unsupported action: {act}")

            if "at" in s and "after" in s:
                raise ValueError("Use either 'at' or 'after', not both.")
            if "at" in s:
                t_abs = float(s["at"])
                t_cursor = t_abs
            elif "after" in s:
                t_cursor += float(s["after"])
                t_abs = t_cursor
            else:
                t_abs = t_cursor

            # targets: play_sources는 targets 없이 동작(소스 ID에 따름)
            ids = []
            if act != "play_sources":
                targets = s.get("targets", None)
                if targets is None:
                    raise ValueError("step missing 'targets'")
                ids = expand_targets(targets)

            # 파라미터 병합(게인 기본값)
            params = {}
            if act == "apply_gains":
                base = (defaults.get("gains") or {})
                p = s.get("params") or {}
                params.update(base)
                params.update(p)
                if "full_state" in params:
                    params["full_state"] = 1 if params["full_state"] else 0
            else:
                params.update(s.get("params") or {})

            actions.append(ScheduledAction(
                t_abs=float(t_abs),
                action=act,
                ids=ids,
                params=params,
                raw_note=s.get("note")
            ))

        actions.sort(key=lambda a: a.t_abs)

        # CSV sources 해석 (파일 존재 체크는 Runner에서 수행 가능)
        sources: List[CsvSourceSpec] = []
        for ent in sources_doc:
            ent = dict(ent)
            file = str(ent["file"])
            sid = int(ent["id"])
            cols = ent.get("columns", {}) or {}
            src = CsvSourceSpec(
                file=file, id=sid,
                col_t   = cols.get("t", "time_s"),
                col_pos = cols.get("pos", None),
                col_vel = cols.get("vel", None),
                col_tor = cols.get("tor", None),
                time_unit = ent.get("time_unit", "s"),
                downsample_hz = ent.get("downsample_hz", None),
                offset_s = float(ent.get("offset_s", 0.0)),
            )
            sources.append(src)

        return ScenarioSpec(name=name, rate_hz=rate_hz, actions=actions, sources=sources)

    def _read_any(self, path: str) -> Dict[str, Any]:
        with open(path, "r", encoding="utf-8") as f:
            txt = f.read()
        txt_stripped = txt.lstrip()
        if HAVE_YAML and (path.endswith(".yaml") or path.endswith(".yml")):
            return yaml.safe_load(txt) or {}
        if txt_stripped.startswith("{") or txt_stripped.startswith("[") or path.endswith(".json"):
            return json.loads(txt)
        raise RuntimeError("Install PyYAML or provide JSON file.")

# ---------- 시나리오: 러너 ----------
class ScenarioRunner:
    """
    - 서버 내부 큐(server.q)에 RawCmd 텍스트를 주입하여 기존 파서/핸들러를 재사용
    - play_sources: YAML 'sources'에서 CSV를 읽어 시간/ID별 명령을 사전 컴파일
    """
    def __init__(self, spec: ScenarioSpec, server: MotorBusServer):
        self.spec = spec
        self.server = server
        self._compiled: List[Tuple[float, List[str]]] = []   # (t_abs, [cmd_text,...])

    # ----- 내부 유틸 -----
    def _enqueue_text(self, text: str):
        now_epoch = time.time()
        now_rel = 0.0
        if getattr(self.server, "_t0", None) is not None:
            now_rel = time.perf_counter() - self.server._t0
        self.server.q.put(RawCmd(text=text, recv_epoch=now_epoch, recv_rel=now_rel))

    # CSV 소스 읽어 (t_abs → 명령리스트) 스케줄로 컴파일
    def _compile_sources(self, play_at: float, params: Dict[str, Any]):
        """
        params:
          align: "relative"(default) | "absolute"
          clamp_to_limits: bool (현재 버전은 서버 측 한계 가드 없음 → 생략)
        """
        align = str(params.get("align", "relative")).lower()
        # downsample 기본: 서버 루프의 절반 정도로 제한(너무 촘촘한 명령 주입 방지)
        default_ds_hz = min(self.server.hz, 100)

        sched_map: Dict[float, List[str]] = {}
        for src in self.spec.sources:
            # CSV 로드
            p = Path(src.file)
            if not p.is_absolute():
                p = (BASE_DIR / src.file).resolve()
            if not p.exists():
                print(f"[scenario] CSV not found: {p}")
                continue

            # 다운샘플 간격
            ds_hz = float(src.downsample_hz) if src.downsample_hz else float(default_ds_hz)
            ds_dt = 1.0 / max(1.0, ds_hz)

            last_emit_t = None
            with open(p, "r", encoding="utf-8") as f:
                rdr = csvmod.DictReader(f)
                for row in rdr:
                    # 시간
                    try:
                        t_val = float(row[src.col_t])
                    except Exception:
                        continue
                    if src.time_unit == "ms":
                        t_rel = t_val * 1e-3
                    else:
                        t_rel = t_val
                    t_rel += src.offset_s
                    t_abs = (play_at + t_rel) if (align != "absolute") else t_rel

                    # 다운샘플
                    if last_emit_t is None or (t_abs - last_emit_t) >= ds_dt - 1e-9:
                        # 값들 조합
                        parts = [f"id={src.id}"]
                        has_any = False
                        if src.col_pos and row.get(src.col_pos, "") != "":
                            parts.append(f"pos={row[src.col_pos]}")
                            has_any = True
                        if src.col_vel and row.get(src.col_vel, "") != "":
                            parts.append(f"vel={row[src.col_vel]}")
                            has_any = True
                        if src.col_tor and row.get(src.col_tor, "") != "":
                            parts.append(f"tor={row[src.col_tor]}")
                            has_any = True
                        if not has_any:
                            # 값이 하나도 없으면 skip
                            last_emit_t = t_abs
                            continue

                        cmd = " ".join(parts)
                        sched_map.setdefault(t_abs, []).append(cmd)
                        last_emit_t = t_abs

        # 합치고 정렬
        items = sorted(sched_map.items(), key=lambda kv: kv[0])
        self._compiled.extend(items)

    # 스케줄(steps + sources) 전체 컴파일
    def _compile_all(self):
        self._compiled = []
        for a in self.spec.actions:
            if a.action == "hold":
                # hold는 명령이 아니라 시간 경과만 의미 → 별도 항목 불필요
                continue
            elif a.action == "apply_gains":
                K = a.params.get("K", None)
                B = a.params.get("B", None)
                full = a.params.get("full_state", None)
                for mid in a.ids:
                    parts = [f"id={mid}"]
                    if K is not None: parts.append(f"K={K}")
                    if B is not None: parts.append(f"B={B}")
                    if full is not None: parts.append(f"full={int(full)}")
                    self._compiled.append((a.t_abs, [" ".join(parts)]))
            elif a.action == "move":
                pos = a.params.get("pos", None)
                if pos is None: continue
                cmds = [f"id={mid} pos={pos}" for mid in a.ids]
                self._compiled.append((a.t_abs, cmds))
            elif a.action == "vel":
                vel = a.params.get("vel", None)
                if vel is None: continue
                cmds = [f"id={mid} vel={vel}" for mid in a.ids]
                self._compiled.append((a.t_abs, cmds))
            elif a.action == "torque":
                tor = a.params.get("torque", None)
                if tor is None: continue
                cmds = [f"id={mid} tor={tor}" for mid in a.ids]
                self._compiled.append((a.t_abs, cmds))
            elif a.action == "start_record":
                cmds = [f"id={mid} start" for mid in a.ids]
                self._compiled.append((a.t_abs, cmds))
            elif a.action == "end_record":
                cmds = [f"id={mid} end" for mid in a.ids]
                self._compiled.append((a.t_abs, cmds))
            elif a.action == "safe_zero":
                dwell = float(a.params.get("dwell_s", 0.05))
                pre_cmds = []
                for mid in a.ids:
                    pre_cmds.append(f"id={mid} K=0 B=0")
                    pre_cmds.append(f"id={mid} vel=0 tor=0")
                self._compiled.append((a.t_abs, pre_cmds))
                # zero는 dwell 후에
                self._compiled.append((a.t_abs + dwell, [f"id={mid} zero" for mid in a.ids]))
            elif a.action == "stop":
                self._compiled.append((a.t_abs, [f"id={mid} stop" for mid in a.ids]))
            elif a.action == "play_sources":
                # sources를 현재 a.t_abs를 기준으로 컴파일
                self._compile_sources(play_at=a.t_abs, params=a.params or {})
            else:
                # 미지원 액션은 무시
                pass

        # 최종 정렬(동시간 다수 명령은 입력 순서를 대략 유지)
        self._compiled.sort(key=lambda item: item[0])

    # 러너 실행(별도 스레드)
    def play(self):
        # 서버 루프 기준시각 준비 대기
        while getattr(self.server, "_t0", None) is None:
            time.sleep(0.01)

        self._compile_all()

        t0 = self.server._t0
        idx = 0
        N = len(self._compiled)
        while idx < N:
            t_abs, cmds = self._compiled[idx]
            now_rel = time.perf_counter() - t0
            dt = t_abs - now_rel
            if dt > 0:
                time.sleep(min(dt, 0.01))
                continue
            for c in cmds:
                self._enqueue_text(c)
            idx += 1

        # 끝나면 로그 한 줄(선택)
        # self._enqueue_text(f"# scenario '{self.spec.name}' completed")

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

def run_spawn(mapping: Dict[str, Dict[int,str]], hz: int, tag: Optional[str] = None):
    if tag is None:
        tag = datetime.now().strftime("%Y%m%d-%H%M%S_%f")
    set_session_tag(tag)

    term = find_terminal()
    if not term:
        print("No terminal emulator found. Install lxterminal/xterm/gnome-terminal.")
        sys.exit(1)

    script_path = str((BASE_DIR / Path(__file__).name).resolve())
    py = f"{shlex.quote(sys.executable)} -u {shlex.quote(script_path)}"

    for bus, id2type in mapping.items():
        idmap_str = ",".join(f"{i}:{t}" for i,t in id2type.items())

        server_log = SESSION_DIR / f"server_{bus}.log"
        client_log = SESSION_DIR / f"client_{bus}.log"

        server_cmd = (
            f"{py} --mode server --session-tag {shlex.quote(tag)} "
            f"--bus {bus} --idmap {shlex.quote(idmap_str)} --hz {hz} "
            f"2>&1 | tee {shlex.quote(str(server_log))}"
        )
        client_cmd = (
            f"{py} --mode client --session-tag {shlex.quote(tag)} "
            f"--bus {bus} "
            f"2>&1 | tee {shlex.quote(str(client_log))}"
        )
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
    print("# Setup Wizard (세션 폴더 방식, spawn에서 생성)")
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

    print("\n[안내] 한 세션용 폴더를 만들고, 버스별 서버/클라이언트를 자동으로 띄웁니다.")
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
    p.add_argument("--session-tag", default=None, help="세션 태그 고정(부모 spawn에서 전달)")
    # 시나리오 옵션
    p.add_argument("--scenario", default=None, help="시나리오 파일 경로(.yaml/.yml/.json)")
    args = p.parse_args()

    # 세션 폴더 생성 시점
    if args.mode == "server":
        set_session_tag(args.session_tag)
    elif args.mode == "client":
        set_session_tag(args.session_tag)
    elif args.mode == "spawn":
        pass
    elif args.mode == "setup":
        pass

    if args.mode == "setup":
        run_setup(args.hz); return

    if args.mode == "spawn":
        mapping = parse_map_multibus(args.map, default_type="AK70-10")
        if not mapping:
            id2type = parse_idmap_onebus(args.idmap, default_type="AK70-10")
            if not id2type:
                print('spawn 모드: --map 또는 (--bus 와 --idmap "1:AK70-10,2:AK80-64") 필요'); sys.exit(2)
            mapping = {args.bus: id2type}
        run_spawn(mapping, args.hz, tag=args.session_tag); return

    if args.mode == "client":
        run_client(args.bus); return

    # server
    id2type = parse_idmap_onebus(args.idmap, default_type="AK70-10")
    if not id2type:
        print('server 모드: --idmap "1:AK70-10,2:AK80-64" 필요'); sys.exit(2)

    try:
        server = MotorBusServer(args.bus, id2type, args.hz)

        # 시나리오가 지정되면 로드 후 러너 스레드 기동
        if args.scenario:
            try:
                loader = ScenarioLoader()
                spec = loader.load(args.scenario, id_pool=list(id2type.keys()))
                runner = ScenarioRunner(spec, server)
                th = threading.Thread(target=runner.play, daemon=True)
                th.start()
                print(f"[scenario] loaded '{spec.name}' and started runner thread.")
            except Exception as e:
                print("[scenario] load error:", e)

        server.run()
    except KeyboardInterrupt:
        print("\n[server] KeyboardInterrupt")
    except Exception as e:
        print("[server] fatal:", e)
        sys.exit(1)

if __name__ == "__main__":
    main()
