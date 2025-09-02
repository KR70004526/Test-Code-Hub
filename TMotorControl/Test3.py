#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
multi_motor_console_setup.py
- 모터별 타입(예: ID1=AK70-10, ID2=AK80-64) 설정 지원
- 단일/다중 채널 spawn 가능
- setup 모드: 한 콘솔에서 버스/ID/타입을 입력 후 자동 스폰
- FIFO는 버스별 분리: cmd_{bus}.fifo (절대경로)
- 클라이언트 입력: id=3 K=.. B=.. pos=.. vel=.. iq=.. tor=.. full=0|1 | zero | quit | help
- MIT_Params는 TMotorCANControl.mit_can에서 import
- TMotorManager_mit_can 초기화 시 max_mosfett_temp=90
"""

import os, sys, csv, shlex, time, stat, math, argparse, threading
from pathlib import Path
from typing import Optional, Dict, List, Tuple

# ---------- 경로/로그 ----------
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

def fifo_path_for_bus(bus: str) -> str:
    return str(BASE_DIR / f"cmd_{bus}.fifo")  # 절대경로

def csv_path_for_bus(bus: str) -> Path:
    return LOG_DIR / f"session_{bus}.csv"

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
특수 명령:
  zero            - 해당 id 또는 id=all 대상의 영점 재설정
  help | h | ?    - 이 도움말 출력
  quit | exit     - 서버 종료 (client 창에서 보냈을 경우 서버도 종료)

예시:
  id=1 K=20 B=0.5 pos=0.0          # 1번 모터 위치 명령
  id=2 vel=0.8                     # 2번 모터 속도 명령
  id=all iq=1.2                    # 모든 모터 q축 전류 명령
  id=1 full=1                      # 전체 상태 기반 임피던스 게인 적용 모드
  id=2 zero                        # 2번 모터 영점 재설정
  quit                             # 서버 종료

참고:
- pos 명령은 하드웨어 프로토콜 특성상 [-12.5, +12.5] rad 범위 내에서 적용됩니다(포화).
- 속도/전류/토크 모드에서는 여러 바퀴 회전 가능하며 피드백 position은 드라이버가 멀티턴으로 누적할 수 있습니다.
- 안전을 위해 K/B/iq/torque에 장치별 합리적 한계를 설정하고 사용하세요.
"""
    )

# ---------- 파서 ----------
def parse_and_apply(targets: Dict[int, "FullStateController"], line: str):
    """
    id=3 K=.. B=.. pos=.. vel=.. iq=.. tor=.. full=0|1
    zero | quit | help
    """
    s = line.strip()
    if not s: return None
    lo = s.lower()
    if lo in ("help","h","?"):
        # 서버 창에 도움말 출력 (클라가 FIFO로 help를 보냈을 때)
        print("\n" + help_text() + "\n")
        return None
    if lo in ("quit","exit"): return "quit"

    try: tokens = shlex.split(s)
    except ValueError: tokens = s.split()

    target_ids: List[int] = []
    kv = {}; full_flag = None; zero_flag = False

    for tok in tokens:
        if "=" not in tok:
            if tok.lower() in ("zero","origin"): zero_flag = True
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

    for mid in target_ids:
        ctrl = targets.get(mid)
        if not ctrl:
            print(f"[server] 알 수 없는 모터 id={mid}")
            continue
        if zero_flag: ctrl.m.set_zero_position()
        else: ctrl.update_full_state(full_state=full_flag, **kv)

    return {"ids": target_ids, **kv, **({"zero": True} if zero_flag else {})}

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

# ---------- 서버(단일 버스, 모터별 타입 지원) ----------
def run_server_singlebus(bus: str, id2type: Dict[int, str], hz: int, csv_path: Path):
    if not TMOTOR_OK:
        print("[server] TMotorCANControl import 실패:", TMOTOR_IMPORT_ERR)
        sys.exit(1)

    force_socketcan_channel(bus)
    fifo = fifo_path_for_bus(bus)
    ensure_fifo(fifo)

    motors: Dict[int, Tuple[TMotorManager_mit_can, FullStateController]] = {}
    try:
        for mid, mtype in id2type.items():
            m = TMotorManager_mit_can(motor_type=mtype, motor_ID=mid, max_mosfett_temp=90)  # 90℃
            motors[mid] = (m, FullStateController(m, mtype))
    except Exception as e:
        print(f"[server] 모터 초기화 실패: {e}")
        sys.exit(1)

    csv_fh = open(csv_path, "w", newline="")
    csv_w = csv.writer(csv_fh)
    csv_w.writerow(["time","bus","id","type","pos","vel","acc","iq","torque","temp","err"])

    stop_flag = threading.Event()

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
        except Exception:
            stop_flag.set()

    # with 컨텍스트
    ctx = [m for m,_ in motors.values()]
    for m in ctx: m.__enter__()
    try:
        for _, ctrl in motors.values():
            ctrl.update_full_state(K=5.0, B=0.1, pos=0.0, vel=0.0, iq=0.0, full_state=True)

        th = threading.Thread(target=fifo_reader, daemon=True)
        th.start()

        loop_iter = SoftRealtimeLoop(dt=1.0/float(hz), report=True, fade=0.0) if HAVE_SRL else soft_loop(1.0/float(hz))
        print(f"[server:{bus}] running. ids={list(id2type.keys())}  (`help`로 도움말, `quit` 종료)")
        t0 = time.perf_counter(); last_flush=0.0; last_print=0.0

        while not stop_flag.is_set():
            t = next(loop_iter)
            for mid, (mgr, ctrl) in motors.items():
                ctrl.tick()
                st = ctrl.read_state()
                csv_w.writerow([f"{time.perf_counter()-t0:.6f}", bus, mid, ctrl.type,
                                st["pos"], st["vel"], st["acc"], st["iq"],
                                st["torque"], st["temp"], st["err"]])
            if (t - last_print) >= 0.1:
                status=[]
                for mid,(mgr,ctrl) in motors.items():
                    st = ctrl.read_state()
                    status.append(f"id{mid}({ctrl.type}): pos={_fmtf(st['pos'])} vel={_fmtf(st['vel'])} iq={_fmtf(st['iq'],'{:+.2f}')} tor={_fmtf(st['torque'],'{:+.2f}')} T={_fmtf(st['temp'],'{:.1f}')} err={st['err']}")
                print("\r" + " | ".join(status) + "   ", end="", flush=True)
                last_print = t
            if (t - last_flush) >= 0.2:
                csv_fh.flush(); last_flush = t

        # 안전 정지
        for _, ctrl in motors.values():
            ctrl.update_full_state(K=5.0, B=0.1, pos=0.0, vel=0.0, iq=0.0)
        for _ in range(5):
            for _, ctrl in motors.values(): ctrl.tick()
            time.sleep(0.01)

    finally:
        csv_fh.flush(); csv_fh.close()
        for m in ctx: m.__exit__(None,None,None)
    print(f"\n[server:{bus}] exit.")

# ---------- 클라이언트 ----------
def run_client(bus: str):
    fifo = fifo_path_for_bus(bus)
    print(f"[client:{bus}] Commands: id=.. K=.. B=.. pos=.. vel=.. iq=.. tor=.. full=0|1 | zero | quit | help")
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
        csv_path = csv_path_for_bus(bus)
        server_cmd = (f"{py} --mode server --bus {bus} --idmap {shlex.quote(idmap_str)} "
                      f"--hz {hz} --csv {shlex.quote(str(csv_path))} "
                      f"2>&1 | tee -a {shlex_quote(str(LOG_DIR / f'server_{bus}.log'))}")
        client_cmd = (f"{py} --mode client --bus {bus} "
                      f"2>&1 | tee -a {shlex_quote(str(LOG_DIR / f'client_{bus}.log'))}")
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

# 작은 오타 방지용
def shlex_quote(s: str) -> str:
    return shlex.quote(s)

# ---------- setup(설정 마법사) ----------
def run_setup(default_hz: int):
    print("# Setup Wizard")
    print("- 버스 이름(can0/can1/...)과 모터 ID:타입을 입력하세요.")
    print("- 예: can0: 1 AK70-10, 2 AK80-64   (쉼표로 구분)")
    print("- 빈 줄 입력하면 종료합니다.\n")

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

    yn = input("이 설정으로 서버/클라이언트를 spawn 할까요? [Y/n] ").strip().lower()
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
    p.add_argument("--csv", default=None, help="server 모드에서 CSV 파일 경로(기본 logs/session_{bus}.csv)")
    p.add_argument("--bus", default="can0", help="server/client 단일 버스 지정")
    p.add_argument("--idmap", default="", help='server 단일 버스: "1:AK70-10,2:AK80-64"')
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
    csv_path = Path(args.csv) if args.csv else csv_path_for_bus(args.bus)
    run_server_singlebus(args.bus, id2type, args.hz, csv_path)

if __name__ == "__main__":
    main()
