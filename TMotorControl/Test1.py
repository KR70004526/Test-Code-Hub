# 핵심만 발췌: SoftRealtimeLoop를 사용해 주기를 관리
from NeuroLocoMiddleware.SoftRealtimeLoop import SoftRealtimeLoop
from TMotorCANControl import TMotorManager_mit_can
import time

# ---- 준비: 매니저 + 컨트롤러(원자적 갱신) ----
m = TMotorManager_mit_can(motor_type="AK70-10", motor_ID=1, max_mosfett_temp=60)

class FullStateController:
    def __init__(self, manager):
        import threading
        self.m = manager
        self._lock = threading.Lock()
        self._K = 5.0; self._B = 0.1
        self._full_state = True
    def update_full_state(self, K=None, B=None, pos=None, vel=None, iq=None, torque=None, *, full_state=None):
        with self._lock:
            if full_state is None: full_state = self._full_state
            if K is None: K = self._K
            if B is None: B = self._B
            # 풀스테이트(전류 FF 포함) 게인 적용
            if full_state: self.m.set_impedance_gains_real_unit_full_state_feedback(K=K, B=B)
            else:          self.m.set_impedance_gains_real_unit(K=K, B=B)
            self._K, self._B, self._full_state = K, B, full_state
            # 명령 저장(전송은 update()에서)
            if pos is not None:    self.m.set_output_angle_radians(pos)
            if vel is not None:    self.m.set_output_velocity_radians_per_second(vel)
            if torque is not None: self.m.set_output_torque_newton_meters(torque)
            if iq is not None:     self.m.set_motor_current_qaxis_amps(iq)

ctrl = FullStateController(m)

with m:
    # 제로 후 잠깐 대기(문서에서도 대기 예시) :contentReference[oaicite:2]{index=2}
    m.set_zero_position()
    time.sleep(1.5)

    # SoftRealtimeLoop: dt=1/hz, report/fade는 필요시 조정
    loop = SoftRealtimeLoop(dt=1.0/200.0, report=True, fade=0.0)  # 200 Hz 루프

    for t in loop:
        # ---- 여기서 원하는 "모드"를 FULL-STATE로 흉내 내기 ----
        if t < 1.0:
            # position hold
            ctrl.update_full_state(K=20.0, B=0.5, pos=0.0, iq=0.0)
        elif t < 2.0:
            # speed-like (K=0, B>0, vel 명령)
            ctrl.update_full_state(K=0.0, B=0.4, vel=0.5)
        elif t < 3.0:
            # torque/current-like (K=B=0, iq 명령)
            ctrl.update_full_state(K=0.0, B=0.0, iq=0.8)
        else:
            # position step
            ctrl.update_full_state(K=20.0, B=0.5, pos=0.3, iq=0.0)

        # == 실제 전송 + 상태 동기화 ==
        m.update()

        # 상태 확인(필요시)
        # print(f"\r pos={m.position:+.3f} vel={m.velocity:+.3f} iq={m.current_qaxis:+.3f}", end="")
