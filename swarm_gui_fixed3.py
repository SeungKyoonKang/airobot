import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import threading
import queue
import multiprocessing
import asyncio
import time
import random
import logging
import json
import math
import os
from datetime import datetime, timedelta

import mini.mini_sdk as MiniSdk
from mini.apis.api_action import PlayAction
from mini.apis.api_sound import PlayAudio, AudioStorageType
from mini.dns.dns_browser import WiFiDevice

# =========================================================
# ⚙️ 설정값
# =========================================================
ROBOT_TYPE = MiniSdk.RobotType.KOR
SEARCH_TIMEOUT = 15
SYNC_WAIT_TIMEOUT = 60
TARGET_AUDIO_STORAGE = AudioStorageType.PRESET_LOCAL
TARGET_AUDIO_VOLUME = 1.0

CONNECT_STEP_TIMEOUT = 60  # 10대가 동시에 같은 공유기에 붙을 때는 개별 연결에 시간이 더 걸릴 수 있음
ENTER_PROGRAM_INTERNAL_DELAY = 6  # enter_program() 내부에서 6초 대기함

# 10대가 동시에 접속을 시도하면 mDNS 브로드캐스트 혼잡/일시적 응답 유실이 생길 수 있으므로,
# 한 번 실패했다고 바로 포기하지 않고 지수 백오프 + 지터를 두고 재시도한다.
MAX_CONNECT_ATTEMPTS = 3
RETRY_BASE_DELAY = 3.0        # 재시도 사이 기본 대기(초), 시도 횟수에 비례해 증가
RETRY_JITTER = 1.5            # 여러 로봇이 동시에 재시도를 걸어 또 충돌하는 것을 막기 위한 무작위 지터(초)

# 프로세스를 한꺼번에 띄우면 공유기/네트워크에 순간 부하가 몰리므로, 기본 간격 + 지터로 스태거링한다.
STAGGER_BASE_DELAY = 0.7
STAGGER_JITTER = 0.5

DEFAULT_DANCE = "dance_0001en"
DEFAULT_SERIALS = "2700620\n3003100\n2800932\n2602331\n2902186\n2800595\n3002201\n2901556\n2902063\n2500139"
DEFAULT_TRIGGER_DELAY = 5  # '설정' 클릭 후 실제 실행까지의 기본 지연 시간(초)
DEFAULT_REPEAT_COUNT = 1
DEFAULT_REPEAT_INTERVAL = 10
MAX_LOG_ENTRIES = 2000

# =========================================================
# 🎨 색상 팔레트 (UI 전용)
# =========================================================
COLOR_BG = "#F1F4F9"          # 전체 배경
COLOR_CARD = "#FFFFFF"        # 카드(섹션) 배경
COLOR_BORDER = "#E2E8F0"      # 카드 테두리
COLOR_TEXT = "#1F2937"        # 기본 텍스트
COLOR_MUTED = "#6B7280"       # 보조 텍스트

COLOR_HEADER_BG = "#1F2A44"   # 상단 헤더 배경
COLOR_HEADER_FG = "#FFFFFF"
COLOR_HEADER_SUB = "#AEB8D6"

# 단계별(스텝) 강조색
COLOR_STEP1 = "#2F6FED"  # 파랑 - 로봇 시리얼 입력
COLOR_STEP2 = "#7C3AED"  # 보라 - 음원/동작 선택
COLOR_STEP3 = "#059669"  # 초록 - 실행 제어
COLOR_STEP4 = "#334155"  # 짙은 회색 - 로그

# 버튼 색상
COLOR_PRIMARY = "#2F6FED"
COLOR_PRIMARY_HOVER = "#255BC7"
COLOR_PRIMARY_DISABLED = "#A9C0F5"
COLOR_DANGER = "#DC2626"
COLOR_DANGER_HOVER = "#B91C1C"
COLOR_DANGER_DISABLED = "#F3A6A6"
COLOR_SUCCESS = "#059669"
COLOR_WARNING = "#D97706"

# 로그 색상 (다크 로그창 기준)
LOG_BG = "#0F172A"
LOG_FG = "#E2E8F0"
LOG_COLOR_SUCCESS = "#4ADE80"
LOG_COLOR_ERROR = "#F87171"
LOG_COLOR_WARNING = "#FBBF24"
LOG_COLOR_INFO = "#60A5FA"
LOG_COLOR_SYSTEM = "#C084FC"


# =========================================================
# 🤖 개별 로봇 프로세스
# 연결 후에는 종료 신호가 켜질 때까지 대기하며, 공유 대기열의
# 실행 요청을 순서대로 처리한다.
# =========================================================
def robot_worker(robot_serial, quit_event, shared, status_queue, worker_stop_event):
    """
    로봇 1대를 담당하는 프로세스.
    1) 연결(재시도 포함) -> status_queue에 "connected"/"failed" 보고
    2) 연결 성공 시, quit_event가 켜질 때까지 공유 실행 대기열을 감시
       -> 새 요청을 순서대로 실행하고 "played" 보고
    3) 종료 또는 개별 취소 이벤트가 켜지면 연결을 정리하고 "disconnected" 보고
    """
    def stop_requested():
        return quit_event.is_set() or worker_stop_event.is_set()

    def quiet_exception_handler(loop, context):
        exc = context.get("exception")
        msg = context.get("message", "")
        print(f"[{robot_serial}] ℹ️ (백그라운드) {type(exc).__name__ if exc else ''}: {msg}")

    async def run_robot():
        MiniSdk.set_robot_type(ROBOT_TYPE)
        MiniSdk.set_log_level(logging.WARNING)

        loop = asyncio.get_running_loop()
        loop.set_exception_handler(quiet_exception_handler)

        connected = False       # enter_program()까지 성공했는지
        transport_open = False  # connect()가 성공해서 실제 연결이 열려있는지

        async def release_transport():
            nonlocal transport_open
            try:
                await MiniSdk.release()
            except Exception as release_err:
                status_queue.put((robot_serial, "cleanup_error", f"release() 실패: {release_err}"))
            finally:
                transport_open = False

        async def try_connect_once():
            """탐색 + connect + enter_program 을 한 번 시도한다.
            성공하면 (True, None), 실패하면 (False, 이유)를 반환하고 예외를 던지지 않는다
            (재시도 루프에서 처리하기 위함)."""
            nonlocal transport_open, connected

            status_queue.put((robot_serial, "state", "검색 중"))
            dev: WiFiDevice = await MiniSdk.get_device_by_name(robot_serial, SEARCH_TIMEOUT)
            if not dev:
                return False, "로봇을 찾을 수 없음"
            try:
                status_queue.put((robot_serial, "state", "연결 중"))
                is_connected = await asyncio.wait_for(MiniSdk.connect(dev), timeout=CONNECT_STEP_TIMEOUT)
            except asyncio.TimeoutError:
                await release_transport()
                return False, "connect() 시간 초과"
            except Exception as connect_err:
                await release_transport()
                return False, f"connect() 실패: {connect_err}"
            if not is_connected:
                await release_transport()
                return False, "connect() 실패"
            transport_open = True
            if stop_requested():
                await release_transport()
                return False, "연결 취소됨"

            try:
                status_queue.put((robot_serial, "state", "프로그램 진입 중"))
                enter_result = await asyncio.wait_for(MiniSdk.enter_program(), timeout=CONNECT_STEP_TIMEOUT)
                if not enter_result:
                    await release_transport()
                    return False, "프로그램 모드 진입 실패 (로봇 응답 없음)"
            except asyncio.TimeoutError:
                await release_transport()
                return False, "프로그램 모드 진입 시간 초과"
            except Exception as enter_err:
                await release_transport()
                return False, f"enter_program() 실패: {enter_err}"

            if stop_requested():
                await release_transport()
                return False, "연결 취소됨"
            connected = True
            return True, None

        try:
            last_err = None
            success = False
            for attempt in range(1, MAX_CONNECT_ATTEMPTS + 1):
                if stop_requested():
                    print(f"[{robot_serial}] 🛑 종료 요청으로 연결 시도를 중단합니다")
                    return

                ok, err = await try_connect_once()
                if ok:
                    success = True
                    break

                last_err = err
                print(f"[{robot_serial}] ⚠️ 연결 시도 {attempt}/{MAX_CONNECT_ATTEMPTS} 실패: {err}")
                if attempt < MAX_CONNECT_ATTEMPTS:
                    backoff = RETRY_BASE_DELAY * attempt + random.uniform(0, RETRY_JITTER)
                    await asyncio.sleep(backoff)

            if not success:
                status_queue.put((robot_serial, "failed", f"{MAX_CONNECT_ATTEMPTS}회 재시도 후 실패 - {last_err}"))
                return

            if stop_requested():
                return
            status_queue.put((robot_serial, "connected", None))

            # ── 연결 완료. 새 실행 요청을 대기열에서 순서대로 처리 ──
            next_command = 0
            last_stop = 0
            while not stop_requested():
                try:
                    commands = shared["commands"]
                    stop_id = shared.get("stop_id", 0)
                except Exception:
                    commands = []
                    stop_id = last_stop

                if stop_id != last_stop:
                    last_stop = stop_id
                    next_command = len(commands)
                    status_queue.put((robot_serial, "stopped", None))

                if next_command < len(commands):
                    action_name, audio_name = commands[next_command]
                    next_command += 1
                    if action_name or audio_name:
                        try:
                            tasks = []
                            if action_name:
                                tasks.append(asyncio.create_task(PlayAction(action_name=action_name).execute()))
                            if audio_name:
                                tasks.append(asyncio.create_task(PlayAudio(
                                    is_serial=False, url=audio_name,
                                    storage_type=TARGET_AUDIO_STORAGE,
                                    volume=TARGET_AUDIO_VOLUME,
                                ).execute()))
                            execution = asyncio.ensure_future(asyncio.gather(*tasks))
                            cancelled = False
                            while not execution.done():
                                await asyncio.sleep(0.1)
                                current_stop = shared.get("stop_id", last_stop)
                                if current_stop != last_stop or stop_requested():
                                    execution.cancel()
                                    cancelled = True
                                    break
                            if cancelled:
                                await asyncio.gather(execution, return_exceptions=True)
                                last_stop = shared.get("stop_id", last_stop)
                                next_command = len(shared["commands"])
                                status_queue.put((robot_serial, "stopped", None))
                            else:
                                await execution
                                status_queue.put((robot_serial, "played", f"{action_name or '-'} / {audio_name or '-'}"))
                        except Exception as media_err:
                            status_queue.put((robot_serial, "play_error", str(media_err)))

                await asyncio.sleep(0.15)

        except Exception as e:
            print(f"[{robot_serial}] ❌ 오류 발생: {type(e).__name__}: {e}")
            if not connected:
                status_queue.put((robot_serial, "failed", str(e)))
        finally:
            if transport_open:
                if connected:
                    try:
                        await MiniSdk.quit_program()
                    except Exception as cleanup_err:
                        status_queue.put(
                            (robot_serial, "cleanup_error", f"quit_program() 실패: {cleanup_err}")
                        )
                await release_transport()
            status_queue.put((robot_serial, "disconnected", None))
            print(f"[{robot_serial}] 🔌 연결이 해제되었습니다.")

    asyncio.run(run_robot())


# =========================================================
# 🚀 군집 연결 + 대기 + 트리거 실행 + 종료를 담당하는 오케스트레이션 함수
# (백그라운드 스레드에서 호출됨. log_q/progress_q로 GUI에 상태 전달)
#
# 동작 단계:
#   1) 연결 단계: 총 total대가 모두 응답(연결 성공/실패)할 때까지 대기
#   2) 대기/실행 단계: quit_event가 켜질 때까지 "played"/"play_error" 로그만 중계
#   3) 종료 단계: quit_event가 켜지면, 스폰된 모든 프로세스가 "disconnected"를
#      보고할 때까지 기다렸다가 프로세스를 정리하고 세션을 마친다
# =========================================================
def run_swarm_session(serial_list, log_q: queue.Queue, progress_q: queue.Queue,
                       quit_event, shared):
    total = len(serial_list)
    status_queue = multiprocessing.Queue()
    processes = []
    process_by_serial = {}
    worker_stop_events = {}
    not_started = {}

    log_q.put(f">>> 시스템 부팅: {total}대 기기 연결을 시도합니다...")

    for index, serial in enumerate(serial_list):
        if quit_event.is_set():
            log_q.put(">>> 🛑 종료 요청으로 나머지 로봇은 연결하지 않습니다.")
            for pending_serial in serial_list[index:]:
                not_started[pending_serial] = "연결 취소됨"
            break
        try:
            worker_stop_event = multiprocessing.Event()
            p = multiprocessing.Process(
                target=robot_worker,
                args=(serial, quit_event, shared, status_queue, worker_stop_event)
            )
            p.start()
        except Exception as e:
            log_q.put(f">>> ❌ {serial} 프로세스 시작 실패: {e}")
            not_started[serial] = "프로세스 시작 실패"
            continue
        processes.append(p)
        process_by_serial[serial] = p
        worker_stop_events[serial] = worker_stop_event
        time.sleep(STAGGER_BASE_DELAY + random.uniform(0, STAGGER_JITTER))  # 동시 접속 부하 완화 + 지터

    total_spawned = len(processes)
    skipped = total - total_spawned

    # 한 로봇이 최대 재시도 횟수를 다 소진할 때 걸릴 수 있는 최악의 시간 +
    # 마지막으로 시작된 로봇이 다른 로봇들보다 늦게 시작하는 시간(스태거링 누적)을 더해서
    # 연결 단계의 대기 한도를 계산한다 (병렬로 실행되므로 total을 곱하지 않는다).
    single_robot_worst_case = (SEARCH_TIMEOUT + CONNECT_STEP_TIMEOUT * 2 + RETRY_BASE_DELAY + RETRY_JITTER) \
        * MAX_CONNECT_ATTEMPTS
    stagger_spread = (STAGGER_BASE_DELAY + STAGGER_JITTER) * total
    connect_deadline = time.time() + single_robot_worst_case + stagger_spread + 30

    log_q.put(f">>> ⏳ 총 {total}대 로봇의 응답을 기다립니다...")

    connected_count = 0
    failed_count = skipped
    disconnected_count = 0
    terminal_serials = set(not_started)
    connected_serials = set()
    disconnected_serials = set()
    dead_since = {}
    connect_phase_done = connected_count + failed_count >= total
    notice_emitted = False
    for serial, reason in not_started.items():
        progress_q.put(("state", serial, "연결 취소됨" if reason == "연결 취소됨" else "연결 실패"))
    if skipped:
        progress_q.put((connected_count, failed_count, total))

    def account_exited_workers():
        nonlocal failed_count, disconnected_count, connect_phase_done
        now = time.monotonic()
        for serial, process in process_by_serial.items():
            if serial in disconnected_serials:
                continue
            if process.exitcode is None:
                dead_since.pop(serial, None)
                continue

            first_seen = dead_since.setdefault(serial, now)
            if now - first_seen < 1.0:
                continue

            disconnected_serials.add(serial)
            was_connected = serial in connected_serials
            if not was_connected and serial not in terminal_serials and not connect_phase_done:
                terminal_serials.add(serial)
                failed_count += 1
                progress_q.put(("state", serial, "연결 실패"))
                progress_q.put((connected_count, failed_count, total))
                if connected_count + failed_count >= total:
                    connect_phase_done = True

            log_q.put(
                f">>> ⚠️ {serial} 프로세스가 상태 보고 없이 종료되었습니다 "
                f"(종료 코드: {process.exitcode})."
            )
            disconnected_count += 1
            if was_connected:
                progress_q.put(("state", serial, "연결 해제"))
            else:
                progress_q.put(("state", serial, "연결 실패"))
            log_q.put(
                f"[본부] 🔌 {serial} 연결 해제 ({disconnected_count}/{total_spawned})"
            )

    while True:
        account_exited_workers()

        # ── 연결 단계 진행 중이면 취소/시간초과를 체크 ──
        if not connect_phase_done:
            if quit_event.is_set():
                missing = total - (connected_count + failed_count)
                if missing > 0:
                    failed_count += missing
                    log_q.put(f">>> 🛑 종료 요청으로 남은 {missing}대는 실패로 처리합니다.")
                connect_phase_done = True
            elif time.time() > connect_deadline and (connected_count + failed_count) < total:
                missing = total - (connected_count + failed_count)
                failed_count += missing
                for serial, worker_stop_event in worker_stop_events.items():
                    if serial not in connected_serials and serial not in terminal_serials:
                        worker_stop_event.set()
                log_q.put(f">>> ⏱️ 응답 대기 시간 초과. {missing}대는 실패로 처리합니다.")
                connect_phase_done = True

        if connect_phase_done and not notice_emitted:
            notice_emitted = True
            if connected_count == 0:
                log_q.put(">>> 🛑 연결된 로봇이 없습니다.")
            else:
                log_q.put(
                    f">>> ✅ 연결 단계 완료! ({connected_count}/{total}대 준비됨) "
                    f"음원/동작 이름을 입력하고 '설정' 버튼을 눌러주세요."
                )
            log_q.put("__CONNECT_PHASE_DONE__")

        # 연결 단계가 끝났고, 스폰된 프로세스가 전부 연결 해제를 마쳤다면 세션 종료
        if connect_phase_done and disconnected_count >= total_spawned:
            break

        try:
            serial, status, payload = status_queue.get(timeout=0.5)
        except queue.Empty:
            continue

        if status == "connected":
            if connect_phase_done:
                log_q.put(f"[본부] ⚠️ {serial} 연결 완료 보고가 제한 시간 이후 도착하여 무시했습니다.")
                continue
            if serial in terminal_serials or serial in connected_serials:
                continue
            connected_serials.add(serial)
            connected_count += 1
            progress_q.put(("state", serial, "연결 완료"))
            log_q.put(f"[본부] ✅ {serial} 연결 완료 ({connected_count + failed_count}/{total})")
            progress_q.put((connected_count, failed_count, total))
            if connected_count + failed_count >= total:
                connect_phase_done = True
        elif status == "failed":
            if connect_phase_done:
                continue
            if serial in terminal_serials:
                continue
            terminal_serials.add(serial)
            failed_count += 1
            progress_q.put(("state", serial, "연결 실패"))
            reason = f" - 사유: {payload}" if payload else ""
            log_q.put(f"[본부] ❌ {serial} 연결 실패{reason} ({connected_count + failed_count}/{total})")
            progress_q.put((connected_count, failed_count, total))
            if connected_count + failed_count >= total:
                connect_phase_done = True
        elif status == "played":
            progress_q.put(("state", serial, "실행 완료"))
            log_q.put(f"[본부] 🎬 {serial} 동작 실행: {payload}")
        elif status == "play_error":
            progress_q.put(("state", serial, "실행 오류"))
            log_q.put(f"[본부] ⚠️ {serial} 동작 실행 중 오류: {payload}")
        elif status == "state":
            log_q.put(f"[본부] {serial} 상태: {payload}")
            progress_q.put(("state", serial, payload))
        elif status == "stopped":
            progress_q.put(("state", serial, "정지됨"))
            log_q.put(f"[본부] ⏹️ {serial} 실행 중지")
        elif status == "disconnected":
            if serial not in disconnected_serials:
                disconnected_serials.add(serial)
                disconnected_count += 1
                progress_q.put(("state", serial, "연결 해제"))
                log_q.put(f"[본부] 🔌 {serial} 연결 해제 ({disconnected_count}/{total_spawned})")
        elif status == "cleanup_error":
            log_q.put(f"[본부] ⚠️ {serial} 연결 정리 오류: {payload}")

    for p in processes:
        p.join(timeout=10)
        if p.is_alive():
            log_q.put(f">>> ⚠️ 프로세스({p.pid})가 응답하지 않아 강제 종료합니다.")
            p.terminate()
            p.join()

    log_q.put(">>> ✅ 모든 로봇의 연결이 해제되었습니다. 세션을 종료합니다.")
    log_q.put("__DONE__")


# =========================================================
# 🖥️ Tkinter GUI
# =========================================================
class SwarmControlApp:
    def __init__(self, root):
        self.root = root
        self.root.title("알파미니 군집 제어")
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        initial_width = min(1000, max(800, screen_width - 80))
        initial_height = min(820, max(600, screen_height - 100))
        self.root.geometry(f"{initial_width}x{initial_height}")
        self.root.minsize(760, 520)
        self.root.configure(bg=COLOR_BG)

        self.log_q = queue.Queue()
        self.progress_q = queue.Queue()

        # 세션 상태 (연결 버튼을 누를 때마다 새로 만들어짐)
        self.manager = None
        self.shared = None
        self.quit_event = None
        self.session_active = False
        self._shutdown_requested = False
        self._pending_after_id = None
        self._last_connected = 0
        self._last_total = 0
        self.robot_states = {}
        self.log_entries = []
        self.log_filter_var = tk.StringVar(value="전체")
        self.history = []
        self._repeat_after_id = None
        self._session_serials = []
        self.profiles_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "swarm_profiles.json")
        self.profiles = self._load_profiles()

        self._setup_style()
        self._build_widgets()
        self._poll_queues()

        # 창을 X 버튼으로 닫아도 종료 절차(연결 해제)를 거치도록 연결
        self.root.protocol("WM_DELETE_WINDOW", self.on_exit_clicked)

    # ---------------------------------------------------
    # ttk 스타일(버튼/진행바 색상) 설정
    # ---------------------------------------------------
    def _setup_style(self):
        style = ttk.Style(self.root)
        try:
            style.theme_use("clam")  # clam 테마라야 배경/글자색 커스터마이징이 제대로 먹힘
        except tk.TclError:
            pass

        style.configure("Primary.TButton", font=("Segoe UI", 10, "bold"),
                         background=COLOR_PRIMARY, foreground="white",
                         padding=(16, 9), borderwidth=0, focusthickness=0)
        style.map("Primary.TButton",
                  background=[("active", COLOR_PRIMARY_HOVER), ("disabled", COLOR_PRIMARY_DISABLED)],
                  foreground=[("disabled", "#EEF2FF")])

        style.configure("Danger.TButton", font=("Segoe UI", 10, "bold"),
                         background=COLOR_DANGER, foreground="white",
                         padding=(16, 9), borderwidth=0, focusthickness=0)
        style.map("Danger.TButton",
                  background=[("active", COLOR_DANGER_HOVER), ("disabled", COLOR_DANGER_DISABLED)],
                  foreground=[("disabled", "#FEECEC")])

        style.configure("Secondary.TButton", font=("Segoe UI", 10, "bold"),
                         background="#EEF2FF", foreground=COLOR_PRIMARY,
                         padding=(14, 8), borderwidth=0, focusthickness=0)
        style.map("Secondary.TButton",
                  background=[("active", "#DCE6FF"), ("disabled", "#F1F4F9")],
                  foreground=[("disabled", COLOR_MUTED)])

        for name, color in (("Blue", COLOR_PRIMARY), ("Green", COLOR_SUCCESS), ("Orange", COLOR_WARNING)):
            style.configure(f"{name}.Horizontal.TProgressbar",
                             troughcolor=COLOR_BORDER, background=color,
                             borderwidth=0, thickness=14)

    # ---------------------------------------------------
    # 단계(step) 카드 - 번호 배지 + 색상 강조선이 있는 섹션 컨테이너
    # ---------------------------------------------------
    def _make_step_card(self, parent, step_no, title, accent_color, expand=False):
        outer = tk.Frame(parent, bg=COLOR_BG)
        outer.pack(fill="both" if expand else "x", expand=expand, padx=14, pady=(0, 12))

        card = tk.Frame(outer, bg=COLOR_CARD, highlightbackground=COLOR_BORDER, highlightthickness=1)
        card.pack(fill="both" if expand else "x", expand=expand)

        accent = tk.Frame(card, bg=accent_color, width=5)
        accent.pack(side="left", fill="y")

        body = tk.Frame(card, bg=COLOR_CARD)
        body.pack(side="left", fill="both", expand=True)

        header = tk.Frame(body, bg=COLOR_CARD)
        header.pack(fill="x", padx=14, pady=(10, 4))

        badge = tk.Label(header, text=str(step_no), bg=accent_color, fg="white",
                          font=("Segoe UI", 9, "bold"), width=2)
        badge.pack(side="left")

        tk.Label(header, text=title, bg=COLOR_CARD, fg=COLOR_TEXT,
                 font=("Segoe UI", 11, "bold")).pack(side="left", padx=(8, 0))

        content = tk.Frame(body, bg=COLOR_CARD)
        content.pack(fill="both", expand=True, padx=14, pady=(0, 12))
        return content

    # ---------------------------------------------------
    # 화면 구성
    # ---------------------------------------------------
    def _build_widgets(self):
        # 상단 헤더
        header = tk.Frame(self.root, bg=COLOR_HEADER_BG)
        header.pack(fill="x")
        tk.Label(header, text="🤖 알파미니 군집 제어", bg=COLOR_HEADER_BG, fg=COLOR_HEADER_FG,
                  font=("Segoe UI", 16, "bold")).pack(anchor="w", padx=20, pady=(16, 0))
        tk.Label(header, text="AlphaMini Swarm Control · 10대 동시 접속 · 이름 설정 · 지연 실행",
                  bg=COLOR_HEADER_BG, fg=COLOR_HEADER_SUB, font=("Segoe UI", 9)).pack(
            anchor="w", padx=20, pady=(2, 14))

        # 화면 높이가 작은 환경에서도 모든 제어 영역에 접근할 수 있도록
        # 본문을 세로 스크롤 영역으로 구성한다.
        body_view = tk.Frame(self.root, bg=COLOR_BG)
        body_view.pack(fill="both", expand=True)

        body_canvas = tk.Canvas(body_view, bg=COLOR_BG, highlightthickness=0, bd=0)
        body_scrollbar = ttk.Scrollbar(body_view, orient="vertical", command=body_canvas.yview)
        body_canvas.configure(yscrollcommand=body_scrollbar.set)
        body_scrollbar.pack(side="right", fill="y")
        body_canvas.pack(side="left", fill="both", expand=True)

        body = tk.Frame(body_canvas, bg=COLOR_BG)
        body_window = body_canvas.create_window((0, 0), window=body, anchor="nw")

        def update_scroll_region(_event=None):
            body_canvas.configure(scrollregion=body_canvas.bbox("all"))

        def stretch_body(event):
            body_canvas.itemconfigure(body_window, width=event.width)

        body.bind("<Configure>", update_scroll_region)
        body_canvas.bind("<Configure>", stretch_body)

        def scroll_body(event):
            if event.delta:
                body_canvas.yview_scroll(int(-event.delta / 120), "units")

        body_canvas.bind_all("<MouseWheel>", scroll_body)

        # ── STEP 1: 로봇 시리얼 입력 (파랑) ──────────────────────
        step1 = self._make_step_card(body, 1, "로봇 시리얼 번호", COLOR_STEP1)

        # 개별 시리얼 번호 입력 필드 (1행 10열 그리드)
        self.serial_entries = []
        default_serials = DEFAULT_SERIALS.strip().split('\n')

        row_frame = tk.Frame(step1, bg=COLOR_CARD)
        row_frame.pack(fill="x")

        for idx in range(10):
            # 라벨 + 엔트리 세트
            entry_container = tk.Frame(row_frame, bg=COLOR_CARD)
            entry_container.pack(side="left", padx=(0, 10) if idx < 9 else 0, fill="x", expand=True)

            tk.Label(entry_container, text=f"#{idx+1}", bg=COLOR_CARD, fg=COLOR_STEP1,
                    font=("Segoe UI", 9, "bold")).pack(anchor="w")

            entry = tk.Entry(
                entry_container, font=("Consolas", 10),
                bg="#F8FAFF", fg=COLOR_TEXT, relief="flat",
                highlightthickness=1, highlightbackground=COLOR_BORDER, highlightcolor=COLOR_STEP1,
            )
            if idx < len(default_serials):
                entry.insert(0, default_serials[idx].strip())
            entry.pack(fill="x")

            self.serial_entries.append(entry)

        preset_row = tk.Frame(step1, bg=COLOR_CARD)
        preset_row.pack(fill="x", pady=(2, 0))
        tk.Label(preset_row, text="프로필", bg=COLOR_CARD, fg=COLOR_MUTED,
                 font=("Segoe UI", 9)).pack(side="left")
        self.profile_name = tk.Entry(preset_row, width=18, font=("Segoe UI", 9))
        self.profile_name.insert(0, "기본 프로필")
        self.profile_name.pack(side="left", padx=(8, 6))
        ttk.Button(preset_row, text="현재 번호 저장", style="Secondary.TButton",
                   command=self.on_save_profile).pack(side="left")
        ttk.Button(preset_row, text="프로필 불러오기", style="Secondary.TButton",
                   command=self.on_load_profile).pack(side="left", padx=(6, 0))

        # ── STEP 2: 연결 (파랑 계열 초록 - 초록) ─────────────────
        step2 = self._make_step_card(body, 2, "10대 동시 연결", COLOR_STEP3)

        connect_row = tk.Frame(step2, bg=COLOR_CARD)
        connect_row.pack(fill="x", pady=(0, 8))
        self.connect_btn = ttk.Button(connect_row, text="🔌 연결", style="Primary.TButton",
                                       command=self.on_connect_clicked)
        self.connect_btn.pack(side="left")

        progress_row = tk.Frame(step2, bg=COLOR_CARD)
        progress_row.pack(fill="x")
        self.progress_var = tk.DoubleVar(value=0)
        self.progress_bar = ttk.Progressbar(progress_row, variable=self.progress_var, maximum=100,
                                              style="Blue.Horizontal.TProgressbar")
        self.progress_bar.pack(side="left", fill="x", expand=True)
        self.progress_label = tk.Label(progress_row, text="0/0", bg=COLOR_CARD, fg=COLOR_TEXT,
                                         font=("Segoe UI", 9, "bold"))
        self.progress_label.pack(side="left", padx=(10, 0))

        self.dashboard_frame = tk.Frame(step2, bg=COLOR_CARD)
        self.dashboard_frame.pack(fill="x", pady=(8, 0))
        self.robot_status_labels = {}

        # ── STEP 3: 음원/동작 이름 입력 + 설정 (보라) ─────────────
        step3 = self._make_step_card(body, 3, "동작 · 음원 설정 및 예약 실행", COLOR_STEP2)

        name_row = tk.Frame(step3, bg=COLOR_CARD)
        name_row.pack(fill="x", pady=(0, 8))
        tk.Label(name_row, text="동작", bg=COLOR_CARD, fg=COLOR_STEP2,
                  font=("Segoe UI", 9, "bold")).pack(side="left")
        self.name_entry = tk.Entry(
            name_row, font=("Segoe UI", 10),
            bg="#F8FAFF", fg=COLOR_TEXT, relief="flat",
            highlightthickness=1, highlightbackground=COLOR_BORDER, highlightcolor=COLOR_STEP2,
        )
        self.name_entry.insert(0, DEFAULT_DANCE)
        self.name_entry.pack(side="left", fill="x", expand=True, padx=(8, 12))

        tk.Label(name_row, text="음원", bg=COLOR_CARD, fg=COLOR_STEP2,
                 font=("Segoe UI", 9, "bold")).pack(side="left")
        self.audio_entry = tk.Entry(name_row, width=16, font=("Segoe UI", 10),
                                    bg="#F8FAFF", fg=COLOR_TEXT, relief="flat",
                                    highlightthickness=1, highlightbackground=COLOR_BORDER,
                                    highlightcolor=COLOR_STEP2)
        self.audio_entry.insert(0, DEFAULT_DANCE)
        self.audio_entry.pack(side="left", padx=(8, 12))

        tk.Label(name_row, text="지연(초)", bg=COLOR_CARD, fg=COLOR_STEP2,
                  font=("Segoe UI", 9, "bold")).pack(side="left")
        self.delay_entry = tk.Entry(
            name_row, width=6, font=("Segoe UI", 10),
            bg="#F8FAFF", fg=COLOR_TEXT, relief="flat", justify="center",
            highlightthickness=1, highlightbackground=COLOR_BORDER, highlightcolor=COLOR_STEP2,
        )
        self.delay_entry.insert(0, str(DEFAULT_TRIGGER_DELAY))
        self.delay_entry.pack(side="left", padx=(8, 12))

        tk.Label(name_row, text="반복", bg=COLOR_CARD, fg=COLOR_STEP2,
                 font=("Segoe UI", 9, "bold")).pack(side="left")
        self.repeat_entry = tk.Entry(name_row, width=4, font=("Segoe UI", 10), justify="center")
        self.repeat_entry.insert(0, str(DEFAULT_REPEAT_COUNT))
        self.repeat_entry.pack(side="left", padx=(8, 12))

        tk.Label(name_row, text="간격(초)", bg=COLOR_CARD, fg=COLOR_STEP2,
                 font=("Segoe UI", 9, "bold")).pack(side="left")
        self.interval_entry = tk.Entry(name_row, width=5, font=("Segoe UI", 10), justify="center")
        self.interval_entry.insert(0, str(DEFAULT_REPEAT_INTERVAL))
        self.interval_entry.pack(side="left", padx=(8, 12))

        self.set_btn = ttk.Button(name_row, text="⚙️ 설정", style="Secondary.TButton",
                                    command=self.on_set_clicked, state="disabled")
        self.set_btn.pack(side="left")

        self.set_status_label = tk.Label(step3, text="먼저 연결을 완료해야 설정할 수 있습니다",
                                          bg=COLOR_CARD, fg=COLOR_MUTED, font=("Segoe UI", 9))
        self.set_status_label.pack(anchor="w")

        command_row = tk.Frame(step3, bg=COLOR_CARD)
        command_row.pack(fill="x", pady=(8, 0))
        tk.Label(command_row, text="예약 시각(HH:MM:SS, 선택)", bg=COLOR_CARD, fg=COLOR_MUTED,
                 font=("Segoe UI", 9)).pack(side="left")
        self.schedule_entry = tk.Entry(command_row, width=12, font=("Consolas", 9), justify="center")
        self.schedule_entry.pack(side="left", padx=(8, 12))
        self.stop_btn = ttk.Button(command_row, text="⏹ 긴급 정지", style="Danger.TButton",
                                    command=self.on_emergency_stop, state="disabled")
        self.stop_btn.pack(side="left")
        ttk.Button(command_row, text="실행 이력 지우기", style="Secondary.TButton",
                   command=self.on_clear_history).pack(side="right")

        history_frame = tk.Frame(step3, bg=COLOR_CARD)
        history_frame.pack(fill="x", pady=(8, 0))
        tk.Label(history_frame, text="실행 이력", bg=COLOR_CARD, fg=COLOR_MUTED,
                 font=("Segoe UI", 9, "bold")).pack(anchor="w")
        self.history_list = tk.Listbox(history_frame, height=3, font=("Consolas", 9),
                                       relief="flat", bg="#F8FAFF", fg=COLOR_TEXT)
        self.history_list.pack(fill="x")
        self.history_list.bind("<Double-Button-1>", self.on_history_replay)

        # ── STEP 4: 연결 관리 ───────────────────────────────────
        step4 = self._make_step_card(body, 4, "연결 관리", COLOR_DANGER)
        exit_row = tk.Frame(step4, bg=COLOR_CARD)
        exit_row.pack(fill="x")
        self.disconnect_btn = ttk.Button(exit_row, text="🔌 연결 해제", style="Secondary.TButton",
                           command=self.on_disconnect_clicked, state="disabled")
        self.disconnect_btn.pack(side="left")
        self.exit_btn = ttk.Button(exit_row, text="🔴 프로그램 종료", style="Danger.TButton",
                                     command=self.on_exit_clicked)
        self.exit_btn.pack(side="left", padx=(8, 0))
        tk.Label(exit_row, text="연결 해제는 창을 유지하고, 프로그램 종료는 창을 닫습니다",
                  bg=COLOR_CARD, fg=COLOR_MUTED, font=("Segoe UI", 9)).pack(side="left", padx=10)

        # ── STEP 5: 로그 (짙은 회색, 남은 공간을 채움) ────────────
        step5 = self._make_step_card(body, 5, "실행 로그", COLOR_STEP4, expand=False)
        filter_row = tk.Frame(step5, bg=COLOR_CARD)
        filter_row.pack(fill="x", pady=(0, 5))
        tk.Label(filter_row, text="필터", bg=COLOR_CARD, fg=COLOR_MUTED,
                 font=("Segoe UI", 9)).pack(side="left")
        self.log_filter = ttk.Combobox(filter_row, textvariable=self.log_filter_var,
                                       values=("전체", "성공", "실패", "연결", "실행", "경고"),
                                       state="readonly", width=8)
        self.log_filter.pack(side="left", padx=6)
        self.log_filter.bind("<<ComboboxSelected>>", lambda _e: self._render_logs())
        ttk.Button(filter_row, text="로그 저장", style="Secondary.TButton",
                   command=self.on_save_log).pack(side="right")
        self.log_text = scrolledtext.ScrolledText(
            step5, state="disabled", font=("Consolas", 9), height=8,
            bg=LOG_BG, fg=LOG_FG, insertbackground=LOG_FG, relief="flat", highlightthickness=0,
        )
        self.log_text.pack(fill="both", expand=True)
        self._configure_log_tags()

    def _configure_log_tags(self):
        self.log_text.tag_configure("success", foreground=LOG_COLOR_SUCCESS)
        self.log_text.tag_configure("error", foreground=LOG_COLOR_ERROR)
        self.log_text.tag_configure("warning", foreground=LOG_COLOR_WARNING)
        self.log_text.tag_configure("info", foreground=LOG_COLOR_INFO)
        self.log_text.tag_configure("system", foreground=LOG_COLOR_SYSTEM, font=("Consolas", 9, "bold"))
        self.log_text.tag_configure("default", foreground=LOG_FG)

    # ---------------------------------------------------
    # 유틸
    # ---------------------------------------------------
    def _get_serial_list(self):
        """개별 입력 필드에서 시리얼 번호를 가져온다."""
        seen = set()
        serials = []
        duplicates = []
        for idx, entry in enumerate(self.serial_entries):
            serial = entry.get().strip()
            if not serial:
                continue
            if serial in seen:
                duplicates.append(serial)
                continue
            seen.add(serial)
            serials.append(serial)
        if duplicates:
            self._append_log(f">>> ⚠️ 중복된 시리얼 번호는 무시했습니다: {', '.join(duplicates)}")
        return serials

    @staticmethod
    def _classify_log(text):
        """로그 문구에 포함된 이모지/키워드로 색상 태그를 자동 결정한다."""
        if "❌" in text:
            return "error"
        if "⚠️" in text or "🛑" in text:
            return "warning"
        if "✅" in text or "완료" in text or "성공" in text:
            return "success"
        if "🚀" in text or "🎬" in text or "=====" in text:
            return "system"
        if "⏳" in text or ">>>" in text:
            return "info"
        return "default"

    def _append_log(self, text):
        tag = self._classify_log(text)
        overflow = len(self.log_entries) >= MAX_LOG_ENTRIES
        removed = self.log_entries.pop(0) if overflow else None
        self.log_entries.append((text, tag))
        selected = self.log_filter_var.get()
        if overflow:
            if self._log_entry_matches(removed, selected):
                self.log_text.configure(state="normal")
                self.log_text.delete("1.0", "2.0")
                self.log_text.configure(state="disabled")
        if self._log_entry_matches((text, tag), selected):
            self.log_text.configure(state="normal")
            self.log_text.insert("end", text + "\n", tag)
            self.log_text.see("end")
            self.log_text.configure(state="disabled")

    @staticmethod
    def _log_entry_matches(entry, selected):
        if entry is None:
            return False
        text, tag = entry
        if selected == "전체":
            return True
        if selected == "성공":
            return tag == "success"
        if selected == "실패":
            return tag == "error"
        if selected == "경고":
            return tag == "warning"
        if selected == "연결":
            return "연결" in text or "해제" in text or "상태" in text
        if selected == "실행":
            return "실행" in text or "동작" in text or "중지" in text
        return False

    def _render_logs(self):
        selected = self.log_filter_var.get()
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", "end")
        for text, tag in self.log_entries:
            if self._log_entry_matches((text, tag), selected):
                self.log_text.insert("end", text + "\n", tag)
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def _load_profiles(self):
        try:
            with open(self.profiles_path, "r", encoding="utf-8") as profile_file:
                profiles = json.load(profile_file)
        except FileNotFoundError:
            return {}
        except (OSError, UnicodeError, json.JSONDecodeError) as err:
            logging.warning("프로필 파일을 읽지 못했습니다 (%s): %s", self.profiles_path, err)
            return {}

        if not isinstance(profiles, dict):
            logging.warning("프로필 파일의 최상위 값은 객체여야 합니다: %s", self.profiles_path)
            return {}

        valid_profiles = {}
        for name, serials in profiles.items():
            if (
                isinstance(name, str)
                and isinstance(serials, list)
                and all(isinstance(serial, str) for serial in serials)
            ):
                valid_profiles[name] = serials
            else:
                logging.warning("잘못된 프로필 항목을 무시합니다: %r", name)
        return valid_profiles

    def on_save_profile(self):
        name = self.profile_name.get().strip() or "기본 프로필"
        serials = [entry.get().strip() for entry in self.serial_entries]
        self.profiles[name] = serials
        try:
            with open(self.profiles_path, "w", encoding="utf-8") as profile_file:
                json.dump(self.profiles, profile_file, ensure_ascii=False, indent=2)
            self._append_log(f">>> ✅ 프로필 저장: {name}")
        except OSError as err:
            messagebox.showerror("프로필 저장 실패", str(err))

    def on_load_profile(self):
        name = self.profile_name.get().strip()
        serials = self.profiles.get(name)
        if serials is None:
            messagebox.showwarning("프로필 없음", f"'{name}' 프로필을 찾을 수 없습니다.")
            return
        for entry in self.serial_entries:
            entry.delete(0, "end")
        for entry, serial in zip(self.serial_entries, serials):
            entry.insert(0, serial)
        self._append_log(f">>> ✅ 프로필 불러오기: {name}")

    def _update_robot_state(self, serial, state):
        self.robot_states[serial] = state
        label = self.robot_status_labels.get(serial)
        if label is None:
            return
        colors = {"연결 완료": COLOR_SUCCESS, "연결 실패": COLOR_DANGER,
                  "실행 완료": COLOR_PRIMARY, "실행 오류": COLOR_DANGER,
                  "연결 해제": COLOR_MUTED}
        label.config(text=f"{serial}  {state}", fg=colors.get(state, COLOR_WARNING))

    def _build_robot_dashboard(self, serials):
        for child in self.dashboard_frame.winfo_children():
            child.destroy()
        self.robot_status_labels = {}
        for index, serial in enumerate(serials):
            label = tk.Label(self.dashboard_frame, text=f"{serial}  대기",
                             bg="#F8FAFF", fg=COLOR_MUTED,
                             relief="solid", bd=1, font=("Consolas", 8))
            label.grid(row=index // 10, column=index % 10, padx=(0, 8), pady=3, sticky="w")
            self.robot_status_labels[serial] = label
            self.robot_states[serial] = "대기"

    def on_save_log(self):
        try:
            path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               f"swarm_log_{datetime.now():%Y%m%d_%H%M%S}.txt")
            with open(path, "w", encoding="utf-8") as log_file:
                log_file.write("\n".join(text for text, _tag in self.log_entries))
            self._append_log(f">>> ✅ 로그 저장: {os.path.basename(path)}")
        except OSError as err:
            messagebox.showerror("로그 저장 실패", str(err))

    def on_clear_history(self):
        self.history.clear()
        self.history_list.delete(0, "end")

    def on_history_replay(self, _event=None):
        selection = self.history_list.curselection()
        if not selection:
            return
        record = self.history[selection[0]]
        self.name_entry.delete(0, "end")
        self.name_entry.insert(0, record["action"])
        self.audio_entry.delete(0, "end")
        self.audio_entry.insert(0, record["audio"])
        self._append_log(">>> 이전 실행 설정을 불러왔습니다. 설정 버튼으로 다시 예약하세요.")

    # ---------------------------------------------------
    # STEP 2: 연결 버튼 핸들러
    # ---------------------------------------------------
    def on_connect_clicked(self):
        if self.session_active:
            messagebox.showinfo("이미 연결 중", "이미 연결 세션이 진행 중입니다.")
            return

        serials = self._get_serial_list()
        if not serials:
            messagebox.showwarning("입력 필요", "로봇 시리얼 번호를 최소 1개 입력하세요.")
            return

        self.connect_btn.config(state="disabled")
        self.set_btn.config(state="disabled")
        self.set_status_label.config(text="먼저 연결을 완료해야 설정할 수 있습니다", fg=COLOR_MUTED)
        self.progress_var.set(0)
        self.progress_bar.configure(style="Blue.Horizontal.TProgressbar")
        self.progress_label.config(text=f"0/{len(serials)}")
        self._append_log(f"===== 연결 시작 (총 {len(serials)}대) =====")
        self._session_serials = serials
        self._build_robot_dashboard(serials)

        # 이전 세션의 매니저가 남아있다면 정리
        if self.manager is not None:
            try:
                self.manager.shutdown()
            except Exception:
                pass

        self.manager = multiprocessing.Manager()
        self.shared = self.manager.dict()
        self.shared["commands"] = self.manager.list()
        self.shared["stop_id"] = 0
        self.quit_event = multiprocessing.Event()
        self.session_active = True
        self._shutdown_requested = False
        self._last_connected = 0
        self._last_total = len(serials)
        self.stop_btn.config(state="normal")
        self.disconnect_btn.config(state="normal")

        threading.Thread(
            target=run_swarm_session,
            args=(serials, self.log_q, self.progress_q, self.quit_event, self.shared),
            daemon=True,
        ).start()

    # ---------------------------------------------------
    # STEP 3: 설정 버튼 핸들러 (이름 지정 -> 일정 시간 후 트리거)
    # ---------------------------------------------------
    def on_set_clicked(self):
        if not self.session_active or self.shared is None:
            messagebox.showwarning("연결 필요", "먼저 '연결'을 눌러 로봇에 접속하세요.")
            return

        action_name = self.name_entry.get().strip()
        audio_name = self.audio_entry.get().strip()
        if not action_name and not audio_name:
            messagebox.showwarning("입력 필요", "동작 또는 음원 이름을 입력하세요.")
            return

        try:
            delay = float(self.delay_entry.get().strip())
            if not math.isfinite(delay) or not math.isfinite(delay * 1000) or delay < 0:
                raise ValueError
        except ValueError:
            messagebox.showwarning("입력 오류", "지연 시간(초)은 0 이상의 숫자로 입력하세요.")
            return

        schedule_text = self.schedule_entry.get().strip()
        if schedule_text:
            try:
                now = datetime.now()
                scheduled = datetime.strptime(schedule_text, "%H:%M:%S").replace(
                    year=now.year, month=now.month, day=now.day)
                if scheduled <= now:
                    scheduled += timedelta(days=1)
                delay = (scheduled - now).total_seconds()
            except ValueError:
                messagebox.showwarning("입력 오류", "예약 시각은 HH:MM:SS 형식으로 입력하세요.")
                return

        try:
            repeat = int(self.repeat_entry.get().strip())
            interval = float(self.interval_entry.get().strip())
            if (
                repeat < 1
                or not math.isfinite(interval)
                or not math.isfinite(interval * 1000)
                or interval < 0
            ):
                raise ValueError
        except ValueError:
            messagebox.showwarning("입력 오류", "반복 횟수는 1 이상, 간격은 0 이상이어야 합니다.")
            return

        # 이전에 예약된 카운트다운이 있다면 취소하고 새로 예약한다
        if self._pending_after_id is not None:
            try:
                self.root.after_cancel(self._pending_after_id)
            except Exception:
                pass
            self._pending_after_id = None

        self._scheduled_action = action_name
        self._scheduled_audio = audio_name
        self._scheduled_repeat = repeat
        self._scheduled_interval = interval
        self._append_log(f">>> ⚙️ 설정 완료: 동작 '{action_name or '-'}', 음원 '{audio_name or '-'}' — {delay:.1f}초 후 실행합니다.")
        self.set_status_label.config(text=f"⏳ {delay:.1f}초 후 실행 예정 · {repeat}회", fg=COLOR_PRIMARY)
        self.set_btn.config(state="disabled")

        self._pending_after_id = self.root.after(int(delay * 1000), self._trigger_play, 1)

    def _trigger_play(self, run_number):
        self._pending_after_id = None
        action = self._scheduled_action
        audio = self._scheduled_audio
        if self.shared is not None:
            self.shared["commands"].append((action, audio))
        self._append_log(f">>> 🚀 실행 신호 전송 {run_number}/{self._scheduled_repeat}: '{action or '-'}' / '{audio or '-'}'")
        self.set_status_label.config(text=f"✅ {run_number}/{self._scheduled_repeat} 실행 신호 전송", fg=COLOR_SUCCESS)
        self.history.append({"time": datetime.now().strftime("%H:%M:%S"), "action": action, "audio": audio})
        self.history_list.insert("end", f"{self.history[-1]['time']}  {action or '-'} / {audio or '-'}")
        if run_number < self._scheduled_repeat and self.session_active:
            self._pending_after_id = self.root.after(int(self._scheduled_interval * 1000),
                                                     self._trigger_play, run_number + 1)
        elif self.session_active:
            self.set_btn.config(state="normal")

    def on_emergency_stop(self):
        if self.shared is None or not self.session_active:
            return
        self.shared["stop_id"] = self.shared.get("stop_id", 0) + 1
        if self._pending_after_id is not None:
            try:
                self.root.after_cancel(self._pending_after_id)
            except Exception:
                pass
            self._pending_after_id = None
        self.set_btn.config(state="normal")
        self.set_status_label.config(text="⏹ 긴급 정지 신호를 전송했습니다", fg=COLOR_DANGER)
        self._append_log(">>> ⏹ 긴급 정지 신호 전송")

    # ---------------------------------------------------
    # STEP 4: 종료 버튼 핸들러
    # ---------------------------------------------------
    def on_disconnect_clicked(self):
        self._request_shutdown(close_after=False)

    def on_exit_clicked(self):
        self._request_shutdown(close_after=True)

    def _request_shutdown(self, close_after):
        if self.session_active and self.quit_event is not None:
            if self.quit_event.is_set():
                self._shutdown_requested = self._shutdown_requested or close_after
                if close_after:
                    self._append_log(">>> 프로그램 종료를 요청했습니다. 연결 해제가 끝나면 창을 닫습니다.")
                return

            if self._pending_after_id is not None:
                try:
                    self.root.after_cancel(self._pending_after_id)
                except Exception:
                    pass
                self._pending_after_id = None

            if close_after:
                self._append_log(">>> 프로그램 종료 요청: 모든 로봇의 연결을 해제하는 중입니다...")
            else:
                self._append_log(">>> 연결 해제 요청: 모든 로봇의 연결을 해제합니다. 프로그램은 계속 실행됩니다.")
            self.quit_event.set()
            self._shutdown_requested = close_after
            self.connect_btn.config(state="disabled")
            self.set_btn.config(state="disabled")
            self.stop_btn.config(state="disabled")
            self.disconnect_btn.config(state="disabled")
            self.set_status_label.config(text="연결 해제 중...", fg=COLOR_WARNING)
            return

        if close_after:
            self._append_log(">>> 연결된 로봇이 없어 프로그램을 종료합니다.")
            if self.manager is not None:
                try:
                    self.manager.shutdown()
                except Exception:
                    pass
                self.manager = None
                self.shared = None
            self.root.after(300, self.root.destroy)
        else:
            self._append_log(">>> 현재 연결된 로봇이 없습니다.")

    # ---------------------------------------------------
    # 백그라운드 스레드의 로그/진행률을 주기적으로 화면에 반영
    # ---------------------------------------------------
    def _poll_queues(self):
        try:
            while True:
                line = self.log_q.get_nowait()
                if line == "__DONE__":
                    self.session_active = False
                    self.connect_btn.config(state="normal")
                    self.set_btn.config(state="disabled")
                    self.stop_btn.config(state="disabled")
                    self.disconnect_btn.config(state="disabled")
                    self.exit_btn.config(state="normal")
                    self.set_status_label.config(text="연결 해제 완료. 다시 연결할 수 있습니다.", fg=COLOR_MUTED)
                    if self.manager is not None:
                        try:
                            self.manager.shutdown()
                        except Exception:
                            pass
                        self.manager = None
                        self.shared = None
                    if self._shutdown_requested:
                        self._append_log(">>> 프로그램을 종료합니다.")
                        self.root.after(500, self.root.destroy)
                elif line == "__CONNECT_PHASE_DONE__":
                    if self._last_connected > 0 and self.session_active:
                        self.set_btn.config(state="normal")
                        self.set_status_label.config(
                            text=f"✅ {self._last_connected}대 연결됨 — 이름/지연 시간을 입력하고 설정을 누르세요",
                            fg=COLOR_SUCCESS,
                        )
                    else:
                        self.set_status_label.config(text="❌ 연결된 로봇이 없습니다", fg=COLOR_DANGER)
                else:
                    self._append_log(line)
        except queue.Empty:
            pass

        try:
            while True:
                progress = self.progress_q.get_nowait()
                if progress and progress[0] == "state":
                    _, serial, state = progress
                    self._update_robot_state(serial, state)
                    continue
                connected, failed, total = progress
                self._last_connected = connected
                self._last_total = total
                done = connected + failed
                pct = (done / total * 100) if total else 0
                self.progress_var.set(pct)
                self.progress_label.config(text=f"{done}/{total} (성공 {connected})")

                if total and done >= total:
                    style_name = "Green.Horizontal.TProgressbar" if failed == 0 else "Orange.Horizontal.TProgressbar"
                else:
                    style_name = "Blue.Horizontal.TProgressbar"
                self.progress_bar.configure(style=style_name)
        except queue.Empty:
            pass

        self.root.after(150, self._poll_queues)

if __name__ == '__main__':
    # Windows에서 PyInstaller 등으로 빌드할 경우 필요 (일반 실행에는 무해)
    multiprocessing.freeze_support()

    root = tk.Tk()
    app = SwarmControlApp(root)
    root.mainloop()
