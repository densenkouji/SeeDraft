use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU32, Ordering},
        mpsc,
    },
    time::Duration,
};

use serde::{Deserialize, Serialize};
use tauri::{
    AppHandle, Emitter, Manager, PhysicalPosition, Runtime,
    menu::MenuBuilder,
    tray::{MouseButton, MouseButtonState, TrayIconBuilder, TrayIconEvent},
};
use tauri_plugin_global_shortcut::{GlobalShortcutExt, Shortcut, ShortcutState};
const TRAY_MENU_SHOW: &str = "show";
const TRAY_MENU_QUIT: &str = "quit";
const GLOBAL_SHORTCUT_EVENT: &str = "seedraft:global-shortcut";
const GLOBAL_ACTION_SHOW_APP: &str = "app.show";
const GLOBAL_ACTION_OPEN_LIVE: &str = "live.open";
const GLOBAL_ACTION_RECORD_HOLD: &str = "record.hold";
pub(crate) const HOLD_OVERLAY_WINDOW_LABEL: &str = "hold_overlay";
pub(crate) const HOLD_OVERLAY_WIDTH: i32 = 260;
pub(crate) const HOLD_OVERLAY_HEIGHT: i32 = 118;

fn show_main_window<R: Runtime>(app: &AppHandle<R>) {
    if let Some(window) = app.get_webview_window("main") {
        if let Err(error) = window.show() {
            eprintln!("failed to show main window: {error}");
        }
        if matches!(window.is_minimized(), Ok(true)) {
            if let Err(error) = window.unminimize() {
                eprintln!("failed to restore main window: {error}");
            }
        }
        if let Err(error) = window.set_focus() {
            eprintln!("failed to focus main window: {error}");
        }
    }
}

fn close_main_window_inner<R: Runtime>(app: &AppHandle<R>) -> Result<(), String> {
    if let Some(window) = app.get_webview_window("main") {
        window.hide().map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn show_main_screen<R: Runtime>(app: &AppHandle<R>) {
    show_main_window(app);
    let Some(window) = app.get_webview_window("main") else {
        return;
    };
    if let Err(error) = window.eval(
        "if (window.seedraftShowMainScreen) { window.seedraftShowMainScreen(); } else { window.__seedraftPendingShowMainScreen = true; }",
    ) {
        eprintln!("failed to show main screen from shortcut: {error}");
    }
}

fn open_live_overlay_window<R: Runtime>(app: &AppHandle<R>) {
    show_main_window(app);
    let Some(window) = app.get_webview_window("main") else {
        return;
    };
    if let Err(error) = window.eval(
        "if (window.seedraftOpenLiveOverlay) { window.seedraftOpenLiveOverlay(); } else { window.__seedraftPendingOpenLiveOverlay = true; }",
    ) {
        eprintln!("failed to open live overlay from shortcut: {error}");
    }
}

pub(crate) struct AppExitState {
    quit_requested: Arc<AtomicBool>,
}

impl AppExitState {
    pub(crate) fn new(quit_requested: Arc<AtomicBool>) -> Self {
        Self { quit_requested }
    }
}

pub(crate) fn build_system_tray<R: Runtime, M: Manager<R>>(
    manager: &M,
    quit_requested: Arc<AtomicBool>,
) -> tauri::Result<()> {
    let menu = MenuBuilder::new(manager)
        .text(TRAY_MENU_SHOW, "表示")
        .separator()
        .text(TRAY_MENU_QUIT, "終了")
        .build()?;

    let mut builder = TrayIconBuilder::new()
        .tooltip("SeeDraft")
        .menu(&menu)
        .show_menu_on_left_click(false)
        .on_tray_icon_event(|tray, event| {
            if let TrayIconEvent::Click {
                button: MouseButton::Left,
                button_state: MouseButtonState::Up,
                ..
            }
            | TrayIconEvent::DoubleClick {
                button: MouseButton::Left,
                ..
            } = event
            {
                show_main_window(tray.app_handle());
            }
        })
        .on_menu_event(move |app, event| {
            let id = event.id().as_ref();
            if id == TRAY_MENU_SHOW {
                show_main_window(app);
            } else if id == TRAY_MENU_QUIT {
                request_graceful_app_exit(app, &quit_requested, 0);
            }
        });

    if let Some(icon) = manager.app_handle().default_window_icon().cloned() {
        builder = builder.icon(icon);
    }

    builder.build(manager)?;
    Ok(())
}

pub(crate) fn close_webview_windows_for_exit<R: Runtime>(app: &AppHandle<R>) {
    if let Some(window) = app.get_webview_window(HOLD_OVERLAY_WINDOW_LABEL) {
        if let Err(error) = window.close() {
            eprintln!("failed to close hold overlay before exit: {error}");
        }
    }
    if let Some(window) = app.get_webview_window("main") {
        if let Err(error) = window.close() {
            eprintln!("failed to close main window before exit: {error}");
        }
    }
}

fn request_graceful_app_exit<R: Runtime>(
    app: &AppHandle<R>,
    quit_requested: &Arc<AtomicBool>,
    exit_code: i32,
) {
    quit_requested.store(true, Ordering::SeqCst);
    close_webview_windows_for_exit(app);
    let app = app.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(150));
        app.exit(exit_code);
    });
}

#[derive(Default)]
pub(crate) struct GlobalShortcutRegistry {
    shortcuts: Mutex<HashMap<u32, RegisteredGlobalShortcut>>,
}

#[derive(Clone)]
struct RegisteredGlobalShortcut {
    action: String,
    accelerator: String,
    shortcut: Shortcut,
}

#[derive(Debug, Deserialize)]
pub(crate) struct GlobalShortcutBinding {
    action: String,
    accelerator: String,
}

#[derive(Clone, Debug, Serialize)]
struct GlobalShortcutPayload {
    action: String,
    accelerator: String,
    state: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HoldRecordOutputMode {
    Insert,
    Copy,
    Both,
}

#[derive(Debug, Deserialize)]
pub(crate) struct HoldRecordCompletion {
    text: String,
    mode: HoldRecordOutputMode,
}

fn is_global_shortcut_action(action: &str) -> bool {
    matches!(
        action,
        GLOBAL_ACTION_SHOW_APP | GLOBAL_ACTION_OPEN_LIVE | GLOBAL_ACTION_RECORD_HOLD
    )
}

fn shortcut_state_label(state: ShortcutState) -> &'static str {
    match state {
        ShortcutState::Pressed => "pressed",
        ShortcutState::Released => "released",
    }
}

fn run_record_hold_shortcut<R: Runtime>(
    app: &AppHandle<R>,
    accelerator: String,
    state: ShortcutState,
) {
    match state {
        ShortcutState::Pressed => {
            if let Err(error) = show_hold_recording_overlay_inner(app, "preparing") {
                eprintln!("failed to show hold recording overlay: {error}");
            }
            if let Some(window) = app.get_webview_window(HOLD_OVERLAY_WINDOW_LABEL) {
                if let Err(error) = window.eval(
                    "if (window.seedraftHoldOverlayShortcutPress) { window.seedraftHoldOverlayShortcutPress(); } else { window.__seedraftPendingHoldPress = true; }",
                ) {
                    eprintln!("failed to start hold recording from overlay: {error}");
                }
            }
        }
        ShortcutState::Released => {
            if let Some(window) = app.get_webview_window(HOLD_OVERLAY_WINDOW_LABEL) {
                if let Err(error) = window.eval(
                    "if (window.seedraftHoldOverlayShortcutRelease) { window.seedraftHoldOverlayShortcutRelease(); } else { window.__seedraftPendingHoldRelease = true; }",
                ) {
                    eprintln!("failed to stop hold recording from overlay: {error}");
                }
            }
        }
    }

    emit_global_shortcut_payload(
        app,
        GLOBAL_ACTION_RECORD_HOLD.to_string(),
        accelerator,
        state,
    );
}

fn emit_global_shortcut_payload<R: Runtime>(
    app: &AppHandle<R>,
    action: String,
    accelerator: String,
    state: ShortcutState,
) {
    if let Err(error) = app.emit(
        GLOBAL_SHORTCUT_EVENT,
        GlobalShortcutPayload {
            action,
            accelerator,
            state: shortcut_state_label(state).to_string(),
        },
    ) {
        eprintln!("failed to emit global shortcut event: {error}");
    }
}

pub(crate) fn handle_global_shortcut<R: Runtime>(
    app: &AppHandle<R>,
    shortcut: &Shortcut,
    state: ShortcutState,
) {
    let registration = {
        let registry = app.state::<GlobalShortcutRegistry>();
        registry
            .shortcuts
            .lock()
            .unwrap()
            .get(&shortcut.id())
            .cloned()
    };

    let Some(registration) = registration else {
        return;
    };

    match (registration.action.as_str(), state) {
        (GLOBAL_ACTION_SHOW_APP, ShortcutState::Pressed) => {
            show_main_screen(app);
        }
        (GLOBAL_ACTION_OPEN_LIVE, ShortcutState::Pressed) => {
            open_live_overlay_window(app);
        }
        (GLOBAL_ACTION_RECORD_HOLD, ShortcutState::Pressed) => {
            run_record_hold_shortcut(app, registration.accelerator, state);
            return;
        }
        (GLOBAL_ACTION_RECORD_HOLD, ShortcutState::Released) => {
            run_record_hold_shortcut(app, registration.accelerator, state);
            return;
        }
        _ => {}
    }

    emit_global_shortcut_payload(app, registration.action, registration.accelerator, state);
}

#[tauri::command]
pub(crate) fn close_main_window<R: Runtime>(app: AppHandle<R>) -> Result<(), String> {
    close_main_window_inner(&app)
}

#[tauri::command]
pub(crate) fn quit_app<R: Runtime>(
    app: AppHandle<R>,
    exit_state: tauri::State<'_, AppExitState>,
) -> Result<(), String> {
    request_graceful_app_exit(&app, &exit_state.quit_requested, 0);
    Ok(())
}

#[cfg(windows)]
const HOLD_KEY_NONE: u32 = 0;
#[cfg(windows)]
const HOLD_KEY_CTRL_ANY: u32 = 1;
#[cfg(windows)]
const HOLD_KEY_CTRL_LEFT: u32 = 2;
#[cfg(windows)]
const HOLD_KEY_CTRL_RIGHT: u32 = 3;
#[cfg(windows)]
const HOLD_KEY_ALT_ANY: u32 = 4;
#[cfg(windows)]
const HOLD_KEY_ALT_LEFT: u32 = 5;
#[cfg(windows)]
const HOLD_KEY_ALT_RIGHT: u32 = 6;
#[cfg(windows)]
const HOLD_KEY_SHIFT_ANY: u32 = 7;
#[cfg(windows)]
const HOLD_KEY_SHIFT_LEFT: u32 = 8;
#[cfg(windows)]
const HOLD_KEY_SHIFT_RIGHT: u32 = 9;
#[cfg(windows)]
const HOLD_KEY_META_ANY: u32 = 10;
#[cfg(windows)]
const HOLD_KEY_META_LEFT: u32 = 11;
#[cfg(windows)]
const HOLD_KEY_META_RIGHT: u32 = 12;

#[cfg(windows)]
#[derive(Clone)]
struct NativeHoldKeyEvent {
    accelerator: String,
    state: ShortcutState,
}

#[cfg(windows)]
static NATIVE_HOLD_KEY_KIND: AtomicU32 = AtomicU32::new(HOLD_KEY_NONE);
#[cfg(windows)]
static NATIVE_HOLD_KEY_IS_DOWN: AtomicBool = AtomicBool::new(false);
#[cfg(windows)]
static NATIVE_HOLD_KEY_ACCELERATOR: OnceLock<Mutex<String>> = OnceLock::new();
#[cfg(windows)]
static NATIVE_HOLD_KEY_EVENT_TX: OnceLock<mpsc::Sender<NativeHoldKeyEvent>> = OnceLock::new();
#[cfg(windows)]
static NATIVE_HOLD_KEY_HOOK_STARTED: AtomicBool = AtomicBool::new(false);

#[cfg(windows)]
fn native_hold_key_accelerator() -> String {
    NATIVE_HOLD_KEY_ACCELERATOR
        .get_or_init(|| Mutex::new(String::new()))
        .lock()
        .unwrap()
        .clone()
}

#[cfg(windows)]
fn set_native_hold_key_accelerator(value: &str) {
    *NATIVE_HOLD_KEY_ACCELERATOR
        .get_or_init(|| Mutex::new(String::new()))
        .lock()
        .unwrap() = value.to_string();
}

#[cfg(windows)]
fn native_hold_key_kind(accelerator: &str) -> Option<u32> {
    let normalized = accelerator
        .trim()
        .chars()
        .filter(|ch| !matches!(ch, ' ' | '-' | '_'))
        .collect::<String>()
        .to_ascii_lowercase();
    match normalized.as_str() {
        "ctrl" | "control" => Some(HOLD_KEY_CTRL_ANY),
        "leftctrl" | "leftcontrol" | "ctrlleft" | "controlleft" => Some(HOLD_KEY_CTRL_LEFT),
        "rightctrl" | "rightcontrol" | "ctrlright" | "controlright" => Some(HOLD_KEY_CTRL_RIGHT),
        "alt" | "option" => Some(HOLD_KEY_ALT_ANY),
        "leftalt" | "leftoption" | "altleft" | "optionleft" => Some(HOLD_KEY_ALT_LEFT),
        "rightalt" | "rightoption" | "altright" | "optionright" => Some(HOLD_KEY_ALT_RIGHT),
        "shift" => Some(HOLD_KEY_SHIFT_ANY),
        "leftshift" | "shiftleft" => Some(HOLD_KEY_SHIFT_LEFT),
        "rightshift" | "shiftright" => Some(HOLD_KEY_SHIFT_RIGHT),
        "meta" | "super" | "win" | "windows" | "command" | "cmd" => Some(HOLD_KEY_META_ANY),
        "leftmeta" | "leftsuper" | "leftwin" | "metaleft" | "superleft" | "winleft" => {
            Some(HOLD_KEY_META_LEFT)
        }
        "rightmeta" | "rightsuper" | "rightwin" | "metaright" | "superright" | "winright" => {
            Some(HOLD_KEY_META_RIGHT)
        }
        _ => None,
    }
}

#[cfg(windows)]
fn is_native_hold_key_binding(accelerator: &str) -> bool {
    native_hold_key_kind(accelerator).is_some()
}

#[cfg(not(windows))]
fn is_native_hold_key_binding(_accelerator: &str) -> bool {
    false
}

#[cfg(windows)]
fn apply_native_hold_key_binding(accelerator: Option<String>) {
    let (kind, label) = accelerator
        .as_deref()
        .and_then(|value| native_hold_key_kind(value).map(|kind| (kind, value.trim().to_string())))
        .unwrap_or((HOLD_KEY_NONE, String::new()));

    if NATIVE_HOLD_KEY_IS_DOWN.swap(false, Ordering::SeqCst) {
        send_native_hold_key_event(ShortcutState::Released);
    }
    set_native_hold_key_accelerator(&label);
    NATIVE_HOLD_KEY_KIND.store(kind, Ordering::SeqCst);
}

#[cfg(not(windows))]
fn apply_native_hold_key_binding(_accelerator: Option<String>) {}

#[cfg(windows)]
fn send_native_hold_key_event(state: ShortcutState) {
    let Some(tx) = NATIVE_HOLD_KEY_EVENT_TX.get() else {
        return;
    };
    let accelerator = native_hold_key_accelerator();
    if accelerator.is_empty() {
        return;
    }
    if let Err(error) = tx.send(NativeHoldKeyEvent { accelerator, state }) {
        eprintln!("failed to dispatch native hold key event: {error}");
    }
}

#[cfg(windows)]
fn native_hold_key_matches(kind: u32, vk: u16, scan_code: u32, flags: u32) -> bool {
    use windows_sys::Win32::UI::{
        Input::KeyboardAndMouse::{
            VK_CONTROL, VK_LCONTROL, VK_LMENU, VK_LSHIFT, VK_LWIN, VK_MENU, VK_RCONTROL, VK_RMENU,
            VK_RSHIFT, VK_RWIN, VK_SHIFT,
        },
        WindowsAndMessaging::LLKHF_EXTENDED,
    };

    let extended = (flags & LLKHF_EXTENDED) != 0;
    match kind {
        HOLD_KEY_CTRL_ANY => vk == VK_CONTROL || vk == VK_LCONTROL || vk == VK_RCONTROL,
        HOLD_KEY_CTRL_LEFT => vk == VK_LCONTROL || (vk == VK_CONTROL && !extended),
        HOLD_KEY_CTRL_RIGHT => vk == VK_RCONTROL || (vk == VK_CONTROL && extended),
        HOLD_KEY_ALT_ANY => vk == VK_MENU || vk == VK_LMENU || vk == VK_RMENU,
        HOLD_KEY_ALT_LEFT => vk == VK_LMENU || (vk == VK_MENU && !extended),
        HOLD_KEY_ALT_RIGHT => vk == VK_RMENU || (vk == VK_MENU && extended),
        HOLD_KEY_SHIFT_ANY => vk == VK_SHIFT || vk == VK_LSHIFT || vk == VK_RSHIFT,
        HOLD_KEY_SHIFT_LEFT => vk == VK_LSHIFT || (vk == VK_SHIFT && scan_code != 0x36),
        HOLD_KEY_SHIFT_RIGHT => vk == VK_RSHIFT || (vk == VK_SHIFT && scan_code == 0x36),
        HOLD_KEY_META_ANY => vk == VK_LWIN || vk == VK_RWIN,
        HOLD_KEY_META_LEFT => vk == VK_LWIN,
        HOLD_KEY_META_RIGHT => vk == VK_RWIN,
        _ => false,
    }
}

#[cfg(windows)]
unsafe extern "system" fn native_hold_keyboard_proc(
    code: i32,
    wparam: windows_sys::Win32::Foundation::WPARAM,
    lparam: windows_sys::Win32::Foundation::LPARAM,
) -> windows_sys::Win32::Foundation::LRESULT {
    use windows_sys::Win32::UI::WindowsAndMessaging::{
        CallNextHookEx, HC_ACTION, KBDLLHOOKSTRUCT, WM_KEYDOWN, WM_KEYUP, WM_SYSKEYDOWN,
        WM_SYSKEYUP,
    };

    if code == HC_ACTION as i32 {
        let kind = NATIVE_HOLD_KEY_KIND.load(Ordering::SeqCst);
        if kind != HOLD_KEY_NONE {
            let info = unsafe { &*(lparam as *const KBDLLHOOKSTRUCT) };
            if native_hold_key_matches(kind, info.vkCode as u16, info.scanCode, info.flags) {
                let message = wparam as u32;
                let is_down = message == WM_KEYDOWN || message == WM_SYSKEYDOWN;
                let is_up = message == WM_KEYUP || message == WM_SYSKEYUP;
                if is_down && !NATIVE_HOLD_KEY_IS_DOWN.swap(true, Ordering::SeqCst) {
                    send_native_hold_key_event(ShortcutState::Pressed);
                } else if is_up && NATIVE_HOLD_KEY_IS_DOWN.swap(false, Ordering::SeqCst) {
                    send_native_hold_key_event(ShortcutState::Released);
                }
            }
        }
    }

    unsafe { CallNextHookEx(std::ptr::null_mut(), code, wparam, lparam) }
}

#[cfg(windows)]
pub(crate) fn start_native_hold_key_hook(app: AppHandle<tauri::Wry>) {
    if NATIVE_HOLD_KEY_HOOK_STARTED
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return;
    }

    let (tx, rx) = mpsc::channel::<NativeHoldKeyEvent>();
    let _ = NATIVE_HOLD_KEY_EVENT_TX.set(tx);

    std::thread::spawn(move || {
        while let Ok(event) = rx.recv() {
            run_record_hold_shortcut(&app, event.accelerator, event.state);
        }
    });

    std::thread::spawn(|| {
        use windows_sys::Win32::UI::WindowsAndMessaging::{
            GetMessageW, MSG, SetWindowsHookExW, UnhookWindowsHookEx, WH_KEYBOARD_LL,
        };

        unsafe {
            let hook = SetWindowsHookExW(
                WH_KEYBOARD_LL,
                Some(native_hold_keyboard_proc),
                std::ptr::null_mut(),
                0,
            );
            if hook.is_null() {
                eprintln!("failed to install native hold-key keyboard hook");
                return;
            }

            let mut message = std::mem::zeroed::<MSG>();
            while GetMessageW(&mut message, std::ptr::null_mut(), 0, 0) > 0 {}
            let _ = UnhookWindowsHookEx(hook);
        }
    });
}

#[cfg(not(windows))]
pub(crate) fn start_native_hold_key_hook<R: Runtime>(_app: AppHandle<R>) {}

#[tauri::command]
pub(crate) fn trigger_hold_recording_shortcut<R: Runtime>(
    app: AppHandle<R>,
    accelerator: String,
    state: String,
) -> Result<(), String> {
    let state = match state.as_str() {
        "pressed" => ShortcutState::Pressed,
        "released" => ShortcutState::Released,
        other => return Err(format!("unsupported hold shortcut state: {other}")),
    };
    run_record_hold_shortcut(&app, accelerator, state);
    Ok(())
}

#[tauri::command]
pub(crate) fn register_global_shortcuts<R: Runtime>(
    app: AppHandle<R>,
    registry: tauri::State<'_, GlobalShortcutRegistry>,
    bindings: Vec<GlobalShortcutBinding>,
) -> Result<(), String> {
    let mut parsed = Vec::new();
    let mut native_hold_binding = None;

    for binding in bindings {
        let action = binding.action.trim().to_string();
        let accelerator = binding.accelerator.trim().to_string();
        if action.is_empty() || accelerator.is_empty() || !is_global_shortcut_action(&action) {
            continue;
        }

        if action == GLOBAL_ACTION_RECORD_HOLD && is_native_hold_key_binding(&accelerator) {
            native_hold_binding = Some(accelerator);
            continue;
        }

        let shortcut = accelerator
            .parse::<Shortcut>()
            .map_err(|error| format!("{accelerator}: {error}"))?;
        parsed.push(RegisteredGlobalShortcut {
            action,
            accelerator,
            shortcut,
        });
    }

    let old_shortcuts = {
        let guard = registry.shortcuts.lock().unwrap();
        guard
            .values()
            .map(|registration| registration.shortcut)
            .collect::<Vec<_>>()
    };
    if !old_shortcuts.is_empty() {
        app.global_shortcut()
            .unregister_multiple(old_shortcuts)
            .map_err(|error| error.to_string())?;
    }
    {
        registry.shortcuts.lock().unwrap().clear();
    }

    let mut registered = HashMap::new();
    let mut newly_registered = Vec::new();
    for registration in parsed {
        let id = registration.shortcut.id();
        if registered.contains_key(&id) {
            continue;
        }
        if let Err(error) = app.global_shortcut().register(registration.shortcut) {
            if !newly_registered.is_empty() {
                let _ = app.global_shortcut().unregister_multiple(newly_registered);
            }
            registry.shortcuts.lock().unwrap().clear();
            return Err(format!("{}: {error}", registration.accelerator));
        }
        newly_registered.push(registration.shortcut);
        registered.insert(id, registration);
    }

    apply_native_hold_key_binding(native_hold_binding);
    *registry.shortcuts.lock().unwrap() = registered;
    Ok(())
}

fn show_hold_recording_overlay_inner<R: Runtime>(
    app: &AppHandle<R>,
    mode: &str,
) -> Result<(), String> {
    let Some(window) = app.get_webview_window(HOLD_OVERLAY_WINDOW_LABEL) else {
        return Ok(());
    };

    let cursor = app.cursor_position().map_err(|error| error.to_string())?;
    let mut x = cursor.x.round() as i32 + 22;
    let mut y = cursor.y.round() as i32 + 22;

    if let Ok(Some(monitor)) = app.monitor_from_point(cursor.x, cursor.y) {
        let origin = monitor.position();
        let size = monitor.size();
        let min_x = origin.x;
        let min_y = origin.y;
        let max_x = origin.x + size.width as i32 - HOLD_OVERLAY_WIDTH;
        let max_y = origin.y + size.height as i32 - HOLD_OVERLAY_HEIGHT;
        x = x.clamp(min_x, max_x.max(min_x));
        y = y.clamp(min_y, max_y.max(min_y));
    }

    window
        .set_position(PhysicalPosition::new(x, y))
        .map_err(|error| error.to_string())?;
    window
        .set_always_on_top(true)
        .map_err(|error| error.to_string())?;
    if let Err(error) = window.set_ignore_cursor_events(true) {
        eprintln!("failed to make hold overlay click-through: {error}");
    }
    window.show().map_err(|error| error.to_string())?;
    let script = format!(
        "if (window.seedraftHoldOverlayShow) {{ window.seedraftHoldOverlayShow({mode:?}); }} else {{ document.body.dataset.mode = {mode:?}; document.body.classList.add('is-visible'); }}"
    );
    if let Err(error) = window.eval(script) {
        eprintln!("failed to animate hold overlay: {error}");
    }
    Ok(())
}

#[tauri::command]
pub(crate) fn show_hold_recording_overlay<R: Runtime>(
    app: AppHandle<R>,
    mode: Option<String>,
) -> Result<(), String> {
    let mode = mode.as_deref().unwrap_or("preparing");
    show_hold_recording_overlay_inner(&app, mode)
}

#[tauri::command]
pub(crate) fn hide_hold_recording_overlay<R: Runtime>(app: AppHandle<R>) -> Result<(), String> {
    if let Some(window) = app.get_webview_window(HOLD_OVERLAY_WINDOW_LABEL) {
        if let Err(error) =
            window.eval("window.seedraftHoldOverlayHide && window.seedraftHoldOverlayHide();")
        {
            eprintln!("failed to animate hold overlay hide: {error}");
        }
        window.hide().map_err(|error| error.to_string())?;
    }
    Ok(())
}

#[tauri::command]
pub(crate) fn complete_hold_recording(input: HoldRecordCompletion) -> Result<(), String> {
    let text = input.text.trim();
    if text.is_empty() {
        return Ok(());
    }

    match input.mode {
        HoldRecordOutputMode::Copy => set_system_clipboard_text(text),
        HoldRecordOutputMode::Insert => {
            set_system_clipboard_text(text)?;
            insert_text_at_cursor(text)
        }
        HoldRecordOutputMode::Both => {
            set_system_clipboard_text(text)?;
            paste_from_clipboard()
        }
    }
}

#[cfg(windows)]
fn set_system_clipboard_text(text: &str) -> Result<(), String> {
    use std::{mem::size_of, ptr::null_mut};
    use windows_sys::Win32::System::{
        DataExchange::{CloseClipboard, EmptyClipboard, OpenClipboard, SetClipboardData},
        Memory::{GMEM_MOVEABLE, GlobalAlloc, GlobalLock, GlobalUnlock},
        Ole::CF_UNICODETEXT,
    };

    let mut wide = text.encode_utf16().collect::<Vec<u16>>();
    wide.push(0);
    let byte_len = wide.len() * size_of::<u16>();

    unsafe {
        if OpenClipboard(null_mut()) == 0 {
            return Err("failed to open clipboard".to_string());
        }

        let result = (|| {
            if EmptyClipboard() == 0 {
                return Err("failed to empty clipboard".to_string());
            }

            let handle = GlobalAlloc(GMEM_MOVEABLE, byte_len);
            if handle.is_null() {
                return Err("failed to allocate clipboard memory".to_string());
            }

            let locked = GlobalLock(handle);
            if locked.is_null() {
                return Err("failed to lock clipboard memory".to_string());
            }

            std::ptr::copy_nonoverlapping(wide.as_ptr() as *const u8, locked as *mut u8, byte_len);
            GlobalUnlock(handle);

            if SetClipboardData(CF_UNICODETEXT as u32, handle).is_null() {
                return Err("failed to set clipboard text".to_string());
            }

            Ok(())
        })();

        CloseClipboard();
        result
    }
}

#[cfg(not(windows))]
fn set_system_clipboard_text(_text: &str) -> Result<(), String> {
    Err("system clipboard integration is only implemented on Windows".to_string())
}

#[cfg(windows)]
fn insert_text_at_cursor(text: &str) -> Result<(), String> {
    use std::mem::size_of;
    use windows_sys::Win32::UI::Input::KeyboardAndMouse::{
        INPUT, INPUT_0, INPUT_KEYBOARD, KEYBDINPUT, KEYEVENTF_KEYUP, KEYEVENTF_UNICODE, SendInput,
    };

    fn unicode_input(unit: u16, flags: u32) -> INPUT {
        INPUT {
            r#type: INPUT_KEYBOARD,
            Anonymous: INPUT_0 {
                ki: KEYBDINPUT {
                    wVk: 0,
                    wScan: unit,
                    dwFlags: KEYEVENTF_UNICODE | flags,
                    time: 0,
                    dwExtraInfo: 0,
                },
            },
        }
    }

    let units = text.encode_utf16().collect::<Vec<_>>();
    if units.is_empty() {
        return Ok(());
    }

    for chunk in units.chunks(64) {
        let mut inputs = Vec::with_capacity(chunk.len() * 2);
        for &unit in chunk {
            inputs.push(unicode_input(unit, 0));
            inputs.push(unicode_input(unit, KEYEVENTF_KEYUP));
        }
        let sent = unsafe {
            SendInput(
                inputs.len() as u32,
                inputs.as_ptr(),
                size_of::<INPUT>() as i32,
            )
        };
        if sent != inputs.len() as u32 {
            return Err("failed to insert text".to_string());
        }
    }

    Ok(())
}

#[cfg(not(windows))]
fn insert_text_at_cursor(_text: &str) -> Result<(), String> {
    Err("text insertion is only implemented on Windows".to_string())
}

#[cfg(windows)]
fn paste_from_clipboard() -> Result<(), String> {
    use std::mem::size_of;
    use windows_sys::Win32::UI::Input::KeyboardAndMouse::{
        INPUT, INPUT_0, INPUT_KEYBOARD, KEYBDINPUT, KEYEVENTF_KEYUP, SendInput, VK_CONTROL, VK_V,
    };

    fn key_input(vk: u16, flags: u32) -> INPUT {
        INPUT {
            r#type: INPUT_KEYBOARD,
            Anonymous: INPUT_0 {
                ki: KEYBDINPUT {
                    wVk: vk,
                    wScan: 0,
                    dwFlags: flags,
                    time: 0,
                    dwExtraInfo: 0,
                },
            },
        }
    }

    let inputs = [
        key_input(VK_CONTROL, 0),
        key_input(VK_V, 0),
        key_input(VK_V, KEYEVENTF_KEYUP),
        key_input(VK_CONTROL, KEYEVENTF_KEYUP),
    ];

    let sent = unsafe {
        SendInput(
            inputs.len() as u32,
            inputs.as_ptr(),
            size_of::<INPUT>() as i32,
        )
    };
    if sent == inputs.len() as u32 {
        Ok(())
    } else {
        Err("failed to send paste shortcut".to_string())
    }
}

#[cfg(not(windows))]
fn paste_from_clipboard() -> Result<(), String> {
    Err("text insertion is only implemented on Windows".to_string())
}
