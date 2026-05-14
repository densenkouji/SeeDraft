use axum::Json;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::{AppError, AppResult};

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub(crate) struct AppSettings {
    locales_dir: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct AppSettingsInput {
    locales_dir: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct AppSettingsResponse {
    configured_locales_dir: Option<String>,
    locales_dir: String,
    default_locales_dir: String,
}

#[derive(Deserialize)]
pub(crate) struct PickFolderRequest {
    title: Option<String>,
    current_dir: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct PickFolderResponse {
    path: Option<String>,
}

fn app_settings_response(settings: AppSettings) -> AppSettingsResponse {
    let configured_locales_dir = normalize_settings_dir(settings.locales_dir);
    AppSettingsResponse {
        configured_locales_dir: configured_locales_dir.clone(),
        locales_dir: locales_dir().display().to_string(),
        default_locales_dir: default_locales_dir().display().to_string(),
    }
}

pub(crate) async fn app_settings_handler() -> AppResult<Json<AppSettingsResponse>> {
    Ok(Json(app_settings_response(load_app_settings())))
}

pub(crate) async fn update_app_settings_handler(
    Json(input): Json<AppSettingsInput>,
) -> AppResult<Json<AppSettingsResponse>> {
    let settings = AppSettings {
        locales_dir: normalize_settings_dir(input.locales_dir),
    };

    let resolved_locales_dir = settings
        .locales_dir
        .as_ref()
        .map(PathBuf::from)
        .unwrap_or_else(default_locales_dir);
    std::fs::create_dir_all(&resolved_locales_dir).map_err(AppError::internal)?;

    save_app_settings(&settings).map_err(AppError::internal)?;
    ensure_default_locales();

    Ok(Json(app_settings_response(settings)))
}

pub(crate) async fn pick_folder_handler(
    Json(request): Json<PickFolderRequest>,
) -> AppResult<Json<PickFolderResponse>> {
    let path = tokio::task::spawn_blocking(move || {
        let mut dialog = rfd::FileDialog::new();
        if let Some(title) = request.title.filter(|value| !value.trim().is_empty()) {
            dialog = dialog.set_title(title);
        }
        if let Some(dir) = request.current_dir.filter(|value| !value.trim().is_empty()) {
            dialog = dialog.set_directory(dir);
        }
        dialog.pick_folder()
    })
    .await
    .map_err(AppError::internal)?
    .map(|path| path.display().to_string());

    Ok(Json(PickFolderResponse { path }))
}

pub(crate) fn storage_db_path() -> PathBuf {
    if let Some(dirs) = directories::ProjectDirs::from("dev", "SeeDraft", "SeeDraft") {
        return dirs.data_dir().join("seedraft.sqlite");
    }
    std::env::temp_dir().join("seedraft.sqlite")
}

fn app_data_dir() -> PathBuf {
    if let Some(dirs) = directories::ProjectDirs::from("dev", "SeeDraft", "SeeDraft") {
        return dirs.data_dir().to_path_buf();
    }
    std::env::temp_dir().join("seedraft")
}

fn app_settings_path() -> PathBuf {
    app_data_dir().join("settings.json")
}

fn executable_dir() -> Option<PathBuf> {
    std::env::current_exe()
        .ok()
        .and_then(|path| path.parent().map(|parent| parent.to_path_buf()))
}

fn default_locales_dir() -> PathBuf {
    executable_dir()
        .unwrap_or_else(app_data_dir)
        .join("locales")
}

fn normalize_settings_dir(value: Option<String>) -> Option<String> {
    value
        .map(|path| path.trim().to_string())
        .filter(|path| !path.is_empty())
}

fn load_app_settings() -> AppSettings {
    let path = app_settings_path();
    let Ok(raw) = std::fs::read_to_string(path) else {
        return AppSettings::default();
    };
    serde_json::from_str(&raw).unwrap_or_default()
}

fn save_app_settings(settings: &AppSettings) -> Result<(), String> {
    let path = app_settings_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let pretty = serde_json::to_string_pretty(settings).map_err(|e| e.to_string())?;
    std::fs::write(path, pretty).map_err(|e| e.to_string())
}

fn locales_dir() -> PathBuf {
    normalize_settings_dir(load_app_settings().locales_dir)
        .map(PathBuf::from)
        .unwrap_or_else(default_locales_dir)
}

/// Default locale files embedded in the binary. These are seeded into the
/// configured locales directory on first launch so they can be freely edited.
/// Extra languages can be added by dropping further `*.json` files into that
/// folder; any locale present there is exposed to the frontend via `/api/locales`.
const DEFAULT_LOCALE_EN: &str = include_str!("locales/en.json");
const DEFAULT_LOCALE_JA: &str = include_str!("locales/ja.json");

/// Copy bundled locale files into the user's data directory the first time
/// the app runs, and keep previously-deployed files in sync as the app adds
/// new translation keys. User edits are preserved — only *missing* keys are
/// added from the bundled defaults.
fn ensure_default_locales() -> PathBuf {
    let dir = locales_dir();
    if let Err(error) = std::fs::create_dir_all(&dir) {
        eprintln!("failed to create locales dir: {error}");
        return dir;
    }
    for (name, contents) in [
        ("en.json", DEFAULT_LOCALE_EN),
        ("ja.json", DEFAULT_LOCALE_JA),
    ] {
        let path = dir.join(name);
        if !path.exists() {
            if let Err(error) = std::fs::write(&path, contents) {
                eprintln!("failed to write default locale {name}: {error}");
            }
            continue;
        }
        // Existing file: merge in any keys the user is missing so newer app
        // versions don't leave English strings untranslated or vice versa.
        if let Err(error) = merge_missing_keys(&path, contents) {
            eprintln!("failed to merge default locale {name}: {error}");
        }
    }
    dir
}

/// Known outdated default values that should be force-refreshed on upgrade.
/// If the user's locale file still has the *old* bundled text for one of these
/// keys, we overwrite it with the new default. User-customized translations are
/// left alone (we only replace values that match a known previous default).
const OUTDATED_DEFAULTS: &[(&str, &str)] = &[
    // Quick-toggle rename: "自動XXX" → just "XXX" (post-process grouping)
    ("quick.autoRefine", "自動整形"),
    ("quick.autoRefine", "Auto refine"),
    ("quick.autoTranslate", "自動翻訳"),
    ("quick.autoTranslate", "Auto translate"),
    ("quick.autoCustom", "自動カスタム"),
    ("quick.autoCustom", "Auto custom"),
    ("quick.autoSave", "自動追加"),
    ("quick.autoSave", "Auto save"),
    // Rename "analyze" → "extract" (the feature extracts tone/summary/keywords
    // from text, which is an extraction task rather than analysis).
    ("quick.autoAnalyze", "分析"),
    ("quick.autoAnalyze", "Analyze"),
    (
        "quick.autoAnalyzeTooltip",
        "文字起こし後に分析します（トーン・要点・キーワード）",
    ),
    (
        "quick.autoAnalyzeTooltip",
        "Analyze after transcription (tone, summary, keywords)",
    ),
    ("settings.analyze.title", "分析設定"),
    ("settings.analyze.title", "Analysis"),
    ("note.analyze", "🔍 分析"),
    ("note.analyze", "🔍 Analyze"),
    ("note.analyzing", "分析中..."),
    ("note.analyzing", "Analyzing..."),
    ("note.analysisFailed", "分析に失敗しました"),
    ("note.analysisFailed", "Analysis failed"),
    ("models.desc.llm.qwen", "テキスト整形・翻訳・分析に使用"),
    (
        "models.desc.llm.qwen",
        "Used for text refinement, translation and analysis",
    ),
    (
        "settings.models.chatDesc",
        "整形・翻訳・分析に使用します。プロジェクト補正や後処理パイプラインの裏で動作します。",
    ),
    (
        "settings.models.chatDesc",
        "Used for refinement, translation and analysis. Runs behind the scenes for post-processing.",
    ),
    // Shortcut hint now mentions native single-key hold recording support.
    (
        "settings.shortcuts.hint",
        "各項目の入力欄をクリックして、割り当てたいキーを押してください。Esc で取り消し、Delete / Backspace で解除できます。",
    ),
    (
        "settings.shortcuts.hint",
        "各項目の入力欄をクリックして、割り当てたいキーを押してください。「押下中だけ録音」は Ctrl 単体などの長押しキーも割り当てられます。Esc で取り消し、Delete / Backspace で解除できます。",
    ),
    (
        "settings.shortcuts.hint",
        "各項目の入力欄をクリックして、割り当てたいキーを押してください。「押下中だけ録音」は右Ctrlなどの単独キーも指定でき、アプリが非アクティブでも動作します。Esc で取り消し、Delete / Backspace で解除できます。",
    ),
    (
        "settings.shortcuts.hint",
        "Click a field and press the keys you want to bind. Esc to cancel, Delete / Backspace to clear.",
    ),
    (
        "settings.shortcuts.hint",
        "Click a field and press the keys you want to bind. Press-and-hold recording can use a single held key such as Ctrl. Esc to cancel, Delete / Backspace to clear.",
    ),
    (
        "settings.shortcuts.hint",
        "Click a field and press the keys you want to bind. Press-and-hold recording can use a single key such as RightCtrl and works even when the app is inactive. Esc to cancel, Delete / Backspace to clear.",
    ),
    // Completion setting was moved out of the linked-context card into its
    // own section; the field label is now just "使用するモデル".
    ("settings.complete.model", "補完に使用するモデル"),
    ("settings.complete.model", "Model for completion"),
    // Live captions no longer run fixed 2-second transcription/translation
    // windows; they buffer audio and transcribe after utterance pauses.
    ("live.chunkHint", "2秒ごとに文字起こしと翻訳を行います"),
    ("live.chunkHint", "2秒ごとに文字起こしします"),
    ("live.chunkHint", "Transcription runs every 2 seconds"),
    (
        "live.chunkHint",
        "Audio is buffered first; translation follows transcription",
    ),
];

fn merge_missing_keys(path: &std::path::Path, defaults_json: &str) -> Result<(), String> {
    let existing_raw = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
    let mut existing: serde_json::Map<String, serde_json::Value> =
        match serde_json::from_str::<serde_json::Value>(&existing_raw) {
            Ok(serde_json::Value::Object(map)) => map,
            Ok(_) => return Err("locale file is not a JSON object".to_string()),
            Err(error) => return Err(error.to_string()),
        };

    let defaults: serde_json::Map<String, serde_json::Value> =
        match serde_json::from_str::<serde_json::Value>(defaults_json) {
            Ok(serde_json::Value::Object(map)) => map,
            _ => return Err("bundled defaults are not a JSON object".to_string()),
        };

    let mut changed = 0usize;

    // 1) Add keys the user is missing.
    for (key, value) in &defaults {
        if !existing.contains_key(key) {
            existing.insert(key.clone(), value.clone());
            changed += 1;
        }
    }

    // 2) Refresh values that still hold an outdated bundled default.
    for (key, old_value) in OUTDATED_DEFAULTS {
        let Some(current) = existing.get(*key) else {
            continue;
        };
        let Some(current_str) = current.as_str() else {
            continue;
        };
        if current_str != *old_value {
            continue;
        }
        let Some(new_default) = defaults.get(*key) else {
            continue;
        };
        existing.insert((*key).to_string(), new_default.clone());
        changed += 1;
    }

    if changed > 0 {
        let merged = serde_json::Value::Object(existing);
        let pretty = serde_json::to_string_pretty(&merged).map_err(|e| e.to_string())?;
        std::fs::write(path, pretty).map_err(|e| e.to_string())?;
    }
    Ok(())
}

#[derive(Serialize)]
pub(crate) struct LocalesResponse {
    /// Map of locale code → translation dictionary. Each dictionary is a
    /// flat `{ key: string }` object matching what the frontend expects.
    locales: std::collections::BTreeMap<String, serde_json::Value>,
    /// Filesystem path where the locale JSON files live — shown to the user
    /// so they can edit or add new languages.
    dir: String,
}

pub(crate) async fn locales_handler() -> AppResult<Json<LocalesResponse>> {
    let dir = ensure_default_locales();
    let mut locales: std::collections::BTreeMap<String, serde_json::Value> =
        std::collections::BTreeMap::new();

    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(_) => {
            return Ok(Json(LocalesResponse {
                locales,
                dir: dir.display().to_string(),
            }));
        }
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        let lower = stem.to_ascii_lowercase();
        if !lower
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            continue;
        }
        match std::fs::read_to_string(&path) {
            Ok(body) => match serde_json::from_str::<serde_json::Value>(&body) {
                Ok(value) => {
                    locales.insert(lower, value);
                }
                Err(error) => {
                    eprintln!("failed to parse locale {}: {error}", path.display());
                }
            },
            Err(error) => {
                eprintln!("failed to read locale {}: {error}", path.display());
            }
        }
    }

    Ok(Json(LocalesResponse {
        locales,
        dir: dir.display().to_string(),
    }))
}
