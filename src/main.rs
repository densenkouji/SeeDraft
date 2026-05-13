#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod storage;

use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Multipart, Path as AxumPath, Query, State},
    http::{StatusCode, header},
    response::{Html, IntoResponse, Response, Sse, sse::Event},
    routing::{delete, get, post, put},
};
use foundry_local_sdk::{
    ChatCompletionRequestMessage, ChatCompletionRequestSystemMessage,
    ChatCompletionRequestUserMessage, FoundryLocalConfig, FoundryLocalManager, Model,
};
use futures::stream::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    convert::Infallible,
    fs::File,
    io::{BufWriter, ErrorKind},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, mpsc},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use storage::{
    DraftWithNotes, GraphData, LiveSegment, LiveSession, LiveSessionDetail, Note, NoteLink,
    NoteWithTags, Project, Storage, Translation,
};
use symphonia::core::{
    audio::{AudioBufferRef, SampleBuffer},
    codecs::DecoderOptions,
    errors::Error as SymphoniaError,
    formats::FormatOptions,
    io::MediaSourceStream,
    meta::MetadataOptions,
    probe::Hint,
};
use tauri::Manager;

/// Preferred loopback port. Chosen from the IANA User-Ports range
/// (1024-49151) to avoid the OS-reserved Dynamic/Private range
/// (49152-65535), which on Windows is guarded by WinError 10013 when a
/// program tries to bind there directly. We also steer clear of common
/// dev defaults (3000, 5000, 5173, 8000, 8080, 9000, 11434 for Ollama,
/// etc.) to minimize collisions on typical developer machines. If this
/// port is still busy, `bind_preferred_port` falls back to an OS-assigned
/// ephemeral port.
const PREFERRED_PORT: u16 = 38713;
const LOOPBACK_IP: &str = "127.0.0.1";
const DEFAULT_SPEECH_MODEL_ALIAS: &str = "whisper-tiny";
const CHAT_MODEL_ALIAS: &str = "qwen2.5-coder-0.5b";
const MAX_AUDIO_BYTES: usize = 200 * 1024 * 1024;
const APP_ICON_ICO: &[u8] = include_bytes!("../res/icon.ico");

fn foundry_config() -> FoundryLocalConfig {
    FoundryLocalConfig::new("seedraft")
}

struct ShutdownState {
    shutdown_tx: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
}

#[derive(Clone)]
struct AppState {
    voice_to_text: Arc<VoiceToTextService>,
    storage: Arc<Storage>,
    live_sessions: Arc<tokio::sync::Mutex<HashMap<String, LiveSessionState>>>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum DownloadEvent {
    Started { alias: String },
    Progress { alias: String, percent: f64 },
    Completed { alias: String },
    Failed { alias: String, message: String },
    Idle,
}

#[derive(Clone, Debug, Serialize)]
struct SpeechModelStatus {
    alias: String,
    downloaded: bool,
    loaded: bool,
    active: bool,
}

#[derive(Serialize)]
struct SpeechModelsResponse {
    models: Vec<SpeechModelStatus>,
}

#[derive(Clone, Debug, Serialize)]
struct ModelInfo {
    alias: String,
    category: String,    // "speech" | "chat" | "other"
    description: String, // short human-readable purpose
    downloaded: bool,
    loaded: bool,
    active: bool,
    size_mb: Option<u64>,
    /// Whether at least one variant of this model can run on the current
    /// machine (i.e. its required execution provider is registered, or the
    /// variant targets CPU). Returned so the UI can warn/dim unsupported
    /// entries before the user hits a download failure.
    compatible: bool,
    /// Human-readable reason for `compatible = false` (e.g. "Needs NvTensorRt
    /// (not registered)"). `None` when the model is compatible.
    incompatibility_reason: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct ExecutionProviderInfo {
    name: String,
    registered: bool,
}

#[derive(Serialize)]
struct ModelsCatalogResponse {
    models: Vec<ModelInfo>,
    cache_dir: Option<String>,
    /// Execution providers available on this machine (and whether they are
    /// currently registered). Exposed so the UI can explain why some models
    /// are marked incompatible.
    execution_providers: Vec<ExecutionProviderInfo>,
}

#[derive(Clone, Debug, Serialize)]
struct RuntimeRequirementInfo {
    name: String,
    ok: bool,
    detail: String,
    version: Option<String>,
    install_command: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct RequiredModelInfo {
    alias: String,
    purpose: String,
    downloaded: bool,
    loaded: bool,
    active: bool,
    message: Option<String>,
}

#[derive(Serialize)]
struct AppRequirementsResponse {
    ok: bool,
    runtime_ready: bool,
    foundry_cli: RuntimeRequirementInfo,
    sdk_runtime: RuntimeRequirementInfo,
    execution_providers: Vec<ExecutionProviderInfo>,
    required_models: Vec<RequiredModelInfo>,
    install_command: String,
    missing_summary: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct AppSettings {
    locales_dir: Option<String>,
}

#[derive(Deserialize)]
struct AppSettingsInput {
    locales_dir: Option<String>,
}

#[derive(Serialize)]
struct AppSettingsResponse {
    configured_locales_dir: Option<String>,
    locales_dir: String,
    default_locales_dir: String,
}

#[derive(Deserialize)]
struct PickFolderRequest {
    title: Option<String>,
    current_dir: Option<String>,
}

#[derive(Serialize)]
struct PickFolderResponse {
    path: Option<String>,
}

/// Diagnostic metadata for a single variant of a model. Used by the UI when a
/// download fails so the user can see which execution providers / devices the
/// native core offers for this alias.
#[derive(Clone, Debug, Serialize)]
struct ModelVariantInfo {
    id: String,
    device_type: String,
    execution_provider: Option<String>,
    file_size_mb: Option<u64>,
    cached: bool,
}

#[derive(Serialize)]
struct ModelVariantsResponse {
    alias: String,
    variants: Vec<ModelVariantInfo>,
}

#[derive(Serialize)]
struct ModelTestResponse {
    alias: String,
    ok: bool,
    results: Vec<ModelCapabilityTestResult>,
    elapsed_ms: u128,
}

#[derive(Serialize)]
struct ModelCapabilityTestResult {
    capability: String,
    ok: bool,
    output: Option<String>,
    error: Option<String>,
    elapsed_ms: u128,
}

struct VoiceToTextService {
    /// Primary (default) chat model. The first one loaded becomes the one tracked
    /// here so that `shutdown()` knows what to unload. Any additional chat models
    /// are stored in `chat_models` below.
    chat_model: tokio::sync::Mutex<Option<Arc<Model>>>,
    /// Cache of chat models keyed by alias, used when a post-processing step
    /// wants to run against a specific LLM.
    chat_models: tokio::sync::Mutex<HashMap<String, Arc<Model>>>,
    eps_ready: tokio::sync::Mutex<bool>,
    speech_models: tokio::sync::Mutex<HashMap<String, Arc<Model>>>,
    speech_model_load_lock: tokio::sync::Mutex<()>,
    active_speech_model: tokio::sync::Mutex<Option<String>>,
    download_tx: tokio::sync::broadcast::Sender<DownloadEvent>,
    latest_download: tokio::sync::Mutex<DownloadEvent>,
}

impl VoiceToTextService {
    fn new() -> Self {
        let (download_tx, _) = tokio::sync::broadcast::channel(64);
        Self {
            chat_model: tokio::sync::Mutex::new(None),
            chat_models: tokio::sync::Mutex::new(HashMap::new()),
            eps_ready: tokio::sync::Mutex::new(false),
            speech_models: tokio::sync::Mutex::new(HashMap::new()),
            speech_model_load_lock: tokio::sync::Mutex::new(()),
            active_speech_model: tokio::sync::Mutex::new(None),
            download_tx,
            latest_download: tokio::sync::Mutex::new(DownloadEvent::Idle),
        }
    }

    async fn emit_download_event(&self, event: DownloadEvent) {
        {
            let mut guard = self.latest_download.lock().await;
            *guard = event.clone();
        }
        let _ = self.download_tx.send(event);
    }

    async fn transcribe(
        &self,
        audio_path: &Path,
        language: Option<String>,
        speech_model_alias: Option<String>,
    ) -> AppResult<foundry_local_sdk::AudioTranscriptionResponse> {
        let model = self.ensure_speech_model(speech_model_alias).await?;
        let mut audio_client = model.create_audio_client();

        if let Some(language) = language.filter(|value| !value.trim().is_empty()) {
            audio_client = audio_client.language(language);
        }

        audio_client
            .transcribe(audio_path)
            .await
            .map_err(AppError::internal)
    }

    async fn refine(
        &self,
        request: RefinementRequest,
        linked_context: Option<String>,
    ) -> AppResult<String> {
        let mut source_text = request.text.trim().to_string();
        if source_text.is_empty() {
            return Err(AppError::bad_request("整形する文字起こし結果がありません"));
        }

        if request.remove_fillers {
            source_text = remove_common_fillers(&source_text);
        }
        if request.voice_commands {
            source_text = apply_spoken_commands(&source_text);
        }

        let model = self
            .ensure_chat_model_by_alias(request.model.as_deref())
            .await?;
        let client = model.create_chat_client().temperature(0.0).max_tokens(1024);
        let filler_instruction = if request.remove_fillers {
            "フィラーや言いよどみは削除済みです。残っている不要な言いよどみだけを追加で除去してください。"
        } else {
            "意味のある言いよどみは残してください。"
        };
        let custom_terms = request
            .custom_terms
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| format!("\n- 次の専門用語・固有名詞は表記を維持してください: {value}"))
            .unwrap_or_default();
        let custom_instruction = request
            .custom_instruction
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| format!("\n- 追加指示: {value}"))
            .unwrap_or_default();

        // Refine = meaning-preserving cleanup ONLY. Rewriting style, tone, or
        // register is explicitly out of scope — users who want those changes
        // should configure a Custom post-processing step instead.
        let user_prompt = format!(
            "次の文字起こしを校正してください。\n\n厳守:\n- 文の意味・内容・情報量を一切変えないでください。言い換え・敬体化・常体化・スタイル変更は禁止です。\n- 可能な限り元の語彙と言い回しをそのまま残してください。\n- {filler_instruction}\n- 明らかな音声認識の誤変換（同音異義語の誤り等）と句読点・表記ゆれのみを補正してください。\n- 要約しないでください。\n- 文や情報を削除しないでください。\n- 入力の文の順序を維持してください。\n- 事実、主語、目的語、専門用語を新たに追加しないでください。\n- 入力と同じ言語で出力してください。\n- 前置き、説明、見出し、引用符は出力しないでください。\n- 出力は校正後の本文のみです。{custom_terms}{custom_instruction}\n\n入力:\n{source_text}"
        );
        let mut messages: Vec<ChatCompletionRequestMessage> = Vec::new();
        messages.push(
            ChatCompletionRequestSystemMessage::from(
                "あなたは文字起こしの校正エンジンです。意味・内容・語彙は一切変えず、明らかな誤変換と不要なフィラー・句読点の乱れのみを補正し、本文のみを出力します。スタイル変更や敬体化などの書き換えは決して行いません。",
            )
            .into(),
        );
        if let Some(ctx) = linked_context.as_deref().filter(|s| !s.is_empty()) {
            messages.push(ChatCompletionRequestSystemMessage::from(ctx).into());
        }
        messages.push(ChatCompletionRequestUserMessage::from(user_prompt.as_str()).into());

        let response = client
            .complete_chat(&messages, None)
            .await
            .map_err(AppError::internal)?;
        let refined = response.choices[0]
            .message
            .content
            .as_deref()
            .unwrap_or("")
            .trim()
            .to_string();

        if refined.is_empty() {
            return Err(AppError::internal("整形結果が空でした"));
        }

        Ok(strip_llm_preamble(&refined))
    }

    async fn translate(
        &self,
        request: TranslationRequest,
        linked_context: Option<String>,
    ) -> AppResult<String> {
        let source_text = request.text.trim();
        if source_text.is_empty() {
            return Err(AppError::bad_request("翻訳する文字起こし結果がありません"));
        }

        let target_language = request
            .target_language
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("ja");
        let source_language = request
            .source_language
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("auto");
        let model_alias = request.model.as_deref();
        let custom_terms = request
            .custom_terms
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| {
                format!("\n- 次の固有名詞・専門用語は可能な限り表記を維持してください: {value}")
            })
            .unwrap_or_default();
        let custom_instruction = request
            .custom_instruction
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| format!("\n- 追加指示: {value}"))
            .unwrap_or_default();

        let model = self.ensure_chat_model_by_alias(model_alias).await?;
        let client = model.create_chat_client().temperature(0.0).max_tokens(1024);
        let user_prompt = format!(
            "次の文字起こし結果を翻訳してください。\n\n条件:\n- 入力言語: {source_language}\n- 出力言語: {target_language}\n- 意味を変えないでください。\n- 要約しないでください。\n- 情報を削除したり追加したりしないでください。\n- Live Caption の字幕として読みやすい自然な文にしてください。\n- 前置き、説明、見出し、引用符は出力しないでください。\n- 出力は翻訳本文のみです。{custom_terms}{custom_instruction}\n\n入力:\n{source_text}"
        );
        let mut messages: Vec<ChatCompletionRequestMessage> = Vec::new();
        messages.push(
            ChatCompletionRequestSystemMessage::from(
                "あなたはリアルタイム字幕向けの翻訳者です。入力の意味と順序を保ち、翻訳本文のみを出力します。",
            )
            .into(),
        );
        if let Some(ctx) = linked_context.as_deref().filter(|s| !s.is_empty()) {
            messages.push(ChatCompletionRequestSystemMessage::from(ctx).into());
        }
        messages.push(ChatCompletionRequestUserMessage::from(user_prompt.as_str()).into());

        let response = client
            .complete_chat(&messages, None)
            .await
            .map_err(AppError::internal)?;
        let translated = response.choices[0]
            .message
            .content
            .as_deref()
            .unwrap_or("")
            .trim()
            .to_string();

        if translated.is_empty() {
            return Err(AppError::internal("翻訳結果が空でした"));
        }

        Ok(strip_llm_preamble(&translated))
    }

    async fn translate_live_caption(
        &self,
        source_text: &str,
        source_language: Option<&str>,
        target_language: &str,
        model_alias: Option<&str>,
        allow_model_fallback: bool,
    ) -> AppResult<String> {
        let source_text = source_text.trim();
        if source_text.is_empty() {
            return Err(AppError::bad_request("翻訳する文字起こし結果がありません"));
        }

        let source_language = source_language
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("auto");
        let source_language_label = live_language_label(source_language);
        let target_language_label = live_language_label(target_language.trim());
        let requested_model_alias = model_alias.map(str::trim).filter(|value| !value.is_empty());
        let (model, effective_model_alias) =
            match self.ensure_chat_model_by_alias(requested_model_alias).await {
                Ok(model) => (model, requested_model_alias),
                Err(error) if allow_model_fallback && requested_model_alias.is_some() => {
                    eprintln!(
                        "live translation model '{}' failed to load; falling back to {}: {:?}",
                        requested_model_alias.unwrap_or_default(),
                        CHAT_MODEL_ALIAS,
                        error
                    );
                    (self.ensure_chat_model_by_alias(None).await?, None)
                }
                Err(error) => return Err(error),
            };
        let client = model.create_chat_client().temperature(0.0).max_tokens(160);

        for attempt in 0..2 {
            let user_prompt = if attempt == 0 {
                format!(
                    "Translate this {source_language_label} live caption into {target_language_label}.\nText:\n{source_text}"
                )
            } else {
                format!("{target_language_label} translation only:\n{source_text}")
            };
            let system_prompt = if attempt == 0 {
                "You translate live captions. Answer with the translation only."
            } else {
                "Translator. No notes."
            };
            let messages: Vec<ChatCompletionRequestMessage> = vec![
                ChatCompletionRequestSystemMessage::from(system_prompt).into(),
                ChatCompletionRequestUserMessage::from(user_prompt.as_str()).into(),
            ];

            let response = client
                .complete_chat(&messages, None)
                .await
                .map_err(AppError::internal)?;
            let translated =
                strip_llm_preamble(response.choices[0].message.content.as_deref().unwrap_or(""));

            if !live_translation_output_is_invalid(&translated) {
                return Ok(translated);
            }

            if !translated.trim().is_empty() {
                eprintln!("live translation output rejected: {translated}");
            }
        }

        let fallback = self
            .translate(
                TranslationRequest {
                    text: source_text.to_string(),
                    source_language: Some(source_language.to_string()),
                    target_language: Some(target_language.to_string()),
                    custom_instruction: Some(
                        "Live caption translation. Output only the translated text.".to_string(),
                    ),
                    custom_terms: None,
                    model: effective_model_alias.map(str::to_string),
                    note_id: None,
                    use_linked_context: false,
                },
                None,
            )
            .await?;

        if live_translation_output_is_invalid(&fallback) {
            eprintln!("live translation fallback output rejected: {fallback}");
            Ok(String::new())
        } else {
            Ok(fallback)
        }
    }

    /// Combine several notes into a single coherent draft document.
    /// `mode`: "concat" — simple concatenation with note titles as headings.
    ///         "llm"    — ask the chat model to rewrite the notes as one article.
    async fn compose_draft(
        &self,
        notes: &[(String, String)], // (title, text) pairs, in desired order
        mode: &str,
        instruction: Option<&str>,
    ) -> AppResult<String> {
        if notes.is_empty() {
            return Err(AppError::bad_request("合成対象のノートがありません"));
        }

        if mode == "concat" {
            // Join as Markdown sections: `## <title>\n\n<body>\n\n---\n\n...`
            let combined = notes
                .iter()
                .map(|(title, text)| {
                    let title = if title.trim().is_empty() {
                        "Note"
                    } else {
                        title.trim()
                    };
                    format!("## {title}\n\n{}", text.trim())
                })
                .collect::<Vec<_>>()
                .join("\n\n---\n\n");
            return Ok(combined);
        }

        // LLM mode
        let model = self.ensure_chat_model().await?;
        let client = model.create_chat_client().temperature(0.2).max_tokens(2048);
        let bodies = notes
            .iter()
            .enumerate()
            .map(|(i, (title, text))| {
                let title = if title.trim().is_empty() {
                    format!("Note {}", i + 1)
                } else {
                    title.trim().to_string()
                };
                format!("### [{i}] {title}\n{}", text.trim(), i = i + 1)
            })
            .collect::<Vec<_>>()
            .join("\n\n");
        let extra = instruction
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| format!("\n- 追加指示: {s}"))
            .unwrap_or_default();
        let user_prompt = format!(
            "次の複数のメモを、一貫性のある1本のドキュメントに再構成してください。\n\n条件:\n- 入力と同じ言語で出力。\n- 重複する内容はまとめる。\n- 適切な見出し (#, ##) と段落、箇条書きを使って読みやすく。\n- 入力にない情報は追加しない。\n- メモの順序を尊重しつつ自然な流れに整える。\n- 前置き、説明、コードフェンスは出力しない。\n- 出力は Markdown 本文のみ。{extra}\n\n入力メモ:\n{bodies}"
        );
        let messages: Vec<ChatCompletionRequestMessage> = vec![
            ChatCompletionRequestSystemMessage::from(
                "あなたは複数のメモから一貫した記事を構成する編集者です。Markdown本文のみを出力します。",
            )
            .into(),
            ChatCompletionRequestUserMessage::from(user_prompt.as_str()).into(),
        ];
        let response = client
            .complete_chat(&messages, None)
            .await
            .map_err(AppError::internal)?;
        let text = response.choices[0]
            .message
            .content
            .as_deref()
            .unwrap_or("")
            .trim()
            .to_string();
        if text.is_empty() {
            return Err(AppError::internal("合成結果が空でした"));
        }
        let cleaned = text
            .trim_start_matches("```markdown")
            .trim_start_matches("```md")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim()
            .to_string();
        Ok(cleaned)
    }

    async fn run_custom_prompt(
        &self,
        text: &str,
        instruction: &str,
        max_tokens: u32,
        model_alias: Option<&str>,
        linked_context: Option<String>,
    ) -> AppResult<String> {
        let source = text.trim();
        if source.is_empty() {
            return Err(AppError::bad_request("処理対象のテキストがありません"));
        }
        let instruction = instruction.trim();
        if instruction.is_empty() {
            return Err(AppError::bad_request("指示が空です"));
        }

        let model = self.ensure_chat_model_by_alias(model_alias).await?;
        let client = model
            .create_chat_client()
            .temperature(0.0)
            .max_tokens(max_tokens.clamp(128, 4096));
        let user_prompt = format!(
            "次の指示に従って、下記の文章を処理してください。\n\n指示:\n{instruction}\n\n条件:\n- 前置き、説明、コードフェンスは出力しないでください。\n- 出力は処理結果の本文のみ。\n\n入力:\n{source}"
        );
        let mut messages: Vec<ChatCompletionRequestMessage> = Vec::new();
        messages.push(
            ChatCompletionRequestSystemMessage::from(
                "あなたは文章を指示に沿って処理するエンジンです。処理結果の本文のみを出力します。",
            )
            .into(),
        );
        if let Some(ctx) = linked_context.as_deref().filter(|s| !s.is_empty()) {
            messages.push(ChatCompletionRequestSystemMessage::from(ctx).into());
        }
        messages.push(ChatCompletionRequestUserMessage::from(user_prompt.as_str()).into());

        let response = client
            .complete_chat(&messages, None)
            .await
            .map_err(AppError::internal)?;
        let text = response.choices[0]
            .message
            .content
            .as_deref()
            .unwrap_or("")
            .trim()
            .to_string();
        if text.is_empty() {
            return Err(AppError::internal("処理結果が空でした"));
        }
        Ok(strip_llm_preamble(&text))
    }

    async fn analyze(
        &self,
        text: &str,
        model_alias: Option<&str>,
        linked_context: Option<String>,
    ) -> AppResult<AnalysisResult> {
        let source = text.trim();
        if source.is_empty() {
            return Err(AppError::bad_request("抽出対象のテキストがありません"));
        }

        let model = self.ensure_chat_model_by_alias(model_alias).await?;
        let client = model.create_chat_client().temperature(0.0).max_tokens(512);
        let user_prompt = format!(
            "次のテキストから情報を抽出し、厳密なJSONのみで出力してください。\n\n要件:\n- JSON以外の文字（前置き、説明、コードフェンス）は一切出力しない\n- キーは \"tone\", \"summary\", \"keywords\" の3つ\n- tone: 全体の話し手のトーンを1つの短いラベル（例: 冷静, 熱意, 中立, 疲労, 前向き）\n- summary: 入力と同じ言語で1-2文の要点\n- keywords: 重要語を3〜6個、配列で\n\n入力:\n{source}"
        );
        let mut messages: Vec<ChatCompletionRequestMessage> = Vec::new();
        messages.push(
            ChatCompletionRequestSystemMessage::from(
                "あなたは文章からトーン・要点・キーワードを抽出するエンジンです。指定された JSON スキーマに厳密に従い、JSON のみを出力します。",
            )
            .into(),
        );
        if let Some(ctx) = linked_context.as_deref().filter(|s| !s.is_empty()) {
            messages.push(ChatCompletionRequestSystemMessage::from(ctx).into());
        }
        messages.push(ChatCompletionRequestUserMessage::from(user_prompt.as_str()).into());

        let response = client
            .complete_chat(&messages, None)
            .await
            .map_err(AppError::internal)?;
        let raw = response.choices[0]
            .message
            .content
            .as_deref()
            .unwrap_or("")
            .trim()
            .to_string();

        // Strip reasoning-style <think> blocks before hunting for JSON so a
        // mention of "{...}" in the model's inner monologue doesn't derail
        // `extract_json_object`.
        let cleaned = strip_think_blocks(&raw);
        let json_slice = extract_json_object(&cleaned).unwrap_or(cleaned.as_str());
        serde_json::from_str::<AnalysisResult>(json_slice).map_err(|error| {
            AppError::internal(format!(
                "抽出結果のJSONパースに失敗しました: {error} / raw: {raw}"
            ))
        })
    }

    /// Continue the user's note by generating additional body text.
    /// The existing content is treated as read-only; the LLM returns only the
    /// text to append. When `linked_context` is supplied, the linked notes are
    /// passed as background so the continuation is topically consistent with
    /// the user's wider project.
    async fn complete_note(
        &self,
        text: &str,
        model_alias: Option<&str>,
        linked_context: Option<String>,
    ) -> AppResult<String> {
        let source_text = text.trim();
        if source_text.is_empty() {
            return Err(AppError::bad_request("補完する本文がありません"));
        }

        let model = self.ensure_chat_model_by_alias(model_alias).await?;
        let client = model.create_chat_client().temperature(0.4).max_tokens(600);
        let user_prompt = format!(
            "次のノート本文の続きを書いてください。\n\n条件:\n- 入力本文の文体・言語・トピックを引き継いでください。\n- 既存の本文は絶対に繰り返さないでください。続きの文章のみを出力してください。\n- 前置き（「続きを書きます」「もちろんです」など）、見出し、引用符、コードフェンスは出力しないでください。\n- 事実は控えめに。入力に書かれていない固有名詞・数値を新たに発明しないでください。\n- 関連ノート（システムに添付されている場合）は背景として参考にし、そのまま引き写さないでください。\n- 3〜6文程度で自然に話題をまとめてください。\n\n既存のノート本文:\n{source_text}"
        );
        let mut messages: Vec<ChatCompletionRequestMessage> = Vec::new();
        messages.push(
            ChatCompletionRequestSystemMessage::from(
                "あなたはノートの続きを書くアシスタントです。既存本文の文体と言語を保ち、続きの本文のみを出力します。前置きや説明は出力しません。",
            )
            .into(),
        );
        if let Some(ctx) = linked_context.as_deref().filter(|s| !s.is_empty()) {
            messages.push(ChatCompletionRequestSystemMessage::from(ctx).into());
        }
        messages.push(ChatCompletionRequestUserMessage::from(user_prompt.as_str()).into());

        let response = client
            .complete_chat(&messages, None)
            .await
            .map_err(AppError::internal)?;
        let raw = response.choices[0]
            .message
            .content
            .as_deref()
            .unwrap_or("")
            .trim()
            .to_string();
        if raw.is_empty() {
            return Err(AppError::internal("補完結果が空でした"));
        }
        Ok(strip_llm_preamble(&raw))
    }

    async fn correct_transcription_with_context(
        &self,
        text: &str,
        context_prompt: &str,
    ) -> AppResult<String> {
        let source_text = text.trim();
        let context_prompt = context_prompt.trim();
        if source_text.is_empty() || context_prompt.is_empty() {
            return Ok(source_text.to_string());
        }

        let model = self.ensure_chat_model().await?;
        let client = model.create_chat_client().temperature(0.0).max_tokens(1024);
        let user_prompt = format!(
            "次の文字起こし結果を、与えられた文脈に基づいて補正してください。\n\n条件:\n- 明らかな誤認識だけを修正してください。\n- 意味を変えないでください。\n- 要約しないでください。\n- 情報を追加したり削除したりしないでください。\n- 前置き、説明、見出し、引用符は出力しないでください。\n- 出力は補正後の本文のみです。\n\n文脈と指示:\n{context_prompt}\n\n文字起こし:\n{source_text}"
        );
        let messages: Vec<ChatCompletionRequestMessage> = vec![
            ChatCompletionRequestSystemMessage::from(
                "あなたは文字起こし結果の表記補正を行う編集者です。文脈を手がかりにしますが、原文にない内容は追加しません。",
            )
            .into(),
            ChatCompletionRequestUserMessage::from(user_prompt.as_str()).into(),
        ];

        let response = client
            .complete_chat(&messages, None)
            .await
            .map_err(AppError::internal)?;
        let corrected = response.choices[0]
            .message
            .content
            .as_deref()
            .unwrap_or("")
            .trim()
            .to_string();

        if corrected.is_empty() {
            return Ok(source_text.to_string());
        }

        Ok(strip_llm_preamble(&corrected))
    }

    /// Release every cached Model so that OGA's Model/Tokenizer instances are
    /// freed before the process exits. Without this, OnnxRuntime GenAI prints
    /// "struct X was leaked" warnings at shutdown because the static destructors
    /// run before our Arc<Model> refs are dropped.
    async fn shutdown(&self) {
        // Primary (default) chat model — clear the reference first.
        let _ = self.chat_model.lock().await.take();

        // All cached chat models
        let chat_models: Vec<Arc<Model>> = {
            let mut guard = self.chat_models.lock().await;
            guard.drain().map(|(_, model)| model).collect()
        };
        for model in chat_models {
            let _ = model.unload().await;
            drop(model);
        }

        // All cached speech models
        let _speech_load_guard = self.speech_model_load_lock.lock().await;
        let speech_models: Vec<Arc<Model>> = {
            let mut guard = self.speech_models.lock().await;
            guard.drain().map(|(_, model)| model).collect()
        };
        for model in speech_models {
            let _ = model.unload().await;
            drop(model);
        }

        *self.active_speech_model.lock().await = None;
    }

    async fn ensure_execution_providers(&self) -> AppResult<&'static FoundryLocalManager> {
        let manager = FoundryLocalManager::create(foundry_config()).map_err(AppError::internal)?;
        let mut guard = self.eps_ready.lock().await;
        if !*guard {
            manager
                .download_and_register_eps(None)
                .await
                .map_err(AppError::internal)?;
            *guard = true;
        }

        Ok(manager)
    }

    /// Pick the best runnable variant of a model for this host and select it
    /// on the shared `Model` handle, so subsequent `download()` / `load()`
    /// calls target a variant the native core can actually run.
    ///
    /// Preference order:
    /// 1. A cached variant on a registered EP (we've already paid the download cost)
    /// 2. A CPU variant (works on every host, simplest path)
    /// 3. Any variant whose EP is registered
    /// 4. Leave the current selection untouched as a last resort
    fn select_best_variant(model: &Model) -> AppResult<()> {
        let manager = FoundryLocalManager::create(foundry_config()).map_err(AppError::internal)?;
        let registered_eps: std::collections::HashSet<String> = manager
            .discover_eps()
            .map_err(AppError::internal)?
            .into_iter()
            .filter(|ep| ep.is_registered)
            .map(|ep| ep.name)
            .collect();

        let variants = model.variants();
        if variants.is_empty() {
            return Ok(());
        }

        let is_cpu = |v: &Arc<Model>| -> bool {
            match v.info().runtime.as_ref() {
                Some(rt) => {
                    rt.device_type == foundry_local_sdk::DeviceType::CPU
                        || rt.execution_provider.is_empty()
                        || rt.execution_provider.eq_ignore_ascii_case("cpu")
                        || rt
                            .execution_provider
                            .to_lowercase()
                            .contains("cpuexecutionprovider")
                }
                None => true, // no runtime info → assume CPU-compatible
            }
        };
        let ep_registered = |v: &Arc<Model>| -> bool {
            match v.info().runtime.as_ref() {
                Some(rt) => registered_eps.contains(&rt.execution_provider),
                None => true,
            }
        };

        // 1. Cached + runnable
        if let Some(pick) = variants
            .iter()
            .find(|v| v.info().cached && (is_cpu(v) || ep_registered(v)))
        {
            let _ = model.select_variant_by_id(pick.id());
            return Ok(());
        }
        // 2. CPU variant
        if let Some(pick) = variants.iter().find(|v| is_cpu(v)) {
            let _ = model.select_variant_by_id(pick.id());
            return Ok(());
        }
        // 3. Any registered EP
        if let Some(pick) = variants.iter().find(|v| ep_registered(v)) {
            let _ = model.select_variant_by_id(pick.id());
            return Ok(());
        }
        // 4. Fall through — caller will likely get a "Download was cancelled"
        //    error from the native core, which the diagnostic panel already
        //    surfaces so the user can see the variant list.
        Ok(())
    }

    async fn download_model_with_progress(&self, model: &Model, alias: &str) -> AppResult<()> {
        // Ensure we're about to download a variant the native core can actually
        // run — otherwise `gpt-oss-20b` / `qwen3-4b` etc. fail with "Operation
        // was cancelled" on hosts without the matching GPU EP.
        Self::select_best_variant(model)?;

        if model.is_cached().await.map_err(AppError::internal)? {
            return Ok(());
        }

        self.emit_download_event(DownloadEvent::Started {
            alias: alias.to_string(),
        })
        .await;

        let tx = self.download_tx.clone();
        let latest = Arc::clone(&Arc::new(alias.to_string()));
        let alias_for_cb = (*latest).clone();
        let callback = move |percent: f64| {
            let _ = tx.send(DownloadEvent::Progress {
                alias: alias_for_cb.clone(),
                percent,
            });
        };

        let result = model.download(Some(callback)).await;

        match result {
            Ok(_) => {
                self.emit_download_event(DownloadEvent::Completed {
                    alias: alias.to_string(),
                })
                .await;
                Ok(())
            }
            Err(error) => {
                let message = error.to_string();
                self.emit_download_event(DownloadEvent::Failed {
                    alias: alias.to_string(),
                    message: message.clone(),
                })
                .await;
                Err(AppError::internal(message))
            }
        }
    }

    fn normalize_speech_model_alias(alias: Option<String>) -> String {
        alias
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| DEFAULT_SPEECH_MODEL_ALIAS.to_string())
    }

    async fn cached_variant_status(model: &Model) -> AppResult<(bool, Option<PathBuf>)> {
        for variant in model.variants() {
            if variant.is_cached().await.map_err(AppError::internal)? {
                return Ok((true, variant.path().await.ok()));
            }
        }
        Ok((false, None))
    }

    async fn has_cached_variant(model: &Model) -> AppResult<bool> {
        Ok(Self::cached_variant_status(model).await?.0)
    }

    async fn warm_up_speech_model(&self, alias: Option<String>) -> AppResult<String> {
        let alias = Self::normalize_speech_model_alias(alias);
        self.ensure_speech_model(Some(alias.clone())).await?;
        Ok(alias)
    }

    async fn ensure_speech_model(&self, alias: Option<String>) -> AppResult<Arc<Model>> {
        let alias = Self::normalize_speech_model_alias(alias);

        let model = {
            let guard = self.speech_models.lock().await;
            guard.get(&alias).map(Arc::clone)
        };

        let model = if let Some(model) = model {
            model
        } else {
            let _load_guard = self.speech_model_load_lock.lock().await;
            if let Some(model) = self.speech_models.lock().await.get(&alias).map(Arc::clone) {
                *self.active_speech_model.lock().await = Some(alias);
                return Ok(model);
            }

            let manager = self.ensure_execution_providers().await?;
            let model = manager
                .catalog()
                .get_model(&alias)
                .await
                .map_err(AppError::internal)?;

            self.download_model_with_progress(&model, &alias).await?;

            if !model.is_loaded().await.map_err(AppError::internal)? {
                model.load().await.map_err(AppError::internal)?;
            }

            self.speech_models
                .lock()
                .await
                .insert(alias.clone(), Arc::clone(&model));
            model
        };

        *self.active_speech_model.lock().await = Some(alias);
        Ok(model)
    }

    /// Compute per-model compatibility by inspecting every variant's required
    /// execution provider / device type against the currently-registered EPs.
    /// A model is compatible if at least one of its variants is runnable.
    ///
    /// Returns `(compatible, reason_when_not)`.
    fn compute_compatibility(
        model: &Model,
        registered_eps: &std::collections::HashSet<String>,
    ) -> (bool, Option<String>) {
        let mut any_runnable = false;
        // Track missing EPs so we can list them in the incompatibility reason.
        let mut missing: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        let mut saw_variant = false;

        for variant in model.variants() {
            saw_variant = true;
            let info = variant.info();
            let runtime = match info.runtime.as_ref() {
                Some(r) => r,
                None => {
                    // No runtime metadata — assume runnable (treat like CPU)
                    any_runnable = true;
                    continue;
                }
            };
            // CPU variants are always runnable.
            if runtime.device_type == foundry_local_sdk::DeviceType::CPU
                || runtime.execution_provider.is_empty()
                || runtime.execution_provider.eq_ignore_ascii_case("cpu")
                || runtime
                    .execution_provider
                    .to_lowercase()
                    .contains("cpuexecutionprovider")
            {
                any_runnable = true;
                continue;
            }
            if registered_eps.contains(&runtime.execution_provider) {
                any_runnable = true;
            } else {
                missing.insert(runtime.execution_provider.clone());
            }
        }

        if !saw_variant {
            return (true, None);
        }
        if any_runnable {
            (true, None)
        } else {
            let list = missing.iter().cloned().collect::<Vec<_>>().join(", ");
            (false, Some(format!("Requires: {list}")))
        }
    }

    async fn list_all_models(
        &self,
    ) -> AppResult<(Vec<ModelInfo>, Option<String>, Vec<ExecutionProviderInfo>)> {
        let cached_guard = self.speech_models.lock().await;
        let active_guard = self.active_speech_model.lock().await;
        let active_speech = active_guard.clone();
        let chat_model_guard = self.chat_model.lock().await;
        let chat_active_alias = chat_model_guard.as_ref().map(|m| m.alias().to_string());
        drop(chat_model_guard);

        let manager = FoundryLocalManager::create(foundry_config()).map_err(AppError::internal)?;
        let all_models = manager
            .catalog()
            .get_models()
            .await
            .map_err(AppError::internal)?;

        // Snapshot of which execution providers are registered right now.
        // Used to mark each model as compatible / incompatible with this host.
        let ep_list = manager.discover_eps().map_err(AppError::internal)?;
        let registered_eps: std::collections::HashSet<String> = ep_list
            .iter()
            .filter(|ep| ep.is_registered)
            .map(|ep| ep.name.clone())
            .collect();
        let execution_providers: Vec<ExecutionProviderInfo> = ep_list
            .iter()
            .map(|ep| ExecutionProviderInfo {
                name: ep.name.clone(),
                registered: ep.is_registered,
            })
            .collect();

        let mut cache_dir: Option<String> = None;
        let mut seen = std::collections::BTreeSet::new();
        let mut results: Vec<ModelInfo> = Vec::new();

        for model in all_models.iter() {
            let alias = model.alias().to_string();
            if !seen.insert(alias.clone()) {
                continue;
            }

            // Determine purpose from modalities.
            let input_mod = model.input_modalities().unwrap_or("");
            let output_mod = model.output_modalities().unwrap_or("");
            let is_speech = alias.to_lowercase().starts_with("whisper")
                || input_mod.to_lowercase().contains("audio");
            let category = if is_speech { "speech" } else { "chat" };

            let description = describe_model_purpose(&alias, input_mod, output_mod, is_speech);

            let loaded_in_cache = cached_guard.contains_key(&alias);
            let is_active = if is_speech {
                active_speech.as_deref() == Some(alias.as_str())
            } else {
                chat_active_alias.as_deref() == Some(alias.as_str())
            };
            // A model alias groups multiple variants. The catalog's `cached`
            // metadata can be stale after an in-app download, so ask the runtime
            // for the current cached IDs and probe every variant.
            let (downloaded, cached_variant_path) = Self::cached_variant_status(model).await?;

            // Capture cache directory (parent of first available path)
            if cache_dir.is_none() {
                if let Some(path) = cached_variant_path {
                    if let Some(parent) = path.parent().and_then(|p| p.parent()) {
                        cache_dir = Some(parent.display().to_string());
                    } else {
                        cache_dir = Some(path.display().to_string());
                    }
                }
            }

            let (compatible, incompatibility_reason) =
                Self::compute_compatibility(model, &registered_eps);

            results.push(ModelInfo {
                alias,
                category: category.to_string(),
                description,
                downloaded,
                loaded: loaded_in_cache,
                active: is_active,
                size_mb: None,
                compatible,
                incompatibility_reason,
            });
        }

        // Sort: compatible first, then by category/alias so unsupported entries
        // drift to the bottom of each group.
        results.sort_by(|a, b| {
            b.compatible
                .cmp(&a.compatible)
                .then_with(|| a.category.cmp(&b.category))
                .then_with(|| a.alias.cmp(&b.alias))
        });
        Ok((results, cache_dir, execution_providers))
    }

    async fn required_model_requirements(&self) -> Vec<RequiredModelInfo> {
        let cached_guard = self.speech_models.lock().await;
        let active_guard = self.active_speech_model.lock().await;
        let active_speech = active_guard.clone();
        let chat_model_guard = self.chat_model.lock().await;
        let chat_active_alias = chat_model_guard.as_ref().map(|m| m.alias().to_string());
        drop(chat_model_guard);

        let required = [
            (
                DEFAULT_SPEECH_MODEL_ALIAS,
                "speech_to_text",
                cached_guard.contains_key(DEFAULT_SPEECH_MODEL_ALIAS),
                active_speech.as_deref() == Some(DEFAULT_SPEECH_MODEL_ALIAS),
            ),
            (
                CHAT_MODEL_ALIAS,
                "post_processing",
                self.chat_models.lock().await.contains_key(CHAT_MODEL_ALIAS),
                chat_active_alias.as_deref() == Some(CHAT_MODEL_ALIAS),
            ),
        ];

        let manager = match FoundryLocalManager::create(foundry_config()) {
            Ok(manager) => manager,
            Err(error) => {
                return required
                    .into_iter()
                    .map(|(alias, purpose, loaded, active)| RequiredModelInfo {
                        alias: alias.to_string(),
                        purpose: purpose.to_string(),
                        downloaded: false,
                        loaded,
                        active,
                        message: Some(error.to_string()),
                    })
                    .collect();
            }
        };

        let mut out = Vec::new();
        for (alias, purpose, loaded, active) in required {
            match manager.catalog().get_model(alias).await {
                Ok(model) => match Self::has_cached_variant(&model).await {
                    Ok(downloaded) => out.push(RequiredModelInfo {
                        alias: alias.to_string(),
                        purpose: purpose.to_string(),
                        downloaded,
                        loaded,
                        active,
                        message: None,
                    }),
                    Err(error) => out.push(RequiredModelInfo {
                        alias: alias.to_string(),
                        purpose: purpose.to_string(),
                        downloaded: false,
                        loaded,
                        active,
                        message: Some(error.message),
                    }),
                },
                Err(error) => out.push(RequiredModelInfo {
                    alias: alias.to_string(),
                    purpose: purpose.to_string(),
                    downloaded: false,
                    loaded,
                    active,
                    message: Some(error.to_string()),
                }),
            }
        }
        out
    }

    /// List every variant the catalog offers for a given alias. Useful for the
    /// UI diagnostic panel shown when a download fails — the user can see
    /// whether the failing alias only has GPU variants, for example.
    async fn list_variants_for_alias(&self, alias: &str) -> AppResult<Vec<ModelVariantInfo>> {
        let manager = FoundryLocalManager::create(foundry_config()).map_err(AppError::internal)?;
        let model = manager
            .catalog()
            .get_model(alias)
            .await
            .map_err(AppError::internal)?;

        let mut out = Vec::new();
        for variant in model.variants() {
            let info = variant.info();
            let (device, ep) = match info.runtime.as_ref() {
                Some(runtime) => (
                    format!("{:?}", runtime.device_type),
                    Some(runtime.execution_provider.clone()),
                ),
                None => ("Unknown".to_string(), None),
            };
            out.push(ModelVariantInfo {
                id: info.id.clone(),
                device_type: device,
                execution_provider: ep,
                file_size_mb: info.file_size_mb,
                cached: variant.is_cached().await.map_err(AppError::internal)?,
            });
        }
        Ok(out)
    }

    async fn delete_model_by_alias(&self, alias: &str) -> AppResult<()> {
        // Refuse to remove an in-use speech model
        if self.active_speech_model.lock().await.as_deref() == Some(alias) {
            return Err(AppError::bad_request(
                "使用中のモデルは削除できません。別のモデルを選択してから削除してください",
            ));
        }

        // Drop cached Arc so OGA can release it
        {
            let mut guard = self.speech_models.lock().await;
            guard.remove(alias);
        }

        // If this is the active chat model, drop it too
        {
            let mut chat_guard = self.chat_model.lock().await;
            let is_current_chat = chat_guard
                .as_ref()
                .map(|m| m.alias() == alias)
                .unwrap_or(false);
            if is_current_chat {
                if let Some(model) = chat_guard.take() {
                    let _ = model.unload().await;
                    drop(model);
                }
            }
        }

        let manager = FoundryLocalManager::create(foundry_config()).map_err(AppError::internal)?;
        let model = manager
            .catalog()
            .get_model(alias)
            .await
            .map_err(AppError::internal)?;

        // Remove every cached variant — a single alias can own both CPU and
        // GPU variants, and we don't know which one the user downloaded.
        for variant in model.variants() {
            if variant.is_cached().await.map_err(AppError::internal)? {
                variant
                    .remove_from_cache()
                    .await
                    .map_err(AppError::internal)?;
            }
        }

        Ok(())
    }

    async fn list_speech_models(&self) -> AppResult<Vec<SpeechModelStatus>> {
        let cached_guard = self.speech_models.lock().await;
        let active_guard = self.active_speech_model.lock().await;
        let active = active_guard.clone();

        let manager = FoundryLocalManager::create(foundry_config()).map_err(AppError::internal)?;

        let all_models = manager
            .catalog()
            .get_models()
            .await
            .map_err(AppError::internal)?;

        let mut seen_aliases = std::collections::BTreeSet::new();
        let mut results = Vec::new();
        for model in all_models.iter() {
            let alias = model.alias().to_string();
            if !alias.to_lowercase().starts_with("whisper") {
                continue;
            }
            if !seen_aliases.insert(alias.clone()) {
                continue;
            }
            let loaded = cached_guard.contains_key(&alias);
            let is_active = active.as_deref() == Some(alias.as_str());
            // Same per-variant runtime probe as list_all_models.
            let downloaded = Self::has_cached_variant(model).await?;

            results.push(SpeechModelStatus {
                alias,
                downloaded,
                loaded,
                active: is_active,
            });
        }

        results.sort_by(|a, b| a.alias.cmp(&b.alias));
        Ok(results)
    }

    async fn delete_speech_model(&self, alias: &str) -> AppResult<()> {
        if self.active_speech_model.lock().await.as_deref() == Some(alias) {
            return Err(AppError::bad_request(
                "使用中のモデルは削除できません。別のモデルを選択してから削除してください",
            ));
        }

        {
            let mut guard = self.speech_models.lock().await;
            guard.remove(alias);
        }

        let manager = FoundryLocalManager::create(foundry_config()).map_err(AppError::internal)?;
        let model = manager
            .catalog()
            .get_model(alias)
            .await
            .map_err(AppError::internal)?;

        // Remove every cached variant (see delete_model_by_alias for rationale).
        for variant in model.variants() {
            if variant.is_cached().await.map_err(AppError::internal)? {
                variant
                    .remove_from_cache()
                    .await
                    .map_err(AppError::internal)?;
            }
        }

        Ok(())
    }

    async fn ensure_chat_model(&self) -> AppResult<Arc<Model>> {
        self.ensure_chat_model_by_alias(None).await
    }

    /// Load (downloading if necessary) a chat-capable LLM by alias.
    /// Passing `None` falls back to the app default (`CHAT_MODEL_ALIAS`).
    /// Models are cached per-alias in `chat_models` so repeated post-processing
    /// with the same selection is fast, and switching models across steps works
    /// without reloading the entire backend.
    async fn ensure_chat_model_by_alias(&self, alias: Option<&str>) -> AppResult<Arc<Model>> {
        let requested = alias
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| CHAT_MODEL_ALIAS.to_string());

        // Fast path: already cached
        if let Some(model) = self.chat_models.lock().await.get(&requested) {
            return Ok(Arc::clone(model));
        }

        let manager = self.ensure_execution_providers().await?;
        let model = manager
            .catalog()
            .get_model(&requested)
            .await
            .map_err(AppError::internal)?;

        self.download_model_with_progress(&model, &requested)
            .await?;

        if !model.is_loaded().await.map_err(AppError::internal)? {
            model.load().await.map_err(AppError::internal)?;
        }

        self.chat_models
            .lock()
            .await
            .insert(requested.clone(), Arc::clone(&model));

        // Track the first loaded chat model as the "default" for shutdown / legacy checks.
        {
            let mut primary = self.chat_model.lock().await;
            if primary.is_none() {
                *primary = Some(Arc::clone(&model));
            }
        }

        Ok(model)
    }
}

type AppResult<T> = Result<T, AppError>;

#[derive(Debug)]
struct AppError {
    status: StatusCode,
    message: String,
}

impl AppError {
    fn bad_request(message: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.to_string(),
        }
    }

    fn internal(error: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: error.to_string(),
        }
    }
}

impl From<storage::StoreError> for AppError {
    fn from(error: storage::StoreError) -> Self {
        AppError::internal(error)
    }
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorResponse {
                error: self.message,
            }),
        )
            .into_response()
    }
}

#[derive(Serialize)]
struct TranscriptionResponse {
    text: String,
    language: Option<String>,
    duration: Option<f64>,
    note_id: Option<String>,
    project_id: Option<String>,
}

#[derive(Deserialize)]
struct RefinementRequest {
    text: String,
    custom_instruction: Option<String>,
    custom_terms: Option<String>,
    remove_fillers: bool,
    /// Accepted for backwards compatibility with older clients but no longer
    /// applied — style rewrites changed meaning too often, so they now live
    /// in custom post-processing steps instead.
    #[serde(default)]
    #[allow(dead_code)]
    style: Option<String>,
    voice_commands: bool,
    /// Optional LLM alias to use for this refinement. Falls back to the app
    /// default when absent so older clients keep working unchanged.
    #[serde(default)]
    model: Option<String>,
    /// ID of the note this request is derived from. When set (and
    /// `use_linked_context` is true), the backend looks up the note's manual
    /// links and injects a short context block into the LLM system prompt so
    /// the model can condition its output on related notes.
    #[serde(default)]
    note_id: Option<String>,
    /// Whether to include context from manually-linked notes. Defaults to
    /// true when a `note_id` is present.
    #[serde(default = "default_true")]
    use_linked_context: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Serialize)]
struct RefinementResponse {
    text: String,
}

#[derive(Deserialize)]
struct TranslationRequest {
    text: String,
    source_language: Option<String>,
    target_language: Option<String>,
    custom_instruction: Option<String>,
    custom_terms: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    note_id: Option<String>,
    #[serde(default = "default_true")]
    use_linked_context: bool,
}

#[derive(Serialize)]
struct TranslationResponse {
    text: String,
}

#[derive(Deserialize)]
struct AnalysisRequest {
    text: String,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    note_id: Option<String>,
    #[serde(default = "default_true")]
    use_linked_context: bool,
}

#[derive(Serialize, Deserialize, Debug)]
struct AnalysisResult {
    tone: String,
    summary: String,
    keywords: Vec<String>,
}

#[derive(Deserialize)]
struct CompleteNoteRequest {
    text: String,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    note_id: Option<String>,
    #[serde(default = "default_true")]
    use_linked_context: bool,
}

#[derive(Serialize)]
struct CompleteNoteResponse {
    /// Text to append to the existing body — does NOT include the original
    /// text, so the frontend can choose how to join it (newline, space, etc.).
    added: String,
}

#[derive(Deserialize)]
struct CustomStepRequest {
    #[allow(dead_code)]
    text: String,
    instruction: String,
    max_tokens: Option<u32>,
    // Optional display name, used only for labelling in the response
    label: Option<String>,
    #[serde(default)]
    model: Option<String>,
    // Accepted for API symmetry; the pipeline handler resolves linked context
    // once at the top level and shares it across all steps.
    #[serde(default)]
    #[allow(dead_code)]
    note_id: Option<String>,
    #[serde(default = "default_true")]
    #[allow(dead_code)]
    use_linked_context: bool,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum PipelineStep {
    Refine {
        #[serde(flatten)]
        config: RefinementRequest,
    },
    Translate {
        #[serde(flatten)]
        config: TranslationRequest,
    },
    Custom {
        #[serde(flatten)]
        config: CustomStepRequest,
    },
}

#[derive(Deserialize)]
struct ProcessPipelineRequest {
    text: String,
    pipeline: Vec<PipelineStep>,
    /// ID of the note this pipeline is processing. Used to look up linked
    /// notes for LLM context. Applied to every step in the pipeline.
    #[serde(default)]
    note_id: Option<String>,
    #[serde(default = "default_true")]
    use_linked_context: bool,
}

#[derive(Serialize)]
struct ProcessPipelineResponse {
    results: Vec<PipelineStepResult>,
}

#[derive(Serialize)]
struct PipelineStepResult {
    step_type: String,
    text: String,
}

async fn index_handler() -> Html<&'static str> {
    let html_text = r##"
        <!doctype html>
        <html lang="ja">
            <head>
                <meta charset="utf-8" />
                <meta name="viewport" content="width=device-width, initial-scale=1" />
                <title data-i18n="app.title">SeeDraft</title>
            </head>
            <body>
                <main class="app-shell">
                    <section class="main-workspace">
                        <header class="topbar">
                            <div class="topbar-left">
                                <span class="app-brand" title="SeeDraft" aria-label="SeeDraft">
                                    <img class="app-brand-icon" src="/assets/icon.ico" alt="SeeDraft" />
                                </span>
                                <div class="topbar-divider"></div>
                                <div class="project-selector" role="group" aria-label="Project">
                                    <select id="projectSelect" class="project-select" aria-label="Project"></select>
                                    <button id="newProjectButton" class="icon-button" type="button" title="" data-i18n-title="project.new">＋</button>
                                </div>
                                <div class="topbar-divider"></div>
                                <button id="openLiveButton" class="icon-button topbar-nav" type="button" data-i18n="button.liveCaption">🎬 ライブ字幕</button>
                                <button id="openDraftsButton" class="icon-button topbar-nav" type="button" data-i18n="button.drafts">📄 ドラフト</button>
                            </div>
                            <div class="topbar-right">
                                <div id="currentModelBadge" class="model-badge" hidden>
                                    <span class="model-badge-icon">🎙</span>
                                    <span class="model-badge-label" id="currentModelLabel">whisper-tiny</span>
                                </div>
                                <label id="currentLanguageBadge" class="model-badge language-badge" data-i18n-title="settings.transcription.language" title="認識言語">
                                    <span class="model-badge-icon">🌐</span>
                                    <select id="language" class="topbar-language-select" data-i18n-title="settings.transcription.language" title="認識言語" aria-label="Recognition language">
                                        <option value="" data-i18n="settings.transcription.auto">自動判定</option>
                                        <option value="ja">日本語</option>
                                        <option value="en">English</option>
                                    </select>
                                </label>
                                <div id="statusBadge" class="status-badge" data-i18n="status.idle">待機中</div>
                                <button id="openSettingsButton" class="icon-button settings-button" type="button" data-i18n-title="button.settings" title="設定" aria-label="Settings">⚙</button>
                            </div>
                        </header>

                        <!-- ノートワークスペース: サイドバー + グラフ + 下部の文字起こしワークエリア -->
                        <div class="notes-workspace main-notes-workspace" id="mainNotesWorkspace">
                            <aside class="notes-sidebar">
                                <div class="notes-sidebar-toolbar">
                                    <input id="historySearch" type="text" class="notes-search" data-i18n-placeholder="history.search" placeholder="検索..." />
                                    <button id="notesSelectToggle" class="icon-button" type="button" data-i18n-title="draft.selectMode" title="選択モード" aria-label="Select mode">☑</button>
                                    <button id="refreshHistoryButton" class="icon-button notes-refresh" type="button" data-i18n-title="history.refresh" title="再読込" aria-label="Refresh">↻</button>
                                </div>
                                <div id="notesSelectionBar" class="notes-selection-bar" hidden>
                                    <span class="notes-selection-count"><span id="notesSelectionCount">0</span> <span data-i18n="draft.selectedCount">件選択中</span></span>
                                    <button id="notesSelectAll" class="text-button" type="button" data-i18n="draft.selectAll">全選択</button>
                                    <button id="notesSelectionCompose" class="text-button primary-inline" type="button" data-i18n="draft.compose">📄 ドラフト化</button>
                                </div>
                                <div class="notes-sidebar-list" id="notesGrid"></div>
                            </aside>
                            <section class="notes-graph-pane">
                                <div class="notes-graph-toolbar">
                                    <span class="graph-legend">
                                        <span class="graph-legend-dot" data-kind="project"></span>
                                        <span data-i18n="history.graph.project">プロジェクト</span>
                                        <span class="graph-legend-dot" data-kind="note"></span>
                                        <span data-i18n="history.graph.note">文字起こし</span>
                                        <span class="graph-legend-dot" data-kind="tag"></span>
                                        <span data-i18n="history.graph.tag">タグ</span>
                                        <span class="graph-legend-line" data-kind="link"></span>
                                        <span data-i18n="history.graph.link">リンク</span>
                                        <span class="graph-legend-line" data-kind="parent"></span>
                                        <span data-i18n="history.graph.parent">親子</span>
                                    </span>
                                    <span class="notes-header-meta" id="notesHeaderMeta"></span>
                                    <button id="graphLinkModeButton" class="icon-button" type="button" data-i18n="history.graph.linkMode">🔗 リンク作成</button>
                                    <button id="refreshGraphButton" class="icon-button" type="button" data-i18n-title="history.refresh" title="再読込" aria-label="Refresh graph">↻</button>
                                </div>
                                <div id="graphLinkBanner" class="graph-link-banner" hidden>
                                    <span id="graphLinkBannerText" data-i18n="history.graph.linkFirst">リンク元のノートを選択</span>
                                    <button id="graphLinkCancelButton" class="text-button" type="button" data-i18n="history.graph.linkCancel">キャンセル</button>
                                </div>
                                <div class="graph-container">
                                    <svg id="graphSvg" class="graph-svg"></svg>
                                    <div class="graph-detail" id="graphDetail" hidden>
                                        <button class="graph-detail-close" id="graphDetailClose" type="button">✕</button>
                                        <h3 id="graphDetailTitle"></h3>
                                        <p class="graph-detail-kind" id="graphDetailKind"></p>
                                        <div class="graph-detail-content" id="graphDetailContent"></div>
                                    </div>
                                </div>

                                <!-- グラフの下: 文字起こし結果 + 録音コントロール -->
                                <div class="work-pane">
                                    <div class="caption-wrapper">
                                        <textarea id="captionContent" class="caption-content" data-i18n-placeholder="caption.placeholder" placeholder="ここに文字起こし結果が表示されます"></textarea>
                                        <span id="captionDirty" class="caption-dirty" hidden data-i18n="caption.unsaved">未保存</span>
                                    </div>
                                    <div id="captionStats" class="caption-stats" hidden>
                                        <span class="caption-stat">
                                            <span class="caption-stat-icon">📝</span>
                                            <span class="caption-stat-value" id="statChars">0</span>
                                        </span>
                                        <span class="caption-stat">
                                            <span class="caption-stat-icon">💭</span>
                                            <span class="caption-stat-value" id="statFillers">0</span>
                                        </span>
                                        <span class="caption-stat" id="statDurationWrap" hidden>
                                            <span class="caption-stat-icon">⏱</span>
                                            <span class="caption-stat-value" id="statDuration">0s</span>
                                        </span>
                                        <span class="caption-stat" id="statSpeedWrap" hidden>
                                            <span class="caption-stat-icon">⚡</span>
                                            <span class="caption-stat-value" id="statSpeed">0</span>
                                        </span>
                                    </div>
                                    <div class="control-bar">
                                        <div class="control-bar-left">
                                            <button id="copyMainButton" class="icon-button" type="button" data-i18n-title="button.copy" title="コピー" aria-label="Copy">📋</button>
                                            <button id="clearMainButton" class="icon-button" type="button" data-i18n-title="button.clear" title="クリア" aria-label="Clear">🗑</button>
                                            <button id="saveToHistoryButton" class="text-button primary-inline" type="button" data-i18n="button.saveToHistory">📥 ノートに追加</button>
                                        </div>
                                        <div class="quick-toggles">
                                            <label class="quick-toggle" data-i18n-title="quick.autoSaveTooltip" title="文字起こし完了後、自動的にノートに追加します">
                                                <input id="quickAutoSave" type="checkbox" />
                                                <span class="quick-toggle-icon">📥</span>
                                                <span class="quick-toggle-text" data-i18n="quick.autoSave">ノート追加</span>
                                            </label>
                                            <span class="quick-toggle-group" role="group" aria-labelledby="quickPostprocessLabel">
                                                <span id="quickPostprocessLabel" class="quick-toggle-group-label" data-i18n="quick.postprocessGroup">後処理</span>
                                                <label class="quick-toggle" data-i18n-title="quick.autoRefineTooltip" title="文字起こし後に整形します">
                                                    <input id="quickAutoRefine" type="checkbox" />
                                                    <span class="quick-toggle-icon">✨</span>
                                                    <span class="quick-toggle-text" data-i18n="quick.autoRefine">整形</span>
                                                </label>
                                                <label class="quick-toggle" data-i18n-title="quick.autoTranslateTooltip" title="文字起こし後に翻訳します">
                                                    <input id="quickAutoTranslate" type="checkbox" />
                                                    <span class="quick-toggle-icon">🌐</span>
                                                    <span class="quick-toggle-text" data-i18n="quick.autoTranslate">翻訳</span>
                                                </label>
                                                <label class="quick-toggle" data-i18n-title="quick.autoAnalyzeTooltip" title="文字起こし後にトーン・要点・キーワードを抽出します">
                                                    <input id="quickAutoAnalyze" type="checkbox" />
                                                    <span class="quick-toggle-icon">🔍</span>
                                                    <span class="quick-toggle-text" data-i18n="quick.autoAnalyze">抽出</span>
                                                </label>
                                                <label class="quick-toggle" data-i18n-title="quick.autoCustomTooltip" title="文字起こし後にカスタム後処理ステップを実行します">
                                                    <input id="quickAutoCustom" type="checkbox" />
                                                    <span class="quick-toggle-icon">⚙</span>
                                                    <span class="quick-toggle-text" data-i18n="quick.autoCustom">カスタム</span>
                                                </label>
                                            </span>
                                        </div>
                                        <div class="record-button-wrap">
                                            <div class="record-progress-ring" id="recordProgressRing" aria-hidden="true"></div>
                                            <button id="recordButton" class="record-button" type="button">
                                                <span class="record-icon" aria-hidden="true">⏺</span>
                                                <span class="record-label" data-i18n="button.record.start">録音開始</span>
                                                <span class="record-countdown" id="recordCountdown" aria-hidden="true" hidden></span>
                                            </button>
                                        </div>
                                    </div>
                                    <div class="record-info" id="recordInfo" data-empty="true" aria-live="polite">
                                        <span class="record-info-icon" aria-hidden="true">i</span>
                                        <span class="record-info-text" id="recordInfoText" data-i18n="record.infoPlaceholder">メッセージがここに表示されます</span>
                                    </div>
                                </div>
                            </section>
                        </div>

                        <div class="drop-overlay" id="dropOverlay">
                            <div class="drop-prompt">
                                <div class="drop-icon">🎤</div>
                                <p data-i18n="drop.title">音声ファイルをドロップ</p>
                                <small data-i18n="drop.formats">mp3 / wav / m4a / webm</small>
                            </div>
                        </div>

                        <div class="download-overlay" id="downloadOverlay" hidden>
                            <div class="download-card">
                                <div class="download-spinner"></div>
                                <h3 id="downloadTitle" data-i18n="download.title">モデルを準備中</h3>
                                <p id="downloadMessage" data-i18n="download.message">初回はダウンロードに数分かかります</p>
                                <div class="progress-bar">
                                    <div class="progress-fill" id="progressFill" style="width: 0%"></div>
                                </div>
                                <div class="progress-text">
                                    <span id="progressPercent">0%</span>
                                    <span id="progressAlias"></span>
                                </div>
                                <div class="download-actions">
                                    <button id="downloadCancelButton" class="secondary-button" type="button" data-i18n="download.cancel">キャンセル</button>
                                </div>
                                <p class="download-cancel-hint" data-i18n="download.cancelHint">キャンセル後もバックグラウンドでダウンロードは続行されます（中断はできません）。ダウンロードが完了すれば、次回以降はキャッシュから起動します。</p>
                            </div>
                        </div>

                        <div id="downloadDiagnostic" class="download-diagnostic" hidden>
                            <div class="download-diagnostic-inner">
                                <div class="download-diagnostic-head">
                                    <strong data-i18n="download.diagnostic.title">ダウンロードに失敗しました</strong>
                                    <button id="downloadDiagnosticClose" class="icon-button" type="button" aria-label="Close">✕</button>
                                </div>
                                <p class="download-diagnostic-alias" id="downloadDiagnosticAlias"></p>
                                <pre class="download-diagnostic-error" id="downloadDiagnosticError"></pre>
                                <div class="download-diagnostic-variants">
                                    <div class="download-diagnostic-variants-head" data-i18n="download.diagnostic.variants">このモデルの利用可能なバリアント</div>
                                    <div id="downloadDiagnosticVariants"></div>
                                </div>
                            </div>
                        </div>

                        <div id="errorBanner" class="error-banner" hidden>
                            <div class="error-banner-content">
                                <span class="error-banner-icon">⚠</span>
                                <div class="error-banner-body">
                                    <strong data-i18n="error.title">エラー</strong>
                                    <p id="errorBannerMessage"></p>
                                </div>
                                <button id="errorBannerClose" type="button" class="error-banner-close" aria-label="Close">✕</button>
                            </div>
                        </div>

                        <div id="requirementsBanner" class="requirements-banner" hidden>
                            <div class="requirements-banner-content">
                                <span class="requirements-banner-icon">!</span>
                                <div class="requirements-banner-body">
                                    <strong id="requirementsBannerTitle" data-i18n="requirements.title">実行環境の準備が必要です</strong>
                                    <p id="requirementsBannerMessage"></p>
                                    <code id="requirementsCommand" class="requirements-command" hidden></code>
                                </div>
                                <div class="requirements-banner-actions">
                                    <button id="requirementsPrimaryButton" type="button" class="text-button primary-inline" data-i18n="requirements.prepareModels">モデルを準備</button>
                                    <button id="requirementsModelsButton" type="button" class="text-button" data-i18n="requirements.openModels">モデル設定</button>
                                    <button id="requirementsBannerClose" type="button" class="icon-button" data-i18n-title="requirements.dismiss" title="閉じる" aria-label="Close">✕</button>
                                </div>
                            </div>
                        </div>

                    </section>

                    <input id="audioFile" type="file" accept="audio/*" style="display: none;" />

                    <!-- Toast notifications -->
                    <div id="toastContainer" class="toast-container" aria-live="polite" aria-atomic="true"></div>

                    <!-- 設定画面（モーダル） -->
                    <div class="settings-modal" id="settingsModal" hidden>
                        <div class="settings-container">
                            <div class="settings-header">
                                <h2 data-i18n="settings.title">設定</h2>
                                <div class="settings-header-actions">
                                    <button id="resetSettingsButton" class="text-button" type="button" data-i18n="settings.reset">初期設定に戻す</button>
                                    <button id="closeSettingsButton" class="icon-button" type="button">✕</button>
                                </div>
                            </div>

                            <div class="settings-tabs">
                                <button class="settings-tab-button" type="button" data-tab="transcription" aria-selected="true" data-i18n="settings.tab.transcription">文字起こし</button>
                                <button class="settings-tab-button" type="button" data-tab="postprocess" data-i18n="settings.tab.postprocess">後処理</button>
                                <button class="settings-tab-button" type="button" data-tab="models" data-i18n="settings.tab.models">モデル</button>
                                <button class="settings-tab-button" type="button" data-tab="output" data-i18n="settings.tab.output">出力</button>
                                <button class="settings-tab-button" type="button" data-tab="appearance" data-i18n="settings.tab.appearance">表示</button>
                                <button class="settings-tab-button" type="button" data-tab="shortcuts" data-i18n="settings.tab.shortcuts">ショートカット</button>
                            </div>

                            <div class="settings-content">
                                <div class="settings-panel" data-panel="transcription">
                                    <label class="field-label" data-i18n="settings.transcription.model">Whisperモデル</label>
                                    <div class="current-model-card" id="currentModelCard">
                                        <div class="current-model-card-head">
                                            <span class="current-model-icon" id="currentModelCardIcon">🎙</span>
                                            <div class="current-model-info">
                                                <code id="currentModelCardAlias" class="current-model-alias">—</code>
                                                <p id="currentModelCardDesc" class="current-model-desc"></p>
                                            </div>
                                            <span id="currentModelCardStatus" class="model-row-status" data-variant="missing"></span>
                                        </div>
                                        <p class="current-model-hint" data-i18n="settings.transcription.manageHint">モデルの切り替えや削除は「モデル」タブから行えます。</p>
                                        <button id="currentModelJumpButton" class="secondary-button" type="button" data-i18n="settings.transcription.openModels">モデル設定を開く</button>
                                    </div>
                                    <input type="hidden" id="speechModel" value="whisper-tiny" />

                                    <label class="field-label" for="transcriptionPrompt" data-i18n="settings.transcription.prompt">カスタムプロンプト</label>
                                    <textarea id="transcriptionPrompt" class="small-textarea" data-i18n-placeholder="settings.transcription.promptPlaceholder" placeholder="会議の議題、話者名、専門用語、表記ルールなど"></textarea>

                                    <label class="field-label" for="microphoneDevice" data-i18n="settings.transcription.microphone">マイクデバイス</label>
                                    <div class="input-action-row">
                                        <select id="microphoneDevice">
                                            <option value="" data-i18n="settings.transcription.microphoneDefault">既定のマイク</option>
                                        </select>
                                        <button id="microphoneRefreshButton" class="icon-button" type="button" data-i18n-title="settings.models.refresh" title="再読込" aria-label="Refresh microphones">↻</button>
                                    </div>
                                    <div class="mic-monitor" id="microphoneMonitor">
                                        <div class="mic-monitor-head">
                                            <span class="mic-monitor-label" data-i18n="settings.transcription.micMonitor">マイク入力</span>
                                        </div>
                                        <canvas id="microphoneMonitorCanvas" class="mic-monitor-canvas" width="640" height="96" aria-label="Microphone input waveform"></canvas>
                                        <span id="microphoneMonitorStatus" class="mic-monitor-status" data-i18n="settings.transcription.micMonitorIdle">停止中</span>
                                    </div>

                                    <label class="field-label" for="maxRecordingSeconds" data-i18n="settings.transcription.maxRecording">最大録音時間（秒）</label>
                                    <input id="maxRecordingSeconds" type="number" min="10" max="7200" step="10" value="300" />

                                    <label class="field-label" for="sampleRate" data-i18n="settings.transcription.sampleRate">サンプルレート</label>
                                    <select id="sampleRate">
                                        <option value="" data-i18n="settings.common.auto">自動</option>
                                        <option value="16000">16 kHz</option>
                                        <option value="24000">24 kHz</option>
                                        <option value="44100">44.1 kHz</option>
                                        <option value="48000">48 kHz</option>
                                    </select>
                                </div>

                                <div class="settings-panel" data-panel="postprocess" hidden>
                                    <div class="postprocess-section">
                                        <h3 class="section-title" data-i18n="settings.linkedContext.title">リンク先ノートを文脈として利用</h3>
                                        <p class="field-hint" data-i18n="settings.linkedContext.hint">手動で張ったリンクの先にあるノートの抜粋を、整形・翻訳・抽出・カスタム後処理のシステムプロンプトに追加します。関連ノートを踏まえた結果が得られます。</p>
                                        <label class="toggle-row">
                                            <input id="linkedContextToggle" type="checkbox" checked />
                                            <span data-i18n="settings.linkedContext.enabled">有効にする</span>
                                        </label>
                                    </div>

                                    <div class="postprocess-section">
                                        <h3 class="section-title" data-i18n="settings.complete.title">補完設定</h3>
                                        <p class="field-hint" data-i18n="settings.complete.hint">ノート編集画面の「✨ 補完」ボタンで本文の続きを生成します。リンク先ノートのコンテキストが有効な場合は関連ノートを踏まえた続きが生成されます。</p>
                                        <label class="field-label" for="completeModel" data-i18n="settings.complete.model">使用するモデル</label>
                                        <select id="completeModel" class="post-model-select"></select>
                                    </div>

                                    <div class="postprocess-section">
                                        <h3 class="section-title" data-i18n="settings.refine.title">整形設定</h3>
                                        <p class="field-hint" data-i18n="settings.refine.hint">文字起こしの明らかな誤変換やフィラー・句読点のゆれのみを補正します。スタイルや言い回しの変更は行いません（必要な場合はカスタム後処理で行ってください）。</p>
                                        <label class="field-label" for="refineModel" data-i18n="settings.refine.model">使用するモデル</label>
                                        <select id="refineModel" class="post-model-select"></select>
                                        <label class="toggle-row">
                                            <input id="removeFillers" type="checkbox" checked />
                                            <span data-i18n="settings.refine.removeFillers">フィラー除去</span>
                                        </label>
                                        <p class="field-hint field-hint-sub" data-i18n="settings.refine.removeFillersHint">「あー」「えー」「えっと」「なんか」「um」「uh」などの言いよどみを除去します。</p>
                                        <label class="toggle-row">
                                            <input id="voiceCommands" type="checkbox" checked />
                                            <span data-i18n="settings.refine.voiceCommands">音声コマンド反映</span>
                                        </label>
                                        <p class="field-hint field-hint-sub" data-i18n="settings.refine.voiceCommandsHint">話し言葉の「改行」「段落」「句点」「読点」などをそれぞれ改行・句読点に置換します。英語は「new line」「new paragraph」「period」「comma」などに対応します。</p>
                                        <label class="field-label" for="customTerms" data-i18n="settings.refine.customTerms">専門用語・固有名詞</label>
                                        <textarea id="customTerms" class="small-textarea" data-i18n-placeholder="settings.refine.customTermsPlaceholder" placeholder="SeeDraft, Foundry Local など"></textarea>
                                        <label class="field-label" for="customInstruction" data-i18n="settings.refine.customInstruction">追加指示</label>
                                        <input id="customInstruction" type="text" data-i18n-placeholder="settings.refine.customInstructionPlaceholder" placeholder="例: 句読点の位置を自然に" />
                                    </div>

                                    <div class="postprocess-section">
                                        <h3 class="section-title" data-i18n="settings.translation.title">翻訳設定</h3>
                                        <label class="field-label" for="translateModel" data-i18n="settings.translation.model">使用するモデル</label>
                                        <select id="translateModel" class="post-model-select"></select>
                                        <label class="field-label" for="sourceLanguage" data-i18n="settings.translation.source">翻訳元言語</label>
                                        <select id="sourceLanguage">
                                            <option value="auto" data-i18n="settings.translation.autoDetect">自動判定</option>
                                            <option value="ja">日本語</option>
                                            <option value="en">English</option>
                                            <option value="ko">한국어</option>
                                            <option value="zh">中文</option>
                                        </select>
                                        <label class="field-label" for="targetLanguage" data-i18n="settings.translation.target">翻訳先言語</label>
                                        <select id="targetLanguage">
                                            <option value="ja">日本語</option>
                                            <option value="en">English</option>
                                            <option value="ko">한국어</option>
                                            <option value="zh">中文</option>
                                        </select>
                                        <label class="field-label" for="translationInstruction" data-i18n="settings.translation.instruction">翻訳指示</label>
                                        <textarea id="translationInstruction" class="small-textarea" data-i18n-placeholder="settings.translation.instructionPlaceholder" placeholder="製品名は英語表記のまま"></textarea>
                                    </div>

                                    <div class="postprocess-section">
                                        <h3 class="section-title" data-i18n="settings.analyze.title">抽出設定</h3>
                                        <p class="field-hint" data-i18n="settings.analyze.hint">トーン・要点・キーワードを抽出します。</p>
                                        <label class="field-label" for="analyzeModel" data-i18n="settings.analyze.model">使用するモデル</label>
                                        <select id="analyzeModel" class="post-model-select"></select>
                                    </div>

                                    <div class="postprocess-section">
                                        <div class="postprocess-section-header">
                                            <h3 class="section-title" data-i18n="settings.custom.title">カスタム後処理</h3>
                                            <div class="postprocess-section-header-actions">
                                                <button id="customStepPresetButton" class="secondary-button" type="button" data-i18n="settings.custom.preset.add">＋ プリセット</button>
                                                <button id="customStepAddButton" class="secondary-button" type="button" data-i18n="settings.custom.add">＋ 追加</button>
                                            </div>
                                        </div>
                                        <p class="field-hint" data-i18n="settings.custom.hint">要約・Markdown化・独自指示のステップを作成し、文字起こし結果に適用できます。「プリセット」からスタイル変更のテンプレート（丁寧・ビジネス・議事録など）をすぐ追加できます。</p>
                                        <div id="customStepList" class="custom-step-list"></div>
                                    </div>

                                    <!-- Preset picker: shown when the user clicks ＋ プリセット -->
                                    <div id="customPresetModal" class="note-editor-modal" hidden>
                                        <div class="note-editor-card">
                                            <div class="note-editor-header">
                                                <h3 data-i18n="settings.custom.preset.pickTitle">プリセットを選択</h3>
                                                <button id="customPresetClose" class="icon-button" type="button" aria-label="Close">✕</button>
                                            </div>
                                            <p class="field-hint" data-i18n="settings.custom.preset.pickHint">選んだプリセットをカスタム後処理に追加します。既に同名のステップがある場合は内容を最新版に更新します。</p>
                                            <div id="customPresetList" class="custom-preset-list"></div>
                                        </div>
                                    </div>

                                </div>

                                <!-- カスタムステップ編集モーダル -->
                                <div class="note-editor-modal" id="customStepEditorModal" hidden>
                                    <div class="note-editor-card">
                                        <div class="note-editor-header">
                                            <h3 data-i18n="settings.custom.editorTitle">カスタムステップ</h3>
                                            <button id="customStepEditorClose" class="icon-button" type="button">✕</button>
                                        </div>
                                        <label class="field-label" data-i18n="settings.custom.name">名前</label>
                                        <input id="customStepName" type="text" data-i18n-placeholder="settings.custom.namePlaceholder" placeholder="例: 議事録スタイルに変換" />

                                        <label class="field-label" data-i18n="settings.custom.instruction">LLMへの指示</label>
                                        <textarea id="customStepInstruction" class="small-textarea" data-i18n-placeholder="settings.custom.instructionPlaceholder" placeholder="例: 議事録スタイルに変換してください。決定事項とToDoを箇条書きで示す。"></textarea>

                                        <label class="field-label" for="customStepModel" data-i18n="settings.custom.model">使用するモデル</label>
                                        <select id="customStepModel" class="post-model-select"></select>


                                        <div class="note-editor-actions">
                                            <button id="customStepDelete" class="danger-button" type="button" hidden data-i18n="settings.custom.delete">削除</button>
                                            <button id="customStepSave" class="primary-button" type="button" data-i18n="settings.custom.save">保存</button>
                                        </div>
                                    </div>
                                </div>

                                <div class="settings-panel" data-panel="models" hidden>
                                    <div class="models-summary" id="modelsSummary">
                                        <div class="models-summary-row">
                                            <span class="models-summary-label" data-i18n="settings.models.cacheDir">Foundry Local キャッシュ</span>
                                            <code id="modelsCachePath" class="models-summary-value">—</code>
                                        </div>
                                        <div class="models-summary-row">
                                            <span class="models-summary-label" data-i18n="settings.models.downloadedCount">ダウンロード済み</span>
                                            <span id="modelsDownloadedCount" class="models-summary-value">0</span>
                                            <button id="modelsRefreshButton" class="icon-button" type="button" data-i18n-title="settings.models.refresh" title="再読込">↻</button>
                                        </div>
                                        <div class="models-summary-row">
                                            <span class="models-summary-label" data-i18n="settings.models.runtime">実行環境</span>
                                            <span id="modelsEpList" class="models-summary-value models-ep-list">—</span>
                                        </div>
                                    </div>

                                    <div class="models-section">
                                        <div class="models-section-header">
                                            <span class="models-section-icon">🎙</span>
                                            <div>
                                                <h3 data-i18n="settings.models.speechGroup">音声認識 (Speech-to-Text)</h3>
                                                <p class="models-section-desc" data-i18n="settings.models.speechDesc">録音した音声をテキストに変換します。精度と速度のトレードオフで選択してください。</p>
                                            </div>
                                        </div>
                                        <div class="models-group-list" id="modelsSpeechList"></div>
                                    </div>

                                    <div class="models-section">
                                        <div class="models-section-header">
                                            <span class="models-section-icon">💬</span>
                                            <div>
                                                <h3 data-i18n="settings.models.chatGroup">テキスト処理 (LLM)</h3>
                                                <p class="models-section-desc" data-i18n="settings.models.chatDesc">整形・翻訳・抽出に使用します。プロジェクト補正や後処理パイプラインの裏で動作します。</p>
                                            </div>
                                        </div>
                                        <div class="models-group-list" id="modelsChatList"></div>
                                    </div>
                                </div>

                                <div class="settings-panel" data-panel="output" hidden>
                                    <label class="field-label" for="saveFolder" data-i18n="settings.output.folder">保存先フォルダ</label>
                                    <div class="folder-row">
                                        <input id="saveFolder" type="text" data-i18n-placeholder="settings.output.folderPlaceholder" placeholder="ブラウザのダウンロード先" readonly />
                                        <button id="chooseSaveFolderButton" class="secondary-button" type="button" data-i18n="button.choose">選択</button>
                                    </div>
                                    <label class="field-label" for="filePrefix" data-i18n="settings.output.prefix">ファイル名プレフィックス</label>
                                    <input id="filePrefix" type="text" value="seedraft" />
                                </div>

                                <div class="settings-panel" data-panel="shortcuts" hidden>
                                    <p class="field-hint" data-i18n="settings.shortcuts.hint">各項目の入力欄をクリックして、割り当てたいキーを押してください。Esc で取り消し、Delete / Backspace で解除できます。</p>
                                    <div id="shortcutList" class="shortcut-list"></div>
                                    <div class="shortcut-actions">
                                        <button id="shortcutResetButton" type="button" class="secondary-button" data-i18n="settings.shortcuts.reset">既定値に戻す</button>
                                    </div>
                                </div>

                                <div class="settings-panel" data-panel="appearance" hidden>
                                    <label class="field-label" for="uiTheme" data-i18n="settings.appearance.theme">テーマ</label>
                                    <select id="uiTheme">
                                        <option value="auto" data-i18n="settings.appearance.themeAuto">システム設定に合わせる</option>
                                        <option value="light" data-i18n="settings.appearance.themeLight">ライト</option>
                                        <option value="dark" data-i18n="settings.appearance.themeDark">ダーク</option>
                                    </select>

                                    <label class="field-label" for="uiLanguage" data-i18n="settings.appearance.language">表示言語</label>
                                    <select id="uiLanguage">
                                        <option value="auto" data-i18n="settings.appearance.languageAuto">OS に従う</option>
                                    </select>
                                    <p class="field-hint" data-i18n="settings.appearance.localeHint">このフォルダに <code>xx.json</code> を追加すると新しい言語を追加できます:</p>
                                    <div class="folder-row">
                                        <input id="localesDirInput" type="text" readonly />
                                        <button id="chooseLocalesDirButton" class="secondary-button" type="button" data-i18n="button.choose">選択</button>
                                        <button id="resetLocalesDirButton" class="secondary-button" type="button" data-i18n="settings.common.reset">既定</button>
                                        <button id="copyLocalesDirButton" class="secondary-button" type="button" data-i18n="button.copy">コピー</button>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>

                    <!-- 履歴画面（フルスクリーンオーバーレイ） -->
                    <!-- ノート編集モーダル -->
                    <div class="note-editor-modal" id="noteEditorModal" hidden>
                        <div class="note-editor-card">
                            <div class="note-editor-header">
                                <h3 data-i18n="note.edit">ノート編集</h3>
                                <button id="noteEditorClose" class="icon-button" type="button">✕</button>
                            </div>
                            <label class="field-label" data-i18n="note.title">タイトル</label>
                            <input id="noteEditorTitle" type="text" />
                            <label class="field-label" data-i18n="note.text">本文</label>
                            <textarea id="noteEditorText" class="note-editor-text"></textarea>
                            <label class="field-label" data-i18n="note.parent">親ノート</label>
                            <div class="note-parent-row">
                                <select id="noteEditorParent" class="note-parent-select"></select>
                                <button id="noteEditorClearParent" class="icon-button" type="button" data-i18n-title="note.clearParent" title="親をクリア" aria-label="Clear parent">✕</button>
                            </div>
                            <label class="field-label" data-i18n="note.tags">タグ（カンマ区切り）</label>
                            <div class="tag-input-wrap">
                                <input id="noteEditorTags" type="text" list="noteTagSuggestions" data-i18n-placeholder="note.tagsPlaceholder" placeholder="会議, プロジェクトA, 重要" autocomplete="off" />
                                <datalist id="noteTagSuggestions"></datalist>
                                <div id="noteTagChips" class="tag-chip-suggestions"></div>
                            </div>
                            <div id="noteAnalysisResult" class="note-analysis" hidden></div>
                            <div class="note-editor-actions">
                                <button id="noteEditorDelete" class="danger-button" type="button" data-i18n="note.delete">削除</button>
                                <button id="noteEditorComplete" class="secondary-button" type="button" data-i18n="note.complete">✨ 補完</button>
                                <button id="noteEditorAnalyze" class="secondary-button" type="button" data-i18n="note.analyze">🔍 抽出</button>
                                <button id="noteEditorSave" class="primary-button" type="button" data-i18n="note.save">保存</button>
                            </div>
                        </div>
                    </div>

                    <!-- 翻訳画面（フルスクリーンオーバーレイ） -->
                    <!-- ライブ字幕オーバーレイ -->
                    <!-- ドラフト合成ダイアログ -->
                    <div class="note-editor-modal" id="composeDraftModal" hidden>
                        <div class="note-editor-card">
                            <div class="note-editor-header">
                                <h3 data-i18n="draft.composeTitle">ノートからドラフトを作成</h3>
                                <button id="composeDraftClose" class="icon-button" type="button">✕</button>
                            </div>
                            <p class="field-hint" id="composeDraftSummary"></p>

                            <label class="field-label" data-i18n="draft.title">タイトル</label>
                            <input id="composeDraftTitle" type="text" data-i18n-placeholder="draft.titlePlaceholder" placeholder="例: 週次まとめ" />

                            <label class="field-label" data-i18n="draft.mode">合成方法</label>
                            <select id="composeDraftMode">
                                <option value="concat" data-i18n="draft.modeConcat">単純結合（見出し付き）</option>
                                <option value="llm" data-i18n="draft.modeLlm">LLM再構成（1本の記事にする）</option>
                            </select>

                            <div id="composeDraftInstructionWrap" hidden>
                                <label class="field-label" data-i18n="draft.instruction">追加指示（任意）</label>
                                <textarea id="composeDraftInstruction" class="small-textarea" data-i18n-placeholder="draft.instructionPlaceholder" placeholder="例: 意思決定を先に、背景は後半に。敬体で。"></textarea>
                            </div>

                            <div class="note-editor-actions">
                                <button id="composeDraftCancel" class="secondary-button" type="button" data-i18n="button.clear">キャンセル</button>
                                <button id="composeDraftConfirm" class="primary-button" type="button" data-i18n="draft.create">作成</button>
                            </div>
                        </div>
                    </div>

                    <!-- ドラフト一覧 + 編集 オーバーレイ -->
                    <div class="fullscreen-overlay" id="draftsOverlay" hidden>
                        <div class="fullscreen-header">
                            <h2 data-i18n="draft.title.plural">📄 ドラフト</h2>
                            <button id="closeDraftsButton" class="icon-button" type="button">✕</button>
                        </div>
                        <div class="fullscreen-body drafts-workspace">
                            <aside class="drafts-sidebar">
                                <h3 class="sidebar-title" data-i18n="draft.list">ドラフト一覧</h3>
                                <ol class="drafts-list" id="draftsList"></ol>
                            </aside>
                            <main class="drafts-main">
                                <div class="drafts-editor-toolbar">
                                    <input id="draftEditorTitle" type="text" class="draft-title-input" data-i18n-placeholder="draft.title" placeholder="タイトル" disabled />
                                    <div class="drafts-editor-actions">
                                        <button id="draftCopyButton" class="icon-button" type="button" data-i18n-title="button.copy" title="コピー" disabled>📋</button>
                                        <button id="draftExportButton" class="icon-button" type="button" data-i18n-title="draft.export" title="エクスポート" disabled>💾</button>
                                        <button id="draftSaveButton" class="text-button primary-inline" type="button" data-i18n="draft.save" disabled>保存</button>
                                        <button id="draftDeleteButton" class="danger-button" type="button" data-i18n="draft.delete" hidden>削除</button>
                                    </div>
                                </div>
                                <div class="drafts-main-body">
                                    <textarea id="draftEditorContent" class="draft-editor-content" data-i18n-placeholder="draft.editorPlaceholder" placeholder="ドラフトを選択するか、ノート画面から新規作成してください" disabled></textarea>
                                    <aside class="draft-references" id="draftReferences" hidden>
                                        <h4 data-i18n="draft.references">元のノート</h4>
                                        <ol id="draftReferencesList"></ol>
                                    </aside>
                                </div>
                            </main>
                        </div>
                    </div>

                    <div class="fullscreen-overlay" id="liveOverlay" hidden>
                        <div class="fullscreen-header">
                            <h2 data-i18n="live.title">🎬 ライブ字幕</h2>
                            <div class="live-header-meta" id="liveHeaderMeta">
                                <span class="live-timer" id="liveTimer">00:00</span>
                                <span id="liveRecBadge" class="live-rec-badge" hidden>REC</span>
                            </div>
                            <button id="closeLiveButton" class="icon-button" type="button">✕</button>
                        </div>
                        <div class="fullscreen-body live-workspace">
                            <aside class="live-sidebar">
                                <h3 class="sidebar-title" data-i18n="live.savedSessions">保存済みセッション</h3>
                                <ol class="live-sidebar-list" id="liveSessionsList"></ol>
                            </aside>
                            <main class="live-main">
                                <div class="live-config-row" id="liveConfigRow">
                                    <label class="field-label-inline" data-i18n="live.sourceLanguage">原言語</label>
                                    <select id="liveSourceLanguage" class="language-inline">
                                        <option value="">自動判定</option>
                                        <option value="ja">日本語</option>
                                        <option value="en">English</option>
                                        <option value="ko">한국어</option>
                                        <option value="zh">中文</option>
                                    </select>
                                    <label class="toggle-row live-toggle">
                                        <input id="liveTranslateToggle" type="checkbox" checked />
                                        <span data-i18n="live.enableTranslation">同時翻訳</span>
                                    </label>
                                    <label class="field-label-inline" data-i18n="live.targetLanguage">訳先</label>
                                    <select id="liveTargetLanguage" class="language-inline">
                                        <option value="ja">日本語</option>
                                        <option value="en" selected>English</option>
                                        <option value="ko">한국어</option>
                                        <option value="zh">中文</option>
                                    </select>
                                </div>
                                <div class="live-model-row" id="liveModelRow">
                                    <label class="field-label-inline" for="liveSpeechModel" data-i18n="live.transcriptionModel">文字起こしモデル</label>
                                    <select id="liveSpeechModel" class="language-inline live-model-select"></select>
                                    <label class="field-label-inline" for="liveTranslateModel" data-i18n="live.translationModel">翻訳モデル</label>
                                    <select id="liveTranslateModel" class="language-inline live-model-select"></select>
                                </div>
                                <section class="live-current" id="liveCurrentPanel">
                                    <div class="live-current-block">
                                        <span class="live-current-label" data-i18n="live.latestTranscription">文字起こし</span>
                                        <p class="live-current-text" id="liveCurrentSource" data-i18n="live.latestWaiting">待機中...</p>
                                    </div>
                                    <div class="live-current-block">
                                        <span class="live-current-label" data-i18n="live.latestTranslation">翻訳</span>
                                        <p class="live-current-text live-current-translation" id="liveCurrentTranslation" data-i18n="live.latestWaiting">待機中...</p>
                                    </div>
                                </section>
                                <div class="live-captions" id="liveCaptions" data-translate="true">
                                    <div class="live-empty" id="liveEmptyMessage" data-i18n="live.emptyHint">
                                        開始ボタンを押すと、話した内容がリアルタイムで字幕化されます。
                                    </div>
                                </div>
                                <div class="live-controls">
                                    <button id="liveStartButton" class="primary-button live-action-button" type="button">
                                        <span class="record-icon">⏺</span>
                                        <span id="liveStartLabel" data-i18n="live.start">開始</span>
                                    </button>
                                    <button id="liveStopButton" class="secondary-button" type="button" disabled data-i18n="live.stop">停止</button>
                                    <span class="live-hint" id="liveHint" data-i18n="live.chunkHint">発話の区切りで文字起こしし、その後に翻訳します</span>
                                </div>
                            </main>
                        </div>
                    </div>

                    <!-- ライブセッション保存モーダル -->
                    <div class="note-editor-modal" id="liveSaveModal" hidden>
                        <div class="note-editor-card">
                            <div class="note-editor-header">
                                <h3 data-i18n="live.saveSession">セッションを保存</h3>
                                <button id="liveSaveClose" class="icon-button" type="button">✕</button>
                            </div>
                            <label class="field-label" data-i18n="live.sessionTitle">タイトル</label>
                            <input id="liveSaveTitle" type="text" data-i18n-placeholder="live.sessionTitlePlaceholder" placeholder="例: 2026/05/10 ミーティング" />
                            <p class="field-hint" id="liveSaveSummary"></p>
                            <div class="note-editor-actions">
                                <button id="liveSaveDiscard" class="danger-button" type="button" data-i18n="live.discard">破棄</button>
                                <button id="liveSaveConfirm" class="primary-button" type="button" data-i18n="live.save">保存</button>
                            </div>
                        </div>
                    </div>

                    <!-- プロジェクト作成モーダル -->
                    <div class="note-editor-modal" id="projectEditorModal" hidden>
                        <div class="note-editor-card">
                            <div class="note-editor-header">
                                <h3 data-i18n="project.new">新しいプロジェクト</h3>
                                <button id="projectEditorClose" class="icon-button" type="button">✕</button>
                            </div>
                            <label class="field-label" data-i18n="project.name">プロジェクト名</label>
                            <input id="projectEditorName" type="text" />
                            <label class="field-label" data-i18n="project.description">説明（任意）</label>
                            <textarea id="projectEditorDescription" class="small-textarea"></textarea>
                            <div class="note-editor-actions">
                                <button id="projectEditorDelete" class="danger-button" type="button" data-i18n="project.delete" hidden>削除</button>
                                <button id="projectEditorSave" class="primary-button" type="button" data-i18n="project.save">保存</button>
                            </div>
                        </div>
                    </div>
                </main>

                <script>
                    // ========== i18n (Internationalization) ==========
                    // Translations live as JSON files in the user's data directory so that
                    // end users can add or edit languages without rebuilding the app. The
                    // backend seeds default en.json / ja.json on first launch, and exposes
                    // every valid `*.json` it finds in that folder via `/api/locales`.
                    //
                    // We load them synchronously here (blocking XHR) so that `t()` is usable
                    // by the time the rest of the script runs. The fallback dictionary below
                    // is only used if the API request fails for some reason (e.g. startup
                    // race where the HTTP server is not yet ready).
                    let TRANSLATIONS = {};
                    let LOCALES_DIR = "";

                    const __FALLBACK_TRANSLATIONS = {
                        ja: {
                            "locale.name": "日本語",
                            "models.desc.whisper.tiny": "軽量で高速。リアルタイム向け",
                            "models.desc.whisper.base": "バランス型。日常用途に最適",
                            "models.desc.whisper.small": "高精度。会議議事録などに",
                            "models.desc.whisper.medium": "さらに高精度。長尺ファイル向け",
                            "models.desc.whisper.large": "最高精度。専門的な音声に",
                            "models.desc.whisper.largeTurbo": "最高精度 + 高速化。大規模素材に",
                            "models.desc.whisper.generic": "音声認識モデル",
                            "models.desc.llm.qwen": "テキスト整形・翻訳・抽出に使用",
                            "models.desc.llm.phi": "小型LLM。整形・翻訳向け",
                            "models.desc.llm.llama": "汎用LLM",
                            "models.desc.llm.image": "画像入力対応モデル",
                            "models.desc.llm.text": "テキスト生成モデル",
                            "models.desc.llm.generic": "AI モデル",
                            "app.title": "SeeDraft",
                            "app.heading": "Project",
                            "button.settings": "⚙ 設定",
                            "button.copy": "コピー",
                            "button.clear": "クリア",
                            "button.choose": "選択",
                            "button.record.start": "録音開始",
                            "button.record.stop": "録音停止",
                            "status.idle": "待機中",
                            "error.title": "エラー",
                            "requirements.title": "実行環境の準備が必要です",
                            "requirements.runtimeMissing": "Foundry Local の実行環境を初期化できません。PowerShell で次のコマンドを実行してから SeeDraft を再起動してください。",
                            "requirements.modelsMissing": "必要なモデルが未ダウンロードです: {models}",
                            "requirements.modelsPrompt": "必要なモデルを準備しますか？\n{models}",
                            "requirements.commandCopied": "インストールコマンドをコピーしました",
                            "requirements.openModels": "モデル設定",
                            "requirements.prepareModels": "モデルを準備",
                            "requirements.copyCommand": "コマンドをコピー",
                            "requirements.dismiss": "閉じる",
                            "status.recording": "録音中",
                            "status.processing": "録音処理中",
                            "status.recordingComplete": "録音完了",
                            "status.transcribing": "文字起こし中",
                            "status.transcribingStart": "文字起こし開始",
                            "status.done": "完了",
                            "status.error": "エラー",
                            "status.notSelected": "音声が未選択",
                            "status.notAudioFile": "音声ファイルではありません",
                            "status.recordingLimit": "録音上限に到達",
                            "status.micUnavailable": "マイクを使用できません",
                            "status.copied": "コピー済み",
                            "status.savedExport": "書き出し済み",
                            "status.translationCopied": "翻訳をコピー済み",
                            "status.translationExported": "翻訳を書き出し済み",
                            "status.folderSet": "保存先を設定",
                            "status.folderUnsupported": "フォルダ選択非対応",
                            "status.folderError": "保存先エラー",
                            "status.postprocessDone": "後処理完了",
                            "status.postprocessing": "後処理実行中",
                            "status.translating": "翻訳中",
                            "status.translationDone": "翻訳完了",
                            "status.refining": "整形中",
                            "status.refineDone": "整形完了",
                            "status.downloading": "ダウンロード中",
                            "status.downloadDone": "ダウンロード完了",
                            "status.downloadFailed": "ダウンロード失敗",
                            "status.resultEmpty": "結果が空です",
                            "status.noProcess": "処理なし",
                            "status.selectMode": "処理モードを選択してください",
                            "drop.title": "音声ファイルをドロップ",
                            "drop.formats": "mp3 / wav / m4a / webm",
                            "download.title": "モデルをダウンロード中",
                            "download.message": "初回はダウンロードに数分かかります",
                            "download.preparing": "モデルを準備中",
                            "download.cancel": "キャンセル",
                            "download.cancelHint": "ダウンロードはバックグラウンドで続行されます。完了すれば次回はキャッシュから起動します。",
                            "download.cancelledHint": "ダウンロードの進捗表示を閉じました（バックグラウンドで継続中）。",
                            "download.diagnostic.title": "ダウンロードに失敗しました",
                            "download.diagnostic.variants": "このモデルの利用可能なバリアント",
                            "caption.label": "文字起こし結果",
                            "caption.placeholder": "ここに文字起こし結果が表示されます",
                            "caption.unsaved": "未保存",
                            "button.saveToHistory": "📥 ノートに追加",
                            "quick.autoSave": "ノート追加",
                            "quick.postprocessGroup": "後処理",
                            "quick.autoRefine": "整形",
                            "quick.autoTranslate": "翻訳",
                            "quick.autoAnalyze": "抽出",
                            "quick.autoCustom": "カスタム",
                            "quick.autoSaveTooltip": "文字起こし完了後、自動的にノートに追加します",
                            "quick.autoRefineTooltip": "文字起こし後に整形します",
                            "quick.autoTranslateTooltip": "文字起こし後に翻訳します",
                            "quick.autoAnalyzeTooltip": "文字起こし後にトーン・要点・キーワードを抽出します",
                            "quick.autoCustomTooltip": "文字起こし後にカスタム後処理ステップを実行します",
                            "settings.linkedContext.title": "リンク先ノートを文脈として利用",
                            "settings.linkedContext.hint": "手動で張ったリンク先ノートの抜粋を、整形・翻訳・抽出・カスタム後処理のシステムプロンプトに追加します。関連ノートを踏まえた結果が得られます。",
                            "settings.linkedContext.enabled": "有効にする",
                            "settings.complete.title": "補完設定",
                            "settings.complete.hint": "ノート編集画面の「✨ 補完」ボタンで本文の続きを生成します。リンク先ノートのコンテキストが有効な場合は関連ノートを踏まえた続きが生成されます。",
                            "settings.complete.model": "使用するモデル",
                            "settings.analyze.title": "抽出設定",
                            "settings.analyze.hint": "トーン・要点・キーワードを抽出します。",
                            "settings.analyze.model": "使用するモデル",
                            "status.savedToHistory": "ノートに追加しました",
                            "status.savedLiveSession": "セッションを保存しました",
                            "status.emptyText": "テキストが空です",
                            "status.noProject": "プロジェクトが選択されていません",
                            "record.ready": "準備完了",
                            "record.recording": "録音中...",
                            "record.completed": "録音完了 ({size} KB)",
                            "record.processing": "{name} を処理中...",
                            "record.done": "完了: {name}",
                            "record.failedTranscribe": "文字起こしに失敗しました",
                            "record.failedPostprocess": "後処理に失敗しました",
                            "record.needAudio": "音声ファイルまたは録音が必要です",
                            "record.modelWarmup": "モデルを準備しています",
                            "record.infoPlaceholder": "メッセージがここに表示されます",
                            "record.stepsRunning": "{steps} を実行しています...",
                            "record.stepsDone": "{steps} 完了",
                            "record.translationFailed": "翻訳に失敗しました",
                            "record.refineFailed": "文章整形に失敗しました",
                            "record.folderUnsupportedMsg": "このブラウザでは保存先フォルダの固定に対応していません",
                            "meta.refined": "整形済み",
                            "meta.fillerRemoved": "フィラー除去",
                            "meta.voiceCommandsApplied": "音声コマンド反映",
                            "meta.automatic": "自動",
                            "meta.translateTo": "翻訳先: {target}",
                            "meta.duration": "長さ: {seconds} 秒",
                            "meta.language": "言語: {lang}",
                            "meta.transcriptionDone": "文字起こし完了",
                            "meta.currentResult": "現在の結果",
                            "meta.corrupt": "保存データが破損しています",
                            "step.refine": "整形",
                            "step.translate": "翻訳",
                            "settings.title": "設定",
                            "settings.tab.transcription": "文字起こし",
                            "settings.tab.postprocess": "後処理",
                            "settings.tab.history": "履歴",
                            "settings.tab.output": "出力",
                            "settings.tab.appearance": "表示",
                            "settings.tab.models": "モデル",
                            "settings.tab.shortcuts": "ショートカット",
                            "settings.shortcuts.hint": "各項目の入力欄をクリックして、割り当てたいキーを押してください。「押下中だけ録音」は Ctrl 単体などの長押しキーも割り当てられます。Esc で取り消し、Delete / Backspace で解除できます。",
                            "settings.shortcuts.reset": "既定値に戻す",
                            "settings.shortcuts.unset": "未設定",
                            "settings.shortcuts.recording": "キー入力を待機中...",
                            "settings.shortcuts.recordTooltip": "この欄にフォーカスしてキーを押すと割り当てられます",
                            "settings.shortcuts.clash": "{label} と重複しています",
                            "settings.shortcuts.linkedContextOff": "リンク先ノートのコンテキストを無効化しました",
                            "settings.shortcuts.action.recordToggle": "録音の開始 / 停止",
                            "settings.shortcuts.action.recordHold": "押下中だけ録音",
                            "settings.shortcuts.action.settingsOpen": "設定を開く",
                            "settings.shortcuts.action.captionSave": "現在の本文をノートに保存",
                            "settings.shortcuts.action.captionCopy": "本文をコピー",
                            "settings.shortcuts.action.captionClear": "本文をクリア",
                            "settings.shortcuts.action.linkedContextToggle": "リンク先コンテキストの有効 / 無効切替",
                            "settings.models.cacheDir": "Foundry Local キャッシュ",
                            "settings.models.downloadedCount": "ダウンロード済み",
                            "settings.models.refresh": "再読込",
                            "settings.models.speechGroup": "音声認識 (Speech-to-Text)",
                            "settings.models.speechDesc": "録音した音声をテキストに変換します。精度と速度のトレードオフで選択してください。",
                            "settings.models.chatGroup": "テキスト処理 (LLM)",
                            "settings.models.chatDesc": "整形・翻訳・抽出に使用します。プロジェクト補正や後処理パイプラインの裏で動作します。",
                            "settings.models.inUse": "使用中",
                            "settings.models.cached": "ダウンロード済み",
                            "settings.models.notCached": "未ダウンロード",
                            "settings.models.downloading": "ダウンロード中",
                            "settings.models.downloadHint": "初回使用時に自動的にダウンロードされます",
                            "settings.models.empty": "モデルが見つかりません",
                            "settings.models.runtime": "実行環境",
                            "settings.models.ep.registered": "この環境で利用可能",
                            "settings.models.ep.notRegistered": "未インストール",
                            "settings.models.incompatible": "非対応",
                            "settings.models.incompatibleTooltip": "この環境では実行できません ({reason})",
                            "settings.models.incompatibleConfirm": "「{alias}」はこの環境では実行できない可能性があります ({reason})。\nそれでもダウンロードしますか？",
                            "settings.models.use": "使用する",
                            "settings.models.useAndDownload": "使用する・ダウンロード",
                            "settings.models.useTooltip": "このモデルを使用する（未ダウンロードなら自動でDL）",
                            "settings.models.download": "ダウンロード",
                            "settings.models.downloadStart": "{alias} のダウンロードを開始しました",
                            "settings.models.downloadStartFailed": "ダウンロードを開始できませんでした",
                            "settings.models.defaultChatDownloadPrompt": "テキスト処理の既定モデル {alias} が未ダウンロードです。整形・翻訳・抽出で使用するため、今ダウンロードしますか？",
                            "settings.models.defaultChatDownloadToast": "テキスト処理の既定モデル {alias} をダウンロードしてください",
                            "settings.models.switchedSpeech": "{alias} を音声認識モデルとして選択しました",
                            "settings.models.chatNoSwitch": "LLMは自動選択されます。ダウンロードのみ行えます。",
                            "settings.models.test": "テスト",
                            "settings.models.testing": "テスト中",
                            "settings.models.testTooltip": "整形・翻訳・抽出でこのモデルの動作を確認します",
                            "settings.models.testOk": "{alias} テスト完了 ({elapsed}ms): {summary}",
                            "settings.models.testFailed": "モデルのテストに失敗しました",
                            "settings.models.testResult": "テスト結果",
                            "settings.models.capability.refine": "整形",
                            "settings.models.capability.translate": "翻訳",
                            "settings.models.capability.extract": "抽出",
                            "settings.common.auto": "自動",
                            "settings.common.reset": "既定",
                            "settings.transcription.auto": "自動判定",
                            "settings.transcription.language": "認識言語",
                            "settings.transcription.model": "Whisperモデル",
                            "settings.transcription.manageHint": "モデルの切り替えや削除は「モデル」タブから行えます。",
                            "settings.transcription.openModels": "モデル設定を開く",
                            "settings.transcription.noneSelected": "モデルが選択されていません",
                            "settings.transcription.model.tiny": "tiny - 速い / 軽量",
                            "settings.transcription.model.base": "base - バランス",
                            "settings.transcription.model.small": "small - 高精度",
                            "settings.transcription.model.medium": "medium - より高精度",
                            "settings.transcription.model.large": "large - 最高精度",
                            "settings.transcription.model.largeV3": "large v3 - 最新の最高精度",
                            "settings.transcription.model.largeTurbo": "large v3 turbo - 高速＆高精度",
                            "settings.transcription.prompt": "カスタムプロンプト",
                            "settings.transcription.promptPlaceholder": "会議の議題、話者名、専門用語、表記ルールなど",
                            "settings.transcription.microphone": "マイクデバイス",
                            "settings.transcription.microphoneDefault": "既定のマイク",
                            "settings.transcription.microphoneUnavailable": "マイクを検出できません",
                            "settings.transcription.microphoneFallback": "選択したマイクを使用できないため、既定のマイクを使用します",
                            "settings.transcription.micMonitor": "マイク入力",
                            "settings.transcription.micMonitorIdle": "停止中",
                            "settings.transcription.micMonitorActive": "入力を監視中",
                            "settings.transcription.micMonitorError": "マイク入力を確認できません",
                            "settings.transcription.maxRecording": "最大録音時間（秒）",
                            "settings.transcription.sampleRate": "サンプルレート",
                            "settings.refine.title": "整形設定",
                            "settings.refine.hint": "文字起こしの明らかな誤変換やフィラー・句読点のゆれのみを補正します。意味は変更しません。スタイルや言い回しの変更が必要な場合はカスタム後処理で行ってください。",
                            "settings.refine.model": "使用するモデル",
                            "settings.translation.model": "使用するモデル",
                            "settings.custom.model": "使用するモデル",
                            "settings.models.useDefault": "（既定のモデル）",
                            "settings.refine.strength": "整形の強度",
                            "settings.refine.strength.minimal": "最小限",
                            "settings.refine.strength.standard": "標準",
                            "settings.refine.strength.thorough": "徹底",
                            "settings.refine.removeFillers": "フィラー除去",
                            "settings.refine.removeFillersHint": "「あー」「えー」「えっと」「なんか」「um」「uh」などの言いよどみを除去します。",
                            "settings.refine.voiceCommands": "音声コマンド反映",
                            "settings.refine.voiceCommandsHint": "話し言葉の「改行」「段落」「句点」「読点」などをそれぞれ改行・句読点に置換します。英語は「new line」「new paragraph」「period」「comma」などに対応します。",
                            "settings.refine.style": "スタイル",
                            "settings.refine.style.natural": "自然文",
                            "settings.refine.style.polite": "丁寧",
                            "settings.refine.style.business": "ビジネス",
                            "settings.refine.style.minutes": "議事録",
                            "settings.refine.customTerms": "専門用語・固有名詞",
                            "settings.refine.customTermsPlaceholder": "SeeDraft, Foundry Local など",
                            "settings.refine.customInstruction": "追加指示",
                            "settings.refine.customInstructionPlaceholder": "箇条書きではなく本文でまとめる",
                            "settings.translation.title": "翻訳設定",
                            "settings.translation.source": "翻訳元言語",
                            "settings.translation.target": "翻訳先言語",
                            "settings.translation.autoDetect": "自動判定",
                            "settings.translation.instruction": "翻訳指示",
                            "settings.translation.instructionPlaceholder": "製品名は英語表記のまま",
                            "settings.reset": "初期設定に戻す",
                            "settings.resetConfirm": "すべての設定を初期値に戻しますか？保存されたプロジェクト・ノート・ドラフトには影響しません。",
                            "settings.resetDone": "設定を初期化しました",
                            "settings.autosaved": "設定を保存しました",
                            "settings.custom.title": "カスタム後処理",
                            "settings.custom.hint": "LLMへの指示を組み合わせて、上から順に実行されます。並び順はそのまま実行順です。",
                            "settings.custom.add": "＋ 追加",
                            "settings.custom.preset.add": "＋ プリセット",
                            "settings.custom.preset.pickTitle": "プリセットを選択",
                            "settings.custom.preset.pickHint": "選んだプリセットをカスタム後処理に追加します。既に同じステップがある場合は内容を最新版に更新します。",
                            "settings.custom.preset.addOne": "追加",
                            "settings.custom.preset.refresh": "最新版に更新",
                            "settings.custom.preset.refreshTooltip": "既に追加済みのため、内容を最新のプリセット定義に更新します",
                            "settings.custom.preset.added": "「{name}」を追加しました",
                            "settings.custom.preset.polite.name": "丁寧体に書き換え",
                            "settings.custom.preset.polite.instruction": "次の文章を丁寧で自然な敬体（です・ます調）に書き換えてください。意味や情報は一切変えず、語尾と助詞の調整のみを行ってください。出力は書き換え後の本文のみです。",
                            "settings.custom.preset.business.name": "ビジネス文体に書き換え",
                            "settings.custom.preset.business.instruction": "次の文章を、業務メモとして共有しやすい簡潔で丁寧なビジネス文体に書き換えてください。冗長な表現は締め、結論を先に示してください。情報は追加せず、意味を変えないでください。出力は書き換え後の本文のみです。",
                            "settings.custom.preset.minutes.name": "議事録フォーマット",
                            "settings.custom.preset.minutes.instruction": "次の文章を議事録形式に整えてください。冒頭に1〜2行の要旨、続いて『決定事項』『対応事項（担当者と期限）』『論点・議論内容』の3セクションを箇条書きで出力してください。原文にない情報は追加しないでください。",
                            "settings.custom.preset.casual.name": "くだけた口語に書き換え",
                            "settings.custom.preset.casual.instruction": "次の文章を自然なくだけた口語体（常体）に書き換えてください。意味と情報は保ちつつ、堅い表現を日常会話に近い言い回しに変えてください。出力は書き換え後の本文のみです。",
                            "settings.custom.preset.summary.name": "3行要約",
                            "settings.custom.preset.summary.instruction": "次の文章の要点を3〜5行で箇条書き要約してください。各行は30文字以内を目安にしてください。出力は箇条書き本文のみで、前置きや見出しは不要です。",
                            "settings.custom.preset.bullets.name": "箇条書きに整理",
                            "settings.custom.preset.bullets.instruction": "次の文章を、見出しごとに階層化した箇条書きに整理してください。重要な数値・固有名詞は必ず残してください。出力は箇条書き本文のみです。",
                            "settings.custom.empty": "まだカスタムステップはありません",
                            "settings.custom.editorTitle": "カスタムステップ",
                            "settings.custom.name": "名前",
                            "settings.custom.namePlaceholder": "例: 議事録スタイルに変換",
                            "settings.custom.instruction": "LLMへの指示",
                            "settings.custom.instructionPlaceholder": "例: 議事録スタイルに変換してください。決定事項とToDoを箇条書きで示す。",
                            "settings.custom.enabled": "有効",
                            "settings.custom.disabled": "無効",
                            "settings.custom.enableTooltip": "このステップを有効化すると、「自動カスタム」トグルON時に実行対象になります",
                            "settings.custom.delete": "削除",
                            "settings.custom.save": "保存",
                            "settings.custom.run": "実行",
                            "settings.custom.edit": "編集",
                            "settings.custom.moveUp": "上へ",
                            "settings.custom.moveDown": "下へ",
                            "settings.custom.deleteConfirm": "カスタムステップ「{name}」を削除しますか？",
                            "settings.custom.running": "{name} を実行中...",
                            "settings.custom.ranOk": "{name} を実行しました",
                            "settings.custom.runFailed": "カスタムステップの実行に失敗しました",
                            "settings.history.transcription": "文字起こし履歴",
                            "settings.history.translation": "翻訳履歴",
                            "settings.history.clear": "履歴をクリア",
                            "settings.history.empty": "まだ履歴はありません",
                            "settings.output.folder": "保存先フォルダ",
                            "settings.output.folderPlaceholder": "ブラウザのダウンロード先",
                            "settings.output.prefix": "ファイル名プレフィックス",
                            "settings.appearance.language": "表示言語",
                            "settings.appearance.languageAuto": "OS に従う",
                            "settings.appearance.localeHint": "このフォルダに xx.json を追加すると新しい言語を追加できます:",
                            "settings.appearance.localesCopied": "パスをコピーしました",
                            "settings.appearance.localesDirChanged": "言語ファイルの保存先を更新しました。画面を再読込します",
                            "settings.appearance.theme": "テーマ",
                            "settings.appearance.themeAuto": "システム設定に合わせる",
                            "settings.appearance.themeLight": "ライト",
                            "settings.appearance.themeDark": "ダーク",
                            "history.noTranscription": "(文字起こし結果なし)",
                            "history.noTranslation": "(翻訳結果なし)",
                            "model.status.active": "使用中",
                            "model.status.downloaded": "ダウンロード済み",
                            "model.status.notDownloaded": "未ダウンロード",
                            "model.status.downloading": "ダウンロード中",
                            "model.action.select": "選択",
                            "model.action.download": "ダウンロード",
                            "model.action.delete": "削除",
                            "model.tooltip.active": "このモデルが現在使われています",
                            "model.tooltip.downloaded": "ダウンロード済み。選択すると使用されます",
                            "model.tooltip.notDownloaded": "未ダウンロード。選択すると次回実行時にダウンロードされます",
                            "model.tooltip.downloading": "ダウンロード進行中です",
                            "model.tooltip.delete": "このモデルをキャッシュから削除",
                            "model.delete.confirm": "{alias} を削除しますか？ディスク容量が解放されます。",
                            "model.delete.failed": "モデルの削除に失敗しました",
                            "model.delete.success": "モデルを削除しました",
                            "model.list.empty": "利用可能な Whisper モデルが見つかりません",
                            "model.list.loading": "モデル一覧を読み込み中...",
                            "project.current": "プロジェクト",
                            "project.new": "新しいプロジェクト",
                            "project.name": "プロジェクト名",
                            "project.description": "説明（任意）",
                            "project.save": "保存",
                            "project.delete": "削除",
                            "project.deleteConfirm": "プロジェクト「{name}」とそのノートを全て削除しますか？",
                            "project.namePlaceholder": "例: 会議メモ",
                            "button.history": "📝 ノート",
                            "button.liveCaption": "🎬 ライブ字幕",
                            "button.drafts": "📄 ドラフト",
                            "draft.title.plural": "📄 ドラフト",
                            "draft.list": "ドラフト一覧",
                            "draft.selectMode": "選択モード",
                            "draft.selectedCount": "件選択中",
                            "draft.selectAll": "全選択",
                            "draft.compose": "📄 ドラフト化",
                            "draft.composeTitle": "ノートからドラフトを作成",
                            "draft.title": "タイトル",
                            "draft.titlePlaceholder": "例: 週次まとめ",
                            "draft.mode": "合成方法",
                            "draft.modeConcat": "単純結合（見出し付き）",
                            "draft.modeLlm": "LLM再構成（1本の記事にする）",
                            "draft.instruction": "追加指示（任意）",
                            "draft.instructionPlaceholder": "例: 意思決定を先に、背景は後半に。敬体で。",
                            "draft.create": "作成",
                            "draft.save": "保存",
                            "draft.delete": "削除",
                            "draft.deleteConfirm": "ドラフト「{title}」を削除しますか？",
                            "draft.export": "Markdown として書き出し",
                            "draft.editorPlaceholder": "ドラフトを選択するか、ノート画面から新規作成してください",
                            "draft.references": "元のノート",
                            "draft.empty": "まだドラフトはありません",
                            "draft.created": "ドラフトを作成しました",
                            "draft.saved": "保存しました",
                            "draft.composing": "合成中...",
                            "draft.composeFailed": "ドラフトの作成に失敗しました",
                            "draft.selectAtLeastOne": "1件以上のノートを選択してください",
                            "draft.summary": "{count}件のノートから合成",
                            "live.title": "🎬 ライブ字幕",
                            "live.savedSessions": "保存済みセッション",
                            "live.sourceLanguage": "原言語",
                            "live.targetLanguage": "訳先",
                            "live.enableTranslation": "同時翻訳",
                            "live.transcriptionModel": "文字起こしモデル",
                            "live.translationModel": "翻訳モデル",
                            "live.latestTranscription": "文字起こし",
                            "live.latestTranslation": "翻訳",
                            "live.latestWaiting": "待機中...",
                            "live.start": "開始",
                            "live.stop": "停止",
                            "live.saveSession": "セッションを保存",
                            "live.sessionTitle": "タイトル",
                            "live.sessionTitlePlaceholder": "例: 2026/05/10 ミーティング",
                            "live.save": "保存",
                            "live.discard": "破棄",
                            "live.emptyHint": "開始ボタンを押すと、話した内容がリアルタイムで字幕化されます。",
                            "live.chunkHint": "発話の区切りで文字起こしし、その後に翻訳します",
                            "live.noSessions": "保存されたセッションはありません",
                            "live.summary": "{count}件のセグメント / {duration}",
                            "live.startFailed": "ライブ字幕を開始できませんでした",
                            "live.micError": "マイクにアクセスできません",
                            "live.deleteConfirm": "このライブセッションを削除しますか？",
                            "live.empty": "セッションに字幕がありませんでした",
                            "history.title": "ノート",
                            "history.search": "検索...",
                            "history.refresh": "再読込",
                            "history.view.list": "リスト",
                            "history.view.graph": "グラフ",
                            "history.graph.project": "プロジェクト",
                            "history.graph.note": "ノート",
                            "history.graph.tag": "タグ",
                            "history.graph.link": "リンク",
                            "history.graph.linkMode": "リンク作成",
                            "history.graph.linkModeHint": "リンクしたい2つのノートを順にクリック。もう一度ボタンでキャンセル。",
                            "history.graph.linkCancel": "キャンセル",
                            "history.graph.linkFirst": "リンク元のノートを選択",
                            "history.graph.linkSecond": "リンク先のノートを選択",
                            "history.graph.linkCreated": "リンクを作成しました",
                            "history.graph.linkFailed": "リンクの作成に失敗しました",
                            "history.graph.linkDelete": "リンクを削除",
                            "history.graph.linkDeleteConfirm": "このリンクを削除しますか？",
                            "history.graph.linkNoteOnly": "ノート同士のみリンクできます",
                            "history.empty": "このプロジェクトにはまだノートがありません",
                            "history.tags": "タグ",
                            "note.edit": "ノート編集",
                            "note.copy": "本文をコピー",
                            "note.title": "タイトル",
                            "note.text": "本文",
                            "note.tags": "タグ（カンマ区切り）",
                            "note.tagsPlaceholder": "会議, プロジェクトA, 重要",
                            "note.delete": "削除",
                            "note.save": "保存",
                            "note.deleteConfirm": "このノートを削除しますか？",
                            "note.parent": "親ノート",
                            "note.parentNone": "（トップレベル）",
                            "note.clearParent": "親をクリア",
                            "note.parentSetFailed": "親ノートを設定できませんでした",
                            "history.graph.parent": "親子",
                            "note.analyze": "🔍 抽出",
                            "note.analyzing": "抽出中...",
                            "note.analysisFailed": "抽出に失敗しました",
                            "note.complete": "✨ 補完",
                            "note.completing": "補完中...",
                            "note.completeFailed": "補完に失敗しました",
                            "note.completeEmpty": "追加する内容を生成できませんでした",
                            "note.completeDone": "本文に続きを追加しました",
                            "note.analysis.tone": "トーン",
                            "note.analysis.summary": "要点",
                            "note.analysis.keywords": "キーワード",
                            "stat.chars": "文字",
                            "stat.fillers": "フィラー",
                            "stat.seconds": "秒",
                            "stat.cpm": "文字/分",
                            "translation.title": "翻訳",
                            "translation.source": "原文",
                            "translation.target": "翻訳結果",
                            "translation.sourcePlaceholder": "翻訳したいテキストを入力...",
                            "translation.resultPlaceholder": "翻訳結果がここに表示されます",
                            "translation.instruction": "翻訳指示",
                            "translation.translate": "翻訳する",
                            "translation.history": "翻訳履歴",
                            "translation.clearAll": "全てクリア",
                            "translation.clearConfirm": "全ての翻訳履歴を削除しますか？",
                            "translation.fromNote": "ノートから取り込み",
                            "translation.selectNote": "取り込むノートを選択してください",
                            "translation.noHistory": "まだ翻訳履歴はありません"
                        },
                        en: {
                            "locale.name": "English",
                            "models.desc.whisper.tiny": "Lightweight and fast. For real-time use.",
                            "models.desc.whisper.base": "Balanced. Best for everyday use.",
                            "models.desc.whisper.small": "Higher accuracy. Good for meeting notes.",
                            "models.desc.whisper.medium": "Even higher accuracy. For long-form audio.",
                            "models.desc.whisper.large": "Top accuracy. For specialized audio.",
                            "models.desc.whisper.largeTurbo": "Top accuracy + speed. For large inputs.",
                            "models.desc.whisper.generic": "Speech recognition model",
                            "models.desc.llm.qwen": "Used for text refinement, translation and analysis",
                            "models.desc.llm.phi": "Compact LLM. Good for refinement and translation",
                            "models.desc.llm.llama": "General-purpose LLM",
                            "models.desc.llm.image": "Image-capable model",
                            "models.desc.llm.text": "Text generation model",
                            "models.desc.llm.generic": "AI model",
                            "app.title": "SeeDraft",
                            "app.heading": "Project",
                            "button.settings": "⚙ Settings",
                            "button.copy": "Copy",
                            "button.clear": "Clear",
                            "button.choose": "Choose",
                            "button.record.start": "Start Recording",
                            "button.record.stop": "Stop Recording",
                            "status.idle": "Idle",
                            "error.title": "Error",
                            "requirements.title": "Runtime setup required",
                            "requirements.runtimeMissing": "Foundry Local could not be initialized. Run this command in PowerShell, then restart SeeDraft.",
                            "requirements.modelsMissing": "Required models are not downloaded: {models}",
                            "requirements.modelsPrompt": "Prepare the required models now?\n{models}",
                            "requirements.commandCopied": "Install command copied",
                            "requirements.openModels": "Models",
                            "requirements.prepareModels": "Prepare models",
                            "requirements.copyCommand": "Copy command",
                            "requirements.dismiss": "Dismiss",
                            "status.recording": "Recording",
                            "status.processing": "Processing recording",
                            "status.recordingComplete": "Recording complete",
                            "status.transcribing": "Transcribing",
                            "status.transcribingStart": "Starting transcription",
                            "status.done": "Done",
                            "status.error": "Error",
                            "status.notSelected": "No audio selected",
                            "status.notAudioFile": "Not an audio file",
                            "status.recordingLimit": "Recording limit reached",
                            "status.micUnavailable": "Microphone unavailable",
                            "status.copied": "Copied",
                            "status.savedExport": "Exported",
                            "status.translationCopied": "Translation copied",
                            "status.translationExported": "Translation exported",
                            "status.folderSet": "Save folder set",
                            "status.folderUnsupported": "Folder picker unsupported",
                            "status.folderError": "Folder error",
                            "status.postprocessDone": "Post-processing done",
                            "status.postprocessing": "Running post-processing",
                            "status.translating": "Translating",
                            "status.translationDone": "Translation done",
                            "status.refining": "Refining",
                            "status.refineDone": "Refinement done",
                            "status.downloading": "Downloading",
                            "status.downloadDone": "Download complete",
                            "status.downloadFailed": "Download failed",
                            "status.resultEmpty": "Result is empty",
                            "status.noProcess": "No steps",
                            "status.selectMode": "Select a processing mode",
                            "drop.title": "Drop audio file",
                            "drop.formats": "mp3 / wav / m4a / webm",
                            "download.title": "Downloading model",
                            "download.message": "The first download may take a few minutes",
                            "download.preparing": "Preparing model",
                            "download.cancel": "Cancel",
                            "download.cancelHint": "The download continues in the background. Once it finishes, the cached copy will be used next time.",
                            "download.cancelledHint": "Download progress dismissed (still running in the background).",
                            "download.diagnostic.title": "Download failed",
                            "download.diagnostic.variants": "Variants available for this model",
                            "caption.label": "Transcription Result",
                            "caption.placeholder": "Transcription result will appear here",
                            "caption.unsaved": "Unsaved",
                            "button.saveToHistory": "📥 Save as note",
                            "quick.autoSave": "Save to note",
                            "quick.postprocessGroup": "Post-process",
                            "quick.autoRefine": "Refine",
                            "quick.autoTranslate": "Translate",
                            "quick.autoAnalyze": "Extract",
                            "quick.autoCustom": "Custom",
                            "quick.autoSaveTooltip": "Automatically save the transcription to notes",
                            "quick.autoRefineTooltip": "Refine after transcription",
                            "quick.autoTranslateTooltip": "Translate after transcription",
                            "quick.autoAnalyzeTooltip": "Extract tone, summary and keywords after transcription",
                            "quick.autoCustomTooltip": "Run custom post-processing steps after transcription",
                            "settings.linkedContext.title": "Use linked notes as context",
                            "settings.linkedContext.hint": "Inject excerpts from manually-linked notes into the system prompt for refinement, translation, extraction and custom post-processing, so the LLM can condition on related notes.",
                            "settings.linkedContext.enabled": "Enabled",
                            "settings.complete.title": "Completion",
                            "settings.complete.hint": "The \"✨ Complete\" button in the note editor extends the body with a continuation. When linked-note context is enabled, related notes are used as background.",
                            "settings.complete.model": "Model to use",
                            "settings.analyze.title": "Extraction",
                            "settings.analyze.hint": "Extract tone, summary and keywords.",
                            "settings.analyze.model": "Model to use",
                            "status.savedToHistory": "Saved as note",
                            "status.savedLiveSession": "Session saved",
                            "status.emptyText": "Text is empty",
                            "status.noProject": "No project selected",
                            "record.ready": "Ready",
                            "record.recording": "Recording...",
                            "record.completed": "Recording complete ({size} KB)",
                            "record.processing": "Processing {name}...",
                            "record.done": "Done: {name}",
                            "record.failedTranscribe": "Transcription failed",
                            "record.failedPostprocess": "Post-processing failed",
                            "record.needAudio": "Audio file or recording required",
                            "record.modelWarmup": "Preparing model",
                            "record.infoPlaceholder": "Messages appear here",
                            "record.stepsRunning": "Running {steps}...",
                            "record.stepsDone": "{steps} complete",
                            "record.translationFailed": "Translation failed",
                            "record.refineFailed": "Text refinement failed",
                            "record.folderUnsupportedMsg": "This browser does not support pinning a save folder",
                            "meta.refined": "Refined",
                            "meta.fillerRemoved": "Filler removed",
                            "meta.voiceCommandsApplied": "Voice commands applied",
                            "meta.automatic": "Auto",
                            "meta.translateTo": "Target: {target}",
                            "meta.duration": "Duration: {seconds}s",
                            "meta.language": "Language: {lang}",
                            "meta.transcriptionDone": "Transcription done",
                            "meta.currentResult": "current result",
                            "meta.corrupt": "Saved data is corrupt",
                            "step.refine": "Refine",
                            "step.translate": "Translate",
                            "settings.title": "Settings",
                            "settings.tab.transcription": "Transcription",
                            "settings.tab.postprocess": "Post-processing",
                            "settings.tab.history": "History",
                            "settings.tab.output": "Output",
                            "settings.tab.appearance": "Appearance",
                            "settings.tab.models": "Models",
                            "settings.tab.shortcuts": "Shortcuts",
                            "settings.shortcuts.hint": "Click a field and press the keys you want to bind. Press-and-hold recording can use a single held key such as Ctrl. Esc to cancel, Delete / Backspace to clear.",
                            "settings.shortcuts.reset": "Reset to defaults",
                            "settings.shortcuts.unset": "unset",
                            "settings.shortcuts.recording": "waiting for keys...",
                            "settings.shortcuts.recordTooltip": "Focus this field and press a key combination to bind it",
                            "settings.shortcuts.clash": "Conflicts with {label}",
                            "settings.shortcuts.linkedContextOff": "Linked-note context disabled",
                            "settings.shortcuts.action.recordToggle": "Toggle recording",
                            "settings.shortcuts.action.recordHold": "Record while held",
                            "settings.shortcuts.action.settingsOpen": "Open settings",
                            "settings.shortcuts.action.captionSave": "Save current caption as note",
                            "settings.shortcuts.action.captionCopy": "Copy caption text",
                            "settings.shortcuts.action.captionClear": "Clear caption text",
                            "settings.shortcuts.action.linkedContextToggle": "Toggle linked-note context",
                            "settings.models.cacheDir": "Foundry Local cache",
                            "settings.models.downloadedCount": "Downloaded",
                            "settings.models.refresh": "Refresh",
                            "settings.models.speechGroup": "Speech-to-Text",
                            "settings.models.speechDesc": "Converts recorded audio into text. Choose based on the accuracy / speed trade-off.",
                            "settings.models.chatGroup": "Text processing (LLM)",
                            "settings.models.chatDesc": "Used for refinement, translation and analysis. Runs behind the scenes for post-processing.",
                            "settings.models.inUse": "In use",
                            "settings.models.cached": "Downloaded",
                            "settings.models.notCached": "Not downloaded",
                            "settings.models.downloading": "Downloading",
                            "settings.models.downloadHint": "Downloaded automatically on first use",
                            "settings.models.empty": "No models found",
                            "settings.models.runtime": "Runtime",
                            "settings.models.ep.registered": "Available on this host",
                            "settings.models.ep.notRegistered": "Not installed",
                            "settings.models.incompatible": "Unsupported",
                            "settings.models.incompatibleTooltip": "Cannot run on this host ({reason})",
                            "settings.models.incompatibleConfirm": "\"{alias}\" may not be runnable on this host ({reason}).\nDownload anyway?",
                            "settings.models.use": "Use",
                            "settings.models.useAndDownload": "Use · Download",
                            "settings.models.useTooltip": "Use this model (auto-downloads if missing)",
                            "settings.models.download": "Download",
                            "settings.models.downloadStart": "Started downloading {alias}",
                            "settings.models.downloadStartFailed": "Failed to start download",
                            "settings.models.defaultChatDownloadPrompt": "The default text-processing model {alias} is not downloaded. Download it now for refinement, translation and extraction?",
                            "settings.models.defaultChatDownloadToast": "Please download the default text-processing model {alias}",
                            "settings.models.switchedSpeech": "Selected {alias} as speech model",
                            "settings.models.chatNoSwitch": "The LLM is selected automatically. You can download only.",
                            "settings.models.test": "Test",
                            "settings.models.testing": "Testing",
                            "settings.models.testTooltip": "Test this model for refinement, translation and extraction",
                            "settings.models.testOk": "{alias} test complete ({elapsed}ms): {summary}",
                            "settings.models.testFailed": "Model test failed",
                            "settings.models.testResult": "Test result",
                            "settings.models.capability.refine": "Refine",
                            "settings.models.capability.translate": "Translate",
                            "settings.models.capability.extract": "Extract",
                            "settings.common.auto": "Auto",
                            "settings.common.reset": "Default",
                            "settings.transcription.auto": "Auto-detect",
                            "settings.transcription.language": "Recognition language",
                            "settings.transcription.model": "Whisper model",
                            "settings.transcription.manageHint": "Switch or delete models from the Models tab.",
                            "settings.transcription.openModels": "Open Models tab",
                            "settings.transcription.noneSelected": "No model selected",
                            "settings.transcription.model.tiny": "tiny - fast / lightweight",
                            "settings.transcription.model.base": "base - balanced",
                            "settings.transcription.model.small": "small - higher accuracy",
                            "settings.transcription.model.medium": "medium - even higher accuracy",
                            "settings.transcription.model.large": "large - highest accuracy",
                            "settings.transcription.model.largeV3": "large v3 - latest highest accuracy",
                            "settings.transcription.model.largeTurbo": "large v3 turbo - fast & high accuracy",
                            "settings.transcription.prompt": "Custom prompt",
                            "settings.transcription.promptPlaceholder": "Meeting topics, speaker names, terminology, etc.",
                            "settings.transcription.microphone": "Microphone",
                            "settings.transcription.microphoneDefault": "Default microphone",
                            "settings.transcription.microphoneUnavailable": "No microphones found",
                            "settings.transcription.microphoneFallback": "Selected microphone is unavailable; using the default microphone",
                            "settings.transcription.micMonitor": "Mic input",
                            "settings.transcription.micMonitorIdle": "Stopped",
                            "settings.transcription.micMonitorActive": "Monitoring input",
                            "settings.transcription.micMonitorError": "Could not check microphone input",
                            "settings.transcription.maxRecording": "Max recording time (sec)",
                            "settings.transcription.sampleRate": "Sample rate",
                            "settings.refine.title": "Refinement",
                            "settings.refine.hint": "Only fixes obvious transcription artifacts (fillers, misrecognitions, punctuation). The meaning is preserved — use a custom post-processing step if you want to change style or tone.",
                            "settings.refine.model": "Model to use",
                            "settings.translation.model": "Model to use",
                            "settings.custom.model": "Model to use",
                            "settings.models.useDefault": "(Default model)",
                            "settings.refine.strength": "Refinement strength",
                            "settings.refine.strength.minimal": "Minimal",
                            "settings.refine.strength.standard": "Standard",
                            "settings.refine.strength.thorough": "Thorough",
                            "settings.refine.removeFillers": "Remove fillers",
                            "settings.refine.removeFillersHint": "Strips common filler words and hesitation markers (um, uh, like, あー, えー, なんか, etc.).",
                            "settings.refine.voiceCommands": "Apply voice commands",
                            "settings.refine.voiceCommandsHint": "Spoken instructions like \"new line\", \"new paragraph\", \"period\", and \"comma\" are replaced with the matching line break or punctuation. Japanese equivalents (改行, 段落, 句点, 読点) are also recognized.",
                            "settings.refine.style": "Style",
                            "settings.refine.style.natural": "Natural",
                            "settings.refine.style.polite": "Polite",
                            "settings.refine.style.business": "Business",
                            "settings.refine.style.minutes": "Meeting minutes",
                            "settings.refine.customTerms": "Custom terminology",
                            "settings.refine.customTermsPlaceholder": "e.g. SeeDraft, Foundry Local",
                            "settings.refine.customInstruction": "Extra instruction",
                            "settings.refine.customInstructionPlaceholder": "Write as prose rather than bullet points",
                            "settings.translation.title": "Translation",
                            "settings.translation.source": "Source language",
                            "settings.translation.target": "Target language",
                            "settings.translation.autoDetect": "Auto-detect",
                            "settings.translation.instruction": "Translation instruction",
                            "settings.translation.instructionPlaceholder": "Keep product names in English",
                            "settings.reset": "Reset to defaults",
                            "settings.resetConfirm": "Reset all settings to defaults? Projects, notes and drafts are preserved.",
                            "settings.resetDone": "Settings reset",
                            "settings.autosaved": "Settings saved",
                            "settings.custom.title": "Custom post-processing",
                            "settings.custom.hint": "Combine LLM instructions — steps run top-to-bottom in the listed order.",
                            "settings.custom.add": "＋ Add",
                            "settings.custom.preset.add": "＋ Preset",
                            "settings.custom.preset.pickTitle": "Pick a preset",
                            "settings.custom.preset.pickHint": "Adds the chosen preset to your custom post-processing list. If a step with the same id already exists, its content is refreshed to the latest preset definition.",
                            "settings.custom.preset.addOne": "Add",
                            "settings.custom.preset.refresh": "Refresh",
                            "settings.custom.preset.refreshTooltip": "Already added — refresh its content to the latest preset definition",
                            "settings.custom.preset.added": "Added \"{name}\"",
                            "settings.custom.preset.polite.name": "Rewrite in polite form",
                            "settings.custom.preset.polite.instruction": "Rewrite the following text in polite, natural Japanese (desu/masu form) — or the equivalent polite register for the source language. Preserve the meaning and information exactly; adjust only the verb endings and particles. Output only the rewritten body.",
                            "settings.custom.preset.business.name": "Rewrite for business",
                            "settings.custom.preset.business.instruction": "Rewrite the following text as a concise, polite business memo. Tighten wordy expressions and lead with the conclusion. Do not add information or change the meaning. Output only the rewritten body.",
                            "settings.custom.preset.minutes.name": "Meeting minutes format",
                            "settings.custom.preset.minutes.instruction": "Reformat the following text as meeting minutes. Start with a 1-2 line summary, then three bulleted sections: 'Decisions', 'Action items (owner & deadline)', and 'Discussion points'. Do not introduce facts that aren't in the source.",
                            "settings.custom.preset.casual.name": "Rewrite casually",
                            "settings.custom.preset.casual.instruction": "Rewrite the following text in a natural casual register, keeping the meaning and information intact but replacing formal phrasing with everyday conversational wording. Output only the rewritten body.",
                            "settings.custom.preset.summary.name": "3-line summary",
                            "settings.custom.preset.summary.instruction": "Summarize the following text as 3-5 bullet points. Keep each point under 30 characters (or ~10 words). Output only the bullets — no preamble or heading.",
                            "settings.custom.preset.bullets.name": "Organize as bullets",
                            "settings.custom.preset.bullets.instruction": "Reorganize the following text into hierarchical bullet points grouped by topic. Preserve every significant number and proper noun. Output only the bullet list.",
                            "settings.custom.empty": "No custom steps yet",
                            "settings.custom.editorTitle": "Custom step",
                            "settings.custom.name": "Name",
                            "settings.custom.namePlaceholder": "e.g. Rewrite as meeting minutes",
                            "settings.custom.instruction": "Instruction for the LLM",
                            "settings.custom.instructionPlaceholder": "e.g. Rewrite as meeting minutes. List decisions and action items as bullets.",
                            "settings.custom.enabled": "Enabled",
                            "settings.custom.disabled": "Disabled",
                            "settings.custom.enableTooltip": "When enabled, this step runs as part of auto-custom post-processing.",
                            "settings.custom.delete": "Delete",
                            "settings.custom.save": "Save",
                            "settings.custom.run": "Run",
                            "settings.custom.edit": "Edit",
                            "settings.custom.moveUp": "Move up",
                            "settings.custom.moveDown": "Move down",
                            "settings.custom.deleteConfirm": "Delete custom step \"{name}\"?",
                            "settings.custom.running": "Running {name}...",
                            "settings.custom.ranOk": "{name} finished",
                            "settings.custom.runFailed": "Custom step failed",
                            "settings.history.transcription": "Transcription history",
                            "settings.history.translation": "Translation history",
                            "settings.history.clear": "Clear history",
                            "settings.history.empty": "No history yet",
                            "settings.output.folder": "Save folder",
                            "settings.output.folderPlaceholder": "Browser default download location",
                            "settings.output.prefix": "Filename prefix",
                            "settings.appearance.language": "Display language",
                            "settings.appearance.languageAuto": "Follow OS",
                            "settings.appearance.localeHint": "Drop a xx.json file into this folder to add a language:",
                            "settings.appearance.localesCopied": "Path copied",
                            "settings.appearance.localesDirChanged": "Locale folder updated. Reloading the UI",
                            "settings.appearance.theme": "Theme",
                            "settings.appearance.themeAuto": "Follow system",
                            "settings.appearance.themeLight": "Light",
                            "settings.appearance.themeDark": "Dark",
                            "history.noTranscription": "(no transcription)",
                            "history.noTranslation": "(no translation)",
                            "model.status.active": "In use",
                            "model.status.downloaded": "Downloaded",
                            "model.status.notDownloaded": "Not downloaded",
                            "model.status.downloading": "Downloading",
                            "model.action.select": "Select",
                            "model.action.download": "Download",
                            "model.action.delete": "Delete",
                            "model.tooltip.active": "This model is currently being used",
                            "model.tooltip.downloaded": "Downloaded. Select to use",
                            "model.tooltip.notDownloaded": "Not downloaded. Selecting will download on next run",
                            "model.tooltip.downloading": "Download in progress",
                            "model.tooltip.delete": "Remove this model from cache",
                            "model.delete.confirm": "Delete {alias}? This will free up disk space.",
                            "model.delete.failed": "Failed to delete model",
                            "model.delete.success": "Model deleted",
                            "model.list.empty": "No Whisper models available",
                            "model.list.loading": "Loading model list...",
                            "project.current": "Project",
                            "project.new": "New project",
                            "project.name": "Project name",
                            "project.description": "Description (optional)",
                            "project.save": "Save",
                            "project.delete": "Delete",
                            "project.deleteConfirm": "Delete project \"{name}\" and all its notes?",
                            "project.namePlaceholder": "e.g. Meeting notes",
                            "button.history": "📝 Notes",
                            "button.liveCaption": "🎬 Live",
                            "button.drafts": "📄 Drafts",
                            "draft.title.plural": "📄 Drafts",
                            "draft.list": "Drafts",
                            "draft.selectMode": "Select mode",
                            "draft.selectedCount": "selected",
                            "draft.selectAll": "Select all",
                            "draft.compose": "📄 Compose draft",
                            "draft.composeTitle": "Compose draft from notes",
                            "draft.title": "Title",
                            "draft.titlePlaceholder": "e.g. Weekly digest",
                            "draft.mode": "Mode",
                            "draft.modeConcat": "Concatenate (with headings)",
                            "draft.modeLlm": "LLM rewrite (into a single article)",
                            "draft.instruction": "Extra instruction (optional)",
                            "draft.instructionPlaceholder": "e.g. Decisions first, background later.",
                            "draft.create": "Create",
                            "draft.save": "Save",
                            "draft.delete": "Delete",
                            "draft.deleteConfirm": "Delete draft \"{title}\"?",
                            "draft.export": "Export as Markdown",
                            "draft.editorPlaceholder": "Select a draft or compose one from the Notes view",
                            "draft.references": "Source notes",
                            "draft.empty": "No drafts yet",
                            "draft.created": "Draft created",
                            "draft.saved": "Saved",
                            "draft.composing": "Composing...",
                            "draft.composeFailed": "Failed to compose draft",
                            "draft.selectAtLeastOne": "Select at least one note",
                            "draft.summary": "Composed from {count} notes",
                            "live.title": "🎬 Live Caption",
                            "live.savedSessions": "Saved sessions",
                            "live.sourceLanguage": "Source",
                            "live.targetLanguage": "Target",
                            "live.enableTranslation": "Live translate",
                            "live.transcriptionModel": "Transcription model",
                            "live.translationModel": "Translation model",
                            "live.latestTranscription": "Transcription",
                            "live.latestTranslation": "Translation",
                            "live.latestWaiting": "Waiting...",
                            "live.start": "Start",
                            "live.stop": "Stop",
                            "live.saveSession": "Save session",
                            "live.sessionTitle": "Title",
                            "live.sessionTitlePlaceholder": "e.g. 2026-05-10 Meeting",
                            "live.save": "Save",
                            "live.discard": "Discard",
                            "live.emptyHint": "Press start — speech will be captioned in real time.",
                            "live.chunkHint": "Transcribes after pauses; translation follows each utterance",
                            "live.noSessions": "No saved sessions yet",
                            "live.summary": "{count} segments / {duration}",
                            "live.startFailed": "Failed to start live caption",
                            "live.micError": "Microphone unavailable",
                            "live.deleteConfirm": "Delete this live session?",
                            "live.empty": "No captions were captured in this session",
                            "history.title": "Notes",
                            "history.search": "Search...",
                            "history.refresh": "Refresh",
                            "history.view.list": "List",
                            "history.view.graph": "Graph",
                            "history.graph.project": "Project",
                            "history.graph.note": "Note",
                            "history.graph.tag": "Tag",
                            "history.graph.link": "Link",
                            "history.graph.linkMode": "Connect notes",
                            "history.graph.linkModeHint": "Click two notes in order to link them. Click the button again to cancel.",
                            "history.graph.linkCancel": "Cancel",
                            "history.graph.linkFirst": "Pick the source note",
                            "history.graph.linkSecond": "Pick the target note",
                            "history.graph.linkCreated": "Link created",
                            "history.graph.linkFailed": "Failed to create link",
                            "history.graph.linkDelete": "Delete link",
                            "history.graph.linkDeleteConfirm": "Delete this link?",
                            "history.graph.linkNoteOnly": "Only note-to-note links are supported",
                            "history.empty": "No notes yet in this project",
                            "history.tags": "Tags",
                            "note.edit": "Edit note",
                            "note.copy": "Copy text",
                            "note.title": "Title",
                            "note.text": "Text",
                            "note.tags": "Tags (comma separated)",
                            "note.tagsPlaceholder": "meeting, projectA, important",
                            "note.delete": "Delete",
                            "note.save": "Save",
                            "note.deleteConfirm": "Delete this note?",
                            "note.parent": "Parent note",
                            "note.parentNone": "(top-level)",
                            "note.clearParent": "Clear parent",
                            "note.parentSetFailed": "Could not set parent note",
                            "history.graph.parent": "Parent",
                            "note.analyze": "🔍 Extract",
                            "note.analyzing": "Extracting...",
                            "note.analysisFailed": "Extraction failed",
                            "note.complete": "✨ Complete",
                            "note.completing": "Completing...",
                            "note.completeFailed": "Completion failed",
                            "note.completeEmpty": "Nothing was added",
                            "note.completeDone": "Appended a continuation",
                            "note.analysis.tone": "Tone",
                            "note.analysis.summary": "Summary",
                            "note.analysis.keywords": "Keywords",
                            "stat.chars": "chars",
                            "stat.fillers": "fillers",
                            "stat.seconds": "sec",
                            "stat.cpm": "chars/min",
                            "translation.title": "Translation",
                            "translation.source": "Source",
                            "translation.target": "Translation",
                            "translation.sourcePlaceholder": "Enter text to translate...",
                            "translation.resultPlaceholder": "Translation will appear here",
                            "translation.instruction": "Translation instruction",
                            "translation.translate": "Translate",
                            "translation.history": "Translation history",
                            "translation.clearAll": "Clear all",
                            "translation.clearConfirm": "Clear all translation history?",
                            "translation.fromNote": "Import from note",
                            "translation.selectNote": "Select a note to import",
                            "translation.noHistory": "No translation history yet"
                        }
                    };

                    // Try to fetch the user's locales directory synchronously. If the backend
                    // isn't reachable (e.g. server not yet ready) fall back to the bundled copy.
                    try {
                        const xhr = new XMLHttpRequest();
                        xhr.open("GET", "/api/locales", false); // synchronous
                        xhr.send(null);
                        if (xhr.status >= 200 && xhr.status < 300) {
                            const payload = JSON.parse(xhr.responseText);
                            TRANSLATIONS = payload.locales || {};
                            LOCALES_DIR = payload.dir || "";
                        }
                    } catch (error) {
                        console.warn("failed to fetch locales from API, using bundled copies", error);
                    }
                    if (!TRANSLATIONS || Object.keys(TRANSLATIONS).length === 0) {
                        TRANSLATIONS = __FALLBACK_TRANSLATIONS;
                    }

                    let SUPPORTED_LOCALES = Object.keys(TRANSLATIONS);
                    const DEFAULT_LOCALE = TRANSLATIONS["en"] ? "en" : SUPPORTED_LOCALES[0] || "en";

                    const detectOsLocale = () => {
                        const candidates = [
                            ...(navigator.languages || []),
                            navigator.language,
                        ].filter(Boolean);
                        for (const candidate of candidates) {
                            const base = candidate.toLowerCase().split("-")[0];
                            if (SUPPORTED_LOCALES.includes(base)) return base;
                        }
                        return DEFAULT_LOCALE;
                    };

                    const OS_LOCALE = detectOsLocale();
                    let currentLocale = OS_LOCALE;

                    const t = (key, params) => {
                        const dict = TRANSLATIONS[currentLocale] || TRANSLATIONS[DEFAULT_LOCALE];
                        let str = dict[key];
                        if (str === undefined) {
                            str = TRANSLATIONS[DEFAULT_LOCALE][key];
                        }
                        if (str === undefined) return key;
                        if (params) {
                            for (const [name, value] of Object.entries(params)) {
                                str = str.replace(new RegExp(`\\{${name}\\}`, "g"), value);
                            }
                        }
                        return str;
                    };

                    const applyTranslations = () => {
                        document.documentElement.lang = currentLocale;
                        document.querySelectorAll("[data-i18n]").forEach((el) => {
                            const key = el.dataset.i18n;
                            const translation = t(key);
                            if (el.tagName === "TITLE") {
                                document.title = translation;
                            } else {
                                el.textContent = translation;
                            }
                        });
                        document.querySelectorAll("[data-i18n-placeholder]").forEach((el) => {
                            el.placeholder = t(el.dataset.i18nPlaceholder);
                        });
                        document.querySelectorAll("[data-i18n-title]").forEach((el) => {
                            el.title = t(el.dataset.i18nTitle);
                        });
                    };

                    const setLocale = (locale) => {
                        const resolved = locale === "auto" ? OS_LOCALE : locale;
                        currentLocale = SUPPORTED_LOCALES.includes(resolved) ? resolved : DEFAULT_LOCALE;
                        applyTranslations();
                        if (typeof renderNotes === "function") renderNotes();
                        if (typeof renderLiveSessionsSidebar === "function") renderLiveSessionsSidebar();
                        if (typeof renderProjectSelect === "function") renderProjectSelect();
                        if (typeof updateCaptionStats === "function") updateCaptionStats();
                        if (latestRequirements && typeof renderRequirementsBanner === "function") {
                            renderRequirementsBanner(latestRequirements);
                        }
                        // Notify listeners (e.g. the Shortcuts settings panel) that
                        // the locale changed so they can re-render their labels.
                        window.dispatchEvent(new CustomEvent("seedraft:locale-changed"));
                    };

                    const loadLocalePreference = () => {
                        try {
                            return localStorage.getItem("seedraft_ui_locale") || "auto";
                        } catch {
                            return "auto";
                        }
                    };

                    const saveLocalePreference = (value) => {
                        try {
                            localStorage.setItem("seedraft_ui_locale", value);
                        } catch {
                            // ignore
                        }
                    };

                    // ========== Theme (light / dark / auto) ==========
                    const THEME_KEY = "seedraft_ui_theme";
                    const systemDarkQuery = typeof window.matchMedia === "function"
                        ? window.matchMedia("(prefers-color-scheme: dark)")
                        : null;

                    const loadThemePreference = () => {
                        try {
                            return localStorage.getItem(THEME_KEY) || "auto";
                        } catch { return "auto"; }
                    };
                    const saveThemePreference = (value) => {
                        try { localStorage.setItem(THEME_KEY, value); } catch {}
                    };

                    const applyTheme = (preference) => {
                        let resolved = preference;
                        if (preference === "auto") {
                            resolved = systemDarkQuery?.matches ? "dark" : "light";
                        }
                        document.body.dataset.theme = resolved;
                    };

                    const setTheme = (preference) => {
                        saveThemePreference(preference);
                        applyTheme(preference);
                    };

                    // Track system theme changes only when "auto" is selected
                    if (systemDarkQuery) {
                        const onSystemThemeChange = () => {
                            if (loadThemePreference() === "auto") applyTheme("auto");
                        };
                        if (systemDarkQuery.addEventListener) {
                            systemDarkQuery.addEventListener("change", onSystemThemeChange);
                        } else if (systemDarkQuery.addListener) {
                            // Safari fallback
                            systemDarkQuery.addListener(onSystemThemeChange);
                        }
                    }

                    const SPEECH_MODEL_STORAGE_KEY = "seedraft_speech_model";
                    const MAX_RECORDING_SECONDS_KEY = "seedraft_max_recording_seconds";
                    const MICROPHONE_DEVICE_KEY = "seedraft_microphone_device_id";
                    const DEFAULT_SPEECH_MODEL_ALIAS = "whisper-tiny";
                    const DEFAULT_CHAT_MODEL_ALIAS = "qwen2.5-coder-0.5b";
                    // Per-post-processing-step LLM selection.
                    // Custom steps carry their own `model` field in the step object itself,
                    // so only refine/translate defaults are persisted here.
                    const POST_MODEL_KEY = "seedraft_post_models";
                    const loadPostModelPrefs = () => {
                        try {
                            const raw = localStorage.getItem(POST_MODEL_KEY);
                            if (!raw) return {};
                            return JSON.parse(raw) || {};
                        } catch { return {}; }
                    };
                    const savePostModelPrefs = () => {
                        try {
                            localStorage.setItem(POST_MODEL_KEY, JSON.stringify({
                                refine: refineModel?.value || "",
                                translate: translateModel?.value || "",
                                analyze: analyzeModel?.value || "",
                                complete: completeModel?.value || ""
                            }));
                        } catch {}
                    };
                    const restorePostModelPreferences = () => {
                        const prefs = loadPostModelPrefs();
                        if (refineModel && prefs.refine) {
                            const exists = Array.from(refineModel.options).some(o => o.value === prefs.refine);
                            if (exists) refineModel.value = prefs.refine;
                        }
                        if (translateModel && prefs.translate) {
                            const exists = Array.from(translateModel.options).some(o => o.value === prefs.translate);
                            if (exists) translateModel.value = prefs.translate;
                        }
                        if (analyzeModel && prefs.analyze) {
                            const exists = Array.from(analyzeModel.options).some(o => o.value === prefs.analyze);
                            if (exists) analyzeModel.value = prefs.analyze;
                        }
                        if (completeModel && prefs.complete) {
                            const exists = Array.from(completeModel.options).some(o => o.value === prefs.complete);
                            if (exists) completeModel.value = prefs.complete;
                        }
                    };
                    const loadSpeechModelPreference = () => {
                        try {
                            return localStorage.getItem(SPEECH_MODEL_STORAGE_KEY);
                        } catch {
                            return null;
                        }
                    };
                    const saveSpeechModelPreference = (value) => {
                        if (!value) return;
                        try {
                            localStorage.setItem(SPEECH_MODEL_STORAGE_KEY, value);
                        } catch {
                            // ignore
                        }
                    };

                    // ========== DOM elements ==========
                    // Main UI elements
                    const audioFile = document.getElementById("audioFile");
                    const captionContent = document.getElementById("captionContent");
                    const captionDisplay = document.getElementById("captionDisplay");
                    const captionDirty = document.getElementById("captionDirty");
                    const clearMainButton = document.getElementById("clearMainButton");
                    const closeSettingsButton = document.getElementById("closeSettingsButton");
                    const copyMainButton = document.getElementById("copyMainButton");
                    const saveToHistoryButton = document.getElementById("saveToHistoryButton");
                    const currentModelBadge = document.getElementById("currentModelBadge");
                    const currentModelLabel = document.getElementById("currentModelLabel");
                    const currentLanguageBadge = document.getElementById("currentLanguageBadge");
                    const currentModelCardAlias = document.getElementById("currentModelCardAlias");
                    const currentModelCardDesc = document.getElementById("currentModelCardDesc");
                    const currentModelCardIcon = document.getElementById("currentModelCardIcon");
                    const currentModelCardStatus = document.getElementById("currentModelCardStatus");
                    const currentModelJumpButton = document.getElementById("currentModelJumpButton");
                    const modelList = document.getElementById("modelList");
                    const downloadOverlay = document.getElementById("downloadOverlay");
                    const downloadTitle = document.getElementById("downloadTitle");
                    const downloadMessage = document.getElementById("downloadMessage");
                    const progressFill = document.getElementById("progressFill");
                    const progressPercent = document.getElementById("progressPercent");
                    const progressAlias = document.getElementById("progressAlias");
                    const downloadCancelButton = document.getElementById("downloadCancelButton");
                    const downloadDiagnostic = document.getElementById("downloadDiagnostic");
                    const downloadDiagnosticAlias = document.getElementById("downloadDiagnosticAlias");
                    const downloadDiagnosticError = document.getElementById("downloadDiagnosticError");
                    const downloadDiagnosticVariants = document.getElementById("downloadDiagnosticVariants");
                    const downloadDiagnosticClose = document.getElementById("downloadDiagnosticClose");
                    const dropOverlay = document.getElementById("dropOverlay");
                    const errorBanner = document.getElementById("errorBanner");
                    const errorBannerMessage = document.getElementById("errorBannerMessage");
                    const errorBannerClose = document.getElementById("errorBannerClose");
                    const requirementsBanner = document.getElementById("requirementsBanner");
                    const requirementsBannerMessage = document.getElementById("requirementsBannerMessage");
                    const requirementsCommand = document.getElementById("requirementsCommand");
                    const requirementsPrimaryButton = document.getElementById("requirementsPrimaryButton");
                    const requirementsModelsButton = document.getElementById("requirementsModelsButton");
                    const requirementsBannerClose = document.getElementById("requirementsBannerClose");
                    const openSettingsButton = document.getElementById("openSettingsButton");
                    const recordButton = document.getElementById("recordButton");
                    const recordProgressRing = document.getElementById("recordProgressRing");
                    const recordCountdown = document.getElementById("recordCountdown");
                    const recordInfo = document.getElementById("recordInfo");
                    const recordInfoText = document.getElementById("recordInfoText");
                    const settingsModal = document.getElementById("settingsModal");
                    const statusBadge = document.getElementById("statusBadge");

                    // Settings elements
                    const chooseSaveFolderButton = document.getElementById("chooseSaveFolderButton");
                    const customInstruction = document.getElementById("customInstruction");
                    const customTerms = document.getElementById("customTerms");
                    const filePrefix = document.getElementById("filePrefix");
                    const language = document.getElementById("language");
                    const microphoneDevice = document.getElementById("microphoneDevice");
                    const microphoneMonitorCanvas = document.getElementById("microphoneMonitorCanvas");
                    const microphoneMonitorStatus = document.getElementById("microphoneMonitorStatus");
                    const microphoneRefreshButton = document.getElementById("microphoneRefreshButton");
                    const maxRecordingSeconds = document.getElementById("maxRecordingSeconds");
                    const refineModel = document.getElementById("refineModel");
                    const translateModel = document.getElementById("translateModel");
                    const analyzeModel = document.getElementById("analyzeModel");
                    const completeModel = document.getElementById("completeModel");
                    const linkedContextToggle = document.getElementById("linkedContextToggle");
                    const linkedContextEnabled = () => !!linkedContextToggle?.checked;
                    const LINKED_CONTEXT_KEY = "seedraft_linked_context_enabled";
                    // Restore previous value (default true) and persist changes
                    if (linkedContextToggle) {
                        const stored = localStorage.getItem(LINKED_CONTEXT_KEY);
                        linkedContextToggle.checked = stored === null ? true : stored === "true";
                        linkedContextToggle.addEventListener("change", () => {
                            localStorage.setItem(LINKED_CONTEXT_KEY, String(linkedContextToggle.checked));
                        });
                    }
                    const removeFillers = document.getElementById("removeFillers");
                    const sampleRate = document.getElementById("sampleRate");
                    const saveFolder = document.getElementById("saveFolder");
                    const resetSettingsButton = document.getElementById("resetSettingsButton");
                    // Custom post-processing steps
                    const customStepListEl = document.getElementById("customStepList");
                    const customStepAddButton = document.getElementById("customStepAddButton");
                    const customStepEditorModal = document.getElementById("customStepEditorModal");
                    const customStepEditorClose = document.getElementById("customStepEditorClose");
                    const customStepName = document.getElementById("customStepName");
                    const customStepInstruction = document.getElementById("customStepInstruction");
                    const customStepModel = document.getElementById("customStepModel");
                    const customStepSave = document.getElementById("customStepSave");
                    const customStepDelete = document.getElementById("customStepDelete");
                    // Quick toggles near the record button
                    const quickAutoSave = document.getElementById("quickAutoSave");
                    const quickAutoRefine = document.getElementById("quickAutoRefine");
                    const quickAutoTranslate = document.getElementById("quickAutoTranslate");
                    const quickAutoAnalyze = document.getElementById("quickAutoAnalyze");
                    const quickAutoCustom = document.getElementById("quickAutoCustom");
                    const settingsTabButtons = Array.from(document.querySelectorAll(".settings-tab-button"));
                    const settingsPanels = Array.from(document.querySelectorAll(".settings-panel"));
                    const speechModel = document.getElementById("speechModel");
                    const sourceLanguage = document.getElementById("sourceLanguage");
                    const targetLanguage = document.getElementById("targetLanguage");
                    const transcriptionPrompt = document.getElementById("transcriptionPrompt");
                    const translationInstruction = document.getElementById("translationInstruction");
                    const uiLanguage = document.getElementById("uiLanguage");
                    const uiTheme = document.getElementById("uiTheme");
                    const localesDirInput = document.getElementById("localesDirInput");
                    const chooseLocalesDirButton = document.getElementById("chooseLocalesDirButton");
                    const resetLocalesDirButton = document.getElementById("resetLocalesDirButton");
                    const copyLocalesDirButton = document.getElementById("copyLocalesDirButton");
                    const voiceCommands = document.getElementById("voiceCommands");

                    // Project selector
                    const projectSelect = document.getElementById("projectSelect");
                    const newProjectButton = document.getElementById("newProjectButton");
                    const projectEditorModal = document.getElementById("projectEditorModal");
                    const projectEditorName = document.getElementById("projectEditorName");
                    const projectEditorDescription = document.getElementById("projectEditorDescription");
                    const projectEditorSave = document.getElementById("projectEditorSave");
                    const projectEditorClose = document.getElementById("projectEditorClose");
                    const projectEditorDelete = document.getElementById("projectEditorDelete");

                    // History overlay
                    // Notes workspace lives in the main view; no overlay to open/close.
                    const notesSelectToggle = document.getElementById("notesSelectToggle");
                    const notesSelectionBar = document.getElementById("notesSelectionBar");
                    const notesSelectionCount = document.getElementById("notesSelectionCount");
                    const notesSelectAll = document.getElementById("notesSelectAll");
                    const notesSelectionCompose = document.getElementById("notesSelectionCompose");

                    // Draft compose modal
                    const composeDraftModal = document.getElementById("composeDraftModal");
                    const composeDraftClose = document.getElementById("composeDraftClose");
                    const composeDraftCancel = document.getElementById("composeDraftCancel");
                    const composeDraftConfirm = document.getElementById("composeDraftConfirm");
                    const composeDraftTitle = document.getElementById("composeDraftTitle");
                    const composeDraftMode = document.getElementById("composeDraftMode");
                    const composeDraftInstructionWrap = document.getElementById("composeDraftInstructionWrap");
                    const composeDraftInstruction = document.getElementById("composeDraftInstruction");
                    const composeDraftSummary = document.getElementById("composeDraftSummary");

                    // Draft overlay
                    const openDraftsButton = document.getElementById("openDraftsButton");
                    const draftsOverlay = document.getElementById("draftsOverlay");
                    const closeDraftsButton = document.getElementById("closeDraftsButton");
                    const draftsList = document.getElementById("draftsList");
                    const draftEditorTitle = document.getElementById("draftEditorTitle");
                    const draftEditorContent = document.getElementById("draftEditorContent");
                    const draftCopyButton = document.getElementById("draftCopyButton");
                    const draftExportButton = document.getElementById("draftExportButton");
                    const draftSaveButton = document.getElementById("draftSaveButton");
                    const draftDeleteButton = document.getElementById("draftDeleteButton");
                    const draftReferences = document.getElementById("draftReferences");
                    const draftReferencesList = document.getElementById("draftReferencesList");
                    const historySearch = document.getElementById("historySearch");
                    const refreshHistoryButton = document.getElementById("refreshHistoryButton");
                    const notesGrid = document.getElementById("notesGrid");
                    const refreshGraphButton = document.getElementById("refreshGraphButton");
                    const graphLinkModeButton = document.getElementById("graphLinkModeButton");
                    const graphLinkBanner = document.getElementById("graphLinkBanner");
                    const graphLinkBannerText = document.getElementById("graphLinkBannerText");
                    const graphLinkCancelButton = document.getElementById("graphLinkCancelButton");
                    const graphSvg = document.getElementById("graphSvg");
                    const graphDetail = document.getElementById("graphDetail");
                    const graphDetailClose = document.getElementById("graphDetailClose");
                    const graphDetailTitle = document.getElementById("graphDetailTitle");
                    const graphDetailKind = document.getElementById("graphDetailKind");
                    const graphDetailContent = document.getElementById("graphDetailContent");
                    const notesHeaderMeta = document.getElementById("notesHeaderMeta");

                    // Note editor
                    const noteEditorModal = document.getElementById("noteEditorModal");
                    const noteEditorTitle = document.getElementById("noteEditorTitle");
                    const noteEditorText = document.getElementById("noteEditorText");
                    const noteEditorTags = document.getElementById("noteEditorTags");
                    const noteEditorParent = document.getElementById("noteEditorParent");
                    const noteEditorClearParent = document.getElementById("noteEditorClearParent");
                    const noteTagSuggestions = document.getElementById("noteTagSuggestions");
                    const noteTagChips = document.getElementById("noteTagChips");
                    const noteEditorSave = document.getElementById("noteEditorSave");
                    const noteEditorDelete = document.getElementById("noteEditorDelete");
                    const noteEditorClose = document.getElementById("noteEditorClose");
                    const noteEditorAnalyze = document.getElementById("noteEditorAnalyze");
                    const noteEditorComplete = document.getElementById("noteEditorComplete");
                    const noteAnalysisResult = document.getElementById("noteAnalysisResult");

                    // Caption stats
                    const captionStats = document.getElementById("captionStats");
                    const statChars = document.getElementById("statChars");
                    const statFillers = document.getElementById("statFillers");
                    const statDurationWrap = document.getElementById("statDurationWrap");
                    const statDuration = document.getElementById("statDuration");
                    const statSpeedWrap = document.getElementById("statSpeedWrap");
                    const statSpeed = document.getElementById("statSpeed");

                    // Live caption overlay
                    const openLiveButton = document.getElementById("openLiveButton");
                    const liveOverlay = document.getElementById("liveOverlay");
                    const closeLiveButton = document.getElementById("closeLiveButton");
                    const liveStartButton = document.getElementById("liveStartButton");
                    const liveStopButton = document.getElementById("liveStopButton");
                    const liveStartLabel = document.getElementById("liveStartLabel");
                    const liveTimer = document.getElementById("liveTimer");
                    const liveRecBadge = document.getElementById("liveRecBadge");
                    const liveCaptions = document.getElementById("liveCaptions");
                    const liveEmptyMessage = document.getElementById("liveEmptyMessage");
                    const liveCurrentSource = document.getElementById("liveCurrentSource");
                    const liveCurrentTranslation = document.getElementById("liveCurrentTranslation");
                    const liveSourceLanguage = document.getElementById("liveSourceLanguage");
                    const liveTargetLanguage = document.getElementById("liveTargetLanguage");
                    const liveTranslateToggle = document.getElementById("liveTranslateToggle");
                    const liveConfigRow = document.getElementById("liveConfigRow");
                    const liveModelRow = document.getElementById("liveModelRow");
                    const liveSpeechModel = document.getElementById("liveSpeechModel");
                    const liveTranslateModel = document.getElementById("liveTranslateModel");
                    const liveSessionsList = document.getElementById("liveSessionsList");
                    const liveSaveModal = document.getElementById("liveSaveModal");
                    const liveSaveTitle = document.getElementById("liveSaveTitle");
                    const liveSaveSummary = document.getElementById("liveSaveSummary");
                    const liveSaveConfirm = document.getElementById("liveSaveConfirm");
                    const liveSaveDiscard = document.getElementById("liveSaveDiscard");
                    const liveSaveClose = document.getElementById("liveSaveClose");
                    const LIVE_PREFS_KEY = "seedraft_live_caption_prefs";

                    let currentText = "";
                    let mediaRecorder = null;
                    let recordedBlob = null;
                    let recordedFileName = "recording.webm";
                    let recordedChunks = [];
                    let recordingTimer = null;
                    let recordingEndsAt = 0;        // epoch ms when the max-duration timer fires
                    let recordingTotalMs = 0;       // original duration for progress calc
                    let recordCountdownInterval = null;
                    let saveDirectoryHandle = null;

                    // Drive the ring + mm:ss countdown while recording. Runs every
                    // 250ms so the ring sweep looks smooth without burning CPU.
                    const formatRecordTime = (ms) => {
                        const total = Math.max(0, Math.ceil(ms / 1000));
                        const m = Math.floor(total / 60);
                        const s = total % 60;
                        return `${m}:${String(s).padStart(2, "0")}`;
                    };
                    const tickRecordCountdown = () => {
                        if (!recordingEndsAt || !recordingTotalMs) return;
                        const remainingMs = Math.max(0, recordingEndsAt - Date.now());
                        const progress = Math.max(0, Math.min(1, remainingMs / recordingTotalMs));
                        if (recordProgressRing) {
                            recordProgressRing.style.setProperty("--record-progress", String(progress));
                            // Flip to warning colour in the final 20%.
                            if (progress < 0.2) {
                                recordProgressRing.dataset.warning = "true";
                            } else {
                                delete recordProgressRing.dataset.warning;
                            }
                        }
                        if (recordCountdown) {
                            recordCountdown.textContent = formatRecordTime(remainingMs);
                        }
                    };
                    const startRecordCountdown = (maxSeconds) => {
                        stopRecordCountdown();
                        recordingTotalMs = maxSeconds * 1000;
                        recordingEndsAt = Date.now() + recordingTotalMs;
                        if (recordProgressRing) {
                            recordProgressRing.dataset.active = "true";
                            recordProgressRing.style.setProperty("--record-progress", "1");
                            delete recordProgressRing.dataset.warning;
                        }
                        if (recordCountdown) {
                            recordCountdown.hidden = false;
                            recordCountdown.textContent = formatRecordTime(recordingTotalMs);
                        }
                        recordCountdownInterval = setInterval(tickRecordCountdown, 250);
                    };
                    function stopRecordCountdown() {
                        if (recordCountdownInterval) {
                            clearInterval(recordCountdownInterval);
                            recordCountdownInterval = null;
                        }
                        recordingEndsAt = 0;
                        recordingTotalMs = 0;
                        if (recordProgressRing) {
                            delete recordProgressRing.dataset.active;
                            delete recordProgressRing.dataset.warning;
                        }
                        if (recordCountdown) {
                            recordCountdown.hidden = true;
                            recordCountdown.textContent = "";
                        }
                    }
                    // Cache the mic stream across recordings so the browser only
                    // prompts for permission once per session. Keyed by the
                    // constraints used to request it, so changing the sample rate
                    // in settings still re-requests a matching stream.
                    let cachedMicStream = null;
                    let cachedMicConstraintsKey = "";

                    const loadMicrophonePreference = () => {
                        try {
                            return localStorage.getItem(MICROPHONE_DEVICE_KEY) || "";
                        } catch {
                            return "";
                        }
                    };

                    const saveMicrophonePreference = () => {
                        if (!microphoneDevice) return;
                        try {
                            localStorage.setItem(MICROPHONE_DEVICE_KEY, microphoneDevice.value || "");
                        } catch {}
                    };

                    const populateMicrophoneDevices = async () => {
                        if (!microphoneDevice) return;
                        const selected = microphoneDevice.value || loadMicrophonePreference();
                        microphoneDevice.replaceChildren();

                        const defaultOption = document.createElement("option");
                        defaultOption.value = "";
                        defaultOption.textContent = t("settings.transcription.microphoneDefault");
                        microphoneDevice.append(defaultOption);

                        if (!navigator.mediaDevices?.enumerateDevices) {
                            microphoneDevice.value = "";
                            return;
                        }

                        try {
                            const devices = (await navigator.mediaDevices.enumerateDevices())
                                .filter((device) => device.kind === "audioinput");
                            devices.forEach((device, index) => {
                                const option = document.createElement("option");
                                option.value = device.deviceId;
                                option.textContent = device.label || `${t("settings.transcription.microphone")} ${index + 1}`;
                                microphoneDevice.append(option);
                            });
                            if (devices.length === 0) {
                                const empty = document.createElement("option");
                                empty.value = "__none";
                                empty.disabled = true;
                                empty.textContent = t("settings.transcription.microphoneUnavailable");
                                microphoneDevice.append(empty);
                            }
                            if (selected && devices.some((device) => device.deviceId === selected)) {
                                microphoneDevice.value = selected;
                            } else {
                                microphoneDevice.value = "";
                            }
                        } catch (error) {
                            console.warn("failed to enumerate microphones", error);
                            microphoneDevice.value = "";
                        }
                    };

                    const buildMicAudioConstraints = (extra = {}) => {
                        const constraints = {};
                        const deviceId = microphoneDevice?.value || "";
                        if (deviceId) constraints.deviceId = { exact: deviceId };
                        if (extra && typeof extra === "object") Object.assign(constraints, extra);
                        return Object.keys(constraints).length > 0 ? constraints : true;
                    };

                    const getMicStream = async (audioConstraints) => {
                        const key = JSON.stringify(audioConstraints);
                        const tracks = cachedMicStream?.getAudioTracks?.() || [];
                        const isLive = tracks.length > 0 && tracks.every(t => t.readyState === "live");
                        if (cachedMicStream && isLive && cachedMicConstraintsKey === key) {
                            return cachedMicStream;
                        }
                        // Old stream is gone or constraints changed — request a new one.
                        if (cachedMicStream) {
                            cachedMicStream.getTracks().forEach(t => t.stop());
                            cachedMicStream = null;
                        }
                        let stream;
                        try {
                            stream = await navigator.mediaDevices.getUserMedia({ audio: audioConstraints });
                        } catch (error) {
                            const hasExactDevice = !!audioConstraints?.deviceId?.exact;
                            const canFallback = hasExactDevice && [
                                "NotFoundError",
                                "OverconstrainedError",
                                "ConstraintNotSatisfiedError",
                                "NotReadableError"
                            ].includes(error?.name);
                            if (!canFallback) throw error;
                            if (microphoneDevice) microphoneDevice.value = "";
                            saveMicrophonePreference();
                            showToast(t("settings.transcription.microphoneFallback"), "warning");
                            const fallback = { ...audioConstraints };
                            delete fallback.deviceId;
                            const fallbackConstraints = Object.keys(fallback).length > 0 ? fallback : true;
                            stream = await navigator.mediaDevices.getUserMedia({ audio: fallbackConstraints });
                            cachedMicConstraintsKey = JSON.stringify(fallbackConstraints);
                            cachedMicStream = stream;
                            populateMicrophoneDevices().catch(() => {});
                            return stream;
                        }
                        cachedMicStream = stream;
                        cachedMicConstraintsKey = key;
                        populateMicrophoneDevices().catch(() => {});
                        return stream;
                    };

                    let micMonitorState = null;
                    let micMonitorStartPromise = null;

                    const syncMicMonitorLabels = () => {
                        if (microphoneMonitorStatus) {
                            microphoneMonitorStatus.textContent = micMonitorState
                                ? t("settings.transcription.micMonitorActive")
                                : t("settings.transcription.micMonitorIdle");
                            microphoneMonitorStatus.title = "";
                        }
                    };

                    const drawMicMonitorIdle = () => {
                        const canvas = microphoneMonitorCanvas;
                        if (!canvas) return;
                        const ctx = canvas.getContext("2d");
                        if (!ctx) return;
                        const rect = canvas.getBoundingClientRect();
                        const scale = window.devicePixelRatio || 1;
                        const width = Math.max(1, Math.floor(rect.width * scale));
                        const height = Math.max(1, Math.floor(rect.height * scale));
                        if (canvas.width !== width || canvas.height !== height) {
                            canvas.width = width;
                            canvas.height = height;
                        }
                        ctx.clearRect(0, 0, width, height);
                        ctx.fillStyle = "#0d1117";
                        ctx.fillRect(0, 0, width, height);
                        ctx.strokeStyle = "#30363d";
                        ctx.lineWidth = Math.max(1, scale);
                        ctx.beginPath();
                        ctx.moveTo(0, height / 2);
                        ctx.lineTo(width, height / 2);
                        ctx.stroke();
                    };

                    const drawMicMonitor = () => {
                        const state = micMonitorState;
                        const canvas = microphoneMonitorCanvas;
                        if (!state || !canvas) return;
                        const ctx = canvas.getContext("2d");
                        if (!ctx) return;
                        const rect = canvas.getBoundingClientRect();
                        const scale = window.devicePixelRatio || 1;
                        const width = Math.max(1, Math.floor(rect.width * scale));
                        const height = Math.max(1, Math.floor(rect.height * scale));
                        if (canvas.width !== width || canvas.height !== height) {
                            canvas.width = width;
                            canvas.height = height;
                        }

                        state.analyser.getByteTimeDomainData(state.wave);
                        let sum = 0;
                        for (const value of state.wave) {
                            const centered = (value - 128) / 128;
                            sum += centered * centered;
                        }
                        const level = Math.min(1, Math.sqrt(sum / state.wave.length) * 3.5);

                        ctx.clearRect(0, 0, width, height);
                        ctx.fillStyle = "#0d1117";
                        ctx.fillRect(0, 0, width, height);
                        ctx.fillStyle = "rgba(46, 160, 67, 0.22)";
                        ctx.fillRect(0, height - (height * level), width, height * level);
                        ctx.strokeStyle = "#30363d";
                        ctx.lineWidth = Math.max(1, scale);
                        ctx.beginPath();
                        ctx.moveTo(0, height / 2);
                        ctx.lineTo(width, height / 2);
                        ctx.stroke();

                        ctx.strokeStyle = level > 0.72 ? "#f0c96f" : "#3fb950";
                        ctx.lineWidth = Math.max(2, 2 * scale);
                        ctx.beginPath();
                        for (let i = 0; i < state.wave.length; i++) {
                            const x = (i / (state.wave.length - 1)) * width;
                            const y = (state.wave[i] / 255) * height;
                            if (i === 0) ctx.moveTo(x, y);
                            else ctx.lineTo(x, y);
                        }
                        ctx.stroke();
                        state.raf = requestAnimationFrame(drawMicMonitor);
                    };

                    const stopMicMonitor = async () => {
                        const state = micMonitorState;
                        micMonitorState = null;
                        if (state?.raf) cancelAnimationFrame(state.raf);
                        try { state?.source?.disconnect(); } catch {}
                        try { await state?.audioCtx?.close?.(); } catch {}
                        syncMicMonitorLabels();
                        drawMicMonitorIdle();
                    };

                    const startMicMonitor = async () => {
                        if (micMonitorState) return;
                        if (!microphoneMonitorCanvas) return;
                        if (!navigator.mediaDevices?.getUserMedia) {
                            throw new Error(t("status.micUnavailable"));
                        }
                        const stream = await getMicStream(buildMicAudioConstraints());
                        const AudioCtx = window.AudioContext || window.webkitAudioContext;
                        if (!AudioCtx) throw new Error("AudioContext is not available");
                        const audioCtx = new AudioCtx();
                        if (audioCtx.state === "suspended") await audioCtx.resume();
                        const source = audioCtx.createMediaStreamSource(stream);
                        const analyser = audioCtx.createAnalyser();
                        analyser.fftSize = 1024;
                        analyser.smoothingTimeConstant = 0.55;
                        source.connect(analyser);
                        micMonitorState = {
                            audioCtx,
                            source,
                            analyser,
                            wave: new Uint8Array(analyser.fftSize),
                            raf: null
                        };
                        syncMicMonitorLabels();
                        drawMicMonitor();
                    };

                    const setMicMonitorError = (error) => {
                        console.warn("mic monitor failed", error);
                        if (microphoneMonitorStatus) {
                            microphoneMonitorStatus.textContent = t("settings.transcription.micMonitorError");
                            if (error?.message) microphoneMonitorStatus.title = error.message;
                        }
                        drawMicMonitorIdle();
                    };

                    const activeSettingsTab = () => {
                        const selected = settingsTabButtons.find((button) => button.getAttribute("aria-selected") === "true");
                        return selected?.dataset?.tab || "transcription";
                    };

                    const syncMicMonitorForSettings = async () => {
                        const shouldRun = !settingsModal.hidden && activeSettingsTab() === "transcription";
                        if (!shouldRun) {
                            await stopMicMonitor();
                            return;
                        }
                        if (micMonitorState || micMonitorStartPromise) return;
                        micMonitorStartPromise = startMicMonitor()
                            .then(async () => {
                                if (settingsModal.hidden || activeSettingsTab() !== "transcription") {
                                    await stopMicMonitor();
                                }
                            })
                            .catch((error) => {
                                if (!settingsModal.hidden && activeSettingsTab() === "transcription") {
                                    setMicMonitorError(error);
                                }
                            })
                            .finally(() => {
                                micMonitorStartPromise = null;
                            });
                        await micMonitorStartPromise;
                    };

                    const restartMicMonitorIfRunning = async () => {
                        if (!micMonitorState) return;
                        await stopMicMonitor();
                        try {
                            await startMicMonitor();
                        } catch (error) {
                            console.warn("mic monitor restart failed", error);
                            if (microphoneMonitorStatus) {
                                microphoneMonitorStatus.textContent = t("settings.transcription.micMonitorError");
                            }
                        }
                    };

                    const setStatus = (text, mode = "idle") => {
                        statusBadge.textContent = text;
                        statusBadge.dataset.mode = mode;
                    };

                    // Lightweight toast popup (top-right). Auto-dismisses.
                    const toastContainer = document.getElementById("toastContainer");
                    const TOAST_ICONS = { success: "✓", info: "ℹ", warning: "!" };
                    const showToast = (message, variant = "success", duration = 2600) => {
                        if (!message || !toastContainer) return;
                        const toast = document.createElement("div");
                        toast.className = "toast";
                        toast.dataset.variant = variant;

                        const icon = document.createElement("span");
                        icon.className = "toast-icon";
                        icon.textContent = TOAST_ICONS[variant] || "•";

                        const body = document.createElement("span");
                        body.className = "toast-message";
                        body.textContent = message;

                        toast.append(icon, body);
                        toastContainer.append(toast);

                        const dismiss = () => {
                            if (!toast.parentNode) return;
                            toast.classList.add("is-leaving");
                            setTimeout(() => toast.remove(), 260);
                        };

                        toast.addEventListener("click", dismiss);
                        setTimeout(dismiss, duration);
                    };

                    const showErrorBanner = (message) => {
                        if (!message) return;
                        errorBannerMessage.textContent = message;
                        errorBanner.hidden = false;
                    };

                    const openModelsSettings = () => {
                        openSettings();
                        activateSettingsTab("models");
                        refreshAllModels();
                    };

                    // Show transient message under the caption area (failures, progress hints).
                    // Passing an empty string hides the banner.
                    const setRecordInfo = (message) => {
                        if (message) {
                            delete recordInfoText.dataset.i18n;
                            recordInfoText.textContent = message;
                            recordInfo.dataset.empty = "false";
                            recordInfo.setAttribute("aria-hidden", "false");
                        } else {
                            recordInfoText.dataset.i18n = "record.infoPlaceholder";
                            recordInfoText.textContent = t("record.infoPlaceholder");
                            recordInfo.dataset.empty = "true";
                            recordInfo.setAttribute("aria-hidden", "true");
                        }
                    };

                    const hideErrorBanner = () => {
                        errorBanner.hidden = true;
                        errorBannerMessage.textContent = "";
                    };

                    errorBannerClose.addEventListener("click", hideErrorBanner);
                    requirementsBannerClose?.addEventListener("click", () => {
                        requirementsDismissed = true;
                        if (requirementsBanner) requirementsBanner.hidden = true;
                    });
                    requirementsModelsButton?.addEventListener("click", openModelsSettings);
                    requirementsPrimaryButton?.addEventListener("click", async () => {
                        const requirements = latestRequirements;
                        if (!requirements) return;
                        if (requirements.runtime_ready === false) {
                            const command = requirements.install_command || "winget install Microsoft.FoundryLocal";
                            if (navigator.clipboard?.writeText) {
                                await navigator.clipboard.writeText(command);
                                showToast(t("requirements.commandCopied"), "success");
                            }
                            requirementsCommand.hidden = false;
                            requirementsCommand.textContent = command;
                            return;
                        }
                        await prepareMissingRequiredModels(requirements);
                    });

                    window.addEventListener("error", (event) => {
                        showErrorBanner(`${event.message}\n${event.filename || ""}:${event.lineno || ""}`);
                    });
                    window.addEventListener("unhandledrejection", (event) => {
                        const reason = event.reason;
                        const msg = reason && (reason.stack || reason.message) ? (reason.stack || reason.message) : String(reason);
                        showErrorBanner(msg);
                    });

                    // ========== Speech model status ==========
                    // Known model descriptions are stored in i18n. Unknown aliases fall back
                    // to a generated description based on size keywords in the alias.
                    const MODEL_DESCRIPTION_KEYS = {
                        "whisper-tiny": "settings.transcription.model.tiny",
                        "whisper-base": "settings.transcription.model.base",
                        "whisper-small": "settings.transcription.model.small",
                        "whisper-medium": "settings.transcription.model.medium",
                        "whisper-large": "settings.transcription.model.large",
                        "whisper-large-v3": "settings.transcription.model.largeV3",
                        "whisper-large-v3-turbo": "settings.transcription.model.largeTurbo"
                    };

                    const describeModel = (alias) => {
                        const key = MODEL_DESCRIPTION_KEYS[alias];
                        if (key) {
                            const value = t(key);
                            if (value !== key) return value;
                        }
                        return alias;
                    };

                    let speechModelStates = [];
                    let activeDownloadAlias = null;
                    let modelListLoading = false;
                    let defaultChatDownloadPrompted = false;
                    let customSteps = [];

                    const updateCurrentModelBadge = () => {
                        const activeModel = speechModelStates.find(m => m.active);
                        if (activeModel) {
                            currentModelBadge.hidden = false;
                            currentModelLabel.textContent = activeModel.alias;
                        } else {
                            currentModelBadge.hidden = true;
                        }
                    };

                    // Transcription tab: show only the currently-selected model.
                    // Full selection / deletion lives in the "Models" tab.
                    const renderModelList = () => {
                        if (!currentModelCardAlias) return;

                        const alias = speechModel.value;
                        const state = speechModelStates.find(s => s.alias === alias);
                        const isDownloading = activeDownloadAlias === alias;

                        if (!alias) {
                            currentModelCardAlias.textContent = t("settings.transcription.noneSelected");
                            currentModelCardDesc.textContent = "";
                            currentModelCardStatus.textContent = "";
                            currentModelCardStatus.dataset.variant = "missing";
                            updateCurrentModelBadge();
                            return;
                        }

                        currentModelCardAlias.textContent = alias;
                        currentModelCardDesc.textContent = describeModel(alias);

                        let iconChar;
                        let statusKey;
                        let variant;
                        if (isDownloading) {
                            iconChar = "⟳";
                            statusKey = "model.status.downloading";
                            variant = "downloading";
                        } else if (state?.active) {
                            iconChar = "●";
                            statusKey = "model.status.active";
                            variant = "active";
                        } else if (state?.downloaded) {
                            iconChar = "✓";
                            statusKey = "model.status.downloaded";
                            variant = "cached";
                        } else {
                            iconChar = "↓";
                            statusKey = "model.status.notDownloaded";
                            variant = "missing";
                        }
                        currentModelCardIcon.textContent = iconChar;
                        currentModelCardIcon.classList.toggle("is-spinning", isDownloading);
                        currentModelCardStatus.textContent = t(statusKey);
                        currentModelCardStatus.dataset.variant = variant;

                        updateCurrentModelBadge();
                        updateLiveModelDisplay();
                        populateLiveSpeechModelSelect();
                    };

                    const deleteModel = async (alias) => {
                        const confirmMessage = t("model.delete.confirm", { alias });
                        if (!window.confirm(confirmMessage)) return;

                        try {
                            const response = await fetch("/api/models/speech/delete", {
                                method: "POST",
                                headers: { "Content-Type": "application/json" },
                                body: JSON.stringify({ alias })
                            });
                            const rawBody = await response.text();
                            let payload = {};
                            try {
                                payload = rawBody ? JSON.parse(rawBody) : {};
                            } catch {
                                payload = { error: rawBody };
                            }
                            if (!response.ok) {
                                throw new Error(payload.error || t("model.delete.failed"));
                            }
                            speechModelStates = payload.models || [];
                            renderModelList();
                            showToast(t("model.delete.success"), "success");
                        } catch (error) {
                            showErrorBanner((error && error.message) || t("model.delete.failed"));
                            setStatus(t("model.delete.failed"), "error");
                        }
                    };

                    // ========== Unified model management (settings → Models tab) ==========
                    const modelsCachePath = document.getElementById("modelsCachePath");
                    const modelsDownloadedCount = document.getElementById("modelsDownloadedCount");
                    const modelsRefreshButton = document.getElementById("modelsRefreshButton");
                    const modelsSpeechList = document.getElementById("modelsSpeechList");
                    const modelsChatList = document.getElementById("modelsChatList");
                    const modelsEpList = document.getElementById("modelsEpList");

                    let allModelsCache = { models: [], cache_dir: null };
                    let appSettingsCache = {
                        configured_locales_dir: null,
                        locales_dir: LOCALES_DIR || "",
                        default_locales_dir: ""
                    };

                    const renderAppSettings = () => {
                        if (localesDirInput) {
                            localesDirInput.value = appSettingsCache.locales_dir || LOCALES_DIR || "";
                        }
                        if (modelsCachePath) {
                            modelsCachePath.textContent = allModelsCache.cache_dir || "—";
                        }
                    };

                    const refreshAppSettings = async () => {
                        try {
                            const response = await fetch("/api/app/settings");
                            if (!response.ok) return;
                            appSettingsCache = await response.json();
                            renderAppSettings();
                        } catch (error) {
                            console.warn("failed to load app settings", error);
                        }
                    };

                    const saveAppSettings = async (patch) => {
                        const body = {
                            locales_dir: appSettingsCache.configured_locales_dir || null,
                            ...patch
                        };
                        const response = await fetch("/api/app/settings", {
                            method: "POST",
                            headers: { "Content-Type": "application/json" },
                            body: JSON.stringify(body)
                        });
                        const payload = await response.json().catch(() => ({}));
                        if (!response.ok) {
                            throw new Error(payload.error || "settings update failed");
                        }
                        appSettingsCache = payload;
                        renderAppSettings();
                        return payload;
                    };

                    const pickFolder = async (title, currentDir) => {
                        const response = await fetch("/api/app/pick-folder", {
                            method: "POST",
                            headers: { "Content-Type": "application/json" },
                            body: JSON.stringify({ title, current_dir: currentDir || null })
                        });
                        const payload = await response.json().catch(() => ({}));
                        if (!response.ok) {
                            throw new Error(payload.error || "folder selection failed");
                        }
                        return payload.path || null;
                    };
                    const modelTestResults = new Map();

                    const renderModelTestResult = (alias) => {
                        const result = modelTestResults.get(alias);
                        if (!result) return null;

                        const wrap = document.createElement("div");
                        wrap.className = "model-test-result";
                        wrap.dataset.ok = result.ok ? "true" : "false";

                        const title = document.createElement("span");
                        title.className = "model-test-title";
                        title.textContent = t("settings.models.testResult");
                        wrap.append(title);

                        const capabilityLabels = {
                            refine: t("settings.models.capability.refine"),
                            translate: t("settings.models.capability.translate"),
                            extract: t("settings.models.capability.extract")
                        };

                        (result.results || []).forEach(item => {
                            const chip = document.createElement("span");
                            chip.className = "model-test-chip";
                            chip.dataset.ok = item.ok ? "true" : "false";
                            const label = capabilityLabels[item.capability] || item.capability;
                            chip.textContent = `${item.ok ? "OK" : "NG"} ${label}`;
                            chip.title = item.ok
                                ? (item.output || "")
                                : (item.error || t("settings.models.testFailed"));
                            wrap.append(chip);
                        });

                        return wrap;
                    };

                    const selectedPostProcessingModelAliases = () => {
                        const aliases = new Set();
                        const fixedSelects = [refineModel, translateModel, analyzeModel, completeModel];
                        const usesDefault = fixedSelects.some(select => select && !select.value);

                        fixedSelects.forEach(select => {
                            const alias = select?.value?.trim();
                            if (alias) aliases.add(alias);
                        });

                        customSteps.forEach(step => {
                            const alias = typeof step.model === "string" ? step.model.trim() : "";
                            if (alias) aliases.add(alias);
                        });

                        if (usesDefault) aliases.add(DEFAULT_CHAT_MODEL_ALIAS);
                        return aliases;
                    };

                    const isModelInUse = (model) => {
                        if (model.active) return true;
                        if (model.category === "speech") return false;
                        return selectedPostProcessingModelAliases().has(model.alias);
                    };

                    const renderModelCatalogEntry = (model, listEl, onAfterDelete) => {
                        const inUse = isModelInUse(model);
                        const row = document.createElement("article");
                        row.className = "model-row";
                        if (inUse) row.dataset.active = "true";
                        if (activeDownloadAlias === model.alias) row.dataset.downloading = "true";
                        if (model.compatible === false) {
                            row.dataset.incompatible = "true";
                            if (model.incompatibility_reason) {
                                row.title = t("settings.models.incompatibleTooltip", {
                                    reason: model.incompatibility_reason
                                });
                            }
                        }

                        const icon = document.createElement("span");
                        icon.className = "model-row-icon";
                        let iconChar = "↓";
                        let statusText = t("settings.models.notCached");
                        let statusVariant = "missing";
                        if (activeDownloadAlias === model.alias) {
                            iconChar = "⟳";
                            statusText = t("settings.models.downloading");
                            statusVariant = "downloading";
                            icon.classList.add("is-spinning");
                        } else if (inUse) {
                            iconChar = "●";
                            statusText = t("settings.models.inUse");
                            statusVariant = "active";
                        } else if (model.downloaded) {
                            iconChar = "✓";
                            statusText = t("settings.models.cached");
                            statusVariant = "cached";
                        }
                        icon.textContent = iconChar;

                        const body = document.createElement("div");
                        body.className = "model-row-body";
                        const nameRow = document.createElement("div");
                        nameRow.className = "model-row-name-row";
                        const name = document.createElement("code");
                        name.className = "model-row-name";
                        name.textContent = model.alias;
                        const badge = document.createElement("span");
                        badge.className = "model-row-status";
                        badge.dataset.variant = statusVariant;
                        badge.textContent = statusText;
                        nameRow.append(name, badge);

                        if (model.compatible === false) {
                            const incompat = document.createElement("span");
                            incompat.className = "model-row-status";
                            incompat.dataset.variant = "incompatible";
                            incompat.textContent = t("settings.models.incompatible");
                            if (model.incompatibility_reason) {
                                incompat.title = model.incompatibility_reason;
                            }
                            nameRow.append(incompat);
                        }

                        const desc = document.createElement("p");
                        desc.className = "model-row-desc";
                        // Backend returns an i18n key (e.g. "models.desc.whisper.tiny");
                        // resolve it so the description matches the active UI language.
                        const descKey = model.description || "";
                        const resolvedDesc = descKey ? t(descKey) : "";
                        desc.textContent = resolvedDesc && resolvedDesc !== descKey
                            ? resolvedDesc
                            : (descKey ? descKey : "");

                        body.append(nameRow, desc);
                        const testResult = renderModelTestResult(model.alias);
                        if (testResult) body.append(testResult);

                        const actions = document.createElement("div");
                        actions.className = "model-row-actions";

                        const isDownloading = activeDownloadAlias === model.alias;
                        const anyDownloading = !!activeDownloadAlias;

                        // Ask the user to confirm before we kick off a download
                        // for a model that has no runnable variant on this host.
                        // The download may still work if the SDK falls back to
                        // CPU, but the user should know what they're getting into.
                        const confirmIfIncompatible = () => {
                            if (model.compatible !== false) return true;
                            const reason = model.incompatibility_reason || "";
                            return window.confirm(
                                t("settings.models.incompatibleConfirm", { alias: model.alias, reason })
                            );
                        };

                        // Primary action: "Use" (speech) / "Download" (chat) — depending on state & category
                        if (model.category === "speech") {
                            if (!inUse) {
                                const useButton = document.createElement("button");
                                useButton.type = "button";
                                useButton.className = "text-button primary-inline model-row-use";
                                useButton.title = t("settings.models.useTooltip");
                                useButton.textContent = model.downloaded
                                    ? t("settings.models.use")
                                    : t("settings.models.useAndDownload");
                                useButton.disabled = anyDownloading;
                                useButton.addEventListener("click", async () => {
                                    if (!confirmIfIncompatible()) return;
                                    speechModel.value = model.alias;
                                    saveSpeechModelPreference(model.alias);
                                    showToast(t("settings.models.switchedSpeech", { alias: model.alias }), "success");
                                    allModelsCache.models = allModelsCache.models.map(m =>
                                        m.category === "speech" ? { ...m, active: m.alias === model.alias } : m
                                    );
                                    renderModelsCatalog();
                                    updateLiveModelDisplay();
                                    requestSpeechModelWarmup(model.alias);
                                });
                                actions.append(useButton);
                            }
                        } else {
                            // Chat models: download-only; the app picks the chat model internally.
                            if (!model.downloaded && !isDownloading) {
                                const downloadButton = document.createElement("button");
                                downloadButton.type = "button";
                                downloadButton.className = "text-button primary-inline";
                                downloadButton.textContent = t("settings.models.download");
                                downloadButton.disabled = anyDownloading;
                                downloadButton.addEventListener("click", () => {
                                    if (!confirmIfIncompatible()) return;
                                    startModelDownload(model.alias);
                                });
                                actions.append(downloadButton);
                            }
                        }

                        if (model.category !== "speech" && model.downloaded && !isDownloading) {
                            const testButton = document.createElement("button");
                            testButton.type = "button";
                            testButton.className = "text-button model-row-test";
                            testButton.textContent = t("settings.models.test");
                            testButton.title = t("settings.models.testTooltip");
                            testButton.addEventListener("click", async () => {
                                testButton.disabled = true;
                                testButton.textContent = t("settings.models.testing");
                                try {
                                    const response = await fetch("/api/models/test", {
                                        method: "POST",
                                        headers: { "Content-Type": "application/json" },
                                        body: JSON.stringify({ alias: model.alias })
                                    });
                                    const payload = await response.json().catch(() => ({}));
                                    if (!response.ok) {
                                        throw new Error(payload.error || t("settings.models.testFailed"));
                                    }
                                    modelTestResults.set(model.alias, payload);
                                    renderModelsCatalog();
                                    const capabilityLabels = {
                                        refine: t("settings.models.capability.refine"),
                                        translate: t("settings.models.capability.translate"),
                                        extract: t("settings.models.capability.extract")
                                    };
                                    const summary = (payload.results || [])
                                        .map(result => {
                                            const label = capabilityLabels[result.capability] || result.capability;
                                            return `${result.ok ? "OK" : "NG"} ${label}`;
                                        })
                                        .join(" / ");
                                    const message = t("settings.models.testOk", {
                                        alias: model.alias,
                                        elapsed: payload.elapsed_ms,
                                        summary,
                                        // Older copied locale files may still use {output}.
                                        // Keep both placeholders populated so the toast remains useful.
                                        output: summary
                                    });
                                    if (payload.ok) {
                                        showToast(message, "success", 5200);
                                    } else {
                                        showToast(message, "warning", 5200);
                                    }
                                } catch (error) {
                                    modelTestResults.set(model.alias, {
                                        ok: false,
                                        results: [
                                            {
                                                capability: "refine",
                                                ok: false,
                                                error: (error && error.message) || t("settings.models.testFailed")
                                            },
                                            {
                                                capability: "translate",
                                                ok: false,
                                                error: (error && error.message) || t("settings.models.testFailed")
                                            },
                                            {
                                                capability: "extract",
                                                ok: false,
                                                error: (error && error.message) || t("settings.models.testFailed")
                                            }
                                        ]
                                    });
                                    renderModelsCatalog();
                                    showErrorBanner((error && error.message) || t("settings.models.testFailed"));
                                } finally {
                                    testButton.disabled = false;
                                    testButton.textContent = t("settings.models.test");
                                }
                            });
                            actions.append(testButton);
                        }

                        // Secondary: delete. Only for downloaded models that are not active or mid-download.
                        if (model.downloaded && !inUse && !isDownloading) {
                            const deleteButton = document.createElement("button");
                            deleteButton.type = "button";
                            deleteButton.className = "icon-button model-row-delete";
                            deleteButton.title = t("model.tooltip.delete");
                            deleteButton.setAttribute("aria-label", t("model.action.delete"));
                            deleteButton.textContent = "🗑";
                            deleteButton.addEventListener("click", async () => {
                                if (!window.confirm(t("model.delete.confirm", { alias: model.alias }))) return;
                                try {
                                    const response = await fetch("/api/models/delete", {
                                        method: "POST",
                                        headers: { "Content-Type": "application/json" },
                                        body: JSON.stringify({ alias: model.alias })
                                    });
                                    if (!response.ok) {
                                        const err = await response.json().catch(() => ({}));
                                        throw new Error(err.error || t("model.delete.failed"));
                                    }
                                    const payload = await response.json();
                                    allModelsCache = payload;
                                    renderModelsCatalog();
                                    showToast(t("model.delete.success"), "success");
                                    // Keep the speech-side UI in sync too
                                    refreshSpeechModels();
                                    if (onAfterDelete) onAfterDelete();
                                } catch (error) {
                                    showErrorBanner((error && error.message) || t("model.delete.failed"));
                                }
                            });
                            actions.append(deleteButton);
                        }

                        row.append(icon, body, actions);
                        listEl.append(row);
                    };

                    // Kick off a model download. The SSE stream updates progress UI.
                    const startModelDownload = async (alias) => {
                        try {
                            const response = await fetch("/api/models/download", {
                                method: "POST",
                                headers: { "Content-Type": "application/json" },
                                body: JSON.stringify({ alias })
                            });
                            if (!response.ok) {
                                const err = await response.json().catch(() => ({}));
                                throw new Error(err.error || t("settings.models.downloadStartFailed"));
                            }
                            activeDownloadAlias = alias;
                            renderModelsCatalog();
                            showToast(t("settings.models.downloadStart", { alias }), "info");
                        } catch (error) {
                            showErrorBanner((error && error.message) || t("settings.models.downloadStartFailed"));
                        }
                    };

                    const promptDefaultChatModelDownload = () => {
                        if (defaultChatDownloadPrompted || activeDownloadAlias) return;
                        const downloadedChatModels = (allModelsCache.models || [])
                            .filter(model => model.category !== "speech" && model.downloaded);
                        if (downloadedChatModels.length > 0) return;

                        const defaultModel = (allModelsCache.models || [])
                            .find(model => model.alias === DEFAULT_CHAT_MODEL_ALIAS);
                        if (!defaultModel || defaultModel.category === "speech" || defaultModel.downloaded) return;

                        defaultChatDownloadPrompted = true;
                        showToast(t("settings.models.defaultChatDownloadToast", {
                            alias: DEFAULT_CHAT_MODEL_ALIAS
                        }), "warning", 5200);

                        if (window.confirm(t("settings.models.defaultChatDownloadPrompt", {
                            alias: DEFAULT_CHAT_MODEL_ALIAS
                        }))) {
                            startModelDownload(DEFAULT_CHAT_MODEL_ALIAS);
                        }
                    };

                    let latestRequirements = null;
                    let requirementsDismissed = false;

                    const missingRequiredModels = (requirements) =>
                        (requirements?.required_models || [])
                            .filter(model => !model.downloaded)
                            .map(model => model.alias);

                    const renderRequirementsBanner = (requirements) => {
                        if (!requirementsBanner) return;
                        latestRequirements = requirements;

                        if (!requirements || requirements.ok || requirementsDismissed) {
                            requirementsBanner.hidden = true;
                            return;
                        }

                        const missingModels = missingRequiredModels(requirements);
                        const runtimeReady = requirements.runtime_ready !== false;
                        const command = requirements.install_command || "winget install Microsoft.FoundryLocal";

                        if (!runtimeReady) {
                            requirementsBannerMessage.textContent = t("requirements.runtimeMissing");
                            requirementsCommand.textContent = command;
                            requirementsCommand.hidden = false;
                            requirementsPrimaryButton.textContent = t("requirements.copyCommand");
                            requirementsModelsButton.hidden = true;
                        } else {
                            requirementsBannerMessage.textContent = t("requirements.modelsMissing", {
                                models: missingModels.join(", ")
                            });
                            requirementsCommand.hidden = true;
                            requirementsPrimaryButton.textContent = t("requirements.prepareModels");
                            requirementsModelsButton.hidden = false;
                        }

                        requirementsBanner.hidden = false;
                    };

                    const refreshRequirements = async ({ silent = true } = {}) => {
                        try {
                            const response = await fetch("/api/app/requirements");
                            if (!response.ok) return null;
                            const payload = await response.json();
                            renderRequirementsBanner(payload);
                            return payload;
                        } catch (error) {
                            if (!silent) {
                                showErrorBanner((error && error.message) || "requirements check failed");
                            }
                            return null;
                        }
                    };

                    const prepareMissingRequiredModels = async (requirements = latestRequirements) => {
                        const missingModels = missingRequiredModels(requirements);
                        if (missingModels.length === 0) return;
                        if (!window.confirm(t("requirements.modelsPrompt", {
                            models: missingModels.join(", ")
                        }))) {
                            return;
                        }
                        await startModelDownload(missingModels[0]);
                        openModelsSettings();
                    };

                    // Populate a <select> with the downloaded LLMs so that each post-processing
                    // step can pick its own model. An empty-value option means "use the app default".
                    const populatePostModelSelect = (selectEl) => {
                        if (!selectEl) return;
                        const previous = selectEl.value;
                        selectEl.replaceChildren();
                        const defaultOpt = document.createElement("option");
                        defaultOpt.value = "";
                        defaultOpt.textContent = t("settings.models.useDefault");
                        selectEl.append(defaultOpt);

                        const chatModels = (allModelsCache.models || [])
                            .filter(m => m.category !== "speech");
                        chatModels.forEach(m => {
                            const opt = document.createElement("option");
                            opt.value = m.alias;
                            // Show a visual hint if the model is not yet
                            // downloaded or if it isn't compatible with this host.
                            const suffix = [];
                            if (!m.downloaded) suffix.push("↓");
                            if (m.compatible === false) suffix.push("⚠");
                            opt.textContent = suffix.length ? `${m.alias} · ${suffix.join(" ")}` : m.alias;
                            if (m.compatible === false) {
                                opt.dataset.incompatible = "true";
                                if (m.incompatibility_reason) {
                                    opt.title = m.incompatibility_reason;
                                }
                            }
                            selectEl.append(opt);
                        });
                        // Preserve the previously chosen alias if still available
                        if (previous && chatModels.some(m => m.alias === previous)) {
                            selectEl.value = previous;
                        } else {
                            selectEl.value = "";
                        }
                    };

                    const repopulateAllPostModelSelects = () => {
                        populatePostModelSelect(refineModel);
                        populatePostModelSelect(translateModel);
                        populatePostModelSelect(analyzeModel);
                        populatePostModelSelect(completeModel);
                        populatePostModelSelect(customStepModel);
                    };

                    const renderModelsCatalog = () => {
                        renderAppSettings();

                        if (modelsEpList) {
                            modelsEpList.replaceChildren();
                            const eps = allModelsCache.execution_providers || [];
                            if (eps.length === 0) {
                                modelsEpList.textContent = "—";
                            } else {
                                eps.forEach(ep => {
                                    const chip = document.createElement("span");
                                    chip.className = "models-ep-chip";
                                    chip.dataset.registered = ep.registered ? "true" : "false";
                                    chip.textContent = ep.name;
                                    chip.title = ep.registered
                                        ? t("settings.models.ep.registered")
                                        : t("settings.models.ep.notRegistered");
                                    modelsEpList.append(chip);
                                });
                            }
                        }

                        const models = allModelsCache.models || [];
                        const speech = models.filter(m => m.category === "speech");
                        const chat = models.filter(m => m.category !== "speech");

                        modelsSpeechList.replaceChildren();
                        modelsChatList.replaceChildren();

                        if (speech.length === 0) {
                            const empty = document.createElement("p");
                            empty.className = "models-empty";
                            empty.textContent = t("settings.models.empty");
                            modelsSpeechList.append(empty);
                        } else {
                            speech.forEach(m => renderModelCatalogEntry(m, modelsSpeechList));
                        }

                        if (chat.length === 0) {
                            const empty = document.createElement("p");
                            empty.className = "models-empty";
                            empty.textContent = t("settings.models.empty");
                            modelsChatList.append(empty);
                        } else {
                            chat.forEach(m => renderModelCatalogEntry(m, modelsChatList));
                        }

                        const downloaded = models.filter(m => m.downloaded).length;
                        modelsDownloadedCount.textContent = `${downloaded} / ${models.length}`;
                    };

                    const refreshAllModels = async () => {
                        try {
                            const response = await fetch("/api/models");
                            if (!response.ok) return;
                            allModelsCache = await response.json();
                            repopulateAllPostModelSelects();
                            populateLiveTranslationModelSelect();
                            // Re-apply any saved per-step model preferences now that options exist
                            restorePostModelPreferences();
                            renderModelsCatalog();
                            populateLiveTranslationModelSelect();
                            updateLiveModelDisplay();
                            promptDefaultChatModelDownload();
                            refreshRequirements();
                        } catch (error) {
                            console.warn("Failed to fetch model catalog", error);
                        }
                    };

                    modelsRefreshButton.addEventListener("click", refreshAllModels);
                    const handlePostModelChange = () => {
                        savePostModelPrefs();
                        renderModelsCatalog();
                        updateLiveModelDisplay();
                    };
                    refineModel?.addEventListener("change", handlePostModelChange);
                    translateModel?.addEventListener("change", handlePostModelChange);
                    analyzeModel?.addEventListener("change", handlePostModelChange);
                    completeModel?.addEventListener("change", handlePostModelChange);
                    const refreshSpeechModels = async () => {
                        if (speechModelStates.length === 0) {
                            modelListLoading = true;
                            renderModelList();
                        }
                        try {
                            const response = await fetch("/api/models/speech");
                            if (!response.ok) return;
                            const payload = await response.json();
                            speechModelStates = payload.models || [];

                            // Ensure current selection still exists. On a fresh or empty
                            // model cache, prefer the lightweight default even if a larger
                            // alias is still stored from a previous cache location.
                            if (speechModelStates.length > 0) {
                                const selected = speechModelStates.find(m => m.alias === speechModel.value);
                                const defaultModel = speechModelStates.find(m => m.alias === DEFAULT_SPEECH_MODEL_ALIAS);
                                const anyDownloaded = speechModelStates.some(m => m.downloaded);

                                if (!anyDownloaded && !activeDownloadAlias && defaultModel) {
                                    speechModel.value = DEFAULT_SPEECH_MODEL_ALIAS;
                                } else if (!selected) {
                                    speechModel.value = defaultModel?.alias || speechModelStates[0].alias;
                                }
                                saveSpeechModelPreference(speechModel.value);
                            }
                        } catch (error) {
                            console.warn("Failed to fetch model status", error);
                        } finally {
                            modelListLoading = false;
                            renderModelList();
                        }
                    };

                    const requestSpeechModelWarmup = async (alias = speechModel.value) => {
                        const requested = (alias || "").trim();
                        if (!requested) return;

                        setSpeechWarmupRunning(true);
                        try {
                            const response = await fetch("/api/models/speech/warmup", {
                                method: "POST",
                                headers: { "Content-Type": "application/json" },
                                body: JSON.stringify({ alias: requested })
                            });
                            if (!response.ok) {
                                const payload = await response.json().catch(() => ({}));
                                throw new Error(payload.error || "speech model warmup failed");
                            }
                        } catch (error) {
                            console.warn("Speech model warmup failed", error);
                        } finally {
                            await refreshSpeechModels();
                            if (!settingsModal.hidden) await refreshAllModels();
                            setSpeechWarmupRunning(false);
                        }
                    };

                    let isDownloading = false;
                    let isSpeechWarmupRunning = false;
                    // Aliases the user has "cancelled" in the UI. The native core
                    // keeps downloading in the background (the SDK does not expose
                    // cancellation), but we suppress the overlay and failure banner
                    // so the user perceives cancellation. If the download later
                    // completes or fails for this alias, the event is ignored.
                    const uiCancelledAliases = new Set();
                    const setDownloading = (downloading) => {
                        isDownloading = downloading;
                        downloadOverlay.hidden = !downloading;
                        const locked = downloading || isSpeechWarmupRunning;
                        recordButton.disabled = locked;
                        openSettingsButton.disabled = locked;
                        if (typeof setCaptionEditable === "function") {
                            setCaptionEditable(!locked && !mediaRecorder);
                        }
                    };

                    const setSpeechWarmupRunning = (running) => {
                        isSpeechWarmupRunning = running;
                        setDownloading(isDownloading);
                    };

                    const showDownloadDiagnostic = async (alias, message) => {
                        if (!downloadDiagnostic) return;
                        downloadDiagnosticAlias.textContent = alias || "";
                        downloadDiagnosticError.textContent = message || "";
                        downloadDiagnosticVariants.replaceChildren();
                        downloadDiagnostic.hidden = false;
                        if (!alias) return;
                        try {
                            const resp = await fetch(`/api/models/variants?alias=${encodeURIComponent(alias)}`);
                            if (!resp.ok) return;
                            const payload = await resp.json();
                            (payload.variants || []).forEach(v => {
                                const row = document.createElement("div");
                                row.className = "download-diagnostic-variant";
                                const left = document.createElement("div");
                                const id = document.createElement("code");
                                id.textContent = v.id;
                                const meta = document.createElement("span");
                                const parts = [v.device_type];
                                if (v.execution_provider) parts.push(v.execution_provider);
                                if (v.file_size_mb != null) parts.push(`${v.file_size_mb} MB`);
                                if (v.cached) parts.push("✓");
                                meta.textContent = parts.join(" · ");
                                meta.className = "download-diagnostic-variant-meta";
                                left.append(id, document.createElement("br"), meta);
                                row.append(left);
                                downloadDiagnosticVariants.append(row);
                            });
                        } catch (error) {
                            console.warn("variant lookup failed", error);
                        }
                    };

                    if (downloadDiagnosticClose) {
                        downloadDiagnosticClose.addEventListener("click", () => {
                            downloadDiagnostic.hidden = true;
                        });
                    }

                    if (downloadCancelButton) {
                        downloadCancelButton.addEventListener("click", () => {
                            if (activeDownloadAlias) {
                                uiCancelledAliases.add(activeDownloadAlias);
                            }
                            activeDownloadAlias = null;
                            setDownloading(false);
                            setStatus(t("status.idle"), "idle");
                            setRecordInfo(t("download.cancelledHint"));
                            renderModelList();
                            if (!settingsModal.hidden) refreshAllModels();
                        });
                    }

                    const handleDownloadEvent = (event) => {
                        const isCancelled = event.alias && uiCancelledAliases.has(event.alias);
                        switch (event.type) {
                            case "started":
                                if (isCancelled) return; // user already dismissed it
                                activeDownloadAlias = event.alias || null;
                                setDownloading(true);
                                downloadTitle.textContent = t("download.title");
                                downloadMessage.textContent = t("download.message");
                                progressAlias.textContent = event.alias || "";
                                progressFill.style.width = "0%";
                                progressPercent.textContent = "0%";
                                setStatus(t("status.downloading"), "busy");
                                renderModelList();
                                break;
                            case "progress": {
                                if (isCancelled) return;
                                const percent = Math.max(0, Math.min(100, event.percent ?? 0));
                                progressFill.style.width = `${percent}%`;
                                progressPercent.textContent = `${percent.toFixed(1)}%`;
                                progressAlias.textContent = event.alias || "";
                                if (event.alias) activeDownloadAlias = event.alias;
                                if (!isDownloading) setDownloading(true);
                                break;
                            }
                            case "completed":
                                progressFill.style.width = "100%";
                                progressPercent.textContent = "100%";
                                activeDownloadAlias = null;
                                renderModelsCatalog();
                                setTimeout(() => setDownloading(false), 300);
                                if (!isCancelled) {
                                    setStatus(t("status.downloadDone"), "ready");
                                }
                                // Even if the user cancelled in the UI, the file is
                                // now cached — clear the flag so next start works.
                                if (event.alias) uiCancelledAliases.delete(event.alias);
                                refreshSpeechModels();
                                refreshAllModels();
                                refreshRequirements();
                                break;
                            case "failed": {
                                const alias = event.alias;
                                activeDownloadAlias = null;
                                renderModelsCatalog();
                                setDownloading(false);
                                if (isCancelled) {
                                    uiCancelledAliases.delete(alias);
                                    refreshSpeechModels();
                                    refreshAllModels();
                                    refreshRequirements();
                                    break;
                                }
                                setStatus(t("status.downloadFailed"), "error");
                                setRecordInfo(event.message || t("status.downloadFailed"));
                                showDownloadDiagnostic(alias, event.message);
                                refreshSpeechModels();
                                refreshAllModels();
                                refreshRequirements();
                                break;
                            }
                            case "idle":
                            default:
                                activeDownloadAlias = null;
                                setDownloading(false);
                                break;
                        }
                    };

                    const connectDownloadEvents = () => {
                        try {
                            const source = new EventSource("/api/download/events");
                            source.onmessage = (messageEvent) => {
                                try {
                                    const data = JSON.parse(messageEvent.data);
                                    handleDownloadEvent(data);
                                } catch (error) {
                                    // keep-alive or malformed message — ignore
                                }
                            };
                            source.onerror = () => {
                                source.close();
                                setTimeout(connectDownloadEvents, 3000);
                            };
                        } catch (error) {
                            console.error("SSE connection failed", error);
                        }
                    };
                    connectDownloadEvents();

                    // Text displayed/edited in the main caption area
                    let textIsDirty = false;

                    const updateDirty = (dirty) => {
                        textIsDirty = dirty;
                        captionDirty.hidden = !dirty;
                    };

                    const syncCaptionButtons = () => {
                        const hasText = !!captionContent.value.trim();
                        copyMainButton.disabled = !hasText;
                        clearMainButton.disabled = !hasText;
                        saveToHistoryButton.disabled = !hasText;
                    };

                    // Simple filler detector (Japanese + English common fillers).
                    // Case-insensitive; whole-"word" matches where applicable.
                    const FILLER_PATTERNS = [
                        /あー/g, /えー/g, /えっと/g, /そのー?/g,
                        /まぁ/g, /まあ/g, /うーん/g, /んー/g, /なんか/g,
                        /\b(um|uh|er|ah|you know|like|i mean)\b/gi,
                    ];

                    let lastTranscriptionDuration = 0;

                    const countFillers = (text) => {
                        let count = 0;
                        for (const pattern of FILLER_PATTERNS) {
                            const matches = text.match(pattern);
                            if (matches) count += matches.length;
                        }
                        return count;
                    };

                    const updateCaptionStats = () => {
                        const text = captionContent.value;
                        const trimmed = text.trim();
                        if (!trimmed) {
                            captionStats.hidden = true;
                            return;
                        }
                        captionStats.hidden = false;
                        const chars = [...trimmed].length; // count by code points (handles emoji)
                        const fillers = countFillers(text);
                        statChars.textContent = chars.toLocaleString(currentLocale);
                        statFillers.textContent = fillers.toLocaleString(currentLocale);

                        if (lastTranscriptionDuration > 0) {
                            statDurationWrap.hidden = false;
                            statDuration.textContent = `${lastTranscriptionDuration.toFixed(1)}s`;
                            const cpm = (chars / (lastTranscriptionDuration / 60));
                            statSpeedWrap.hidden = false;
                            statSpeed.textContent = `${Math.round(cpm).toLocaleString(currentLocale)} cpm`;
                        } else {
                            statDurationWrap.hidden = true;
                            statSpeedWrap.hidden = true;
                        }
                    };

                    // Called programmatically when a new transcription/refinement lands.
                    // Marks the editor as clean (freshly-synced content).
                    const updateMainDisplay = (text) => {
                        currentText = text;
                        captionContent.value = text || "";
                        updateDirty(false);
                        syncCaptionButtons();
                        updateCaptionStats();
                    };

                    // User-triggered edits: keep `currentText` in sync and mark dirty.
                    captionContent.addEventListener("input", () => {
                        currentText = captionContent.value;
                        updateDirty(true);
                        syncCaptionButtons();
                        updateCaptionStats();
                    });

                    const setCaptionEditable = (editable) => {
                        captionContent.readOnly = !editable;
                        captionContent.dataset.readonly = String(!editable);
                    };

                    const setBusy = (busy) => {
                        recordButton.disabled = (busy && !mediaRecorder) || isDownloading || isSpeechWarmupRunning;
                        openSettingsButton.disabled = busy || isDownloading || isSpeechWarmupRunning;
                        setCaptionEditable(!busy && !mediaRecorder && !isDownloading && !isSpeechWarmupRunning);
                    };

                    // Post-processing configuration is driven by the quick toggles next to the
                    // record button. The settings panel only keeps per-step preferences
                    // (translation languages, custom terms, etc.) — enablement is now a
                    // single-click decision at the point of recording.
                    //
                    // Refinement is strictly meaning-preserving now: only transcription
                    // artifacts (fillers, misrecognitions, punctuation) are corrected.
                    // Stylistic rewrites belong in custom post-processing steps.
                    const getPostProcessingConfig = () => {
                        return {
                            refinement: {
                                enabled: !!quickAutoRefine?.checked,
                                removeFillers: removeFillers.checked,
                                voiceCommands: voiceCommands.checked,
                                customTerms: customTerms.value,
                                customInstruction: customInstruction.value,
                                model: refineModel?.value || ""
                            },
                            translation: {
                                enabled: !!quickAutoTranslate?.checked,
                                sourceLanguage: sourceLanguage.value,
                                targetLanguage: targetLanguage.value,
                                customInstruction: translationInstruction.value,
                                model: translateModel?.value || ""
                            }
                        };
                    };


                    // Persist refinement / translation knobs automatically. Every change to a
                    // field inside the post-process panel writes to this key, and we hydrate it
                    // once on startup. The quick toggles have their own key managed elsewhere.
                    const POSTPROCESS_KEY = 'seedraft_postprocess_preset';

                    const savePostprocessNow = () => {
                        try {
                            localStorage.setItem(POSTPROCESS_KEY, JSON.stringify(getPostProcessingConfig()));
                        } catch (_) { /* ignore quota */ }
                    };

                    // Debounce autosave to smooth out rapid typing in text fields.
                    let postprocessSaveTimer = null;
                    const autoSavePostprocess = () => {
                        if (postprocessSaveTimer) clearTimeout(postprocessSaveTimer);
                        postprocessSaveTimer = setTimeout(savePostprocessNow, 250);
                    };

                    const hydratePostprocessFromStorage = () => {
                        const saved = localStorage.getItem(POSTPROCESS_KEY);
                        if (!saved) return;
                        try {
                            const config = JSON.parse(saved);
                            removeFillers.checked = config.refinement?.removeFillers ?? true;
                            voiceCommands.checked = config.refinement?.voiceCommands ?? true;
                            customTerms.value = config.refinement?.customTerms || '';
                            customInstruction.value = config.refinement?.customInstruction || '';
                            sourceLanguage.value = config.translation?.sourceLanguage || 'auto';
                            targetLanguage.value = config.translation?.targetLanguage || 'ja';
                            translationInstruction.value = config.translation?.customInstruction || '';
                        } catch (_) {
                            // Corrupt preset — leave the defaults alone.
                        }
                    };

                    // All inputs that should trigger an autosave when the user changes them.
                    // `linkedContextToggle` has its own dedicated storage key — it's not
                    // part of the post-process preset so we don't include it here.
                    const POSTPROCESS_AUTOSAVE_INPUTS = [
                        refineModel, translateModel, analyzeModel,
                        removeFillers, voiceCommands,
                        customTerms, customInstruction,
                        sourceLanguage, targetLanguage, translationInstruction,
                    ].filter(Boolean);
                    POSTPROCESS_AUTOSAVE_INPUTS.forEach(el => {
                        el.addEventListener("change", autoSavePostprocess);
                        if (el.tagName === "INPUT" || el.tagName === "TEXTAREA") {
                            el.addEventListener("input", autoSavePostprocess);
                        }
                    });
                    hydratePostprocessFromStorage();

                    // Also autosave the output-tab fields (save folder is chosen via picker and
                    // handled elsewhere; filename prefix autosaves here).
                    const OUTPUT_PREFIX_KEY = 'seedraft_output_prefix';
                    try {
                        const savedPrefix = localStorage.getItem(OUTPUT_PREFIX_KEY);
                        if (savedPrefix != null && filePrefix) filePrefix.value = savedPrefix;
                    } catch (_) {}
                    filePrefix?.addEventListener("input", () => {
                        try { localStorage.setItem(OUTPUT_PREFIX_KEY, filePrefix.value); } catch (_) {}
                    });

                    // Reset everything back to defaults. Preserves user data (projects, notes,
                    // drafts, live sessions, tags, links). Only clears UI preferences.
                    const resetSettings = async () => {
                        if (!window.confirm(t("settings.resetConfirm"))) return;
                        const UI_KEYS = [
                            POSTPROCESS_KEY,
                            OUTPUT_PREFIX_KEY,
                            "seedraft_quick_toggles",
                            LIVE_PREFS_KEY,
                            MAX_RECORDING_SECONDS_KEY,
                            MICROPHONE_DEVICE_KEY,
                            "seedraft_post_models",
                            "seedraft_speech_model",
                            "seedraft_custom_steps",
                            "seedraft_ui_theme",
                            "seedraft_ui_locale",
                        ];
                        try {
                            for (const key of UI_KEYS) localStorage.removeItem(key);
                        } catch (_) {}
                        try {
                            await saveAppSettings({ locales_dir: null });
                        } catch (error) {
                            console.warn("failed to reset app settings", error);
                        }
                        showToast(t("settings.resetDone"), "success");
                        // Easiest way to re-apply every default: reload the page.
                        // Tauri WebView supports location.reload().
                        setTimeout(() => location.reload(), 400);
                    };

                    // ========== Projects, notes, translations (backed by SQLite) ==========
                    let projects = [];
                    let currentProjectId = null;
                    let currentNotes = [];
                    let editingNoteId = null;
                    let editingProjectId = null;
                    let lastTranscribedNoteId = null;

                    const saveCurrentProject = (id) => {
                        try {
                            if (id) localStorage.setItem("seedraft_project_id", id);
                        } catch {
                            // ignore
                        }
                    };

                    const loadSavedProject = () => {
                        try {
                            return localStorage.getItem("seedraft_project_id");
                        } catch {
                            return null;
                        }
                    };

                    const renderProjectSelect = () => {
                        projectSelect.replaceChildren();
                        projects.forEach((project) => {
                            const option = document.createElement("option");
                            option.value = project.id;
                            option.textContent = project.name;
                            if (project.id === currentProjectId) option.selected = true;
                            projectSelect.append(option);
                        });
                        const divider = document.createElement("option");
                        divider.disabled = true;
                        divider.textContent = "────────";
                        projectSelect.append(divider);
                        const editOption = document.createElement("option");
                        editOption.value = "__edit__";
                        editOption.textContent = t("project.delete") + " / " + t("note.edit");
                        projectSelect.append(editOption);
                    };

                    const refreshProjects = async () => {
                        try {
                            const response = await fetch("/api/projects");
                            if (!response.ok) return;
                            const payload = await response.json();
                            projects = payload.projects || [];
                            if (projects.length > 0 && !projects.find(p => p.id === currentProjectId)) {
                                const saved = loadSavedProject();
                                const match = saved && projects.find(p => p.id === saved);
                                currentProjectId = match ? match.id : projects[0].id;
                                saveCurrentProject(currentProjectId);
                            }
                            renderProjectSelect();
                        } catch (error) {
                            console.warn("Failed to fetch projects", error);
                        }
                    };

                    const selectedProject = () => projects.find(p => p.id === currentProjectId);

                    projectSelect.addEventListener("change", async () => {
                        const value = projectSelect.value;
                        if (value === "__edit__") {
                            openProjectEditor(selectedProject());
                            projectSelect.value = currentProjectId;
                            return;
                        }
                        currentProjectId = value;
                        saveCurrentProject(currentProjectId);
                        await refreshHistoryView();
                        await renderGraph();
                    });

                    const openProjectEditor = (project) => {
                        editingProjectId = project ? project.id : null;
                        projectEditorName.value = project ? project.name : "";
                        projectEditorDescription.value = project ? (project.description || "") : "";
                        projectEditorDelete.hidden = !project || projects.length <= 1;
                        projectEditorModal.hidden = false;
                    };

                    const closeProjectEditor = () => {
                        projectEditorModal.hidden = true;
                        editingProjectId = null;
                    };

                    newProjectButton.addEventListener("click", () => openProjectEditor(null));
                    projectEditorClose.addEventListener("click", closeProjectEditor);
                    projectEditorModal.addEventListener("click", (event) => {
                        if (event.target === projectEditorModal) closeProjectEditor();
                    });

                    projectEditorSave.addEventListener("click", async () => {
                        const name = projectEditorName.value.trim();
                        if (!name) return;
                        const description = projectEditorDescription.value.trim() || null;
                        try {
                            if (editingProjectId) {
                                await fetch(`/api/projects/${editingProjectId}`, {
                                    method: "PUT",
                                    headers: { "Content-Type": "application/json" },
                                    body: JSON.stringify({ name, description })
                                });
                            } else {
                                const response = await fetch("/api/projects", {
                                    method: "POST",
                                    headers: { "Content-Type": "application/json" },
                                    body: JSON.stringify({ name, description })
                                });
                                const project = await response.json();
                                currentProjectId = project.id;
                                saveCurrentProject(currentProjectId);
                            }
                            await refreshProjects();
                            await refreshHistoryView();
                            await renderGraph();
                            closeProjectEditor();
                        } catch (error) {
                            console.warn("project save failed", error);
                        }
                    });

                    projectEditorDelete.addEventListener("click", async () => {
                        if (!editingProjectId) return;
                        const project = projects.find(p => p.id === editingProjectId);
                        const confirmMessage = t("project.deleteConfirm", { name: project?.name || "" });
                        if (!window.confirm(confirmMessage)) return;
                        try {
                            await fetch(`/api/projects/${editingProjectId}`, { method: "DELETE" });
                            if (editingProjectId === currentProjectId) currentProjectId = null;
                            await refreshProjects();
                            await refreshHistoryView();
                            await renderGraph();
                            closeProjectEditor();
                        } catch (error) {
                            console.warn("project delete failed", error);
                        }
                    });

                    const fetchNotes = async () => {
                        if (!currentProjectId) return [];
                        try {
                            const response = await fetch(`/api/notes?project_id=${encodeURIComponent(currentProjectId)}`);
                            if (!response.ok) return [];
                            const payload = await response.json();
                            return payload.notes || [];
                        } catch (error) {
                            console.warn("fetch notes failed", error);
                            return [];
                        }
                    };

                    const formatDate = (ms) => {
                        if (!ms) return "";
                        const date = new Date(ms);
                        return date.toLocaleString(currentLocale);
                    };

                    // Compact date formatter used in the high-density note list.
                    // Returns a very short string — "5m", "2h", "3d", or a
                    // month/day like "Mar 12" — so the right edge of each row
                    // stays narrow and the title gets the remaining width.
                    const formatRelativeDate = (ms) => {
                        if (!ms) return "";
                        const diffMs = Date.now() - ms;
                        const diffMin = Math.floor(diffMs / 60000);
                        if (diffMin < 1) return "now";
                        if (diffMin < 60) return `${diffMin}m`;
                        const diffHr = Math.floor(diffMin / 60);
                        if (diffHr < 24) return `${diffHr}h`;
                        const diffDay = Math.floor(diffHr / 24);
                        if (diffDay < 7) return `${diffDay}d`;
                        // Older than a week: fall back to a compact locale date.
                        try {
                            return new Date(ms).toLocaleDateString(currentLocale, {
                                month: "short",
                                day: "numeric"
                            });
                        } catch {
                            return new Date(ms).toLocaleDateString();
                        }
                    };

                    let highlightedNoteId = null;
                    let highlightedGraphNodeId = null;
                    // Multi-select for draft composition
                    let noteSelectionMode = false;
                    const selectedNoteIds = new Set();
                    const selectedGraphParentNoteId = () => (
                        highlightedNoteId && currentNotes.some(note => note.id === highlightedNoteId)
                            ? highlightedNoteId
                            : null
                    );

                    // ---- Note reorder / reparent (drag & drop) ----
                    // Drop zones per card:
                    //   above  (top 28%)    → reorder as sibling just before target
                    //   inside (middle 44%) → set target as parent (append as last child)
                    //   below  (bottom 28%) → reorder as sibling just after target's subtree
                    // Separate drop band at the top of the list moves the note to top-level.
                    let draggedNoteId = null;

                    const clearDragDecorations = () => {
                        notesGrid.querySelectorAll(".note-card").forEach(el => {
                            el.classList.remove("is-dragging");
                            delete el.dataset.dropZone;
                        });
                        const rootBand = notesGrid.querySelector(".notes-drop-root");
                        if (rootBand) delete rootBand.dataset.dropZone;
                    };

                    const zoneFromEvent = (event, rect) => {
                        const offsetY = event.clientY - rect.top;
                        const ratio = offsetY / rect.height;
                        if (ratio < 0.28) return "above";
                        if (ratio > 0.72) return "below";
                        return "inside";
                    };

                    const handleNoteDragStart = (event, noteId) => {
                        draggedNoteId = noteId;
                        event.dataTransfer.effectAllowed = "move";
                        try {
                            // Firefox requires some data to start the drag
                            event.dataTransfer.setData("text/plain", noteId);
                        } catch (_) {}
                        event.currentTarget?.classList?.add("is-dragging");
                    };

                    const handleNoteDragOver = (event, targetCard, targetNoteId) => {
                        if (!draggedNoteId || draggedNoteId === targetNoteId) return;
                        // Prevent dropping onto own descendant
                        const descendants = collectDescendantIds(draggedNoteId);
                        if (descendants.has(targetNoteId)) return;
                        event.preventDefault();
                        event.stopPropagation();
                        event.dataTransfer.dropEffect = "move";
                        const rect = targetCard.getBoundingClientRect();
                        targetCard.dataset.dropZone = zoneFromEvent(event, rect);
                    };

                    // Build a flat DFS order of note ids from a parent map.
                    // `parentMap`: Map<parentId|null, id[]> — children in order.
                    const dfsOrder = (parentMap) => {
                        const out = [];
                        const walk = (pid) => {
                            const kids = parentMap.get(pid) || [];
                            for (const id of kids) {
                                out.push(id);
                                walk(id);
                            }
                        };
                        walk(null);
                        return out;
                    };

                    // Collect a note's own id plus all descendant ids from a parentMap.
                    const subtreeIds = (parentMap, rootId) => {
                        const out = [];
                        const walk = (id) => {
                            out.push(id);
                            const kids = parentMap.get(id) || [];
                            for (const k of kids) walk(k);
                        };
                        walk(rootId);
                        return out;
                    };

                    const handleNoteDrop = async (event, targetNoteId, dropZoneOverride) => {
                        event.preventDefault();
                        event.stopPropagation();
                        const sourceId = draggedNoteId;
                        draggedNoteId = null;
                        const zoneRaw = dropZoneOverride
                            || event.currentTarget?.dataset?.dropZone
                            || null;
                        clearDragDecorations();
                        if (!sourceId || !currentProjectId) return;
                        if (targetNoteId && sourceId === targetNoteId) return;

                        const source = currentNotes.find(n => n.id === sourceId);
                        if (!source) return;

                        // Build current parent->children map (children already ordered by position)
                        const parentMap = new Map();
                        currentNotes.forEach(n => {
                            const pid = n.parent_id || null;
                            if (!parentMap.has(pid)) parentMap.set(pid, []);
                            parentMap.get(pid).push(n.id);
                        });

                        // Disallow moving into own subtree
                        const movingIds = new Set(subtreeIds(parentMap, sourceId));
                        if (targetNoteId && movingIds.has(targetNoteId)) return;

                        // Decide new parent and insertion index
                        let newParentId;   // string | null
                        let insertAfterId; // id of the note whose subtree the source should follow, or null = prepend
                        const zone = zoneRaw === "root" ? "root" : (zoneRaw || "below");

                        if (zone === "root") {
                            // Move to top-level, prepend before all roots
                            newParentId = null;
                            insertAfterId = null;
                        } else {
                            const target = currentNotes.find(n => n.id === targetNoteId);
                            if (!target) return;
                            if (zone === "inside") {
                                newParentId = target.id;
                                // append as last child of target
                                const kids = parentMap.get(target.id) || [];
                                insertAfterId = kids.length ? kids[kids.length - 1] : target.id;
                            } else if (zone === "above") {
                                newParentId = target.parent_id || null;
                                // insert just before target: after target's previous sibling (or null if first)
                                const siblings = parentMap.get(newParentId) || [];
                                const idx = siblings.indexOf(target.id);
                                insertAfterId = idx > 0 ? siblings[idx - 1] : null;
                            } else {
                                // "below"
                                newParentId = target.parent_id || null;
                                insertAfterId = target.id;
                            }
                        }

                        // If inserting below a node, we need to land after that node's entire subtree.
                        // We rebuild the parent map without the moving subtree first, then splice it back in.
                        const remainingParentMap = new Map();
                        currentNotes.forEach(n => {
                            if (movingIds.has(n.id)) return;
                            const pid = n.parent_id || null;
                            // Skip the source's own entry in its old parent list
                            if (n.id === sourceId) return;
                            if (!remainingParentMap.has(pid)) remainingParentMap.set(pid, []);
                            remainingParentMap.get(pid).push(n.id);
                        });
                        // Also remove source from its old parent's list (if still referenced — it's
                        // filtered above but guard against edge cases)
                        for (const [pid, list] of remainingParentMap) {
                            remainingParentMap.set(pid, list.filter(id => id !== sourceId));
                        }

                        // Splice source into its new parent's children at the right spot
                        const newSiblings = (remainingParentMap.get(newParentId) || []).slice();
                        if (insertAfterId === null) {
                            newSiblings.unshift(sourceId);
                        } else {
                            const at = newSiblings.indexOf(insertAfterId);
                            if (at < 0) newSiblings.push(sourceId);
                            else newSiblings.splice(at + 1, 0, sourceId);
                        }
                        remainingParentMap.set(newParentId, newSiblings);

                        // Build a merged parent map: the moving subtree keeps its internal structure,
                        // but source's parent is now newParentId.
                        const mergedParentMap = new Map(remainingParentMap);
                        currentNotes.forEach(n => {
                            if (!movingIds.has(n.id)) return;
                            if (n.id === sourceId) return; // already placed under newParentId
                            const pid = n.parent_id || null;
                            if (!mergedParentMap.has(pid)) mergedParentMap.set(pid, []);
                            // Preserve original child order for inner subtree
                            if (!mergedParentMap.get(pid).includes(n.id)) {
                                mergedParentMap.get(pid).push(n.id);
                            }
                        });

                        const newOrder = dfsOrder(mergedParentMap);

                        // Capture the old parent BEFORE the optimistic mutation below, since
                        // `source` and `moved` reference the same live object in `currentNotes`.
                        const oldParent = source.parent_id || null;

                        // Optimistic UI: update local notes' parent_id and re-order
                        const byId = new Map(currentNotes.map(n => [n.id, n]));
                        const moved = byId.get(sourceId);
                        if (moved) moved.parent_id = newParentId;
                        currentNotes = newOrder.map(id => byId.get(id)).filter(Boolean);
                        renderNotes();

                        try {
                            // Persist parent change if needed
                            if (oldParent !== newParentId) {
                                const resp = await fetch(`/api/notes/${sourceId}/parent`, {
                                    method: "POST",
                                    headers: { "Content-Type": "application/json" },
                                    body: JSON.stringify({ parent_id: newParentId })
                                });
                                if (!resp.ok) {
                                    const err = await resp.json().catch(() => ({}));
                                    throw new Error(err.error || t("note.parentSetFailed"));
                                }
                            }
                            const resp2 = await fetch("/api/notes/reorder", {
                                method: "POST",
                                headers: { "Content-Type": "application/json" },
                                body: JSON.stringify({ project_id: currentProjectId, note_ids: newOrder })
                            });
                            if (!resp2.ok) {
                                const err = await resp2.json().catch(() => ({}));
                                throw new Error(err.error || "reorder failed");
                            }
                            graphLayoutCache = null;
                            await renderGraph();
                        } catch (error) {
                            console.warn("note move failed, reverting", error);
                            showErrorBanner((error && error.message) || "move failed");
                            await refreshHistoryView();
                        }
                    };

                    const handleNoteDragEnd = () => {
                        draggedNoteId = null;
                        clearDragDecorations();
                    };

                    const updateSelectionBar = () => {
                        notesSelectionBar.hidden = !noteSelectionMode;
                        notesSelectionCount.textContent = String(selectedNoteIds.size);
                        notesSelectionCompose.disabled = selectedNoteIds.size === 0;
                        notesSelectToggle.dataset.active = noteSelectionMode ? "true" : "false";
                    };

                    // Sync picked markers on the already-rendered graph without
                    // triggering a full re-layout. Used when selection changes
                    // happen in the sidebar and we want the graph to keep up.
                    const syncGraphPickedState = () => {
                        const svg = document.querySelector(".graph-svg");
                        if (!svg) return;
                        svg.querySelectorAll(".graph-node").forEach(group => {
                            const nodeId = group.dataset.nodeId || "";
                            if (!nodeId.startsWith("note:")) return;
                            const rawId = nodeId.slice("note:".length);
                            if (selectedNoteIds.has(rawId)) {
                                group.dataset.picked = "true";
                            } else {
                                delete group.dataset.picked;
                            }
                        });
                    };

                    const setNoteSelectionCascade = (noteId, picked) => {
                        const ids = new Set([noteId, ...collectDescendantIds(noteId)]);
                        ids.forEach(id => {
                            if (picked) selectedNoteIds.add(id);
                            else selectedNoteIds.delete(id);
                        });
                    };

                    const toggleNoteSelectionCascade = (noteId) => {
                        setNoteSelectionCascade(noteId, !selectedNoteIds.has(noteId));
                    };

                    const setNoteSelectionMode = (enabled) => {
                        noteSelectionMode = enabled;
                        if (!enabled) selectedNoteIds.clear();
                        updateSelectionBar();
                        renderNotes();
                        syncGraphPickedState();
                    };

                    notesSelectToggle.addEventListener("click", () => setNoteSelectionMode(!noteSelectionMode));
                    notesSelectAll.addEventListener("click", () => {
                        const filteredIds = getFilteredNotes().map(n => n.id);
                        const allSelected = filteredIds.every(id => selectedNoteIds.has(id));
                        if (allSelected) {
                            filteredIds.forEach(id => selectedNoteIds.delete(id));
                        } else {
                            filteredIds.forEach(id => selectedNoteIds.add(id));
                        }
                        updateSelectionBar();
                        renderNotes();
                        syncGraphPickedState();
                    });

                    const getFilteredNotes = () => {
                        const search = historySearch.value.trim().toLowerCase();
                        return search
                            ? currentNotes.filter(n =>
                                n.title.toLowerCase().includes(search) ||
                                n.text.toLowerCase().includes(search) ||
                                (n.tags || []).some(tag => tag.toLowerCase().includes(search))
                            )
                            : currentNotes;
                    };

                    const renderNotes = () => {
                        notesGrid.replaceChildren();
                        const search = historySearch.value.trim().toLowerCase();
                        const filtered = getFilteredNotes();

                        if (notesHeaderMeta) {
                            const total = currentNotes.length;
                            const shown = filtered.length;
                            notesHeaderMeta.textContent = search
                                ? `${shown} / ${total}`
                                : `${total}`;
                        }

                        if (filtered.length === 0) {
                            const empty = document.createElement("div");
                            empty.className = "notes-empty";
                            empty.textContent = t("history.empty");
                            notesGrid.append(empty);
                            return;
                        }

                        // Drag & drop is only meaningful when the whole list is visible,
                        // i.e. no search filter is active. Otherwise indices would not
                        // correspond 1:1 with `currentNotes`, and a reorder would apply
                        // to the hidden items too.
                        const dragEnabled = !noteSelectionMode && !search;

                        // Top-level drop band for moving a note out to the root
                        if (dragEnabled) {
                            const rootBand = document.createElement("div");
                            rootBand.className = "notes-drop-root";
                            rootBand.textContent = t("note.parentNone");
                            rootBand.addEventListener("dragover", (event) => {
                                if (!draggedNoteId) return;
                                event.preventDefault();
                                event.stopPropagation();
                                event.dataTransfer.dropEffect = "move";
                                rootBand.dataset.dropZone = "root";
                            });
                            rootBand.addEventListener("dragleave", () => {
                                delete rootBand.dataset.dropZone;
                            });
                            rootBand.addEventListener("drop", (event) => handleNoteDrop(event, null, "root"));
                            notesGrid.append(rootBand);
                        }

                        // Build a parent->children map from the filtered set so we can render as a tree.
                        // If the search filter hides some parents, render matched notes flat to avoid
                        // confusing "orphan" indentation.
                        const renderAsTree = dragEnabled; // tree only when full list visible
                        const filteredIdSet = new Set(filtered.map(n => n.id));
                        const childrenMap = new Map();
                        const roots = [];
                        if (renderAsTree) {
                            filtered.forEach(n => {
                                const pid = (n.parent_id && filteredIdSet.has(n.parent_id)) ? n.parent_id : null;
                                if (!childrenMap.has(pid)) childrenMap.set(pid, []);
                                childrenMap.get(pid).push(n);
                                if (pid === null) roots.push(n);
                            });
                        }

                        const buildCard = (note, depth) => {
                            const card = document.createElement("article");
                            card.className = "note-card note-card-compact";
                            card.dataset.noteId = note.id;
                            if (renderAsTree && depth > 0) {
                                card.dataset.depth = String(depth);
                                card.style.marginLeft = (depth * 18) + "px";
                            }
                            if (dragEnabled) {
                                card.draggable = true;
                                card.addEventListener("dragstart", (event) => handleNoteDragStart(event, note.id));
                                card.addEventListener("dragover", (event) => handleNoteDragOver(event, card, note.id));
                                card.addEventListener("dragleave", () => delete card.dataset.dropZone);
                                card.addEventListener("drop", (event) => handleNoteDrop(event, note.id));
                                card.addEventListener("dragend", handleNoteDragEnd);
                            }
                            if (note.id === highlightedNoteId) card.dataset.selected = "true";
                            if (selectedNoteIds.has(note.id)) card.dataset.picked = "true";

                            if (noteSelectionMode) {
                                card.classList.add("is-selectable");
                                const checkbox = document.createElement("input");
                                checkbox.type = "checkbox";
                                checkbox.className = "note-card-checkbox";
                                checkbox.checked = selectedNoteIds.has(note.id);
                                checkbox.addEventListener("click", e => e.stopPropagation());
                                checkbox.addEventListener("change", () => {
                                    setNoteSelectionCascade(note.id, checkbox.checked);
                                    updateSelectionBar();
                                    renderNotes();
                                    syncGraphPickedState();
                                });
                                card.append(checkbox);
                            }

                            card.addEventListener("click", () => {
                                if (noteSelectionMode) {
                                    toggleNoteSelectionCascade(note.id);
                                    updateSelectionBar();
                                    renderNotes();
                                    syncGraphPickedState();
                                    return;
                                }
                                highlightedNoteId = note.id;
                                renderNotes();
                                highlightGraphNode(`note:${note.id}`);
                            });
                            card.addEventListener("dblclick", () => {
                                if (!noteSelectionMode) openNoteEditor(note);
                            });

                            // Row 1: title + compact date. Title takes the space
                            // it needs up to the date column on the right.
                            const header = document.createElement("div");
                            header.className = "note-card-header";
                            const title = document.createElement("h4");
                            title.textContent = note.title || t("history.noTranscription");
                            // Children badge (● + count) hints at a collapsed subtree
                            // so users can tell parents apart at a glance.
                            const childCount = (childrenMap.get(note.id) || []).length;
                            if (renderAsTree && childCount > 0) {
                                const badge = document.createElement("span");
                                badge.className = "note-card-child-badge";
                                badge.textContent = `▸ ${childCount}`;
                                badge.title = `${childCount} note(s)`;
                                title.append(" ");
                                title.append(badge);
                            }
                            const date = document.createElement("span");
                            date.className = "note-card-date";
                            // Relative date (e.g. "3d") keeps the row narrow so
                            // the title gets more space.
                            date.textContent = formatRelativeDate(note.created_at);
                            date.title = formatDate(note.created_at);
                            header.append(title, date);

                            // Row 2 (meta): tags only, shown inline. Skipped when
                            // there are no tags — the whole row collapses and the
                            // card stays as a tight single-line entry.
                            const tags = note.tags || [];
                            let meta = null;
                            if (tags.length > 0) {
                                meta = document.createElement("div");
                                meta.className = "note-card-meta";
                                tags.forEach((tag) => {
                                    const chip = document.createElement("span");
                                    chip.className = "tag-chip tag-chip-inline";
                                    chip.textContent = "#" + tag;
                                    meta.append(chip);
                                });
                            }

                            // Hover-revealed action buttons. Kept out of the normal
                            // flow so the default row height stays compact.
                            const actions = document.createElement("div");
                            actions.className = "note-card-actions note-card-actions-floating";

                            const copyButton = document.createElement("button");
                            copyButton.type = "button";
                            copyButton.className = "note-card-edit";
                            copyButton.textContent = "⧉";
                            copyButton.title = t("note.copy");
                            copyButton.addEventListener("click", async (event) => {
                                event.stopPropagation();
                                const body = note.text || "";
                                if (!body) {
                                    showToast(t("status.emptyText"), "warning");
                                    return;
                                }
                                if (navigator.clipboard?.writeText) {
                                    await navigator.clipboard.writeText(body);
                                    showToast(t("status.copied"), "success");
                                }
                            });

                            const editButton = document.createElement("button");
                            editButton.type = "button";
                            editButton.className = "note-card-edit";
                            editButton.textContent = "✎";
                            editButton.title = t("note.edit");
                            editButton.addEventListener("click", (event) => {
                                event.stopPropagation();
                                openNoteEditor(note);
                            });

                            actions.append(copyButton, editButton);

                            card.append(header, actions);
                            if (meta) card.append(meta);
                            return card;
                        };

                        if (renderAsTree) {
                            const walk = (note, depth) => {
                                notesGrid.append(buildCard(note, depth));
                                const kids = childrenMap.get(note.id) || [];
                                for (const kid of kids) walk(kid, depth + 1);
                            };
                            for (const root of roots) walk(root, 0);
                        } else {
                            filtered.forEach((note) => {
                                notesGrid.append(buildCard(note, 0));
                            });
                        }
                    };

                    const refreshHistoryView = async () => {
                        currentNotes = await fetchNotes();
                        renderNotes();
                    };

                    const collectAllTags = () => {
                        const set = new Set();
                        (currentNotes || []).forEach(n => (n.tags || []).forEach(tag => set.add(tag)));
                        return Array.from(set).sort((a, b) => a.localeCompare(b, currentLocale));
                    };

                    const refreshTagSuggestions = (selectedTags = []) => {
                        const all = collectAllTags();
                        // Populate <datalist> for free-text autocomplete
                        noteTagSuggestions.replaceChildren();
                        all.forEach(tag => {
                            const opt = document.createElement("option");
                            opt.value = tag;
                            noteTagSuggestions.append(opt);
                        });
                        // Render one-click chips for quick insertion
                        noteTagChips.replaceChildren();
                        if (all.length === 0) return;
                        all.forEach(tag => {
                            const chip = document.createElement("button");
                            chip.type = "button";
                            chip.className = "tag-suggestion-chip";
                            chip.textContent = "#" + tag;
                            if (selectedTags.includes(tag)) chip.dataset.selected = "true";
                            chip.addEventListener("click", () => {
                                const current = noteEditorTags.value
                                    .split(",").map(s => s.trim()).filter(Boolean);
                                if (current.includes(tag)) {
                                    // Toggle off
                                    const next = current.filter(x => x !== tag);
                                    noteEditorTags.value = next.join(", ");
                                    delete chip.dataset.selected;
                                } else {
                                    current.push(tag);
                                    noteEditorTags.value = current.join(", ");
                                    chip.dataset.selected = "true";
                                }
                            });
                            noteTagChips.append(chip);
                        });
                    };

                    // Build the set of descendant ids of `rootId` so we can exclude them from the
                    // parent-picker (a note cannot become a child of its own descendant).
                    const collectDescendantIds = (rootId) => {
                        const result = new Set();
                        const childrenMap = new Map();
                        (currentNotes || []).forEach(n => {
                            const pid = n.parent_id || null;
                            if (!childrenMap.has(pid)) childrenMap.set(pid, []);
                            childrenMap.get(pid).push(n.id);
                        });
                        const stack = [rootId];
                        while (stack.length > 0) {
                            const id = stack.pop();
                            const kids = childrenMap.get(id) || [];
                            for (const kid of kids) {
                                if (!result.has(kid)) {
                                    result.add(kid);
                                    stack.push(kid);
                                }
                            }
                        }
                        return result;
                    };

                    const populateParentSelect = (currentNote) => {
                        noteEditorParent.replaceChildren();
                        const noneOpt = document.createElement("option");
                        noneOpt.value = "";
                        noneOpt.textContent = t("note.parentNone");
                        noteEditorParent.append(noneOpt);

                        const excluded = currentNote ? collectDescendantIds(currentNote.id) : new Set();
                        if (currentNote) excluded.add(currentNote.id);

                        (currentNotes || []).forEach(n => {
                            if (excluded.has(n.id)) return;
                            const opt = document.createElement("option");
                            opt.value = n.id;
                            opt.textContent = n.title || n.source_name || "(untitled)";
                            noteEditorParent.append(opt);
                        });
                        noteEditorParent.value = currentNote?.parent_id || "";
                    };

                    const openNoteEditor = (note) => {
                        editingNoteId = note.id;
                        noteEditorTitle.value = note.title || "";
                        noteEditorText.value = note.text || "";
                        noteEditorTags.value = (note.tags || []).join(", ");
                        populateParentSelect(note);
                        noteEditorModal.hidden = false;
                        refreshTagSuggestions(note.tags || []);
                        if (typeof clearAnalysisResult === "function") clearAnalysisResult();
                    };

                    noteEditorClearParent.addEventListener("click", () => {
                        noteEditorParent.value = "";
                    });

                    const closeNoteEditor = () => {
                        noteEditorModal.hidden = true;
                        editingNoteId = null;
                        if (typeof clearAnalysisResult === "function") clearAnalysisResult();
                    };

                    // Keep suggestion chips' selected state in sync with textual edits
                    noteEditorTags.addEventListener("input", () => {
                        const selected = noteEditorTags.value
                            .split(",").map(s => s.trim()).filter(Boolean);
                        noteTagChips.querySelectorAll(".tag-suggestion-chip").forEach(chip => {
                            const tag = chip.textContent.replace(/^#/, "");
                            if (selected.includes(tag)) {
                                chip.dataset.selected = "true";
                            } else {
                                delete chip.dataset.selected;
                            }
                        });
                    });

                    noteEditorClose.addEventListener("click", closeNoteEditor);
                    noteEditorModal.addEventListener("click", (event) => {
                        if (event.target === noteEditorModal) closeNoteEditor();
                    });
                    document.addEventListener("keydown", (event) => {
                        if (event.key !== "Escape") return;
                        if (noteEditorModal.hidden) return;
                        event.preventDefault();
                        closeNoteEditor();
                    });

                    const clearAnalysisResult = () => {
                        noteAnalysisResult.hidden = true;
                        noteAnalysisResult.replaceChildren();
                    };

                    const renderAnalysisResult = (analysis) => {
                        noteAnalysisResult.replaceChildren();

                        const makeRow = (labelKey, valueNode) => {
                            const row = document.createElement("div");
                            row.className = "note-analysis-row";
                            const label = document.createElement("span");
                            label.className = "note-analysis-label";
                            label.textContent = t(labelKey);
                            row.append(label, valueNode);
                            return row;
                        };

                        const toneValue = document.createElement("span");
                        toneValue.className = "note-analysis-tone";
                        toneValue.textContent = analysis.tone || "-";
                        noteAnalysisResult.append(makeRow("note.analysis.tone", toneValue));

                        const summaryValue = document.createElement("p");
                        summaryValue.className = "note-analysis-summary";
                        summaryValue.textContent = analysis.summary || "";
                        noteAnalysisResult.append(makeRow("note.analysis.summary", summaryValue));

                        const keywordsWrap = document.createElement("div");
                        keywordsWrap.className = "note-analysis-keywords";
                        (analysis.keywords || []).forEach((kw) => {
                            const chip = document.createElement("span");
                            chip.className = "tag-chip";
                            chip.textContent = kw;
                            chip.title = t("note.tags");
                            chip.addEventListener("click", () => {
                                // Click a keyword to add it to the tags field
                                const existing = noteEditorTags.value.split(",").map(s => s.trim()).filter(Boolean);
                                if (!existing.includes(kw)) {
                                    existing.push(kw);
                                    noteEditorTags.value = existing.join(", ");
                                }
                            });
                            keywordsWrap.append(chip);
                        });
                        noteAnalysisResult.append(makeRow("note.analysis.keywords", keywordsWrap));

                        noteAnalysisResult.hidden = false;
                    };

                    noteEditorComplete.addEventListener("click", async () => {
                        const existing = noteEditorText.value;
                        const trimmed = existing.trim();
                        if (!trimmed) {
                            showToast(t("status.emptyText"), "warning");
                            return;
                        }
                        noteEditorComplete.disabled = true;
                        const originalLabel = noteEditorComplete.textContent;
                        noteEditorComplete.textContent = t("note.completing");
                        try {
                            const response = await fetch("/api/complete", {
                                method: "POST",
                                headers: { "Content-Type": "application/json" },
                                body: JSON.stringify({
                                    text: existing,
                                    // Fall back to refineModel when the user hasn't
                                    // explicitly set a completion model — matches the
                                    // prior behaviour before the dedicated setting.
                                    model: completeModel?.value || refineModel?.value || undefined,
                                    note_id: editingNoteId || undefined,
                                    use_linked_context: linkedContextEnabled()
                                })
                            });
                            const body = await response.text();
                            let payload = {};
                            try { payload = body ? JSON.parse(body) : {}; } catch { payload = { error: body }; }
                            if (!response.ok) throw new Error(payload.error || t("note.completeFailed"));
                            const added = (payload.added || "").trim();
                            if (!added) {
                                showToast(t("note.completeEmpty"), "warning");
                                return;
                            }
                            // Join with a blank line when the existing body ends with text,
                            // so the continuation reads as a new paragraph rather than
                            // bolted onto the last sentence.
                            const joiner = existing.endsWith("\n\n")
                                ? ""
                                : existing.endsWith("\n") ? "\n" : "\n\n";
                            const next = existing + joiner + added;
                            noteEditorText.value = next;
                            // Let the user see what was added by scrolling to the bottom.
                            noteEditorText.scrollTop = noteEditorText.scrollHeight;
                            showToast(t("note.completeDone"), "success");
                        } catch (error) {
                            console.error("complete failed", error);
                            showErrorBanner((error && error.message) || t("note.completeFailed"));
                        } finally {
                            noteEditorComplete.disabled = false;
                            noteEditorComplete.textContent = originalLabel;
                        }
                    });

                    noteEditorAnalyze.addEventListener("click", async () => {
                        const text = noteEditorText.value.trim();
                        if (!text) return;
                        noteEditorAnalyze.disabled = true;
                        const originalLabel = noteEditorAnalyze.textContent;
                        noteEditorAnalyze.textContent = t("note.analyzing");
                        try {
                            const response = await fetch("/api/analyze", {
                                method: "POST",
                                headers: { "Content-Type": "application/json" },
                                body: JSON.stringify({
                                    text,
                                    model: analyzeModel?.value || undefined,
                                    note_id: editingNoteId || undefined,
                                    use_linked_context: linkedContextEnabled()
                                })
                            });
                            const body = await response.text();
                            let payload = {};
                            try { payload = body ? JSON.parse(body) : {}; } catch { payload = { error: body }; }
                            if (!response.ok) throw new Error(payload.error || t("note.analysisFailed"));
                            renderAnalysisResult(payload);
                        } catch (error) {
                            console.error("analyze failed", error);
                            showErrorBanner((error && error.message) || t("note.analysisFailed"));
                        } finally {
                            noteEditorAnalyze.disabled = false;
                            noteEditorAnalyze.textContent = originalLabel;
                        }
                    });

                    noteEditorSave.addEventListener("click", async () => {
                        if (!editingNoteId) return;
                        const title = noteEditorTitle.value.trim();
                        const text = noteEditorText.value;
                        const tags = noteEditorTags.value.split(",").map(t => t.trim()).filter(Boolean);
                        const editingNote = (currentNotes || []).find(n => n.id === editingNoteId);
                        const prevParent = editingNote?.parent_id || "";
                        const newParent = noteEditorParent.value || "";
                        try {
                            await fetch(`/api/notes/${editingNoteId}`, {
                                method: "PUT",
                                headers: { "Content-Type": "application/json" },
                                body: JSON.stringify({ title, text, tags })
                            });
                            // Persist parent change separately so we can report a cycle/self-reference
                            if (prevParent !== newParent) {
                                const response = await fetch(`/api/notes/${editingNoteId}/parent`, {
                                    method: "POST",
                                    headers: { "Content-Type": "application/json" },
                                    body: JSON.stringify({ parent_id: newParent || null })
                                });
                                if (!response.ok) {
                                    const err = await response.json().catch(() => ({}));
                                    throw new Error(err.error || t("note.parentSetFailed"));
                                }
                            }
                            await refreshHistoryView();
                            // Invalidate graph layout so the new hierarchy edge is placed freshly
                            graphLayoutCache = null;
                            await renderGraph();
                            closeNoteEditor();
                        } catch (error) {
                            console.warn("note save failed", error);
                            showErrorBanner((error && error.message) || t("note.parentSetFailed"));
                        }
                    });

                    const deleteNoteById = async (noteId, { closeEditor = false } = {}) => {
                        if (!noteId) return;
                        if (!window.confirm(t("note.deleteConfirm"))) return;
                        try {
                            await fetch(`/api/notes/${noteId}`, { method: "DELETE" });
                            if (highlightedNoteId === noteId) highlightedNoteId = null;
                            if (highlightedGraphNodeId === `note:${noteId}`) highlightedGraphNodeId = null;
                            if (lastTranscribedNoteId === noteId) lastTranscribedNoteId = null;
                            graphDetail.hidden = true;
                            await refreshHistoryView();
                            graphLayoutCache = null;
                            await renderGraph();
                            if (closeEditor) closeNoteEditor();
                        } catch (error) {
                            console.warn("note delete failed", error);
                        }
                    };

                    noteEditorDelete.addEventListener("click", async () => {
                        await deleteNoteById(editingNoteId, { closeEditor: true });
                    });

                    refreshHistoryButton.addEventListener("click", async () => {
                        await refreshHistoryView();
                        await renderGraph();
                    });
                    historySearch.addEventListener("input", renderNotes);

                    refreshGraphButton.addEventListener("click", () => renderGraph());
                    graphDetailClose.addEventListener("click", () => { graphDetail.hidden = true; });

                    // ========== Graph rendering ==========
                    let graphData = { nodes: [], edges: [] };
                    let graphSimulation = null;

                    // Manual note-to-note link creation
                    let linkMode = false;            // true when user is picking nodes to link
                    let linkFirstNoteId = null;      // first note picked in the current link session

                    const setLinkMode = (enabled) => {
                        linkMode = enabled;
                        linkFirstNoteId = null;
                        if (enabled) {
                            graphLinkModeButton.dataset.active = "true";
                            graphLinkBanner.hidden = false;
                            graphLinkBannerText.textContent = t("history.graph.linkFirst");
                            graphSvg.classList.add("is-linking");
                        } else {
                            delete graphLinkModeButton.dataset.active;
                            graphLinkBanner.hidden = true;
                            graphSvg.classList.remove("is-linking");
                        }
                    };

                    graphLinkModeButton.addEventListener("click", () => setLinkMode(!linkMode));
                    graphLinkCancelButton.addEventListener("click", () => setLinkMode(false));

                    const handleLinkModeClick = async (node) => {
                        if (node.kind !== "note") {
                            showToast(t("history.graph.linkNoteOnly"), "warning");
                            return;
                        }
                        const noteId = node.id.replace(/^note:/, "");
                        if (!linkFirstNoteId) {
                            linkFirstNoteId = noteId;
                            graphLinkBannerText.textContent = t("history.graph.linkSecond");
                            // Visually mark the pending source
                            graphSvg.querySelectorAll(".graph-node").forEach(el => {
                                if (el.dataset.nodeId === node.id) el.dataset.linkPending = "true";
                                else delete el.dataset.linkPending;
                            });
                            return;
                        }
                        if (linkFirstNoteId === noteId) {
                            // User clicked the same node twice; keep waiting for a different target
                            return;
                        }
                        try {
                            const response = await fetch("/api/note-links", {
                                method: "POST",
                                headers: { "Content-Type": "application/json" },
                                body: JSON.stringify({
                                    from_note_id: linkFirstNoteId,
                                    to_note_id: noteId
                                })
                            });
                            if (!response.ok) {
                                const err = await response.json().catch(() => ({}));
                                throw new Error(err.error || t("history.graph.linkFailed"));
                            }
                            showToast(t("history.graph.linkCreated"), "success");
                            setLinkMode(false);
                            await renderGraph();
                        } catch (error) {
                            showErrorBanner((error && error.message) || t("history.graph.linkFailed"));
                            setLinkMode(false);
                        }
                    };

                    const deleteNoteLink = async (linkId) => {
                        if (!linkId) return;
                        if (!window.confirm(t("history.graph.linkDeleteConfirm"))) return;
                        try {
                            await fetch(`/api/note-links/${encodeURIComponent(linkId)}`, { method: "DELETE" });
                            await renderGraph();
                        } catch (error) {
                            showErrorBanner((error && error.message) || t("history.graph.linkFailed"));
                        }
                    };

                    const highlightGraphNode = (nodeId) => {
                        highlightedGraphNodeId = nodeId;
                        graphSvg.querySelectorAll(".graph-node").forEach((el) => {
                            if (el.dataset.nodeId === nodeId) {
                                el.dataset.highlighted = "true";
                                el.scrollIntoView?.({ behavior: "smooth", block: "center", inline: "center" });
                            } else {
                                delete el.dataset.highlighted;
                            }
                        });
                    };

                    const renderGraph = async () => {
                        if (!currentProjectId) return;
                        try {
                            const response = await fetch(`/api/graph?project_id=${encodeURIComponent(currentProjectId)}`);
                            if (!response.ok) return;
                            graphData = await response.json();
                        } catch (error) {
                            console.warn("fetch graph failed", error);
                            return;
                        }
                        drawGraph();
                    };

                    // Redraw the graph when its container resizes (window resize, sidebar layout
                    // changes, etc.) so the force-directed layout fits the new viewport.
                    //
                    // Important: only redraw when the dimensions change meaningfully (>= 8 px),
                    // otherwise sub-pixel jitter from scrollbars / font-loading triggers an
                    // endless chain of redraws that looks like continuous zoom.
                    let graphResizeTimer = null;
                    let graphSizeCache = { w: 0, h: 0 };
                    let graphResizeObserverInitialized = false;

                    const scheduleGraphResizeRedraw = (w, h) => {
                        const dw = Math.abs(w - graphSizeCache.w);
                        const dh = Math.abs(h - graphSizeCache.h);
                        if (graphResizeObserverInitialized && dw < 8 && dh < 8) return;
                        graphSizeCache = { w, h };
                        graphResizeObserverInitialized = true;
                        if (graphResizeTimer) clearTimeout(graphResizeTimer);
                        graphResizeTimer = setTimeout(() => {
                            if (graphData && graphData.nodes && graphData.nodes.length > 0) {
                                // Invalidate cached layout: the viewport size has genuinely changed
                                graphLayoutCache = null;
                                drawGraph();
                            }
                        }, 200);
                    };

                    const graphContainerEl = graphSvg.parentElement;
                    if (graphContainerEl && typeof ResizeObserver !== "undefined") {
                        const observer = new ResizeObserver((entries) => {
                            const entry = entries[0];
                            if (!entry) return;
                            const w = Math.round(entry.contentRect.width);
                            const h = Math.round(entry.contentRect.height);
                            if (w <= 0 || h <= 0) return;
                            scheduleGraphResizeRedraw(w, h);
                        });
                        observer.observe(graphContainerEl);
                    } else {
                        window.addEventListener("resize", () => {
                            const el = graphSvg.parentElement;
                            if (!el) return;
                            scheduleGraphResizeRedraw(el.clientWidth, el.clientHeight);
                        });
                    }

                    // Zoom/pan transform for the graph. Applied on a wrapping <g>.
                    let graphTransform = { scale: 1, tx: 0, ty: 0 };
                    const applyGraphTransform = () => {
                        const root = graphSvg.querySelector("#graphRoot");
                        if (!root) return;
                        root.setAttribute(
                            "transform",
                            `translate(${graphTransform.tx}, ${graphTransform.ty}) scale(${graphTransform.scale})`
                        );
                    };

                    // Cached node layout. Keyed by node.id → {x, y}. We compute the force-directed
                    // layout once for a given dataset + viewport and re-use it until the data
                    // actually changes. This prevents ResizeObserver-triggered redraws from
                    // reshuffling positions (and appearing to zoom continuously).
                    let graphLayoutCache = null;   // { key, width, height, positions: Map<id, {x,y}> }

                    const graphDataKey = () => {
                        const nodeKey = graphData.nodes.map(n => n.id).sort().join("|");
                        const edgeKey = graphData.edges.map(e => `${e.source}>${e.target}`).sort().join("|");
                        return `${nodeKey}::${edgeKey}`;
                    };

                    const drawGraph = () => {
                        while (graphSvg.firstChild) graphSvg.removeChild(graphSvg.firstChild);
                        const width = graphSvg.clientWidth || 800;
                        const height = graphSvg.clientHeight || 600;
                        graphSvg.setAttribute("viewBox", `0 0 ${width} ${height}`);
                        const svgNS_outer = "http://www.w3.org/2000/svg";
                        const rootGroup = document.createElementNS(svgNS_outer, "g");
                        rootGroup.setAttribute("id", "graphRoot");
                        graphSvg.appendChild(rootGroup);

                        const currentKey = graphDataKey();
                        const reuseLayout = graphLayoutCache
                            && graphLayoutCache.key === currentKey
                            && graphLayoutCache.width === width
                            && graphLayoutCache.height === height;

                        const nodeById = new Map();
                        const nodes = graphData.nodes.map((node, index) => {
                            let x, y;
                            if (reuseLayout && graphLayoutCache.positions.has(node.id)) {
                                const cached = graphLayoutCache.positions.get(node.id);
                                x = cached.x;
                                y = cached.y;
                            } else {
                                const angle = (index / Math.max(1, graphData.nodes.length)) * Math.PI * 2;
                                x = width / 2 + Math.cos(angle) * Math.min(width, height) * 0.3;
                                y = height / 2 + Math.sin(angle) * Math.min(width, height) * 0.3;
                            }
                            const copy = { ...node, x, y, vx: 0, vy: 0 };
                            nodeById.set(copy.id, copy);
                            return copy;
                        });
                        const edges = graphData.edges
                            .map(edge => ({
                                ...edge,
                                source: nodeById.get(edge.source),
                                target: nodeById.get(edge.target)
                            }))
                            .filter(edge => edge.source && edge.target);

                        // Force-directed layout — run only when layout is stale.
                        const iterations = reuseLayout ? 0 : 300;
                        const area = width * height;
                        const k = Math.sqrt(area / Math.max(1, nodes.length)) * 0.6;
                        for (let iter = 0; iter < iterations; iter++) {
                            // repulsion
                            for (let i = 0; i < nodes.length; i++) {
                                let fx = 0, fy = 0;
                                for (let j = 0; j < nodes.length; j++) {
                                    if (i === j) continue;
                                    const dx = nodes[i].x - nodes[j].x;
                                    const dy = nodes[i].y - nodes[j].y;
                                    const dist = Math.max(0.01, Math.sqrt(dx * dx + dy * dy));
                                    const f = (k * k) / dist;
                                    fx += (dx / dist) * f;
                                    fy += (dy / dist) * f;
                                }
                                nodes[i].vx = fx;
                                nodes[i].vy = fy;
                            }
                            // attraction along edges
                            for (const edge of edges) {
                                const dx = edge.source.x - edge.target.x;
                                const dy = edge.source.y - edge.target.y;
                                const dist = Math.max(0.01, Math.sqrt(dx * dx + dy * dy));
                                const f = (dist * dist) / k;
                                const fxd = (dx / dist) * f;
                                const fyd = (dy / dist) * f;
                                edge.source.vx -= fxd;
                                edge.source.vy -= fyd;
                                edge.target.vx += fxd;
                                edge.target.vy += fyd;
                            }
                            const temp = Math.max(1, 20 * (1 - iter / iterations));
                            for (const node of nodes) {
                                const speed = Math.sqrt(node.vx * node.vx + node.vy * node.vy);
                                const cap = Math.min(speed, temp);
                                if (speed > 0) {
                                    node.x += (node.vx / speed) * cap;
                                    node.y += (node.vy / speed) * cap;
                                }
                                node.x = Math.max(40, Math.min(width - 40, node.x));
                                node.y = Math.max(40, Math.min(height - 40, node.y));
                            }
                        }

                        // Persist the computed positions so subsequent redraws (resize, zoom
                        // reset, highlight changes) don't recompute a brand-new layout that
                        // visually drifts.
                        const positions = new Map();
                        for (const n of nodes) positions.set(n.id, { x: n.x, y: n.y });
                        graphLayoutCache = { key: currentKey, width, height, positions };

                        const svgNS = "http://www.w3.org/2000/svg";

                        // Draw edges
                        for (const edge of edges) {
                            const line = document.createElementNS(svgNS, "line");
                            line.setAttribute("x1", edge.source.x);
                            line.setAttribute("y1", edge.source.y);
                            line.setAttribute("x2", edge.target.x);
                            line.setAttribute("y2", edge.target.y);
                            line.setAttribute("class", "graph-edge");
                            if (edge.kind) line.dataset.kind = edge.kind;
                            if (edge.kind === "link" && edge.id) {
                                line.dataset.linkId = edge.id;
                                line.classList.add("graph-edge-manual");
                                line.setAttribute("stroke-linecap", "round");
                                line.addEventListener("click", (event) => {
                                    event.stopPropagation();
                                    deleteNoteLink(edge.id);
                                });
                            }
                            rootGroup.appendChild(line);
                        }

                        // Draw nodes
                        for (const node of nodes) {
                            const group = document.createElementNS(svgNS, "g");
                            group.setAttribute("class", `graph-node graph-node-${node.kind}`);
                            group.setAttribute("transform", `translate(${node.x}, ${node.y})`);
                            group.dataset.nodeId = node.id;
                            if ((highlightedNoteId && node.id === `note:${highlightedNoteId}`)
                                || highlightedGraphNodeId === node.id) {
                                group.dataset.highlighted = "true";
                            }
                            if (node.kind === "note") {
                                const rawId = node.id.replace(/^note:/, "");
                                if (selectedNoteIds.has(rawId)) {
                                    group.dataset.picked = "true";
                                }
                            }

                            const circle = document.createElementNS(svgNS, "circle");
                            const r = node.kind === "project" ? 24 : node.kind === "note" ? 12 : 8;
                            circle.setAttribute("r", r);
                            group.appendChild(circle);

                            const label = document.createElementNS(svgNS, "text");
                            label.setAttribute("y", r + 14);
                            label.setAttribute("text-anchor", "middle");
                            label.setAttribute("class", "graph-node-label");
                            const text = node.label.length > 24 ? node.label.substring(0, 22) + "…" : node.label;
                            label.textContent = text;
                            group.appendChild(label);

                            group.addEventListener("click", () => {
                                if (linkMode) {
                                    handleLinkModeClick(node);
                                    return;
                                }
                                // In selection mode, clicking a note toggles its
                                // pick state instead of opening the detail panel.
                                if (noteSelectionMode && node.kind === "note") {
                                    const id = node.id.replace(/^note:/, "");
                                    toggleNoteSelectionCascade(id);
                                    updateSelectionBar();
                                    renderNotes();
                                    syncGraphPickedState();
                                    return;
                                }
                                graphDetailTitle.textContent = node.label;
                                graphDetailKind.textContent = node.kind;
                                graphDetailContent.replaceChildren();
                                if (node.kind === "note") {
                                    const id = node.id.replace(/^note:/, "");
                                    highlightedNoteId = id;
                                    highlightGraphNode(node.id);
                                    renderNotes();
                                    // Scroll the matching sidebar item into view
                                    const selectedCard = notesGrid.querySelector('.note-card[data-selected="true"]');
                                    selectedCard?.scrollIntoView?.({ behavior: "smooth", block: "nearest" });
                                    const note = currentNotes.find(n => n.id === id);
                                    if (note) {
                                        const pre = document.createElement("p");
                                        pre.textContent = note.text;
                                        graphDetailContent.append(pre);
                                        const editButton = document.createElement("button");
                                        editButton.className = "secondary-button";
                                        editButton.textContent = t("note.edit");
                                        editButton.addEventListener("click", () => {
                                            openNoteEditor(note);
                                            graphDetail.hidden = true;
                                        });
                                        graphDetailContent.append(editButton);
                                    }
                                } else {
                                    highlightedNoteId = null;
                                    highlightGraphNode(node.id);
                                    renderNotes();
                                }
                                graphDetail.hidden = false;
                            });

                            rootGroup.appendChild(group);
                        }

                        applyGraphTransform();
                    };

                    // Mouse wheel → zoom around the cursor. Middle/left drag → pan.
                    graphSvg.addEventListener("wheel", (event) => {
                        event.preventDefault();
                        const rect = graphSvg.getBoundingClientRect();
                        const cx = event.clientX - rect.left;
                        const cy = event.clientY - rect.top;
                        const factor = event.deltaY < 0 ? 1.15 : 1 / 1.15;
                        const newScale = Math.min(4, Math.max(0.3, graphTransform.scale * factor));
                        // Zoom toward cursor position
                        graphTransform.tx = cx - ((cx - graphTransform.tx) * newScale) / graphTransform.scale;
                        graphTransform.ty = cy - ((cy - graphTransform.ty) * newScale) / graphTransform.scale;
                        graphTransform.scale = newScale;
                        applyGraphTransform();
                    }, { passive: false });

                    let panState = null;
                    graphSvg.addEventListener("pointerdown", (event) => {
                        // Pan only with middle click or when clicking empty space
                        if (event.button === 1 || (event.button === 0 && event.target === graphSvg)) {
                            panState = {
                                startX: event.clientX,
                                startY: event.clientY,
                                origTx: graphTransform.tx,
                                origTy: graphTransform.ty
                            };
                            graphSvg.setPointerCapture(event.pointerId);
                            graphSvg.classList.add("is-panning");
                        }
                    });
                    graphSvg.addEventListener("pointermove", (event) => {
                        if (!panState) return;
                        graphTransform.tx = panState.origTx + (event.clientX - panState.startX);
                        graphTransform.ty = panState.origTy + (event.clientY - panState.startY);
                        applyGraphTransform();
                    });
                    const endPan = (event) => {
                        if (panState) {
                            graphSvg.releasePointerCapture?.(event.pointerId);
                            panState = null;
                            graphSvg.classList.remove("is-panning");
                        }
                    };
                    graphSvg.addEventListener("pointerup", endPan);
                    graphSvg.addEventListener("pointercancel", endPan);
                    graphSvg.addEventListener("dblclick", (event) => {
                        // Reset view on empty-space double-click
                        if (event.target === graphSvg) {
                            graphTransform = { scale: 1, tx: 0, ty: 0 };
                            applyGraphTransform();
                        }
                    });

                    // ========== Live caption overlay ==========
                    // Capture audio continuously first, then process utterance-sized chunks.
                    // A short silence closes the current utterance, which avoids fixed-window
                    // overlap duplicates and gives translation more complete sentences.
                    const LIVE_SAMPLE_RATE = 16000;
                    const LIVE_SPEECH_START_RMS = 0.018;
                    const LIVE_SPEECH_CONTINUE_RMS = 0.010;
                    const LIVE_SILENCE_CLOSE_MS = 900;
                    const LIVE_UTTERANCE_PREROLL_MS = 250;
                    const LIVE_UTTERANCE_TAIL_MS = 250;
                    const LIVE_MIN_UTTERANCE_MS = 700;
                    const LIVE_MAX_UTTERANCE_MS = 12000;
                    const LIVE_TRANSLATION_IDLE_FLUSH_MS = 700;
                    let liveState = null;
                    let liveSavedSessions = [];

                    const optionExists = (selectEl, value) =>
                        !!selectEl && Array.from(selectEl.options).some(option => option.value === value);

                    const loadLiveCaptionPrefs = () => {
                        try {
                            const raw = localStorage.getItem(LIVE_PREFS_KEY);
                            return raw ? (JSON.parse(raw) || {}) : {};
                        } catch {
                            return {};
                        }
                    };

                    const saveLiveCaptionPrefs = () => {
                        try {
                            localStorage.setItem(LIVE_PREFS_KEY, JSON.stringify({
                                sourceLanguage: liveSourceLanguage?.value || "",
                                targetLanguage: liveTargetLanguage?.value || "",
                                translate: !!liveTranslateToggle?.checked,
                                speechModel: liveSpeechModel?.value || "",
                                translateModel: liveTranslateModel?.value || ""
                            }));
                        } catch {}
                    };

                    const restoreLiveCaptionPrefs = () => {
                        const prefs = loadLiveCaptionPrefs();
                        if (typeof prefs.sourceLanguage === "string" && optionExists(liveSourceLanguage, prefs.sourceLanguage)) {
                            liveSourceLanguage.value = prefs.sourceLanguage;
                        }
                        if (typeof prefs.targetLanguage === "string" && optionExists(liveTargetLanguage, prefs.targetLanguage)) {
                            liveTargetLanguage.value = prefs.targetLanguage;
                        }
                        if (typeof prefs.translate === "boolean") {
                            liveTranslateToggle.checked = prefs.translate;
                        }
                    };

                    const getSavedLiveSpeechModel = () => {
                        const prefs = loadLiveCaptionPrefs();
                        return typeof prefs.speechModel === "string" ? prefs.speechModel.trim() : "";
                    };

                    const getSavedLiveTranslateModel = () => {
                        const prefs = loadLiveCaptionPrefs();
                        return typeof prefs.translateModel === "string" ? prefs.translateModel.trim() : "";
                    };

                    const populateLiveSpeechModelSelect = () => {
                        if (!liveSpeechModel) return;
                        const previous = liveSpeechModel.value || getSavedLiveSpeechModel() || speechModel?.value || "";
                        liveSpeechModel.replaceChildren();

                        const aliases = speechModelStates.length > 0
                            ? speechModelStates.map(model => model.alias)
                            : [speechModel?.value || DEFAULT_SPEECH_MODEL_ALIAS];
                        aliases
                            .filter(Boolean)
                            .forEach(alias => {
                                const opt = document.createElement("option");
                                opt.value = alias;
                                opt.textContent = alias;
                                liveSpeechModel.append(opt);
                            });

                        if (previous && optionExists(liveSpeechModel, previous)) {
                            liveSpeechModel.value = previous;
                        } else if (optionExists(liveSpeechModel, speechModel?.value || "")) {
                            liveSpeechModel.value = speechModel.value;
                        }
                    };

                    const populateLiveTranslationModelSelect = () => {
                        if (!liveTranslateModel) return;
                        const previous = liveTranslateModel.value || getSavedLiveTranslateModel() || loadPostModelPrefs().translate || "";
                        liveTranslateModel.replaceChildren();

                        const defaultOpt = document.createElement("option");
                        defaultOpt.value = "";
                        defaultOpt.textContent = `${DEFAULT_CHAT_MODEL_ALIAS} (${t("settings.models.useDefault")})`;
                        liveTranslateModel.append(defaultOpt);

                        const chatModels = (allModelsCache.models || []).filter(model => model.category !== "speech");
                        const aliases = new Set(chatModels.map(model => model.alias));
                        if (previous && previous !== "" && !aliases.has(previous)) {
                            aliases.add(previous);
                        }
                        aliases.forEach(alias => {
                            const opt = document.createElement("option");
                            opt.value = alias;
                            const model = chatModels.find(item => item.alias === alias);
                            const suffix = [];
                            if (model && !model.downloaded) suffix.push("↓");
                            if (model?.compatible === false) suffix.push("!");
                            opt.textContent = suffix.length ? `${alias} · ${suffix.join(" ")}` : alias;
                            liveTranslateModel.append(opt);
                        });

                        liveTranslateModel.value = previous && optionExists(liveTranslateModel, previous)
                            ? previous
                            : "";
                    };

                    const updateLiveModelDisplay = () => {
                        if (liveTranslateModel) {
                            liveTranslateModel.disabled = !liveTranslateToggle?.checked;
                            liveTranslateModel.title = getEffectiveLiveTranslationModel() || "";
                        }
                        if (liveSpeechModel) {
                            liveSpeechModel.title = liveSpeechModel.value || "";
                        }
                    };

                    const getEffectiveLiveTranslationModel = () => {
                        if (!liveTranslateToggle?.checked) return null;
                        const selected = liveTranslateModel?.value?.trim();
                        if (selected) return selected;
                        const savedLive = getSavedLiveTranslateModel();
                        if (savedLive) return savedLive;
                        const saved = loadPostModelPrefs().translate?.trim();
                        return saved || DEFAULT_CHAT_MODEL_ALIAS;
                    };

                    const formatElapsed = (ms) => {
                        if (!Number.isFinite(ms) || ms < 0) ms = 0;
                        const total = Math.floor(ms / 1000);
                        const m = Math.floor(total / 60);
                        const s = total % 60;
                        return `${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
                    };

                    const openLiveOverlay = async () => {
                        liveOverlay.hidden = false;
                        document.body.style.overflow = "hidden";
                        await refreshLiveSessions();
                    };

                    const closeLiveOverlay = async () => {
                        if (liveState) {
                            await stopLiveSession({ prompt: true });
                            if (liveState) return; // user cancelled
                        }
                        liveOverlay.hidden = true;
                        document.body.style.overflow = "";
                    };

                    openLiveButton.addEventListener("click", openLiveOverlay);

                    // ========== Draft composition ==========
                    const openComposeDraftModal = () => {
                        if (selectedNoteIds.size === 0) {
                            showToast(t("draft.selectAtLeastOne"), "warning");
                            return;
                        }
                        composeDraftTitle.value = "";
                        composeDraftMode.value = "concat";
                        composeDraftInstruction.value = "";
                        composeDraftInstructionWrap.hidden = true;
                        composeDraftSummary.textContent = t("draft.summary", { count: selectedNoteIds.size });
                        composeDraftModal.hidden = false;
                    };
                    const closeComposeDraftModal = () => { composeDraftModal.hidden = true; };

                    composeDraftMode.addEventListener("change", () => {
                        composeDraftInstructionWrap.hidden = composeDraftMode.value !== "llm";
                    });
                    notesSelectionCompose.addEventListener("click", openComposeDraftModal);
                    composeDraftClose.addEventListener("click", closeComposeDraftModal);
                    composeDraftCancel.addEventListener("click", closeComposeDraftModal);
                    composeDraftModal.addEventListener("click", (event) => {
                        if (event.target === composeDraftModal) closeComposeDraftModal();
                    });

                    composeDraftConfirm.addEventListener("click", async () => {
                        if (selectedNoteIds.size === 0 || !currentProjectId) return;
                        // Preserve the order of the currently filtered list
                        const ordered = getFilteredNotes()
                            .map(n => n.id)
                            .filter(id => selectedNoteIds.has(id));
                        composeDraftConfirm.disabled = true;
                        const mode = composeDraftMode.value;
                        showToast(t("draft.composing"), "info");
                        try {
                            const response = await fetch("/api/drafts/compose", {
                                method: "POST",
                                headers: { "Content-Type": "application/json" },
                                body: JSON.stringify({
                                    project_id: currentProjectId,
                                    title: composeDraftTitle.value.trim(),
                                    note_ids: ordered,
                                    mode,
                                    instruction: mode === "llm" ? composeDraftInstruction.value : null
                                })
                            });
                            if (!response.ok) {
                                const err = await response.json().catch(() => ({}));
                                throw new Error(err.error || t("draft.composeFailed"));
                            }
                            const draft = await response.json();
                            closeComposeDraftModal();
                            setNoteSelectionMode(false);
                            showToast(t("draft.created"), "success");
                            await openDraftsOverlay(draft.id);
                        } catch (error) {
                            showErrorBanner((error && error.message) || t("draft.composeFailed"));
                        } finally {
                            composeDraftConfirm.disabled = false;
                        }
                    });

                    // ========== Draft overlay ==========
                    let draftsCache = [];
                    let activeDraftId = null;
                    let activeDraftDirty = false;

                    const renderDraftsList = () => {
                        draftsList.replaceChildren();
                        if (draftsCache.length === 0) {
                            const empty = document.createElement("li");
                            empty.className = "notes-empty";
                            empty.textContent = t("draft.empty");
                            draftsList.append(empty);
                            return;
                        }
                        draftsCache.forEach((draftWithNotes) => {
                            const d = draftWithNotes.draft || draftWithNotes;
                            const li = document.createElement("li");
                            li.className = "drafts-list-item";
                            if (d.id === activeDraftId) li.dataset.active = "true";

                            const title = document.createElement("strong");
                            title.textContent = d.title || "(untitled)";
                            const meta = document.createElement("p");
                            meta.className = "drafts-list-meta";
                            const noteIds = draftWithNotes.note_ids || [];
                            meta.textContent = `${t("draft.summary", { count: noteIds.length })} · ${formatDate(d.updated_at)}`;

                            li.append(title, meta);
                            li.addEventListener("click", () => loadDraft(d.id));
                            draftsList.append(li);
                        });
                    };

                    const refreshDrafts = async () => {
                        if (!currentProjectId) return;
                        try {
                            const response = await fetch(`/api/drafts?project_id=${encodeURIComponent(currentProjectId)}`);
                            if (!response.ok) return;
                            const payload = await response.json();
                            draftsCache = payload.drafts || [];
                            renderDraftsList();
                        } catch (error) {
                            console.warn("fetch drafts failed", error);
                        }
                    };

                    const setDraftEditorEnabled = (enabled) => {
                        draftEditorTitle.disabled = !enabled;
                        draftEditorContent.disabled = !enabled;
                        draftCopyButton.disabled = !enabled;
                        draftExportButton.disabled = !enabled;
                        draftSaveButton.disabled = !enabled;
                        draftDeleteButton.hidden = !enabled;
                    };

                    const loadDraft = async (id) => {
                        try {
                            const response = await fetch(`/api/drafts/${encodeURIComponent(id)}`);
                            if (!response.ok) return;
                            const data = await response.json();
                            const d = data.draft || data;
                            activeDraftId = d.id;
                            activeDraftDirty = false;
                            draftEditorTitle.value = d.title || "";
                            draftEditorContent.value = d.content || "";
                            setDraftEditorEnabled(true);
                            renderDraftsList();
                            // Source notes
                            const noteIds = data.note_ids || [];
                            if (noteIds.length === 0) {
                                draftReferences.hidden = true;
                            } else {
                                const byId = new Map((currentNotes || []).map(n => [n.id, n]));
                                draftReferencesList.replaceChildren();
                                noteIds.forEach(nid => {
                                    const li = document.createElement("li");
                                    const note = byId.get(nid);
                                    li.textContent = note ? (note.title || "(untitled)") : nid;
                                    if (note) {
                                        li.style.cursor = "pointer";
                                        li.title = note.text.slice(0, 160);
                                        li.addEventListener("click", () => openNoteEditor(note));
                                    }
                                    draftReferencesList.append(li);
                                });
                                draftReferences.hidden = false;
                            }
                        } catch (error) {
                            console.warn("load draft failed", error);
                        }
                    };

                    const openDraftsOverlay = async (preselectId) => {
                        draftsOverlay.hidden = false;
                        document.body.style.overflow = "hidden";
                        await refreshDrafts();
                        if (preselectId) {
                            await loadDraft(preselectId);
                        } else if (draftsCache.length > 0) {
                            const first = (draftsCache[0].draft || draftsCache[0]).id;
                            await loadDraft(first);
                        } else {
                            activeDraftId = null;
                            draftEditorTitle.value = "";
                            draftEditorContent.value = "";
                            setDraftEditorEnabled(false);
                            draftReferences.hidden = true;
                        }
                    };
                    const closeDraftsOverlay = () => {
                        if (activeDraftDirty && !window.confirm("未保存の変更があります。閉じてもよいですか？")) return;
                        draftsOverlay.hidden = true;
                        document.body.style.overflow = "";
                    };

                    openDraftsButton.addEventListener("click", () => openDraftsOverlay());
                    closeDraftsButton.addEventListener("click", closeDraftsOverlay);

                    const markDraftDirty = () => { activeDraftDirty = true; };
                    draftEditorTitle.addEventListener("input", markDraftDirty);
                    draftEditorContent.addEventListener("input", markDraftDirty);

                    draftSaveButton.addEventListener("click", async () => {
                        if (!activeDraftId) return;
                        try {
                            const response = await fetch(`/api/drafts/${encodeURIComponent(activeDraftId)}`, {
                                method: "PUT",
                                headers: { "Content-Type": "application/json" },
                                body: JSON.stringify({
                                    title: draftEditorTitle.value.trim() || "Draft",
                                    content: draftEditorContent.value
                                })
                            });
                            if (!response.ok) {
                                const err = await response.json().catch(() => ({}));
                                throw new Error(err.error || t("draft.composeFailed"));
                            }
                            activeDraftDirty = false;
                            showToast(t("draft.saved"), "success");
                            await refreshDrafts();
                        } catch (error) {
                            showErrorBanner((error && error.message) || t("draft.composeFailed"));
                        }
                    });

                    draftDeleteButton.addEventListener("click", async () => {
                        if (!activeDraftId) return;
                        const title = draftEditorTitle.value || "";
                        if (!window.confirm(t("draft.deleteConfirm", { title }))) return;
                        try {
                            await fetch(`/api/drafts/${encodeURIComponent(activeDraftId)}`, { method: "DELETE" });
                            activeDraftId = null;
                            activeDraftDirty = false;
                            await refreshDrafts();
                            if (draftsCache.length > 0) {
                                const first = (draftsCache[0].draft || draftsCache[0]).id;
                                await loadDraft(first);
                            } else {
                                draftEditorTitle.value = "";
                                draftEditorContent.value = "";
                                setDraftEditorEnabled(false);
                                draftReferences.hidden = true;
                            }
                        } catch (error) {
                            showErrorBanner((error && error.message) || "delete failed");
                        }
                    });

                    draftCopyButton.addEventListener("click", async () => {
                        if (!draftEditorContent.value) return;
                        if (navigator.clipboard?.writeText) {
                            await navigator.clipboard.writeText(draftEditorContent.value);
                            showToast(t("status.copied"), "success");
                        }
                    });

                    draftExportButton.addEventListener("click", async () => {
                        const content = draftEditorContent.value;
                        if (!content) return;
                        const title = draftEditorTitle.value.trim() || "draft";
                        const blob = new Blob([content], { type: "text/markdown;charset=utf-8" });
                        const url = URL.createObjectURL(blob);
                        const link = document.createElement("a");
                        link.href = url;
                        link.download = `${title.replace(/[\\/:*?"<>|]+/g, "-")}.md`;
                        link.click();
                        URL.revokeObjectURL(url);
                        showToast(t("status.savedExport"), "success");
                    });
                    closeLiveButton.addEventListener("click", closeLiveOverlay);

                    const refreshLiveSessions = async () => {
                        try {
                            const response = await fetch("/api/live/sessions");
                            if (!response.ok) return;
                            const payload = await response.json();
                            liveSavedSessions = payload.sessions || [];
                        } catch (error) {
                            console.warn("fetch live sessions failed", error);
                            liveSavedSessions = [];
                        }
                        renderLiveSessionsSidebar();
                    };

                    const renderLiveSessionsSidebar = () => {
                        liveSessionsList.replaceChildren();
                        if (liveSavedSessions.length === 0) {
                            const empty = document.createElement("li");
                            empty.className = "notes-empty";
                            empty.textContent = t("live.noSessions");
                            liveSessionsList.append(empty);
                            return;
                        }
                        liveSavedSessions.forEach((session) => {
                            const li = document.createElement("li");
                            li.className = "live-sidebar-item";

                            const header = document.createElement("div");
                            header.className = "live-sidebar-item-header";
                            const title = document.createElement("strong");
                            title.textContent = session.title;
                            const date = document.createElement("span");
                            date.className = "note-card-date";
                            date.textContent = formatDate(session.started_at);
                            header.append(title, date);

                            const summary = document.createElement("p");
                            summary.className = "live-sidebar-summary";
                            summary.textContent = t("live.summary", {
                                count: session.segment_count,
                                duration: formatElapsed(session.duration_ms || 0)
                            });

                            const actions = document.createElement("div");
                            actions.className = "live-sidebar-actions";
                            const viewButton = document.createElement("button");
                            viewButton.type = "button";
                            viewButton.className = "text-button";
                            viewButton.textContent = t("button.copy");
                            viewButton.addEventListener("click", async (event) => {
                                event.stopPropagation();
                                try {
                                    const response = await fetch(`/api/live/sessions/${encodeURIComponent(session.id)}`);
                                    const detail = await response.json();
                                    const lines = (detail.segments || []).map(seg =>
                                        seg.translated_text
                                            ? `[${formatElapsed(seg.start_ms)}] ${seg.source_text}\n  → ${seg.translated_text}`
                                            : `[${formatElapsed(seg.start_ms)}] ${seg.source_text}`
                                    ).join("\n");
                                    if (navigator.clipboard?.writeText) {
                                        await navigator.clipboard.writeText(lines);
                                        showToast(t("status.copied"), "success");
                                    }
                                } catch (err) {
                                    console.warn(err);
                                }
                            });
                            const deleteButton = document.createElement("button");
                            deleteButton.type = "button";
                            deleteButton.className = "text-button";
                            deleteButton.textContent = "🗑";
                            deleteButton.addEventListener("click", async (event) => {
                                event.stopPropagation();
                                if (!window.confirm(t("live.deleteConfirm"))) return;
                                await fetch(`/api/live/sessions/${encodeURIComponent(session.id)}`, { method: "DELETE" });
                                await refreshLiveSessions();
                            });
                            actions.append(viewButton, deleteButton);

                            li.append(header, summary, actions);
                            li.addEventListener("click", () => loadLiveSessionDetail(session.id));
                            liveSessionsList.append(li);
                        });
                    };

                    const loadLiveSessionDetail = async (id) => {
                        try {
                            const response = await fetch(`/api/live/sessions/${encodeURIComponent(id)}`);
                            if (!response.ok) return;
                            const detail = await response.json();
                            renderLiveCaptions(detail.segments || []);
                        } catch (error) {
                            console.warn("load live session detail failed", error);
                        }
                    };

                    const renderLiveCaptions = (segments) => {
                        liveCaptions.replaceChildren();
                        if (!segments || segments.length === 0) {
                            const empty = document.createElement("div");
                            empty.className = "live-empty";
                            empty.textContent = t("live.empty");
                            liveCaptions.append(empty);
                            updateLiveCurrent(null);
                            return;
                        }
                        segments.forEach((seg) => {
                            liveCaptions.append(buildCaptionRow(seg));
                        });
                        liveCaptions.scrollTop = liveCaptions.scrollHeight;
                        updateLiveCurrent(segments[segments.length - 1]);
                    };

                    const updateLiveCurrent = (segment) => {
                        if (!liveCurrentSource || !liveCurrentTranslation) return;
                        if (!segment) {
                            liveCurrentSource.textContent = t("live.latestWaiting");
                            liveCurrentTranslation.textContent = t("live.latestWaiting");
                            return;
                        }
                        const sourceUnits = splitLiveSourceUnitsForDisplay(segment.source_text);
                        const translatedUnits = splitLiveTranslatedUnitsForDisplay(segment.translated_text);
                        liveCurrentSource.textContent = sourceUnits[sourceUnits.length - 1] || segment.source_text || t("live.latestWaiting");
                        liveCurrentTranslation.textContent = translatedUnits[translatedUnits.length - 1] || segment.translated_text || t("live.latestWaiting");
                    };

                    const buildCaptionRow = (segment) => {
                        const row = document.createElement("article");
                        row.className = "live-caption-row";
                        if (Number.isFinite(Number(segment.sequence))) {
                            row.dataset.liveSequence = String(segment.sequence);
                        }
                        const time = document.createElement("span");
                        time.className = "live-caption-time";
                        time.textContent = formatElapsed(segment.start_ms || 0);
                        const body = document.createElement("div");
                        body.className = "live-caption-body";
                        const pairs = document.createElement("div");
                        pairs.className = "live-caption-pairs";
                        pairs.dataset.role = "pairs";
                        body.append(pairs);
                        const sourceUnits = splitLiveSourceUnitsForDisplay(segment.source_text);
                        const translatedUnits = splitLiveTranslatedUnitsForDisplay(segment.translated_text);
                        const unitCount = Math.max(sourceUnits.length, translatedUnits.length, segment.source_text || segment.translated_text ? 1 : 0);
                        for (let index = 0; index < unitCount; index++) {
                            appendLiveCaptionPair(body, sourceUnits[index] || "", translatedUnits[index] || "");
                        }
                        row.append(time, body);
                        return row;
                    };

                    const normalizeLivePairText = (value) => (value || "").trim().replace(/\s+/g, " ");

                    const appendLiveCaptionPair = (body, sourceText, translatedText, options = {}) => {
                        if (!body) return null;
                        let pairs = body.querySelector('[data-role="pairs"]');
                        if (!pairs) {
                            pairs = document.createElement("div");
                            pairs.className = "live-caption-pairs";
                            pairs.dataset.role = "pairs";
                            body.append(pairs);
                        }

                        const source = (sourceText || "").trim();
                        const translated = (translatedText || "").trim();
                        if (!source && !translated) return null;

                        const sourceKey = normalizeLivePairText(source);
                        if (!options.fallback && sourceKey) {
                            const matchingPending = Array.from(pairs.querySelectorAll(".live-caption-pair[data-source-key]"))
                                .find((item) => item.dataset.sourceKey === sourceKey && !item.querySelector('[data-role="translation"]'));
                            if (matchingPending) {
                                if (translated) {
                                    const translatedEl = document.createElement("p");
                                    translatedEl.className = "live-caption-translated";
                                    translatedEl.dataset.role = "translation";
                                    translatedEl.textContent = translated;
                                    matchingPending.append(translatedEl);
                                }
                                return matchingPending;
                            }
                        }

                        if (options.fallback) {
                            pairs.replaceChildren();
                        }

                        const pair = document.createElement("div");
                        pair.className = "live-caption-pair";
                        if (options.fallback) pair.dataset.fallback = "true";
                        if (sourceKey) pair.dataset.sourceKey = sourceKey;
                        if (source) {
                            const sourceEl = document.createElement("p");
                            sourceEl.className = "live-caption-source";
                            sourceEl.textContent = source;
                            pair.append(sourceEl);
                        }
                        if (translated) {
                            const translatedEl = document.createElement("p");
                            translatedEl.className = "live-caption-translated";
                            translatedEl.dataset.role = "translation";
                            translatedEl.textContent = translated;
                            pair.append(translatedEl);
                        }
                        pairs.append(pair);
                        return pair;
                    };

                    const appendLiveSegment = (segment) => {
                        if (liveEmptyMessage.parentNode === liveCaptions) {
                            liveCaptions.replaceChildren();
                        }
                        const row = buildCaptionRow(segment);
                        liveCaptions.append(row);
                        liveCaptions.scrollTop = liveCaptions.scrollHeight;
                        updateLiveCurrent(segment);
                        return row;
                    };

                    const applyLiveTranslation = (sequence, translatedText, sourceText = null, translatedUnit = null) => {
                        if (!translatedText) return;
                        const key = String(sequence);
                        const rows = liveCaptions.querySelectorAll(".live-caption-row");
                        const row = Array.from(rows).find((item) => item.dataset.liveSequence === key);
                        if (!row) return;
                        const body = row.querySelector(".live-caption-body");
                        if (!body) return;
                        if (sourceText) {
                            appendLiveCaptionPair(body, sourceText, translatedUnit || translatedText);
                        } else {
                            appendLiveCaptionPair(body, null, translatedText, { fallback: true });
                        }
                        const latest = liveState?.segments?.find((item) => item.sequence === sequence);
                        if (latest) {
                            latest.translated_text = translatedText;
                            updateLiveCurrent(latest);
                        } else if (liveCurrentTranslation && row === liveCaptions.lastElementChild) {
                            liveCurrentTranslation.textContent = translatedText;
                        }
                    };

                    const copyLiveAudioRange = (chunks, startSample, endSample) => {
                        const total = Math.max(0, endSample - startSample);
                        const out = new Float32Array(total);
                        let offset = 0;
                        for (const chunk of chunks) {
                            const chunkStart = chunk.start;
                            const chunkEnd = chunk.start + chunk.data.length;
                            if (chunkEnd <= startSample || chunkStart >= endSample) continue;
                            const from = Math.max(startSample, chunkStart) - chunkStart;
                            const to = Math.min(endSample, chunkEnd) - chunkStart;
                            if (to > from) {
                                out.set(chunk.data.subarray(from, to), offset);
                                offset += to - from;
                            }
                        }
                        return offset === total ? out : out.subarray(0, offset);
                    };

                    // ---------- Capture + chunk loop ----------
                    const floatToPCM16 = (float32) => {
                        const buffer = new ArrayBuffer(44 + float32.length * 2);
                        const view = new DataView(buffer);
                        const write = (offset, str) => {
                            for (let i = 0; i < str.length; i++) view.setUint8(offset + i, str.charCodeAt(i));
                        };
                        const dataSize = float32.length * 2;
                        write(0, "RIFF");
                        view.setUint32(4, 36 + dataSize, true);
                        write(8, "WAVE");
                        write(12, "fmt ");
                        view.setUint32(16, 16, true);
                        view.setUint16(20, 1, true);
                        view.setUint16(22, 1, true);
                        view.setUint32(24, LIVE_SAMPLE_RATE, true);
                        view.setUint32(28, LIVE_SAMPLE_RATE * 2, true);
                        view.setUint16(32, 2, true);
                        view.setUint16(34, 16, true);
                        write(36, "data");
                        view.setUint32(40, dataSize, true);
                        let offset = 44;
                        for (let i = 0; i < float32.length; i++) {
                            const s = Math.max(-1, Math.min(1, float32[i]));
                            view.setInt16(offset, s < 0 ? s * 0x8000 : s * 0x7FFF, true);
                            offset += 2;
                        }
                        return new Blob([buffer], { type: "audio/wav" });
                    };

                    const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

                    const LIVE_SENTENCE_ABBREVIATIONS = new Set([
                        "mr", "mrs", "ms", "dr", "prof", "sr", "jr", "st", "vs", "etc"
                    ]);

                    const liveSentenceBoundaryIndex = (text) => {
                        const source = (text || "").trimStart();
                        if (!source) return -1;
                        const enders = new Set(["。", "．", ".", "!", "?", "！", "？"]);
                        for (let i = 0; i < source.length; i++) {
                            const ch = source[i];
                            if (!enders.has(ch)) continue;
                            const prev = source[i - 1] || "";
                            const next = source[i + 1] || "";
                            if (ch === "." && /\d/.test(prev) && /\d/.test(next)) continue;
                            if (ch === ".") {
                                const before = source.slice(0, i).match(/([A-Za-z]+)$/)?.[1]?.toLowerCase();
                                if (before && LIVE_SENTENCE_ABBREVIATIONS.has(before)) continue;
                            }
                            const after = source.slice(i + 1);
                            if (!after || /^[\s"'”’)\]}]/.test(after)) return i + 1;
                        }
                        return -1;
                    };

                    const takeFirstLiveSentence = (text) => {
                        const source = (text || "").trim();
                        const boundary = liveSentenceBoundaryIndex(source);
                        if (boundary < 0) return null;
                        return {
                            sentence: source.slice(0, boundary).trim(),
                            remainder: source.slice(boundary).trim()
                        };
                    };

                    const splitLiveSourceUnitsForDisplay = (text) => {
                        const units = [];
                        let rest = (text || "").trim();
                        while (rest) {
                            const first = takeFirstLiveSentence(rest);
                            if (!first?.sentence) {
                                units.push(rest);
                                break;
                            }
                            units.push(first.sentence);
                            rest = first.remainder;
                        }
                        return units.filter(Boolean);
                    };

                    const splitLiveTranslatedUnitsForDisplay = (text) => (text || "")
                        .split(/\n+/)
                        .map((item) => item.trim())
                        .filter(Boolean);

                    const queueLiveTranslationJob = (state, job) => {
                        if (!state || !liveTranslateToggle.checked || !job?.sourceText?.trim()) return;
                        state.translationQueue.push(job);
                        drainLiveTranslationQueue(state);
                    };

                    const clearLiveTranslationFlushTimer = (state) => {
                        if (state?.translationFlushTimer) {
                            clearTimeout(state.translationFlushTimer);
                            state.translationFlushTimer = null;
                        }
                    };

                    const scheduleLiveTranslationFlush = (state) => {
                        if (!state || state.stopping) return;
                        clearLiveTranslationFlushTimer(state);
                        state.translationFlushTimer = setTimeout(() => {
                            state.translationFlushTimer = null;
                            flushLiveTranslationBuffer(state, true);
                        }, LIVE_TRANSLATION_IDLE_FLUSH_MS);
                    };

                    const flushLiveTranslationBuffer = (state, force = false) => {
                        if (!state || !liveTranslateToggle.checked) return;
                        let text = state.translationBufferText.trim();
                        if (!text || state.translationBufferSequences.length === 0) return;

                        const queueUnit = (sourceText) => {
                            const sequences = [...state.translationBufferSequences];
                            const sequence = sequences[sequences.length - 1];
                            queueLiveTranslationJob(state, {
                                sequence,
                                sequences,
                                sourceText,
                                displaySourceText: sourceText,
                                append: true
                            });
                            return sequence;
                        };

                        while (text) {
                            const firstSentence = takeFirstLiveSentence(text);
                            if (!firstSentence?.sentence) break;
                            const sequence = queueUnit(firstSentence.sentence);
                            text = firstSentence.remainder;
                            state.translationBufferText = text;
                            state.translationBufferSequences = text ? [sequence] : [];
                        }

                        if (!text) {
                            clearLiveTranslationFlushTimer(state);
                            return;
                        }

                        // Do not send arbitrary long multi-sentence buffers by
                        // length/count. Complete sentences above are dispatched
                        // one by one; only an idle/stop flush may send the
                        // remaining unfinished fragment.
                        if (!force) {
                            scheduleLiveTranslationFlush(state);
                            return;
                        }
                        clearLiveTranslationFlushTimer(state);

                        queueUnit(text);
                        state.translationBufferText = "";
                        state.translationBufferSequences = [];
                    };

                    const appendLiveTranslationBuffer = (state, segment) => {
                        if (!state || !liveTranslateToggle.checked || !segment?.source_text) return;
                        const text = segment.source_text.trim();
                        if (!text) return;
                        state.translationBufferText = state.translationBufferText
                            ? `${state.translationBufferText}\n${text}`
                            : text;
                        state.translationBufferSequences.push(segment.sequence);
                        flushLiveTranslationBuffer(state, false);
                        if (state.translationBufferSequences.length > 0) {
                            scheduleLiveTranslationFlush(state);
                        }
                    };

                    const drainLiveTranslationQueue = async (state) => {
                        if (!state || state.translationSending) return;
                        state.translationSending = true;
                        try {
                            while ((liveState === state || state.stopping) && state.translationQueue.length > 0) {
                                const translationJob = state.translationQueue.shift();
                                const request = (async () => {
                                    const response = await fetch("/api/live/translate", {
                                        method: "POST",
                                        headers: { "Content-Type": "application/json" },
                                        body: JSON.stringify({
                                            session_id: state.sessionId,
                                            sequence: translationJob.sequence,
                                            sequences: translationJob.sequences,
                                            source_text: translationJob.sourceText,
                                            append: !!translationJob.append
                                        })
                                    });
                                    if (!response.ok) {
                                        const err = await response.json().catch(() => ({}));
                                        throw new Error(err.error || "translation failed");
                                    }
                                    const payload = await response.json();
                                    if (payload.translated_text) {
                                        const segment = state.segments.find((item) => item.sequence === payload.sequence);
                                        if (segment) segment.translated_text = payload.translated_text;
                                        applyLiveTranslation(
                                            payload.sequence,
                                            payload.translated_text,
                                            payload.source_text || translationJob.displaySourceText || translationJob.sourceText,
                                            payload.translated_unit || payload.translated_text
                                        );
                                    } else {
                                        console.warn("live translation returned empty", payload);
                                    }
                                })();
                                state.pendingTranslations.add(request);
                                try {
                                    await request;
                                } catch (error) {
                                    console.warn("live translation failed", error);
                                } finally {
                                    state.pendingTranslations.delete(request);
                                }
                            }
                        } finally {
                            state.translationSending = false;
                            if ((liveState === state || state.stopping) && state.translationQueue.length > 0) {
                                drainLiveTranslationQueue(state);
                            }
                        }
                    };

                    const queueLiveUtterance = (state = liveState, force = false) => {
                        if (!state) return;
                        if (state.utteranceStartSample === null) return;

                        const minSamples = Math.round(LIVE_MIN_UTTERANCE_MS / 1000 * LIVE_SAMPLE_RATE);
                        const preRollSamples = Math.round(LIVE_UTTERANCE_PREROLL_MS / 1000 * LIVE_SAMPLE_RATE);
                        const tailSamples = Math.round(LIVE_UTTERANCE_TAIL_MS / 1000 * LIVE_SAMPLE_RATE);
                        const startSample = Math.max(
                            state.lastQueuedEndSample,
                            state.utteranceStartSample - preRollSamples
                        );
                        const speechEnd = Math.max(
                            state.lastSpeechSample,
                            state.utteranceStartSample
                        );
                        const endSample = Math.min(
                            state.totalSamples,
                            force ? state.totalSamples : speechEnd + tailSamples
                        );
                        if (!force && endSample - startSample < minSamples) {
                            state.utteranceStartSample = null;
                            state.lastSpeechSample = 0;
                            return;
                        }
                        if (endSample <= startSample) {
                            state.utteranceStartSample = null;
                            return;
                        }

                        const snapshot = copyLiveAudioRange(state.audioChunks, startSample, endSample);
                        if (snapshot.length === 0) return;

                        state.chunkQueue.push({
                            audio: snapshot,
                            startMs: Math.round(startSample / LIVE_SAMPLE_RATE * 1000)
                        });
                        state.lastQueuedEndSample = endSample;
                        state.utteranceStartSample = null;
                        state.lastSpeechSample = 0;

                        const pruneBefore = Math.max(0, endSample - LIVE_SAMPLE_RATE);
                        while (state.audioChunks.length > 1) {
                            const first = state.audioChunks[0];
                            if (first.start + first.data.length > pruneBefore) break;
                            state.audioChunks.shift();
                        }

                        drainLiveChunkQueue(state);
                    };

                    const updateLiveUtteranceState = (state, chunkStart, chunkEnd, rms) => {
                        const isSpeech =
                            rms >= (state.utteranceStartSample === null
                                ? LIVE_SPEECH_START_RMS
                                : LIVE_SPEECH_CONTINUE_RMS);
                        if (isSpeech) {
                            if (state.utteranceStartSample === null) {
                                state.utteranceStartSample = Math.max(state.lastQueuedEndSample, chunkStart);
                            }
                            state.lastSpeechSample = chunkEnd;
                        }

                        if (state.utteranceStartSample === null) return;

                        const silenceSamples = Math.round(LIVE_SILENCE_CLOSE_MS / 1000 * LIVE_SAMPLE_RATE);
                        const maxSamples = Math.round(LIVE_MAX_UTTERANCE_MS / 1000 * LIVE_SAMPLE_RATE);
                        const silenceReady = !isSpeech && chunkEnd - state.lastSpeechSample >= silenceSamples;
                        const tooLong = chunkEnd - state.utteranceStartSample >= maxSamples;
                        if (silenceReady || tooLong) {
                            queueLiveUtterance(state, tooLong);
                            if (tooLong && isSpeech) {
                                state.utteranceStartSample = state.lastQueuedEndSample;
                                state.lastSpeechSample = chunkEnd;
                            }
                        }
                    };

                    const drainLiveChunkQueue = async (state) => {
                        if (!state || state.chunkSending) return;
                        state.chunkSending = true;
                        try {
                            while ((liveState === state || state.stopping) && state.chunkQueue.length > 0) {
                                const item = state.chunkQueue.shift();
                                const wav = floatToPCM16(item.audio);
                                const formData = new FormData();
                                formData.append("session_id", state.sessionId);
                                formData.append("chunk_start_ms", String(item.startMs));
                                formData.append("audio", wav, "chunk.wav");

                                try {
                                    const response = await fetch("/api/live/chunk", {
                                        method: "POST",
                                        body: formData
                                    });
                                    if (!response.ok) {
                                        const err = await response.json().catch(() => ({}));
                                        throw new Error(err.error || "chunk failed");
                                    }
                                    const payload = await response.json();
                                    const payloadSegments = Array.isArray(payload.segments)
                                        ? payload.segments
                                        : (payload.source_text ? [{
                                            sequence: payload.sequence,
                                            elapsed_ms: payload.elapsed_ms,
                                            source_text: payload.source_text,
                                            translated_text: payload.translated_text || null
                                        }] : []);
                                    for (const payloadSegment of payloadSegments) {
                                        if (!payloadSegment?.source_text) continue;
                                        const segment = {
                                            sequence: payloadSegment.sequence,
                                            start_ms: payloadSegment.elapsed_ms ?? payload.elapsed_ms,
                                            source_text: payloadSegment.source_text,
                                            translated_text: payloadSegment.translated_text || null
                                        };
                                        state.segments.push(segment);
                                        appendLiveSegment(segment);
                                        appendLiveTranslationBuffer(state, segment);
                                    }
                                } catch (error) {
                                    console.warn("live chunk failed", error);
                                }
                            }
                        } finally {
                            state.chunkSending = false;
                            if ((liveState === state || state.stopping) && state.chunkQueue.length > 0) {
                                drainLiveChunkQueue(state);
                            }
                        }
                    };

                    const waitForLiveQueues = async (state, timeoutMs = 5000) => {
                        const deadline = Date.now() + timeoutMs;
                        while (Date.now() < deadline) {
                            const idle = !state.chunkSending &&
                                state.chunkQueue.length === 0 &&
                                !state.translationSending &&
                                state.translationQueue.length === 0 &&
                                state.pendingTranslations.size === 0;
                            if (idle) return;
                            await sleep(100);
                        }
                    };

                    const waitForLiveChunkQueue = async (state, timeoutMs = 5000) => {
                        const deadline = Date.now() + timeoutMs;
                        while (Date.now() < deadline) {
                            if (!state.chunkSending && state.chunkQueue.length === 0) return;
                            await sleep(100);
                        }
                    };

                    const startLiveSession = async () => {
                        if (liveState) return;
                        liveStartButton.disabled = true;
                        try {
                            const startResp = await fetch("/api/live/start", {
                                method: "POST",
                                headers: { "Content-Type": "application/json" },
                                body: JSON.stringify({
                                    title: "Live",
                                    source_language: liveSourceLanguage.value || null,
                                    target_language: liveTargetLanguage.value || null,
                                    speech_model: liveSpeechModel.value || speechModel.value || null,
                                    translate_model: getEffectiveLiveTranslationModel(),
                                    translate: liveTranslateToggle.checked
                                })
                            });
                            if (!startResp.ok) {
                                const err = await startResp.json().catch(() => ({}));
                                throw new Error(err.error || t("live.startFailed"));
                            }
                            const startJson = await startResp.json();

                            const stream = await getMicStream(buildMicAudioConstraints());
                            const audioCtx = new (window.AudioContext || window.webkitAudioContext)();
                            const source = audioCtx.createMediaStreamSource(stream);
                            const processor = audioCtx.createScriptProcessor(4096, 1, 1);
                            const sourceRate = audioCtx.sampleRate;
                            const resampleRatio = sourceRate / LIVE_SAMPLE_RATE;

                            liveState = {
                                sessionId: startJson.session_id,
                                audioCtx, source, processor, stream,
                                startedAt: Date.now(),
                                audioChunks: [],
                                totalSamples: 0,
                                lastQueuedEndSample: 0,
                                utteranceStartSample: null,
                                lastSpeechSample: 0,
                                chunkQueue: [],
                                chunkSending: false,
                                translationQueue: [],
                                translationSending: false,
                                translationBufferText: "",
                                translationBufferSequences: [],
                                translationFlushTimer: null,
                                pendingTranslations: new Set(),
                                stopping: false,
                                segments: []
                            };

                            processor.onaudioprocess = (event) => {
                                const state = liveState;
                                if (!state || state.processor !== processor) return;
                                const input = event.inputBuffer.getChannelData(0);
                                const outLen = Math.floor(input.length / resampleRatio);
                                if (outLen <= 0) return;
                                const down = new Float32Array(outLen);
                                let energy = 0;
                                for (let i = 0; i < outLen; i++) {
                                    const srcPos = i * resampleRatio;
                                    const idx = Math.floor(srcPos);
                                    const frac = srcPos - idx;
                                    const a = input[idx] || 0;
                                    const b = input[Math.min(idx + 1, input.length - 1)] || 0;
                                    const sample = a + (b - a) * frac;
                                    down[i] = sample;
                                    energy += sample * sample;
                                }
                                const chunkStart = state.totalSamples;
                                const chunkEnd = chunkStart + down.length;
                                const rms = Math.sqrt(energy / down.length);
                                state.audioChunks.push({ start: chunkStart, data: down });
                                state.totalSamples += down.length;
                                updateLiveUtteranceState(state, chunkStart, chunkEnd, rms);
                            };

                            source.connect(processor);
                            processor.connect(audioCtx.destination);

                            // Timer loop; caption chunks are queued by speech/silence detection.
                            const tick = async () => {
                                if (!liveState) return;
                                const elapsed = Date.now() - liveState.startedAt;
                                liveTimer.textContent = formatElapsed(elapsed);
                            };
                            liveState.timerInterval = setInterval(tick, 250);
                            tick();

                            liveStartButton.disabled = true;
                            liveStopButton.disabled = false;
                            liveRecBadge.hidden = false;
                            liveConfigRow.classList.add("is-locked");
                            liveModelRow.classList.add("is-locked");
                            liveCaptions.replaceChildren();
                            updateLiveCurrent(null);
                        } catch (error) {
                            console.error("live start failed", error);
                            showErrorBanner((error && error.message) || t("live.micError"));
                            liveStartButton.disabled = false;
                        }
                    };

                    const stopLiveSession = async ({ prompt = true } = {}) => {
                        if (!liveState) return;
                        const state = liveState;
                        state.stopping = true;

                        if (state.timerInterval) clearInterval(state.timerInterval);
                        try { state.processor.disconnect(); } catch {}
                        try { state.source.disconnect(); } catch {}
                        // Keep the mic stream alive (cachedMicStream) so re-starting
                        // doesn't re-prompt for permission. The AudioContext closes
                        // below will release the per-session processor nodes.
                        try { await state.audioCtx.close(); } catch {}
                        queueLiveUtterance(state, true);
                        await waitForLiveChunkQueue(state, 5000);
                        clearLiveTranslationFlushTimer(state);
                        flushLiveTranslationBuffer(state, true);
                        await waitForLiveQueues(state, 5000);
                        state.stopping = false;
                        if (liveState === state) liveState = null;

                        liveRecBadge.hidden = true;
                        liveStartButton.disabled = false;
                        liveStopButton.disabled = true;
                        liveConfigRow.classList.remove("is-locked");
                        liveModelRow.classList.remove("is-locked");

                        if (state.segments.length === 0) {
                            await fetch("/api/live/stop", {
                                method: "POST",
                                headers: { "Content-Type": "application/json" },
                                body: JSON.stringify({ session_id: state.sessionId, save: false })
                            });
                            return;
                        }

                        if (!prompt) {
                            await fetch("/api/live/stop", {
                                method: "POST",
                                headers: { "Content-Type": "application/json" },
                                body: JSON.stringify({ session_id: state.sessionId, save: false })
                            });
                            return;
                        }

                        // Ask the user whether to save
                        const defaultTitle = new Date(state.startedAt).toLocaleString(currentLocale);
                        liveSaveTitle.value = defaultTitle;
                        liveSaveSummary.textContent = t("live.summary", {
                            count: state.segments.length,
                            duration: formatElapsed(Date.now() - state.startedAt)
                        });
                        liveSaveModal.hidden = false;
                        liveSaveConfirm.onclick = async () => {
                            const title = liveSaveTitle.value.trim() || defaultTitle;
                            liveSaveModal.hidden = true;
                            await fetch("/api/live/stop", {
                                method: "POST",
                                headers: { "Content-Type": "application/json" },
                                body: JSON.stringify({
                                    session_id: state.sessionId,
                                    save: true,
                                    title
                                })
                            });
                            await refreshLiveSessions();
                            showToast(t("status.savedLiveSession"), "success");
                        };
                        liveSaveDiscard.onclick = async () => {
                            liveSaveModal.hidden = true;
                            await fetch("/api/live/stop", {
                                method: "POST",
                                headers: { "Content-Type": "application/json" },
                                body: JSON.stringify({ session_id: state.sessionId, save: false })
                            });
                        };
                        liveSaveClose.onclick = () => {
                            liveSaveModal.hidden = true;
                            // keep segments on screen; session still registered on server but never saved
                            fetch("/api/live/stop", {
                                method: "POST",
                                headers: { "Content-Type": "application/json" },
                                body: JSON.stringify({ session_id: state.sessionId, save: false })
                            });
                        };
                    };

                    liveStartButton.addEventListener("click", () => startLiveSession());
                    liveStopButton.addEventListener("click", () => stopLiveSession({ prompt: true }));
                    [liveSourceLanguage, liveTargetLanguage, liveTranslateToggle, liveSpeechModel, liveTranslateModel].forEach((el) => {
                        el?.addEventListener("change", () => {
                            saveLiveCaptionPrefs();
                            updateLiveModelDisplay();
                        });
                    });

                    const openSettings = () => {
                        settingsModal.hidden = false;
                        document.body.style.overflow = "hidden";
                        populateMicrophoneDevices().catch(() => {});
                        drawMicMonitorIdle();
                        syncMicMonitorForSettings().catch(() => {});
                        refreshAppSettings();
                        refreshSpeechModels();
                        refreshAllModels();
                    };

                    const closeSettings = () => {
                        settingsModal.hidden = true;
                        document.body.style.overflow = "";
                        stopMicMonitor().catch(() => {});
                    };

                    const activateSettingsTab = (tabName) => {
                        settingsTabButtons.forEach((button) => {
                            const selected = button.dataset.tab === tabName;
                            button.setAttribute("aria-selected", String(selected));
                        });
                        settingsPanels.forEach((panel) => {
                            panel.hidden = panel.dataset.panel !== tabName;
                        });
                        syncMicMonitorForSettings().catch(() => {});
                    };

                    currentModelJumpButton.addEventListener("click", () => {
                        activateSettingsTab("models");
                        refreshAllModels();
                    });

                    openSettingsButton.addEventListener("click", openSettings);
                    closeSettingsButton.addEventListener("click", closeSettings);

                    // Close the settings modal on Esc. Captured at the document
                    // level so it works no matter which control inside the modal
                    // has focus. We skip the shortcut-recording inputs — they
                    // already handle Esc to cancel a binding.
                    document.addEventListener("keydown", (event) => {
                        if (event.key !== "Escape") return;
                        if (settingsModal.hidden) return;
                        if (document.activeElement?.classList?.contains("shortcut-input") &&
                            document.activeElement.dataset?.recording === "true") {
                            return;
                        }
                        event.preventDefault();
                        closeSettings();
                    });

                    settingsTabButtons.forEach((button) => {
                        button.addEventListener("click", () => activateSettingsTab(button.dataset.tab));
                    });

                    settingsModal.addEventListener("click", (event) => {
                        if (event.target === settingsModal) {
                            closeSettings();
                        }
                    });


                    // Populate the language <select> with every locale we know about.
                    const populateLanguageSelector = () => {
                        const prev = uiLanguage.value;
                        // Keep only the "auto" option, drop previously-rendered locale options
                        const autoOption = uiLanguage.querySelector('option[value="auto"]');
                        uiLanguage.replaceChildren();
                        if (autoOption) uiLanguage.append(autoOption);
                        Object.keys(TRANSLATIONS).sort().forEach(code => {
                            const dict = TRANSLATIONS[code] || {};
                            const label = dict["locale.name"] || code;
                            const option = document.createElement("option");
                            option.value = code;
                            option.textContent = label;
                            uiLanguage.append(option);
                        });
                        uiLanguage.value = prev || "auto";
                    };
                    populateLanguageSelector();

                    refreshAppSettings();
                    copyLocalesDirButton?.addEventListener("click", async () => {
                        const path = localesDirInput?.value || appSettingsCache.locales_dir || LOCALES_DIR;
                        if (!path) return;
                        if (navigator.clipboard?.writeText) {
                            await navigator.clipboard.writeText(path);
                            showToast(t("settings.appearance.localesCopied"), "success");
                        } else {
                            localesDirInput?.select();
                        }
                    });
                    chooseLocalesDirButton?.addEventListener("click", async () => {
                        try {
                            const path = await pickFolder(t("settings.appearance.language"), localesDirInput?.value || appSettingsCache.locales_dir || LOCALES_DIR);
                            if (!path) return;
                            await saveAppSettings({ locales_dir: path });
                            showToast(t("settings.appearance.localesDirChanged"), "success");
                            setTimeout(() => location.reload(), 450);
                        } catch (error) {
                            showErrorBanner((error && error.message) || "settings update failed");
                        }
                    });
                    resetLocalesDirButton?.addEventListener("click", async () => {
                        try {
                            await saveAppSettings({ locales_dir: null });
                            showToast(t("settings.appearance.localesDirChanged"), "success");
                            setTimeout(() => location.reload(), 450);
                        } catch (error) {
                            showErrorBanner((error && error.message) || "settings update failed");
                        }
                    });

                    // Initialize UI language (from saved preference or OS)
                    const savedLocale = loadLocalePreference();
                    uiLanguage.value = savedLocale;
                    setLocale(savedLocale);

                    // Initialize theme
                    const savedTheme = loadThemePreference();
                    uiTheme.value = savedTheme;
                    applyTheme(savedTheme);
                    uiTheme.addEventListener("change", () => setTheme(uiTheme.value));

                    uiLanguage.addEventListener("change", () => {
                        saveLocalePreference(uiLanguage.value);
                        setLocale(uiLanguage.value);
                        renderModelList();
                        if (!mediaRecorder) {
                            setRecordInfo("");
                        }
                    });

                    updateMainDisplay("");
                    copyMainButton.disabled = true;
                    clearMainButton.disabled = true;

                    // Restore last-selected speech model before first render.
                    // refreshSpeechModels() will validate this against the server response.
                    const savedSpeechModel = loadSpeechModelPreference();
                    if (savedSpeechModel) speechModel.value = savedSpeechModel;

                    const restoreMaxRecordingSeconds = () => {
                        if (!maxRecordingSeconds) return;
                        try {
                            const saved = localStorage.getItem(MAX_RECORDING_SECONDS_KEY);
                            if (saved === null) return;
                            const value = Number(saved);
                            const min = Number(maxRecordingSeconds.min) || 10;
                            const max = Number(maxRecordingSeconds.max) || 7200;
                            if (Number.isFinite(value) && value >= min && value <= max) {
                                maxRecordingSeconds.value = String(value);
                            }
                        } catch {}
                    };
                    const saveMaxRecordingSeconds = () => {
                        if (!maxRecordingSeconds) return;
                        const value = Number(maxRecordingSeconds.value);
                        const min = Number(maxRecordingSeconds.min) || 10;
                        const max = Number(maxRecordingSeconds.max) || 7200;
                        if (!Number.isFinite(value) || value < min || value > max) return;
                        try { localStorage.setItem(MAX_RECORDING_SECONDS_KEY, String(value)); }
                        catch {}
                    };
                    restoreMaxRecordingSeconds();
                    maxRecordingSeconds?.addEventListener("change", saveMaxRecordingSeconds);
                    maxRecordingSeconds?.addEventListener("input", saveMaxRecordingSeconds);

                    populateMicrophoneDevices().catch(() => {});
                    syncMicMonitorLabels();
                    drawMicMonitorIdle();
                    microphoneDevice?.addEventListener("change", () => {
                        saveMicrophonePreference();
                        restartMicMonitorIfRunning().catch(() => {});
                    });
                    microphoneRefreshButton?.addEventListener("click", () => {
                        populateMicrophoneDevices().catch(() => {});
                    });
                    navigator.mediaDevices?.addEventListener?.("devicechange", () => {
                        populateMicrophoneDevices().catch(() => {});
                        restartMicMonitorIfRunning().catch(() => {});
                    });
                    window.addEventListener("seedraft:locale-changed", () => {
                        populateMicrophoneDevices().catch(() => {});
                        syncMicMonitorLabels();
                    });

                    // Restore the last-used recognition language. Empty string
                    // means "auto" (first option) so we don't need to validate.
                    const RECOGNITION_LANG_KEY = "seedraft_recognition_language";
                    const savedLanguage = (() => {
                        try { return localStorage.getItem(RECOGNITION_LANG_KEY); }
                        catch { return null; }
                    })();
                    if (savedLanguage !== null && language) {
                        const exists = Array.from(language.options)
                            .some(o => o.value === savedLanguage);
                        if (exists) language.value = savedLanguage;
                    }
                    restoreLiveCaptionPrefs();
                    populateLiveSpeechModelSelect();
                    populateLiveTranslationModelSelect();
                    updateLiveModelDisplay();

                    // Keep the top-bar recognition language selector persisted.
                    const syncLanguageBadge = () => {
                        if (!currentLanguageBadge || !language) return;
                        currentLanguageBadge.title = t("settings.transcription.language");
                        language.title = t("settings.transcription.language");
                        language.setAttribute("aria-label", t("settings.transcription.language"));
                    };
                    syncLanguageBadge();
                    language?.addEventListener("change", () => {
                        try { localStorage.setItem(RECOGNITION_LANG_KEY, language.value); }
                        catch {}
                        syncLanguageBadge();
                    });
                    // Re-render the badge label when the UI locale changes so the
                    // "自動判定"/"Auto" text stays aligned with the active language.
                    window.addEventListener("seedraft:locale-changed", syncLanguageBadge);

                    // Load speech model statuses and projects on startup.
                    // After projects load, the Notes workspace (list + graph) populates itself.
                    refreshSpeechModels().then(() => requestSpeechModelWarmup(speechModel.value));
                    refreshAllModels();
                    refreshRequirements();
                    (async () => {
                        await refreshProjects();
                        await refreshHistoryView();
                        await renderGraph();
                    })();

                    // ========== Keyboard shortcuts ==========
                    // A fixed list of actions that can be bound to a key combo from
                    // the Settings → Shortcuts tab. Each action has a stable id (used
                    // as the localStorage key), a label key for i18n, and a handler
                    // that runs the matching UI button programmatically so the
                    // behaviour stays in sync with the on-screen control.
                    const SHORTCUT_ACTIONS = [
                        {
                            id: "record.toggle",
                            labelKey: "settings.shortcuts.action.recordToggle",
                            default: "Ctrl+Shift+R",
                            run: () => { recordButton?.click(); }
                        },
                        {
                            id: "record.hold",
                            labelKey: "settings.shortcuts.action.recordHold",
                            default: "",
                            hold: true
                        },
                        {
                            id: "settings.open",
                            labelKey: "settings.shortcuts.action.settingsOpen",
                            default: "Ctrl+,",
                            run: () => { openSettingsButton?.click(); }
                        },
                        {
                            id: "caption.save",
                            labelKey: "settings.shortcuts.action.captionSave",
                            default: "Ctrl+S",
                            run: () => {
                                if (!saveToHistoryButton?.disabled) saveToHistoryButton?.click();
                            }
                        },
                        {
                            id: "caption.copy",
                            labelKey: "settings.shortcuts.action.captionCopy",
                            default: "Ctrl+Shift+C",
                            run: () => {
                                if (!copyMainButton?.disabled) copyMainButton?.click();
                            }
                        },
                        {
                            id: "caption.clear",
                            labelKey: "settings.shortcuts.action.captionClear",
                            default: "Ctrl+Shift+X",
                            run: () => {
                                if (!clearMainButton?.disabled) clearMainButton?.click();
                            }
                        },
                        {
                            id: "linkedContext.toggle",
                            labelKey: "settings.shortcuts.action.linkedContextToggle",
                            default: "",
                            run: () => {
                                if (!linkedContextToggle) return;
                                linkedContextToggle.checked = !linkedContextToggle.checked;
                                linkedContextToggle.dispatchEvent(new Event("change"));
                                showToast(
                                    linkedContextToggle.checked
                                        ? t("settings.linkedContext.enabled")
                                        : t("settings.shortcuts.linkedContextOff"),
                                    "info"
                                );
                            }
                        }
                    ];

                    const SHORTCUTS_KEY = "seedraft_shortcuts_v1";

                    // Build a stable key string from a KeyboardEvent (e.g. "Ctrl+Shift+R").
                    // We intentionally ignore repeats and modifier-only presses so the
                    // user can record a combo in one gesture.
                    const isModifierOnly = (key) =>
                        key === "Control" || key === "Shift" || key === "Alt" || key === "Meta" ||
                        key === "CapsLock" || key === "NumLock";

                    // Mapping from KeyboardEvent.key for modifiers → the token we use
                    // in stored combos (e.g. "Control" → "Ctrl"). Only these keys are
                    // eligible for a double-tap shortcut.
                    const DOUBLE_TAP_LABELS = {
                        Control: "Ctrl",
                        Alt: "Alt",
                        Shift: "Shift",
                        Meta: "Meta"
                    };
                    const DOUBLE_TAP_WINDOW_MS = 500;
                    // Guard against slow taps registering as double-taps. A clean tap
                    // must release within this many ms after pressing.
                    const DOUBLE_TAP_MAX_HOLD_MS = 500;

                    const formatKeyCombo = (event) => {
                        if (isModifierOnly(event.key)) return "";
                        const parts = [];
                        if (event.ctrlKey) parts.push("Ctrl");
                        if (event.altKey) parts.push("Alt");
                        if (event.shiftKey) parts.push("Shift");
                        if (event.metaKey) parts.push("Meta");
                        let key = event.key;
                        if (key === " ") key = "Space";
                        // Single letters: uppercase for readability.
                        if (key.length === 1) key = key.toUpperCase();
                        parts.push(key);
                        return parts.join("+");
                    };

                    const keyToken = (key) => {
                        if (DOUBLE_TAP_LABELS[key]) return DOUBLE_TAP_LABELS[key];
                        if (key === " ") return "Space";
                        return key && key.length === 1 ? key.toUpperCase() : key;
                    };

                    const formatHoldShortcutCombo = (event) => {
                        if (DOUBLE_TAP_LABELS[event.key] && countModsActive(event) === 1) {
                            return DOUBLE_TAP_LABELS[event.key];
                        }
                        return formatKeyCombo(event);
                    };

                    const isShortcutReleaseKey = (event, combo) => {
                        const token = keyToken(event.key);
                        return (combo || "").split("+").includes(token);
                    };

                    // Double-tap detector. Emits a combo like "Ctrl Ctrl" when the same
                    // modifier is pressed and released twice within DOUBLE_TAP_WINDOW_MS
                    // with no other keys held in between.
                    //
                    // Rules for a "clean tap":
                    //   - The modifier is pressed and released alone — no other
                    //     modifier (ctrl/alt/shift/meta) and no other key held.
                    //   - Press → release happens within DOUBLE_TAP_MAX_HOLD_MS.
                    //
                    // Implementation note: we derive "no other modifier held" from
                    // the live `event.ctrlKey / altKey / shiftKey / metaKey` flags
                    // at each event rather than a manually-maintained Set. That
                    // avoids stale state when a `keyup` is swallowed by the OS
                    // (IME, Alt+Tab, native hotkeys), which previously left a
                    // modifier "stuck" and broke every subsequent double-tap.
                    const countModsActive = (event) =>
                        (event.ctrlKey ? 1 : 0) +
                        (event.altKey ? 1 : 0) +
                        (event.shiftKey ? 1 : 0) +
                        (event.metaKey ? 1 : 0);
                    const makeDoubleTapTracker = (onDoubleTap) => {
                        let pendingTap = null; // { key, pressTime } for the active hold
                        let lastTap = null;    // { key, time } of the last completed clean tap
                        return {
                            handleKeydown(event) {
                                if (event.repeat) return;
                                const key = event.key;
                                if (!DOUBLE_TAP_LABELS[key]) {
                                    // A non-modifier press cancels any pending tap.
                                    pendingTap = null;
                                    return;
                                }
                                // On keydown of a modifier, its own flag is already
                                // TRUE. We want exactly ONE modifier active (no
                                // combo like Ctrl+Shift).
                                if (countModsActive(event) !== 1) {
                                    pendingTap = null;
                                    return;
                                }
                                pendingTap = { key, pressTime: Date.now() };
                            },
                            handleKeyup(event) {
                                const key = event.key;
                                if (!DOUBLE_TAP_LABELS[key]) return;
                                if (!pendingTap || pendingTap.key !== key) return;
                                // On keyup of a modifier, its own flag is FALSE.
                                // If any other modifier flag is still set, the
                                // user was holding more than one — not a clean tap.
                                if (countModsActive(event) !== 0) {
                                    pendingTap = null;
                                    return;
                                }
                                const now = Date.now();
                                const hold = now - pendingTap.pressTime;
                                pendingTap = null;
                                if (hold > DOUBLE_TAP_MAX_HOLD_MS) {
                                    lastTap = null;
                                    return;
                                }
                                if (lastTap && lastTap.key === key &&
                                    (now - lastTap.time) <= DOUBLE_TAP_WINDOW_MS) {
                                    const label = DOUBLE_TAP_LABELS[key];
                                    lastTap = null;
                                    onDoubleTap(`${label} ${label}`);
                                } else {
                                    lastTap = { key, time: now };
                                }
                            },
                            reset() {
                                pendingTap = null;
                                lastTap = null;
                            }
                        };
                    };

                    const loadShortcuts = () => {
                        try {
                            const raw = localStorage.getItem(SHORTCUTS_KEY);
                            if (raw) return JSON.parse(raw);
                        } catch {}
                        return null;
                    };

                    // Current bindings: action id → key combo string (or "" for unbound).
                    let shortcutBindings = (() => {
                        const saved = loadShortcuts() || {};
                        const out = {};
                        for (const action of SHORTCUT_ACTIONS) {
                            out[action.id] = saved[action.id] !== undefined ? saved[action.id] : action.default;
                        }
                        return out;
                    })();

                    const persistShortcuts = () => {
                        try {
                            localStorage.setItem(SHORTCUTS_KEY, JSON.stringify(shortcutBindings));
                        } catch {}
                    };

                    // Inverse map for fast dispatch (combo → action id). Rebuilt on
                    // every change so duplicate combos are resolved in registration order.
                    let shortcutByCombo = new Map();
                    const rebuildShortcutIndex = () => {
                        shortcutByCombo = new Map();
                        for (const action of SHORTCUT_ACTIONS) {
                            if (action.hold) continue;
                            const combo = (shortcutBindings[action.id] || "").trim();
                            if (!combo) continue;
                            if (!shortcutByCombo.has(combo)) {
                                shortcutByCombo.set(combo, action);
                            }
                        }
                    };
                    rebuildShortcutIndex();

                    // Whether the event target is an editable text control. We skip
                    // single-key shortcuts when the user is typing; combos with Ctrl/Alt/Meta
                    // still fire so Ctrl+S works inside textareas.
                    const isTypingTarget = (target) => {
                        if (!target) return false;
                        const tag = (target.tagName || "").toLowerCase();
                        if (tag === "input" || tag === "textarea") return true;
                        if (target.isContentEditable) return true;
                        return false;
                    };

                    const runShortcutAction = (action) => {
                        try { action.run(); } catch (error) { console.warn("shortcut failed", error); }
                    };

                    const shortcutOverlayOpen = () =>
                        !!document.querySelector(".note-editor-modal:not([hidden]), .settings-modal:not([hidden]), .fullscreen-overlay:not([hidden])");

                    const HOLD_RECORD_DELAY_MS = 220;
                    const holdRecordState = {
                        combo: "",
                        timer: null,
                        started: false,
                        stopRequested: false
                    };

                    const holdRecordCombo = () => (shortcutBindings["record.hold"] || "").trim();

                    const stopHoldRecordingWhenReady = (attempt = 0) => {
                        if (mediaRecorder?.state === "recording") {
                            recordButton?.click();
                            holdRecordState.started = false;
                            holdRecordState.stopRequested = false;
                            holdRecordState.combo = "";
                            return;
                        }
                        if (attempt < 40 && holdRecordState.started) {
                            setTimeout(() => stopHoldRecordingWhenReady(attempt + 1), 50);
                            return;
                        }
                        holdRecordState.started = false;
                        holdRecordState.stopRequested = false;
                        holdRecordState.combo = "";
                    };

                    const cancelHoldRecording = () => {
                        if (holdRecordState.timer) {
                            clearTimeout(holdRecordState.timer);
                            holdRecordState.timer = null;
                        }
                        if (holdRecordState.started) {
                            holdRecordState.stopRequested = true;
                            stopHoldRecordingWhenReady();
                        } else {
                            holdRecordState.combo = "";
                            holdRecordState.stopRequested = false;
                        }
                    };

                    const scheduleHoldRecording = (combo) => {
                        if (!combo || holdRecordState.timer || holdRecordState.started) return;
                        if (mediaRecorder?.state === "recording") return;
                        holdRecordState.combo = combo;
                        holdRecordState.stopRequested = false;
                        holdRecordState.timer = setTimeout(() => {
                            holdRecordState.timer = null;
                            holdRecordState.started = true;
                            recordButton?.click();
                            if (holdRecordState.stopRequested) {
                                stopHoldRecordingWhenReady();
                            }
                        }, HOLD_RECORD_DELAY_MS);
                    };

                    document.addEventListener("keydown", (event) => {
                        if (document.activeElement?.classList?.contains("shortcut-input") &&
                            document.activeElement.dataset?.recording === "true") {
                            return;
                        }
                        const holdCombo = holdRecordCombo();
                        if (holdCombo && !event.repeat && !shortcutOverlayOpen() && !isTypingTarget(event.target) &&
                            formatHoldShortcutCombo(event) === holdCombo) {
                            event.preventDefault();
                            scheduleHoldRecording(holdCombo);
                            return;
                        }
                    });

                    document.addEventListener("keyup", (event) => {
                        if (!holdRecordState.combo) return;
                        if (isShortcutReleaseKey(event, holdRecordState.combo)) {
                            event.preventDefault();
                            cancelHoldRecording();
                        }
                    });

                    document.addEventListener("keydown", (event) => {
                        if (event.key !== "Delete") return;
                        if (event.repeat || event.ctrlKey || event.altKey || event.metaKey) return;
                        if (isTypingTarget(event.target)) return;
                        if (!highlightedNoteId) return;
                        if (shortcutOverlayOpen()) return;
                        event.preventDefault();
                        event.stopImmediatePropagation();
                        deleteNoteById(highlightedNoteId);
                    });

                    const globalDoubleTap = makeDoubleTapTracker((combo) => {
                        const action = shortcutByCombo.get(combo);
                        if (!action) return;
                        // Don't fire double-tap shortcuts while the user is typing
                        // — they may hit a modifier twice accidentally. Single-key
                        // modifier taps would also be annoyingly eager otherwise.
                        if (isTypingTarget(document.activeElement)) return;
                        runShortcutAction(action);
                    });

                    document.addEventListener("keydown", (event) => {
                        // Skip while the user is recording a new binding — the recording
                        // UI handles its own keydown.
                        if (document.activeElement?.classList?.contains("shortcut-input") &&
                            document.activeElement.dataset?.recording === "true") {
                            return;
                        }
                        globalDoubleTap.handleKeydown(event);
                        if (event.repeat) return;
                        const combo = formatKeyCombo(event);
                        if (!combo) return;
                        const action = shortcutByCombo.get(combo);
                        if (!action) return;
                        // Single-key (no modifier) shortcuts shouldn't steal keystrokes
                        // from normal typing.
                        const hasModifier = event.ctrlKey || event.altKey || event.metaKey;
                        if (!hasModifier && isTypingTarget(event.target)) return;
                        event.preventDefault();
                        runShortcutAction(action);
                    });

                    document.addEventListener("keyup", (event) => {
                        if (document.activeElement?.classList?.contains("shortcut-input") &&
                            document.activeElement.dataset?.recording === "true") {
                            return;
                        }
                        globalDoubleTap.handleKeyup(event);
                    });
                    // Any time the window loses focus, drop the pending tap state
                    // so a tap-focus-change-tap sequence isn't misread as a double.
                    window.addEventListener("blur", () => {
                        globalDoubleTap.reset();
                        cancelHoldRecording();
                    });

                    // ---- Shortcut settings UI ----
                    const shortcutList = document.getElementById("shortcutList");
                    const shortcutResetButton = document.getElementById("shortcutResetButton");

                    const renderShortcutList = () => {
                        if (!shortcutList) return;
                        shortcutList.replaceChildren();
                        for (const action of SHORTCUT_ACTIONS) {
                            const row = document.createElement("div");
                            row.className = "shortcut-row";

                            const label = document.createElement("div");
                            label.className = "shortcut-row-label";
                            label.textContent = t(action.labelKey);

                            const input = document.createElement("input");
                            input.type = "text";
                            input.className = "shortcut-input";
                            input.readOnly = true;
                            input.value = shortcutBindings[action.id] || "";
                            input.placeholder = t("settings.shortcuts.unset");
                            input.title = t("settings.shortcuts.recordTooltip");
                            // Shared routine: attempt to commit a combo. Returns true
                            // when the input is about to blur (success or clash handled).
                            const tryBindCombo = (combo) => {
                                if (!combo) return false;
                                const clash = SHORTCUT_ACTIONS.find(a =>
                                    a.id !== action.id && (shortcutBindings[a.id] || "") === combo
                                );
                                if (clash) {
                                    input.value = combo + "  ⚠";
                                    input.title = t("settings.shortcuts.clash", { label: t(clash.labelKey) });
                                    return false;
                                }
                                shortcutBindings[action.id] = combo;
                                input.value = combo;
                                input.title = t("settings.shortcuts.recordTooltip");
                                persistShortcuts();
                                rebuildShortcutIndex();
                                delete input.dataset.recording;
                                delete input.dataset.originalValue;
                                input.blur();
                                return true;
                            };

                            // Per-input double-tap tracker, active only while recording.
                            const inputDoubleTap = makeDoubleTapTracker((combo) => {
                                tryBindCombo(combo);
                            });

                            input.addEventListener("focus", () => {
                                input.dataset.recording = "true";
                                input.dataset.originalValue = input.value;
                                input.value = t("settings.shortcuts.recording");
                                inputDoubleTap.reset();
                            });
                            input.addEventListener("blur", () => {
                                if (input.dataset.recording === "true") {
                                    // User left the field without recording — restore previous value.
                                    input.value = input.dataset.originalValue || "";
                                }
                                delete input.dataset.recording;
                                delete input.dataset.originalValue;
                                inputDoubleTap.reset();
                            });
                            input.addEventListener("keydown", (event) => {
                                // Allow the user to cancel / clear bindings.
                                if (event.key === "Escape") {
                                    event.preventDefault();
                                    input.value = input.dataset.originalValue || "";
                                    input.blur();
                                    return;
                                }
                                if (event.key === "Delete" || event.key === "Backspace") {
                                    event.preventDefault();
                                    shortcutBindings[action.id] = "";
                                    input.value = "";
                                    persistShortcuts();
                                    rebuildShortcutIndex();
                                    // Clear the recording flag BEFORE blur so the
                                    // blur handler doesn't restore the previous
                                    // value and undo our unbind.
                                    delete input.dataset.recording;
                                    delete input.dataset.originalValue;
                                    input.blur();
                                    return;
                                }
                                inputDoubleTap.handleKeydown(event);
                                if (action.hold && DOUBLE_TAP_LABELS[event.key]) {
                                    event.preventDefault();
                                    event.stopPropagation();
                                    tryBindCombo(DOUBLE_TAP_LABELS[event.key]);
                                    return;
                                }
                                // Modifier presses don't commit a normal combo — they
                                // might be the first half of a double-tap, which is
                                // resolved in keyup.
                                if (isModifierOnly(event.key)) return;
                                event.preventDefault();
                                event.stopPropagation();
                                tryBindCombo(formatKeyCombo(event));
                            });
                            input.addEventListener("keyup", (event) => {
                                inputDoubleTap.handleKeyup(event);
                            });

                            row.append(label, input);
                            shortcutList.append(row);
                        }
                    };
                    renderShortcutList();

                    shortcutResetButton?.addEventListener("click", () => {
                        for (const action of SHORTCUT_ACTIONS) {
                            shortcutBindings[action.id] = action.default;
                        }
                        persistShortcuts();
                        rebuildShortcutIndex();
                        renderShortcutList();
                    });

                    // Re-render when the UI language changes so the labels follow suit.
                    window.addEventListener("seedraft:locale-changed", renderShortcutList);

                    // Drag and drop on entire window
                    // Use a drag-enter/leave counter so that transitions between
                    // child elements don't close the overlay prematurely.
                    let dragDepth = 0;
                    const showDropOverlay = () => {
                        dropOverlay.style.display = "flex";
                    };
                    const hideDropOverlay = () => {
                        dropOverlay.style.display = "none";
                        dragDepth = 0;
                    };

                    document.addEventListener("dragenter", (event) => {
                        if (!event.dataTransfer || !Array.from(event.dataTransfer.types || []).includes("Files")) return;
                        event.preventDefault();
                        dragDepth += 1;
                        showDropOverlay();
                    });

                    document.addEventListener("dragover", (event) => {
                        if (!event.dataTransfer || !Array.from(event.dataTransfer.types || []).includes("Files")) return;
                        event.preventDefault();
                        event.dataTransfer.dropEffect = "copy";
                    });

                    document.addEventListener("dragleave", (event) => {
                        dragDepth = Math.max(0, dragDepth - 1);
                        if (dragDepth === 0) hideDropOverlay();
                    });

                    const AUDIO_EXTENSIONS = ["wav", "mp3", "m4a", "mp4", "webm", "ogg", "flac", "aac", "opus"];
                    const isAudioFile = (file) => {
                        if (file.type && file.type.startsWith("audio/")) return true;
                        const name = (file.name || "").toLowerCase();
                        const ext = name.includes(".") ? name.split(".").pop() : "";
                        return AUDIO_EXTENSIONS.includes(ext);
                    };

                    document.addEventListener("drop", async (event) => {
                        // Ignore non-file drops (e.g. note reordering inside the sidebar).
                        // Their own handlers have already handled the event.
                        const types = Array.from(event.dataTransfer?.types || []);
                        if (!types.includes("Files")) return;

                        event.preventDefault();
                        hideDropOverlay();
                        hideErrorBanner();

                        try {
                            const file = event.dataTransfer?.files?.[0];
                            if (!file) {
                                showErrorBanner("No file detected in drop event");
                                return;
                            }

                            if (!isAudioFile(file)) {
                                const msg = `Not an audio file: ${file.name || "(unknown)"} (type="${file.type || "unknown"}")`;
                                setStatus(t("status.notAudioFile"), "error");
                                setRecordInfo(msg);
                                showErrorBanner(msg);
                                return;
                            }

                            recordedBlob = null;
                            // Do NOT rely on DataTransfer/audioFile.files — pass the file directly.
                            setStatus(t("status.transcribingStart"), "busy");
                            setRecordInfo(t("record.processing", { name: file.name }));
                            await transcribeAudio(file, file.name);
                        } catch (error) {
                            console.error("drop handler failed", error);
                            showErrorBanner((error && error.stack) || (error && error.message) || String(error));
                        }
                    });

                    recordButton.addEventListener("click", async () => {
                        if (mediaRecorder && mediaRecorder.state === "recording") {
                            mediaRecorder.stop();
                            setStatus(t("status.processing"), "busy");
                            const recordLabel = recordButton.querySelector(".record-label");
                            recordLabel.textContent = t("button.record.start");
                            recordButton.dataset.recording = "false";
                            stopRecordCountdown();
                            return;
                        }

                        try {
                            if (!navigator.mediaDevices?.getUserMedia || !window.MediaRecorder) {
                                throw new Error(t("status.micUnavailable"));
                            }

                            const requestedSampleRate = Number(sampleRate.value);
                            const audioConstraints = buildMicAudioConstraints(
                                Number.isFinite(requestedSampleRate) && requestedSampleRate > 0
                                    ? { sampleRate: requestedSampleRate }
                                    : {}
                            );
                            const stream = await getMicStream(audioConstraints);
                            recordedChunks = [];
                            mediaRecorder = new MediaRecorder(stream);

                            mediaRecorder.addEventListener("dataavailable", (event) => {
                                if (event.data.size > 0) recordedChunks.push(event.data);
                            });

                            mediaRecorder.addEventListener("stop", async () => {
                                if (recordingTimer) {
                                    clearTimeout(recordingTimer);
                                    recordingTimer = null;
                                }
                                stopRecordCountdown();
                                const mimeType = mediaRecorder.mimeType || "audio/webm";
                                const rawBlob = new Blob(recordedChunks, { type: mimeType });
                                // Keep the cached mic stream alive so subsequent
                                // recordings don't re-trigger the permission prompt.
                                mediaRecorder = null;
                                audioFile.value = "";
                                recordButton.dataset.recording = "false";

                                try {
                                    recordedBlob = await convertBlobToWav(rawBlob);
                                    recordedFileName = "recording.wav";
                                } catch (error) {
                                    console.warn("WAV conversion failed, falling back to raw blob", error);
                                    recordedBlob = rawBlob;
                                    recordedFileName = mimeType.includes("mp4") ? "recording.m4a" : "recording.webm";
                                }

                                const sizeKB = Math.round(recordedBlob.size / 1024);
                                setRecordInfo(t("record.completed", { size: sizeKB }));
                                setStatus(t("status.transcribingStart"), "busy");

                                await transcribeAudio();
                            });

                            mediaRecorder.start();
                            const recordLabel = recordButton.querySelector(".record-label");
                            recordLabel.textContent = t("button.record.stop");
                            recordButton.dataset.recording = "true";
                            setRecordInfo(t("record.recording"));
                            setStatus(t("status.recording"), "busy");

                            const maxSeconds = Number(maxRecordingSeconds.value);
                            if (Number.isFinite(maxSeconds) && maxSeconds > 0) {
                                startRecordCountdown(maxSeconds);
                                recordingTimer = setTimeout(() => {
                                    if (mediaRecorder?.state === "recording") {
                                        mediaRecorder.stop();
                                        setStatus(t("status.recordingLimit"), "busy");
                                    }
                                }, maxSeconds * 1000);
                            }
                        } catch (error) {
                            setStatus(t("status.micUnavailable"), "error");
                            setRecordInfo(error.message || t("status.micUnavailable"));
                        }
                    });

                    const transcribeAudio = async (explicitFile, explicitName) => {
                        hideErrorBanner();
                        const selectedFile = explicitFile || audioFile.files?.[0] || recordedBlob;
                        const sourceName = explicitName
                            || (explicitFile && explicitFile.name)
                            || (audioFile.files?.[0] && audioFile.files[0].name)
                            || recordedFileName
                            || "audio";
                        if (!selectedFile) {
                            setStatus(t("status.notSelected"), "error");
                            setRecordInfo(t("record.needAudio"));
                            showErrorBanner(t("record.needAudio"));
                            return;
                        }

                        if (!selectedFile.size) {
                            const msg = `Empty file: ${sourceName}`;
                            setStatus(t("status.error"), "error");
                            setRecordInfo(msg);
                            showErrorBanner(msg);
                            return;
                        }

                        const formData = new FormData();
                        formData.append("language", language.value);
                        formData.append("speech_model", speechModel.value);
                        formData.append("transcription_prompt", transcriptionPrompt.value);
                        if (currentProjectId) formData.append("project_id", currentProjectId);
                        const parentNoteId = selectedGraphParentNoteId();
                        if (parentNoteId) formData.append("parent_id", parentNoteId);
                        // Tell the backend whether to persist this transcription as
                        // a note. Controlled by the "📥 ノートに追加" quick toggle —
                        // off means the text only shows on screen.
                        formData.append("save_as_note", quickAutoSave?.checked ? "true" : "false");
                        formData.append("audio", selectedFile, sourceName);

                        setBusy(true);
                        setStatus(t("status.transcribing"), "busy");
                        setRecordInfo(t("record.modelWarmup"));

                        try {
                            const response = await fetch("/api/transcribe", {
                                method: "POST",
                                body: formData,
                            });
                            const rawBody = await response.text();
                            let payload = {};
                            try {
                                payload = rawBody ? JSON.parse(rawBody) : {};
                            } catch {
                                payload = { error: rawBody };
                            }

                            if (!response.ok) {
                                throw new Error(payload.error || t("record.failedTranscribe"));
                            }

                            const transcribedText = payload.text || "";
                            lastTranscriptionDuration = payload.duration || 0;
                            updateMainDisplay(transcribedText);
                            lastTranscribedNoteId = payload.note_id || null;
                            if (lastTranscribedNoteId) {
                                highlightedNoteId = lastTranscribedNoteId;
                                highlightedGraphNodeId = `note:${lastTranscribedNoteId}`;
                            }

                            setRecordInfo("");
                            showToast(t("record.done", { name: sourceName }), "success");
                            setStatus(t("status.done"), "ready");
                            refreshSpeechModels();
                            await refreshHistoryView();
                            if (lastTranscribedNoteId) await renderGraph();

                            // Enablement is driven entirely by the quick toggles now —
                            // `getPostProcessingConfig` reads their state.
                            const config = getPostProcessingConfig();
                            const shouldAutoProcess = config.refinement.enabled || config.translation.enabled;

                            if (shouldAutoProcess && transcribedText.trim()) {
                                await executeAutoPostProcessing(config);
                            }

                            // Run analysis when its quick toggle is on. Analysis does not
                            // modify the caption text — results land on the active note, or
                            // are surfaced via a toast if no note is linked yet.
                            if (quickAutoAnalyze?.checked && captionContent.value.trim()) {
                                try {
                                    await runAutoAnalysis();
                                } catch (error) {
                                    console.warn("auto-analysis failed", error);
                                }
                            }

                            // Run custom post-processing steps only when the quick toggle is on.
                            // Each step's own `auto` flag still filters which ones actually run,
                            // so users can prepare several steps but keep some manual.
                            if (quickAutoCustom?.checked && captionContent.value.trim()) {
                                const autoCustomSteps = customSteps.filter(s => s.auto);
                                for (const step of autoCustomSteps) {
                                    await runCustomStep(step);
                                }
                            }

                            // Auto-save: if the quick toggle is on, push the current caption to notes.
                            if (quickAutoSave?.checked && captionContent.value.trim() && currentProjectId) {
                                try {
                                    saveToHistoryButton.click();
                                } catch (_) {}
                            }
                        } catch (error) {
                            console.error("transcribe failed", error);
                            setStatus(t("status.error"), "error");
                            showErrorBanner((error && error.message) || t("record.failedTranscribe"));
                            setRecordInfo((error && error.message) || t("record.failedTranscribe"));
                        } finally {
                            setBusy(false);
                        }
                    };

                    copyMainButton.addEventListener("click", async () => {
                        const value = captionContent.value;
                        if (!value) return;
                        if (navigator.clipboard?.writeText) {
                            await navigator.clipboard.writeText(value);
                            showToast(t("status.copied"), "success");
                        }
                    });

                    clearMainButton.addEventListener("click", () => {
                        updateMainDisplay("");
                        setRecordInfo("");
                        setStatus(t("status.idle"), "idle");
                    });

                    saveToHistoryButton.addEventListener("click", async () => {
                        const text = captionContent.value.trim();
                        if (!text) {
                            setStatus(t("status.emptyText"), "error");
                            return;
                        }
                        if (!currentProjectId) {
                            setStatus(t("status.noProject"), "error");
                            return;
                        }
                        try {
                            if (lastTranscribedNoteId) {
                                // Update the current note with edited content
                                const response = await fetch(`/api/notes/${lastTranscribedNoteId}`, {
                                    method: "PUT",
                                    headers: { "Content-Type": "application/json" },
                                    body: JSON.stringify({
                                        title: "",
                                        text,
                                        meta: null
                                    })
                                });
                                if (!response.ok) {
                                    const err = await response.json().catch(() => ({}));
                                    throw new Error(err.error || "save failed");
                                }
                            } else {
                                const parentNoteId = selectedGraphParentNoteId();
                                // No linked note yet: create a manual note via transcribe alternative.
                                // We use an internal POST to /api/notes is not available; re-use the notes
                                // creation path via a synthetic transcription record is overkill. Instead,
                                // POST a minimal request: create a note by calling the update endpoint on a
                                // newly-created dummy entry would also be overkill. So we create via the
                                // projects DB path using the transcribe endpoint is not applicable either.
                                // Simpler: add a dedicated endpoint is ideal, but we can use the update path
                                // after creating a note record through a new endpoint. For now, call a
                                // fallback endpoint:
                                const response = await fetch(`/api/notes`, {
                                    method: "POST",
                                    headers: { "Content-Type": "application/json" },
                                    body: JSON.stringify({
                                        project_id: currentProjectId,
                                        title: text.slice(0, 40),
                                        text,
                                        parent_id: parentNoteId
                                    })
                                });
                                if (!response.ok) {
                                    const err = await response.json().catch(() => ({}));
                                    throw new Error(err.error || "save failed");
                                }
                                const created = await response.json();
                                lastTranscribedNoteId = created.id || null;
                                if (lastTranscribedNoteId) {
                                    highlightedNoteId = lastTranscribedNoteId;
                                    highlightedGraphNodeId = `note:${lastTranscribedNoteId}`;
                                }
                            }
                            currentText = text;
                            updateDirty(false);
                            await refreshHistoryView();
                            await renderGraph();
                            showToast(t("status.savedToHistory"), "success");
                        } catch (error) {
                            console.error("save to history failed", error);
                            showErrorBanner((error && error.message) || "save failed");
                            setStatus(t("status.error"), "error");
                        }
                    });

                    // Decode arbitrary audio Blob to 16kHz mono PCM16 WAV.
                    // Foundry Local AudioDecoder cannot sniff WebM/Opus, so we hand it WAV.
                    const convertBlobToWav = async (blob) => {
                        const AudioCtx = window.AudioContext || window.webkitAudioContext;
                        if (!AudioCtx) throw new Error("AudioContext is not available");

                        const arrayBuffer = await blob.arrayBuffer();
                        const decodeCtx = new AudioCtx();
                        const decoded = await new Promise((resolve, reject) => {
                            decodeCtx.decodeAudioData(
                                arrayBuffer.slice(0),
                                (buffer) => resolve(buffer),
                                (error) => reject(error || new Error("decodeAudioData failed"))
                            );
                        });

                        // Downmix to mono
                        const sourceChannels = decoded.numberOfChannels;
                        const sourceLength = decoded.length;
                        const mono = new Float32Array(sourceLength);
                        for (let ch = 0; ch < sourceChannels; ch++) {
                            const data = decoded.getChannelData(ch);
                            for (let i = 0; i < sourceLength; i++) mono[i] += data[i];
                        }
                        if (sourceChannels > 1) {
                            for (let i = 0; i < sourceLength; i++) mono[i] /= sourceChannels;
                        }

                        // Resample to 16kHz using linear interpolation
                        const targetRate = 16000;
                        const sourceRate = decoded.sampleRate;
                        let pcm;
                        if (sourceRate === targetRate) {
                            pcm = mono;
                        } else {
                            const ratio = sourceRate / targetRate;
                            const outLength = Math.floor(sourceLength / ratio);
                            pcm = new Float32Array(outLength);
                            for (let i = 0; i < outLength; i++) {
                                const srcPos = i * ratio;
                                const idx = Math.floor(srcPos);
                                const frac = srcPos - idx;
                                const a = mono[idx] || 0;
                                const b = mono[Math.min(idx + 1, sourceLength - 1)] || 0;
                                pcm[i] = a + (b - a) * frac;
                            }
                        }

                        decodeCtx.close?.();

                        // Encode WAV (PCM 16-bit mono)
                        const numSamples = pcm.length;
                        const bytesPerSample = 2;
                        const dataSize = numSamples * bytesPerSample;
                        const buffer = new ArrayBuffer(44 + dataSize);
                        const view = new DataView(buffer);

                        const writeString = (offset, value) => {
                            for (let i = 0; i < value.length; i++) {
                                view.setUint8(offset + i, value.charCodeAt(i));
                            }
                        };

                        writeString(0, "RIFF");
                        view.setUint32(4, 36 + dataSize, true);
                        writeString(8, "WAVE");
                        writeString(12, "fmt ");
                        view.setUint32(16, 16, true);           // fmt chunk size
                        view.setUint16(20, 1, true);            // PCM
                        view.setUint16(22, 1, true);            // mono
                        view.setUint32(24, targetRate, true);   // sample rate
                        view.setUint32(28, targetRate * bytesPerSample, true); // byte rate
                        view.setUint16(32, bytesPerSample, true); // block align
                        view.setUint16(34, 16, true);           // bits per sample
                        writeString(36, "data");
                        view.setUint32(40, dataSize, true);

                        let offset = 44;
                        for (let i = 0; i < numSamples; i++) {
                            const sample = Math.max(-1, Math.min(1, pcm[i]));
                            view.setInt16(offset, sample < 0 ? sample * 0x8000 : sample * 0x7FFF, true);
                            offset += 2;
                        }

                        return new Blob([buffer], { type: "audio/wav" });
                    };

                    const safeFileName = (name) =>
                        (name || "seedraft").trim().replace(/[\\/:*?"<>|]+/g, "-") || "seedraft";

                    const writeTextFile = async (text, suffix = "txt") => {
                        const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
                        const fileName = `${safeFileName(filePrefix.value)}-${timestamp}.${suffix}`;
                        if (saveDirectoryHandle?.getFileHandle) {
                            const fileHandle = await saveDirectoryHandle.getFileHandle(fileName, { create: true });
                            const writable = await fileHandle.createWritable();
                            await writable.write(text);
                            await writable.close();
                            return;
                        }

                        const blob = new Blob([text], { type: "text/plain;charset=utf-8" });
                        const url = URL.createObjectURL(blob);
                        const link = document.createElement("a");
                        link.href = url;
                        link.download = fileName;
                        link.click();
                        URL.revokeObjectURL(url);
                    };

                    chooseSaveFolderButton.addEventListener("click", async () => {
                        if (!window.showDirectoryPicker) {
                            setStatus(t("status.folderUnsupported"), "error");
                            setRecordInfo(t("record.folderUnsupportedMsg"));
                            return;
                        }

                        try {
                            saveDirectoryHandle = await window.showDirectoryPicker();
                            saveFolder.value = saveDirectoryHandle.name;
                            showToast(t("status.folderSet"), "success");
                        } catch (error) {
                            if (error?.name !== "AbortError") {
                                setStatus(t("status.folderError"), "error");
                                setRecordInfo(error.message || t("status.folderError"));
                            }
                        }
                    });

                    // Invoked from the post-transcription auto pipeline when the analyze
                    // quick toggle is on. Runs a tone / summary / keyword extraction and
                    // reports the result via a toast — the caption area is not replaced.
                    const runAutoAnalysis = async () => {
                        const text = captionContent.value.trim();
                        if (!text) return;
                        try {
                            const response = await fetch("/api/analyze", {
                                method: "POST",
                                headers: { "Content-Type": "application/json" },
                                body: JSON.stringify({
                                    text,
                                    model: analyzeModel?.value || undefined,
                                    note_id: lastTranscribedNoteId || undefined,
                                    use_linked_context: linkedContextEnabled()
                                })
                            });
                            if (!response.ok) {
                                const err = await response.json().catch(() => ({}));
                                throw new Error(err.error || t("note.analysisFailed"));
                            }
                            const payload = await response.json();
                            const tone = payload.tone || "";
                            const summary = payload.summary || "";
                            const toastLines = [tone ? `${t("note.analysis.tone")}: ${tone}` : "", summary]
                                .filter(Boolean)
                                .join(" — ");
                            if (toastLines) showToast(toastLines, "info", 4200);
                        } catch (error) {
                            showToast((error && error.message) || t("note.analysisFailed"), "warning");
                        }
                    };

                    const executeAutoPostProcessing = async (config) => {
                        const pipeline = [];

                        if (config.refinement.enabled) {
                            pipeline.push({
                                type: 'refine',
                                text: currentText,
                                remove_fillers: config.refinement.removeFillers,
                                voice_commands: config.refinement.voiceCommands,
                                custom_terms: config.refinement.customTerms,
                                custom_instruction: config.refinement.customInstruction,
                                model: config.refinement.model || undefined
                            });
                        }

                        // Translation runs when the quick toggle is on. If refinement is
                        // also enabled it naturally happens after refinement because the
                        // pipeline is executed in insertion order.
                        if (config.translation.enabled) {
                            {
                                pipeline.push({
                                    type: 'translate',
                                    text: currentText,
                                    source_language: config.translation.sourceLanguage,
                                    target_language: config.translation.targetLanguage,
                                    custom_terms: config.refinement.customTerms,
                                    custom_instruction: config.translation.customInstruction,
                                    model: config.translation.model || undefined
                                });
                            }
                        }

                        if (pipeline.length === 0) {
                            return;
                        }

                        await executePipeline(pipeline, true);
                    };

                    const executePipeline = async (pipeline, automatic = false) => {
                        if (pipeline.length === 0) {
                            setStatus(t("status.noProcess"), "error");
                            return;
                        }

                        setBusy(true);
                        setStatus(t("status.postprocessing"), "busy");
                        const stepNames = pipeline.map(step =>
                            step.type === 'refine' ? t("step.refine") : t("step.translate")
                        ).join(' → ');
                        setRecordInfo(t("record.stepsRunning", { steps: stepNames }));

                        try {
                            const response = await fetch("/api/process", {
                                method: "POST",
                                headers: { "Content-Type": "application/json" },
                                body: JSON.stringify({
                                    text: currentText,
                                    pipeline,
                                    note_id: lastTranscribedNoteId || undefined,
                                    use_linked_context: linkedContextEnabled()
                                }),
                            });
                            const rawBody = await response.text();
                            let payload = {};
                            try {
                                payload = rawBody ? JSON.parse(rawBody) : {};
                            } catch {
                                payload = { error: rawBody };
                            }

                            if (!response.ok) {
                                throw new Error(payload.error || t("record.failedPostprocess"));
                            }

                            const results = payload.results || [];
                            for (const result of results) {
                                if (result.step_type === 'refine') {
                                    updateMainDisplay(result.text);
                                    // Update the existing note with refined text
                                    if (lastTranscribedNoteId) {
                                        try {
                                            await fetch(`/api/notes/${lastTranscribedNoteId}`, {
                                                method: "PUT",
                                                headers: { "Content-Type": "application/json" },
                                                body: JSON.stringify({
                                                    title: "",
                                                    text: result.text,
                                                    meta: t("meta.refined")
                                                })
                                            });
                                        } catch (error) {
                                            console.warn("refine update failed", error);
                                        }
                                    }
                                } else if (result.step_type === 'translate') {
                                    // Persist translation
                                    if (currentProjectId) {
                                        try {
                                            await fetch("/api/translations", {
                                                method: "POST",
                                                headers: { "Content-Type": "application/json" },
                                                body: JSON.stringify({
                                                    note_id: lastTranscribedNoteId,
                                                    source_text: currentText,
                                                    source_language: sourceLanguage.value,
                                                    target_language: targetLanguage.value
                                                })
                                            });
                                        } catch (error) {
                                            console.warn("translation persist failed", error);
                                        }
                                    }
                                }
                            }

                            refreshHistoryView();

                            setRecordInfo("");
                            showToast(t("record.stepsDone", { steps: stepNames }), "success");
                            setStatus(t("status.postprocessDone"), "ready");
                        } catch (error) {
                            setStatus(t("status.error"), "error");
                            setRecordInfo(error.message || t("record.failedPostprocess"));
                        } finally {
                            setBusy(false);
                        }
                    };

                    // ========== Custom post-processing steps ==========
                    const CUSTOM_STEPS_KEY = "seedraft_custom_steps";
                    const CUSTOM_STEPS_SEEDED_KEY = "seedraft_custom_steps_seeded_v1";
                    const QUICK_TOGGLE_KEY = "seedraft_quick_toggles";

                    // Built-in style presets. `id` is stable so that re-adding
                    // from the "プリセット" menu upgrades an existing entry
                    // instead of creating a duplicate. Names / instructions
                    // resolve through i18n at display time.
                    const CUSTOM_STEP_PRESETS = [
                        {
                            id: "preset.polite",
                            nameKey: "settings.custom.preset.polite.name",
                            instructionKey: "settings.custom.preset.polite.instruction",
                            auto: false
                        },
                        {
                            id: "preset.business",
                            nameKey: "settings.custom.preset.business.name",
                            instructionKey: "settings.custom.preset.business.instruction",
                            auto: false
                        },
                        {
                            id: "preset.minutes",
                            nameKey: "settings.custom.preset.minutes.name",
                            instructionKey: "settings.custom.preset.minutes.instruction",
                            auto: false
                        },
                        {
                            id: "preset.casual",
                            nameKey: "settings.custom.preset.casual.name",
                            instructionKey: "settings.custom.preset.casual.instruction",
                            auto: false
                        },
                        {
                            id: "preset.summary",
                            nameKey: "settings.custom.preset.summary.name",
                            instructionKey: "settings.custom.preset.summary.instruction",
                            auto: false
                        },
                        {
                            id: "preset.bullets",
                            nameKey: "settings.custom.preset.bullets.name",
                            instructionKey: "settings.custom.preset.bullets.instruction",
                            auto: false
                        }
                    ];

                    const materializePreset = (preset) => ({
                        id: preset.id,
                        name: t(preset.nameKey),
                        instruction: t(preset.instructionKey),
                        auto: !!preset.auto,
                        model: ""
                    });

                    const loadCustomSteps = () => {
                        try {
                            const raw = localStorage.getItem(CUSTOM_STEPS_KEY);
                            if (!raw) return [];
                            const parsed = JSON.parse(raw);
                            return Array.isArray(parsed) ? parsed : [];
                        } catch { return []; }
                    };
                    const saveCustomSteps = (steps) => {
                        try { localStorage.setItem(CUSTOM_STEPS_KEY, JSON.stringify(steps)); } catch {}
                    };

                    customSteps = loadCustomSteps();

                    // One-time seed of style presets so new users immediately
                    // have working examples to enable. Skipped on subsequent
                    // launches (flag in localStorage) so a user who deletes
                    // them doesn't get them back on every restart.
                    const seedPresetsIfNeeded = () => {
                        try {
                            if (localStorage.getItem(CUSTOM_STEPS_SEEDED_KEY) === "true") return;
                        } catch {}
                        // Only auto-seed when the user has no custom steps yet.
                        if (customSteps.length === 0) {
                            customSteps = CUSTOM_STEP_PRESETS.map(materializePreset);
                            saveCustomSteps(customSteps);
                            renderModelsCatalog();
                        }
                        try { localStorage.setItem(CUSTOM_STEPS_SEEDED_KEY, "true"); } catch {}
                    };
                    seedPresetsIfNeeded();
                    let editingStepId = null;

                    const renderCustomSteps = () => {
                        if (!customStepListEl) return;
                        customStepListEl.replaceChildren();
                        if (customSteps.length === 0) {
                            const empty = document.createElement("p");
                            empty.className = "field-hint";
                            empty.textContent = t("settings.custom.empty");
                            customStepListEl.append(empty);
                            return;
                        }
                        customSteps.forEach((step, index) => {
                            const row = document.createElement("div");
                            row.className = "custom-step-row";
                            // `auto` doubles as the enabled flag: disabled steps are skipped
                            // when the quick "auto custom" toggle is on, and also dimmed in the UI.
                            row.dataset.enabled = step.auto === false ? "false" : "true";

                            const orderBadge = document.createElement("span");
                            orderBadge.className = "custom-step-order";
                            orderBadge.textContent = String(index + 1);

                            const body = document.createElement("div");
                            body.className = "custom-step-body";
                            const nameEl = document.createElement("strong");
                            nameEl.textContent = step.name || t("settings.custom.editorTitle");
                            const meta = document.createElement("p");
                            meta.className = "custom-step-meta";
                            const preview = (step.instruction || "").replace(/\s+/g, " ").trim();
                            meta.textContent = preview.length > 80 ? preview.slice(0, 80) + "…" : preview;
                            body.append(nameEl, meta);

                            const actions = document.createElement("div");
                            actions.className = "custom-step-actions";

                            // Enable / disable pill. Click toggles without opening the editor.
                            const enableButton = document.createElement("button");
                            enableButton.type = "button";
                            enableButton.className = "custom-step-enable";
                            const isEnabled = step.auto !== false;
                            enableButton.dataset.enabled = String(isEnabled);
                            enableButton.textContent = isEnabled
                                ? t("settings.custom.enabled")
                                : t("settings.custom.disabled");
                            enableButton.title = t("settings.custom.enableTooltip");
                            enableButton.addEventListener("click", () => {
                                step.auto = !isEnabled;
                                saveCustomSteps(customSteps);
                                renderCustomSteps();
                                renderModelsCatalog();
                            });
                            actions.append(enableButton);

                            const upButton = document.createElement("button");
                            upButton.type = "button";
                            upButton.className = "icon-button";
                            upButton.textContent = "↑";
                            upButton.title = t("settings.custom.moveUp");
                            upButton.disabled = index === 0;
                            upButton.addEventListener("click", () => moveCustomStep(index, index - 1));
                            actions.append(upButton);

                            const downButton = document.createElement("button");
                            downButton.type = "button";
                            downButton.className = "icon-button";
                            downButton.textContent = "↓";
                            downButton.title = t("settings.custom.moveDown");
                            downButton.disabled = index === customSteps.length - 1;
                            downButton.addEventListener("click", () => moveCustomStep(index, index + 1));
                            actions.append(downButton);

                            const runButton = document.createElement("button");
                            runButton.type = "button";
                            runButton.className = "text-button";
                            runButton.textContent = t("settings.custom.run");
                            runButton.addEventListener("click", () => runCustomStep(step));
                            actions.append(runButton);

                            const editButton = document.createElement("button");
                            editButton.type = "button";
                            editButton.className = "icon-button";
                            editButton.textContent = "✎";
                            editButton.title = t("settings.custom.edit");
                            editButton.addEventListener("click", () => openCustomStepEditor(step));
                            actions.append(editButton);

                            const deleteButton = document.createElement("button");
                            deleteButton.type = "button";
                            deleteButton.className = "icon-button";
                            deleteButton.textContent = "🗑";
                            deleteButton.title = t("settings.custom.delete");
                            deleteButton.addEventListener("click", () => {
                                const msg = t("settings.custom.deleteConfirm", { name: step.name || "" });
                                if (!window.confirm(msg)) return;
                                customSteps = customSteps.filter(s => s.id !== step.id);
                                saveCustomSteps(customSteps);
                                renderCustomSteps();
                                renderModelsCatalog();
                            });
                            actions.append(deleteButton);

                            row.append(orderBadge, body, actions);
                            customStepListEl.append(row);
                        });
                    };

                    const moveCustomStep = (from, to) => {
                        if (to < 0 || to >= customSteps.length) return;
                        const [item] = customSteps.splice(from, 1);
                        customSteps.splice(to, 0, item);
                        saveCustomSteps(customSteps);
                        renderCustomSteps();
                        renderModelsCatalog();
                    };

                    const openCustomStepEditor = (step) => {
                        editingStepId = step ? step.id : null;
                        customStepName.value = step?.name || "";
                        customStepInstruction.value = step?.instruction || "";
                        customStepDelete.hidden = !step;
                        // Refresh the model options in case new models were downloaded,
                        // then restore the step's preferred model (if any).
                        populatePostModelSelect(customStepModel);
                        if (customStepModel) customStepModel.value = step?.model || "";
                        customStepEditorModal.hidden = false;
                    };

                    const closeCustomStepEditor = () => {
                        customStepEditorModal.hidden = true;
                        editingStepId = null;
                    };

                    customStepAddButton.addEventListener("click", () => openCustomStepEditor(null));
                    customStepEditorClose.addEventListener("click", closeCustomStepEditor);

                    // ---- Preset picker ----
                    const customPresetModal = document.getElementById("customPresetModal");
                    const customPresetClose = document.getElementById("customPresetClose");
                    const customPresetList = document.getElementById("customPresetList");
                    const customStepPresetButton = document.getElementById("customStepPresetButton");

                    const insertOrUpdatePreset = (preset) => {
                        const step = materializePreset(preset);
                        const idx = customSteps.findIndex(s => s.id === step.id);
                        if (idx >= 0) {
                            // Preserve the user's existing enabled flag & model choice
                            // so re-adding doesn't revert their tweaks.
                            const prev = customSteps[idx];
                            customSteps[idx] = {
                                ...step,
                                auto: prev.auto,
                                model: prev.model || step.model
                            };
                        } else {
                            customSteps.push(step);
                        }
                        saveCustomSteps(customSteps);
                        renderCustomSteps();
                        renderModelsCatalog();
                    };

                    const renderCustomPresetList = () => {
                        if (!customPresetList) return;
                        customPresetList.replaceChildren();
                        CUSTOM_STEP_PRESETS.forEach(preset => {
                            const row = document.createElement("div");
                            row.className = "custom-preset-row";

                            const body = document.createElement("div");
                            body.className = "custom-preset-body";
                            const title = document.createElement("strong");
                            title.textContent = t(preset.nameKey);
                            const desc = document.createElement("p");
                            desc.className = "custom-step-meta";
                            const instr = t(preset.instructionKey);
                            desc.textContent = instr.length > 90 ? instr.slice(0, 90) + "…" : instr;
                            body.append(title, desc);

                            const addButton = document.createElement("button");
                            addButton.type = "button";
                            addButton.className = "secondary-button";
                            addButton.textContent = t("settings.custom.preset.addOne");
                            const existing = customSteps.some(s => s.id === preset.id);
                            if (existing) {
                                addButton.textContent = t("settings.custom.preset.refresh");
                                addButton.title = t("settings.custom.preset.refreshTooltip");
                            }
                            addButton.addEventListener("click", () => {
                                insertOrUpdatePreset(preset);
                                renderCustomPresetList(); // refresh labels
                                showToast(
                                    t("settings.custom.preset.added", { name: t(preset.nameKey) }),
                                    "success"
                                );
                            });

                            row.append(body, addButton);
                            customPresetList.append(row);
                        });
                    };

                    customStepPresetButton?.addEventListener("click", () => {
                        if (!customPresetModal) return;
                        renderCustomPresetList();
                        customPresetModal.hidden = false;
                    });
                    customPresetClose?.addEventListener("click", () => {
                        if (customPresetModal) customPresetModal.hidden = true;
                    });
                    customPresetModal?.addEventListener("click", (event) => {
                        if (event.target === customPresetModal) customPresetModal.hidden = true;
                    });
                    customStepEditorModal.addEventListener("click", (event) => {
                        if (event.target === customStepEditorModal) closeCustomStepEditor();
                    });

                    customStepSave.addEventListener("click", () => {
                        const instruction = customStepInstruction.value.trim();
                        if (!instruction) {
                            showToast(t("settings.custom.instructionPlaceholder"), "warning");
                            return;
                        }
                        const name = customStepName.value.trim() || t("settings.custom.editorTitle");
                        // Preserve the step's enabled state when editing; new steps default to
                        // enabled so they show up immediately in auto-custom runs.
                        const previous = editingStepId
                            ? customSteps.find(s => s.id === editingStepId)
                            : null;
                        const step = {
                            id: editingStepId || (crypto.randomUUID?.() || String(Date.now())),
                            name,
                            instruction,
                            auto: previous ? !!previous.auto : true,
                            model: customStepModel?.value || ""
                        };
                        const idx = customSteps.findIndex(s => s.id === step.id);
                        if (idx >= 0) customSteps[idx] = step;
                        else customSteps.push(step);
                        saveCustomSteps(customSteps);
                        renderCustomSteps();
                        renderModelsCatalog();
                        closeCustomStepEditor();
                    });

                    customStepDelete.addEventListener("click", () => {
                        if (!editingStepId) return;
                        const step = customSteps.find(s => s.id === editingStepId);
                        const msg = t("settings.custom.deleteConfirm", { name: step?.name || "" });
                        if (!window.confirm(msg)) return;
                        customSteps = customSteps.filter(s => s.id !== editingStepId);
                        saveCustomSteps(customSteps);
                        renderCustomSteps();
                        renderModelsCatalog();
                        closeCustomStepEditor();
                    });

                    // Every custom step is a single LLM instruction now.
                    const buildCustomStepPayload = (step) => ({
                        type: "custom",
                        instruction: step.instruction,
                        label: step.name,
                        model: step.model || undefined
                    });

                    const runCustomStep = async (step) => {
                        const text = captionContent.value.trim();
                        if (!text) {
                            showToast(t("status.emptyText"), "warning");
                            return;
                        }
                        setBusy(true);
                        showToast(t("settings.custom.running", { name: step.name }), "info");
                        try {
                            const payload = buildCustomStepPayload(step);
                            const response = await fetch("/api/process", {
                                method: "POST",
                                headers: { "Content-Type": "application/json" },
                                body: JSON.stringify({
                                    text,
                                    pipeline: [payload],
                                    note_id: lastTranscribedNoteId || undefined,
                                    use_linked_context: linkedContextEnabled()
                                })
                            });
                            if (!response.ok) {
                                const err = await response.json().catch(() => ({}));
                                throw new Error(err.error || t("settings.custom.runFailed"));
                            }
                            const data = await response.json();
                            const result = (data.results || []).slice(-1)[0];
                            if (result?.text) {
                                updateMainDisplay(result.text);
                            }
                            showToast(t("settings.custom.ranOk", { name: step.name }), "success");
                        } catch (error) {
                            showErrorBanner((error && error.message) || t("settings.custom.runFailed"));
                        } finally {
                            setBusy(false);
                        }
                    };

                    renderCustomSteps();

                    // ========== Quick toggles next to the record button ==========
                    const QUICK_TOGGLE_DEFAULTS = {
                        autoSave: false, autoRefine: false, autoTranslate: false,
                        autoAnalyze: false, autoCustom: false,
                    };
                    const loadQuickToggles = () => {
                        try {
                            const raw = localStorage.getItem(QUICK_TOGGLE_KEY);
                            if (!raw) return { ...QUICK_TOGGLE_DEFAULTS };
                            return { ...QUICK_TOGGLE_DEFAULTS, ...JSON.parse(raw) };
                        } catch { return { ...QUICK_TOGGLE_DEFAULTS }; }
                    };
                    const saveQuickToggles = () => {
                        try {
                            localStorage.setItem(QUICK_TOGGLE_KEY, JSON.stringify({
                                autoSave: quickAutoSave.checked,
                                autoRefine: quickAutoRefine.checked,
                                autoTranslate: quickAutoTranslate.checked,
                                autoAnalyze: quickAutoAnalyze.checked,
                                autoCustom: quickAutoCustom.checked
                            }));
                        } catch {}
                    };
                    const quickToggles = loadQuickToggles();
                    quickAutoSave.checked = !!quickToggles.autoSave;
                    quickAutoRefine.checked = !!quickToggles.autoRefine;
                    quickAutoTranslate.checked = !!quickToggles.autoTranslate;
                    quickAutoAnalyze.checked = !!quickToggles.autoAnalyze;
                    quickAutoCustom.checked = !!quickToggles.autoCustom;
                    quickAutoSave.addEventListener("change", saveQuickToggles);
                    quickAutoRefine.addEventListener("change", saveQuickToggles);
                    quickAutoTranslate.addEventListener("change", saveQuickToggles);
                    quickAutoAnalyze.addEventListener("change", saveQuickToggles);
                    quickAutoCustom.addEventListener("change", saveQuickToggles);

                    resetSettingsButton?.addEventListener("click", resetSettings);

                    // Any change to a quick-toggle state also counts as a settings change
                    // and should be persisted immediately.
                    [quickAutoRefine, quickAutoTranslate, quickAutoAnalyze, quickAutoCustom, quickAutoSave]
                        .filter(Boolean)
                        .forEach(el => el.addEventListener("change", autoSavePostprocess));
                </script>
            </body>
            <style>
                /* ---- Theme variables ----
                   Colors originally hard-coded throughout the stylesheet are
                   kept as defaults on :root (dark). A `[data-theme="light"]`
                   override on <body> swaps only the layer-1 surface, text and
                   border colors; accent colors (blue/green/yellow/red) stay
                   visually balanced in both themes. Any stylesheet rule that
                   continues to use literal hex values remains correct for
                   dark mode and is overridden via CSS variable aliasing in
                   the light theme block below. */
                :root {
                    color-scheme: light dark;
                    font-family: "Segoe UI", "Yu Gothic UI", "Hiragino Sans", sans-serif;

                    /* Dark theme (default) */
                    --bg-app: #0f1419;
                    --bg-surface: #0d1117;
                    --bg-surface-2: #161b22;
                    --bg-surface-3: #21262d;
                    --bg-inset: #010409;
                    --text-primary: #e6edf3;
                    --text-secondary: #c9d1d9;
                    --text-muted: #7d8590;
                    --text-subtle: #6e7681;
                    --border-subtle: #21262d;
                    --border-default: #30363d;
                    --border-strong: #484f58;

                    background: var(--bg-app);
                    color: var(--text-primary);
                }

                body[data-theme="light"] {
                    color-scheme: light;
                    --bg-app: #f7f8fa;
                    --bg-surface: #ffffff;
                    --bg-surface-2: #f2f4f7;
                    --bg-surface-3: #e6e9ef;
                    --bg-inset: #ffffff;
                    --text-primary: #1f2329;
                    --text-secondary: #2f3540;
                    --text-muted: #5f6673;
                    --text-subtle: #7c8390;
                    --border-subtle: #e1e4ea;
                    --border-default: #c9cdd6;
                    --border-strong: #a9afbb;
                    /* Lighter hover surfaces so dark body text stays legible. */
                    --note-card-hover-bg: #eaf2ff;
                }

                /* Light-theme tweaks for accent surfaces originally drawn dark.
                   Without these the status badges, selection highlights and
                   monochrome chips look out of place against a light layout. */
                body[data-theme="light"] .status-badge[data-mode="busy"] {
                    background: #fff5d6; color: #7a5d00; border-color: #e0b84f;
                }
                body[data-theme="light"] .status-badge[data-mode="ready"] {
                    background: #dcf5e0; color: #166534; border-color: #4ac26b;
                }
                body[data-theme="light"] .status-badge[data-mode="error"] {
                    background: #fde2e1; color: #8c1a17; border-color: #e17372;
                }

                body[data-theme="light"] .model-row[data-active="true"],
                body[data-theme="light"] .drafts-list-item[data-active="true"],
                body[data-theme="light"] .note-card[data-selected="true"],
                body[data-theme="light"] .note-card[data-picked="true"],
                body[data-theme="light"] .tag-suggestion-chip[data-selected="true"],
                body[data-theme="light"] .live-sidebar-item:hover,
                body[data-theme="light"] .model-item[data-active="true"] {
                    background: #dbe7fd;
                    color: #0a3480;
                }

                body[data-theme="light"] .model-row[data-downloading="true"],
                body[data-theme="light"] .model-item[data-downloading="true"] {
                    background: #fff3d0;
                    color: #6e4b00;
                }

                body[data-theme="light"] .model-row-status[data-variant="active"] {
                    background: #dbe7fd; color: #0a3480;
                }
                body[data-theme="light"] .model-row-status[data-variant="cached"] {
                    background: #dcf5e0; color: #166534;
                }
                body[data-theme="light"] .model-row-status[data-variant="missing"] {
                    background: #edeff3; color: #4b5563;
                }
                body[data-theme="light"] .model-row-status[data-variant="downloading"] {
                    background: #fff3d0; color: #6e4b00;
                }
                body[data-theme="light"] .model-row-status[data-variant="incompatible"] {
                    background: #fde0de; color: #82181a;
                }
                body[data-theme="light"] .models-ep-chip[data-registered="true"] {
                    background: #dcf5e0; color: #166534; border-color: #dcf5e0;
                }
                body[data-theme="light"] .models-ep-chip[data-registered="false"] {
                    background: #edeff3; color: #4b5563;
                }

                body[data-theme="light"] .caption-dirty {
                    background: #fff3d0; color: #7a5d00;
                }

                body[data-theme="light"] .tag-chip {
                    background: #dbe7fd; color: #0a3480;
                }

                body[data-theme="light"] .quick-toggle:has(input:checked) {
                    background: #dbe7fd; color: #0a3480;
                }

                body[data-theme="light"] .graph-link-banner {
                    background: #fff3d0; color: #6e4b00; border-color: #e0b84f;
                }

                body[data-theme="light"] .notes-selection-bar {
                    background: #dbe7fd; color: #0a3480;
                }

                body[data-theme="light"] .error-banner {
                    background: #fde2e1; color: #8c1a17;
                }
                body[data-theme="light"] .error-banner-body p {
                    color: #6b1412;
                }
                body[data-theme="light"] .requirements-banner {
                    background: #fff3d0; color: #6e4b00; border-color: #e0b84f;
                }
                body[data-theme="light"] .requirements-banner-body p,
                body[data-theme="light"] .requirements-command {
                    color: #4f3600;
                }

                body[data-theme="light"] .caption-content {
                    background: #ffffff; color: #1f2329;
                }
                body[data-theme="light"] .caption-content:focus {
                    background: #eef4ff;
                }

                body[data-theme="light"] .graph-node-label { fill: #2f3540; }
                /* `:where()` keeps specificity 0 for `.graph-edge` so kind-specific
                   overrides (link / parent) still win in light theme. */
                body[data-theme="light"] :where(.graph-edge) { stroke: #a9afbb; }

                body[data-theme="light"] .drop-overlay {
                    background: rgba(15, 20, 25, 0.35);
                }
                body[data-theme="light"] .drop-prompt {
                    background: #ffffff;
                    border-color: #a9afbb;
                }
                body[data-theme="light"] .drop-prompt p {
                    color: #1f2329;
                }

                body[data-theme="light"] .toast {
                    background: rgba(255, 255, 255, 0.95);
                    color: #1f2329;
                }
                body[data-theme="light"] .toast[data-variant="success"] {
                    background: rgba(220, 245, 224, 0.95);
                }
                body[data-theme="light"] .toast[data-variant="info"] {
                    background: rgba(219, 231, 253, 0.95);
                }
                body[data-theme="light"] .toast[data-variant="warning"] {
                    background: rgba(255, 243, 208, 0.95);
                }

                body[data-theme="light"] .record-button {
                    box-shadow: 0 6px 18px rgba(46, 160, 67, 0.2);
                }
                body[data-theme="light"] .record-info {
                    background: rgba(218, 54, 51, 0.08);
                }

                * {
                    box-sizing: border-box;
                }

                html, body {
                    height: 100%;
                }

                body {
                    margin: 0;
                    height: 100vh;
                    overflow: hidden;
                    background: var(--bg-app);
                }

                button,
                input,
                select,
                textarea {
                    font: inherit;
                }

                input[type="number"],
                input[type="text"] {
                    width: 100%;
                    border: 1px solid var(--border-default);
                    border-radius: 8px;
                    padding: 10px 12px;
                    background: var(--bg-surface-2);
                    color: var(--text-primary);
                }

                .app-shell {
                    height: 100vh;
                    display: flex;
                    flex-direction: column;
                    overflow: hidden;
                }

                /* Main workspace */
                .main-workspace {
                    flex: 1;
                    display: flex;
                    flex-direction: column;
                    width: 100%;
                    margin: 0 auto;
                    padding: 12px 16px 16px;
                    gap: 12px;
                    min-height: 0;
                }

                .app-brand {
                    width: 32px;
                    height: 32px;
                    display: inline-flex;
                    align-items: center;
                    justify-content: center;
                    flex: 0 0 auto;
                }

                .app-brand-icon {
                    width: 28px;
                    height: 28px;
                    display: block;
                    object-fit: contain;
                }

                /* Topbar - single compact row */
                .topbar {
                    display: flex;
                    align-items: center;
                    justify-content: space-between;
                    gap: 12px;
                    padding: 8px 12px;
                    border: 1px solid var(--bg-surface-3);
                    border-radius: 12px;
                    background: var(--bg-surface);
                }

                .topbar-left,
                .topbar-right {
                    display: flex;
                    align-items: center;
                    gap: 8px;
                    min-width: 0;
                }

                .topbar-left {
                    flex: 1;
                }

                .topbar-divider {
                    width: 1px;
                    height: 22px;
                    background: var(--bg-surface-3);
                    margin: 0 4px;
                }

                .topbar-nav {
                    font-size: 0.85rem;
                    padding: 0 10px;
                }

                h1, h2, h3, p {
                    margin: 0;
                }

                h2 {
                    font-size: 1.25rem;
                    line-height: 1.3;
                    color: var(--text-primary);
                }

                h3 {
                    font-size: 1rem;
                    line-height: 1.3;
                    color: var(--text-primary);
                }

                /* Project selector */
                .project-selector {
                    display: inline-flex;
                    align-items: center;
                    gap: 4px;
                    padding: 2px 4px 2px 10px;
                    border: 1px solid var(--border-default);
                    border-radius: 10px;
                    background: var(--bg-surface-2);
                }

                .project-selector::before {
                    content: "📁";
                    font-size: 0.9rem;
                    opacity: 0.7;
                }

                .project-select {
                    border: 0;
                    background: transparent;
                    color: var(--text-primary);
                    padding: 4px 6px;
                    font-size: 0.9rem;
                    font-weight: 600;
                    min-width: 140px;
                }

                .project-select:focus {
                    outline: none;
                }

                .project-selector .icon-button {
                    min-height: 28px;
                    width: 28px;
                    padding: 0;
                    font-size: 1rem;
                    line-height: 1;
                }

                /* Fullscreen overlay */
                .fullscreen-overlay {
                    position: fixed;
                    inset: 0;
                    background: var(--bg-app);
                    z-index: 800;
                    display: flex;
                    flex-direction: column;
                }

                .fullscreen-overlay[hidden] {
                    display: none;
                }

                .fullscreen-header {
                    display: flex;
                    align-items: center;
                    justify-content: space-between;
                    padding: 16px 24px;
                    border-bottom: 1px solid var(--bg-surface-3);
                    background: var(--bg-surface-2);
                    gap: 16px;
                }

                .fullscreen-body {
                    flex: 1;
                    overflow: hidden;
                    display: flex;
                    flex-direction: column;
                    min-height: 0;
                }

                /* Notes workspace: sidebar (list+search) + graph pane */
                .notes-workspace {
                    flex-direction: row;
                    overflow: hidden;
                }

                /* Main-embedded version: frame it like a card */
                .main-notes-workspace {
                    flex: 1;
                    display: flex;
                    flex-direction: row;
                    border: 1px solid var(--border-default);
                    border-radius: 12px;
                    overflow: hidden;
                    background: var(--bg-surface);
                    min-height: 0;
                }

                /* Right pane: graph on top, work pane below */
                /* Work pane (caption editor + record controls) */
                .work-pane {
                    display: flex;
                    flex-direction: column;
                    gap: 10px;
                    padding: 12px 16px 14px;
                    border-top: 1px solid var(--bg-surface-3);
                    background: var(--bg-surface);
                    min-height: 240px;
                    max-height: 48%;
                }

                .caption-wrapper {
                    position: relative;
                    flex: 1;
                    display: flex;
                    min-height: 100px;
                }

                .caption-wrapper .caption-dirty {
                    position: absolute;
                    top: 8px;
                    right: 10px;
                    pointer-events: none;
                }

                .work-pane .caption-content {
                    flex: 1;
                    padding: 14px 16px;
                    font-size: 1.1rem;
                    line-height: 1.55;
                    border: 1px solid var(--bg-surface-3);
                    border-radius: 10px;
                    background: var(--bg-inset);
                    min-height: 100px;
                }

                .work-pane .caption-stats {
                    padding: 6px 12px;
                    border-radius: 8px;
                    border: 1px solid var(--bg-surface-3);
                    background: var(--bg-inset);
                    gap: 12px;
                }

                .work-pane .caption-stat {
                    gap: 6px;
                }

                .caption-stat-icon {
                    font-size: 0.9rem;
                    opacity: 0.8;
                }

                /* New single-row control bar: actions on the left, record on the right */
                .work-pane .control-bar {
                    display: flex;
                    align-items: center;
                    justify-content: space-between;
                    gap: 12px;
                    padding: 8px 12px;
                }

                .control-bar-left {
                    display: flex;
                    align-items: center;
                    gap: 6px;
                }

                .work-pane .record-button {
                    min-height: 44px;
                    padding: 0 22px;
                    font-size: 0.95rem;
                    margin-left: auto;
                }

                /* Inline error/hint line under the control bar */
                .record-info {
                    margin: 0;
                    padding: 6px 12px;
                    min-height: 32px;
                    border-radius: 8px;
                    border: 1px solid var(--border-default);
                    background: var(--bg-surface-2);
                    color: var(--text-muted);
                    font-size: 0.82rem;
                    line-height: 1.4;
                    display: flex;
                    align-items: center;
                    gap: 8px;
                }

                .record-info[data-empty="true"] {
                    border-style: dashed;
                    background: var(--bg-inset);
                    color: var(--text-subtle);
                    opacity: 0.82;
                }

                .record-info-icon {
                    display: inline-grid;
                    place-items: center;
                    flex: 0 0 auto;
                    width: 18px;
                    height: 18px;
                    border-radius: 50%;
                    border: 1px solid currentColor;
                    font-size: 0.72rem;
                    font-weight: 700;
                    line-height: 1;
                    opacity: 0.78;
                }

                .record-info-text {
                    min-width: 0;
                    overflow: hidden;
                    text-overflow: ellipsis;
                    white-space: nowrap;
                }

                /* Tag suggestions (autocomplete) */
                .tag-input-wrap {
                    display: flex;
                    flex-direction: column;
                    gap: 6px;
                }

                .tag-chip-suggestions {
                    display: flex;
                    flex-wrap: wrap;
                    gap: 4px;
                    max-height: 90px;
                    overflow-y: auto;
                }

                .tag-chip-suggestions:empty {
                    display: none;
                }

                .tag-suggestion-chip {
                    padding: 3px 10px;
                    border: 1px solid var(--border-default);
                    border-radius: 12px;
                    background: var(--bg-surface-2);
                    color: var(--text-secondary);
                    font-size: 0.78rem;
                    cursor: pointer;
                    min-height: 0;
                    line-height: 1.4;
                }

                .tag-suggestion-chip:hover {
                    border-color: #1f6feb;
                    color: #79c0ff;
                }

                .tag-suggestion-chip[data-selected="true"] {
                    background: #1f6feb;
                    border-color: #1f6feb;
                    color: #ffffff;
                }

                .notes-header-meta {
                    margin-left: auto;
                    margin-right: 12px;
                    padding: 2px 10px;
                    border-radius: 10px;
                    background: var(--bg-surface-2);
                    border: 1px solid var(--border-default);
                    color: var(--text-muted);
                    font-size: 0.78rem;
                    font-weight: 600;
                }

                .notes-sidebar {
                    width: 360px;
                    min-width: 300px;
                    max-width: 440px;
                    display: flex;
                    flex-direction: column;
                    border-right: 1px solid var(--bg-surface-3);
                    background: var(--bg-surface);
                    min-height: 0;
                }

                .notes-sidebar-toolbar {
                    display: flex;
                    align-items: center;
                    gap: 8px;
                    padding: 12px 14px;
                    border-bottom: 1px solid var(--bg-surface-3);
                    background: var(--bg-surface-2);
                }

                .notes-search {
                    flex: 1;
                    min-width: 0;
                }

                .notes-refresh {
                    width: 32px;
                    min-height: 32px;
                    padding: 0;
                    font-size: 0.95rem;
                }

                .notes-sidebar-list {
                    flex: 1;
                    overflow-y: auto;
                    display: flex;
                    flex-direction: column;
                    /* Tighter gap for the compact list — still enough to tell
                       rows apart without wasting vertical space. */
                    gap: 2px;
                    padding: 8px;
                    min-height: 0;
                }

                .notes-graph-pane {
                    flex: 1;
                    display: flex;
                    flex-direction: column;
                    padding: 14px 16px;
                    gap: 10px;
                    min-width: 0;
                    min-height: 0;
                }

                .notes-graph-toolbar {
                    display: flex;
                    align-items: center;
                    justify-content: space-between;
                    gap: 12px;
                    flex-wrap: wrap;
                }

                .note-card {
                    padding: 14px;
                    border: 1px solid var(--border-default);
                    border-radius: 10px;
                    background: var(--bg-surface-2);
                    cursor: pointer;
                    display: flex;
                    flex-direction: column;
                    gap: 8px;
                    transition: all 0.15s ease;
                }

                .note-card:hover {
                    border-color: #1f6feb;
                    /* Use a theme-aware hover surface so light-mode text stays readable.
                       The dark value matches the old literal #1a2332 (a very dim navy). */
                    background: var(--note-card-hover-bg, #1a2332);
                }

                /* High-density row. One line for title + date; an optional
                   second row for tag chips. Action buttons are hidden until
                   the row is hovered to keep the eye on the titles. */
                .note-card-compact {
                    padding: 6px 10px;
                    gap: 2px;
                    position: relative;
                }

                /* Drag & drop feedback for note reordering / reparenting */
                .note-card[draggable="true"] {
                    cursor: grab;
                }

                .note-card[draggable="true"]:active {
                    cursor: grabbing;
                }

                .note-card.is-dragging {
                    opacity: 0.45;
                }

                /* Reorder feedback: a 2px accent bar between cards */
                .note-card[data-drop-zone="above"] {
                    box-shadow: 0 -2px 0 0 #1f6feb;
                }

                .note-card[data-drop-zone="below"] {
                    box-shadow: 0 2px 0 0 #1f6feb;
                }

                /* Reparent feedback: highlight the whole target card */
                .note-card[data-drop-zone="inside"] {
                    border-color: #1f6feb;
                    background: rgba(31, 111, 235, 0.16);
                    box-shadow: inset 0 0 0 1px #1f6feb;
                }

                /* Nested-note visual: a subtle left guide for depth */
                .note-card[data-depth] {
                    border-left: 2px solid var(--border-default);
                }

                /* Top-level drop band: shown while dragging to move a note out to the root */
                .notes-drop-root {
                    display: block;
                    padding: 6px 10px;
                    margin-bottom: 4px;
                    border: 1px dashed var(--border-default);
                    border-radius: 8px;
                    font-size: 0.8rem;
                    color: var(--text-muted);
                    text-align: center;
                    opacity: 0.55;
                    transition: opacity 0.15s ease, border-color 0.15s ease, background 0.15s ease;
                }

                .notes-drop-root[data-drop-zone="root"] {
                    opacity: 1;
                    border-color: #1f6feb;
                    color: #79c0ff;
                    background: rgba(31, 111, 235, 0.12);
                }

                .note-card[data-selected="true"] {
                    border-color: #1f6feb;
                    background: #0c2d6b;
                    box-shadow: inset 3px 0 0 #79c0ff;
                }

                .note-card-footer {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    gap: 8px;
                    margin-top: 2px;
                }

                .note-card-actions {
                    display: inline-flex;
                    gap: 4px;
                    flex-shrink: 0;
                }

                /* Action buttons float over the row's right edge and fade in
                   on hover. This keeps the default row height tight while
                   still giving one-click access to copy / edit. */
                .note-card-actions-floating {
                    position: absolute;
                    top: 3px;
                    right: 6px;
                    opacity: 0;
                    pointer-events: none;
                    transition: opacity 0.12s ease;
                    background: var(--note-card-hover-bg, #1a2332);
                    padding: 2px;
                    border-radius: 6px;
                }
                .note-card:hover .note-card-actions-floating,
                .note-card:focus-within .note-card-actions-floating {
                    opacity: 1;
                    pointer-events: auto;
                }

                .note-card-edit {
                    width: 22px;
                    height: 22px;
                    min-height: 22px;
                    padding: 0;
                    border: 1px solid var(--border-default);
                    border-radius: 5px;
                    background: var(--bg-surface-2);
                    color: var(--text-muted);
                    font-size: 0.78rem;
                    cursor: pointer;
                    flex-shrink: 0;
                }

                .note-card-edit:hover {
                    border-color: #1f6feb;
                    color: #79c0ff;
                }

                /* Meta row — tag chips only. Collapsed when the note has no tags. */
                .note-card-meta {
                    display: flex;
                    flex-wrap: wrap;
                    gap: 3px;
                    margin-top: 2px;
                }
                .tag-chip-inline {
                    padding: 1px 6px;
                    font-size: 0.66rem;
                    border-radius: 8px;
                }

                /* Tiny badge next to the title for parent notes with children. */
                .note-card-child-badge {
                    display: inline-block;
                    padding: 1px 5px;
                    margin-left: 4px;
                    border-radius: 8px;
                    background: var(--bg-surface-3);
                    color: var(--text-muted);
                    font-size: 0.68rem;
                    font-weight: 600;
                    vertical-align: middle;
                }

                .note-card-header {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    gap: 8px;
                }

                .note-card-header h4 {
                    margin: 0;
                    font-size: 0.95rem;
                    font-weight: 700;
                    color: var(--text-primary);
                    overflow: hidden;
                    text-overflow: ellipsis;
                    white-space: nowrap;
                }

                .note-card-date {
                    color: var(--text-muted);
                    font-size: 0.75rem;
                    white-space: nowrap;
                }

                .note-card-preview {
                    margin: 0;
                    color: var(--text-secondary);
                    font-size: 0.85rem;
                    line-height: 1.4;
                    display: -webkit-box;
                    -webkit-line-clamp: 2;
                    -webkit-box-orient: vertical;
                    overflow: hidden;
                }

                .note-card-compact .note-card-header h4 {
                    font-size: 0.88rem;
                    font-weight: 600;
                }
                /* Relative dates are short; align baselines with the title. */
                .note-card-compact .note-card-date {
                    font-size: 0.72rem;
                    align-self: center;
                }

                .note-card-tags {
                    display: flex;
                    flex-wrap: wrap;
                    gap: 4px;
                }

                .tag-chip {
                    padding: 2px 8px;
                    border-radius: 12px;
                    background: #1f6feb;
                    color: #ffffff;
                    font-size: 0.72rem;
                    font-weight: 600;
                }

                .notes-empty {
                    padding: 48px 24px;
                    text-align: center;
                    color: var(--text-muted);
                    font-size: 0.95rem;
                    grid-column: 1 / -1;
                }

                /* Graph */
                .graph-container {
                    flex: 1 1 0;
                    min-height: 0;
                    min-width: 0;
                    position: relative;
                    border: 1px solid var(--border-default);
                    border-radius: 10px;
                    background: var(--bg-surface);
                    overflow: hidden;
                }

                .graph-svg {
                    display: block;
                    width: 100%;
                    height: 100%;
                    /* viewBox would otherwise impose an intrinsic aspect ratio on the SVG,
                       causing the element to keep its shape when the window shrinks. */
                    aspect-ratio: auto;
                    cursor: grab;
                    touch-action: none;
                }

                .graph-svg.is-panning {
                    cursor: grabbing;
                }

                .graph-edge {
                    stroke: var(--border-default);
                    stroke-width: 1.5;
                    opacity: 0.7;
                }

                .graph-edge[data-kind="link"] {
                    stroke: #f0c96f;
                    stroke-width: 2;
                    opacity: 0.85;
                    stroke-dasharray: 6 4;
                }

                .graph-edge[data-kind="parent"] {
                    stroke: #bc8cff;
                    stroke-width: 2.25;
                    opacity: 0.9;
                }

                .graph-legend-line[data-kind="link"] {
                    background: linear-gradient(to right, #f0c96f 60%, transparent 60%);
                    background-size: 6px 2px;
                }

                .graph-legend-line[data-kind="parent"] {
                    background: #bc8cff;
                }

                /* Parent note selector in the note editor */
                .note-parent-row {
                    display: flex;
                    gap: 6px;
                    align-items: center;
                }

                .note-parent-select {
                    flex: 1;
                    min-width: 0;
                }

                .note-parent-row .icon-button {
                    width: 34px;
                    min-height: 34px;
                    padding: 0;
                }

                .graph-edge-manual {
                    cursor: pointer;
                    pointer-events: stroke;
                }

                .graph-edge-manual:hover {
                    stroke: #f85149;
                    opacity: 1;
                }

                .graph-svg.is-linking {
                    cursor: crosshair;
                }

                .graph-node[data-link-pending="true"] circle {
                    stroke: #f0c96f;
                    stroke-width: 4;
                    filter: drop-shadow(0 0 8px rgba(240, 201, 111, 0.7));
                }

                .graph-link-banner {
                    display: flex;
                    align-items: center;
                    justify-content: space-between;
                    padding: 8px 12px;
                    border: 1px solid #bb8009;
                    border-radius: 8px;
                    background: #302608;
                    color: #f0c96f;
                    font-size: 0.82rem;
                    font-weight: 600;
                }

                .graph-link-banner[hidden] {
                    display: none;
                }

                .graph-link-banner .text-button {
                    min-height: 28px;
                    padding: 0 10px;
                    font-size: 0.78rem;
                    color: #f0c96f;
                    border-color: #bb8009;
                    background: transparent;
                }

                #graphLinkModeButton[data-active="true"] {
                    background: #302608;
                    border-color: #bb8009;
                    color: #f0c96f;
                }

                .graph-legend-line {
                    display: inline-block;
                    width: 20px;
                    height: 2px;
                    background: #f0c96f;
                    background-image: linear-gradient(to right, #f0c96f 60%, transparent 60%);
                    background-size: 6px 2px;
                    margin-right: 2px;
                    vertical-align: middle;
                }

                .graph-node {
                    cursor: pointer;
                }

                .graph-node circle {
                    stroke: var(--bg-surface);
                    stroke-width: 2;
                    transition: all 0.15s ease;
                }

                .graph-node-project circle {
                    fill: #f0c96f;
                }

                .graph-node-note circle {
                    fill: #1f6feb;
                }

                .graph-node-tag circle {
                    fill: #2ea043;
                }

                .graph-node:hover circle {
                    stroke: var(--text-primary);
                    stroke-width: 3;
                }

                .graph-node[data-highlighted="true"] circle {
                    stroke: #f0c96f;
                    stroke-width: 4;
                    filter: drop-shadow(0 0 6px rgba(240, 201, 111, 0.55));
                }

                .graph-node[data-highlighted="true"] .graph-node-label {
                    fill: #f0c96f;
                    font-weight: 700;
                }

                /* Picked state for selection mode — mirrors the sidebar's
                   data-picked styling so users can tell picked notes apart on
                   the graph too. */
                .graph-node[data-picked="true"] circle {
                    stroke: #2ea043;
                    stroke-width: 4;
                    filter: drop-shadow(0 0 6px rgba(46, 160, 67, 0.55));
                }
                .graph-node[data-picked="true"] .graph-node-label {
                    fill: #2ea043;
                    font-weight: 700;
                }

                .graph-node-label {
                    fill: var(--text-secondary);
                    font-size: 11px;
                    font-weight: 600;
                    pointer-events: none;
                }

                .graph-legend {
                    display: inline-flex;
                    align-items: center;
                    gap: 8px;
                    flex-wrap: wrap;
                    color: var(--text-secondary);
                    font-size: 0.85rem;
                }

                .graph-legend-dot {
                    display: inline-block;
                    width: 12px;
                    height: 12px;
                    border-radius: 50%;
                    margin-right: 2px;
                }

                .graph-legend-dot[data-kind="project"] { background: #f0c96f; }
                .graph-legend-dot[data-kind="note"] { background: #1f6feb; }
                .graph-legend-dot[data-kind="tag"] { background: #2ea043; }

                .graph-detail {
                    position: absolute;
                    top: 16px;
                    right: 16px;
                    width: 320px;
                    max-height: calc(100% - 32px);
                    padding: 16px;
                    background: var(--bg-surface-2);
                    border: 1px solid var(--border-default);
                    border-radius: 10px;
                    box-shadow: 0 8px 24px rgba(1, 4, 9, 0.4);
                    overflow-y: auto;
                }

                .graph-detail[hidden] {
                    display: none;
                }

                .graph-detail-close {
                    float: right;
                    padding: 4px 8px;
                    border: 0;
                    background: transparent;
                    color: var(--text-muted);
                    min-height: 24px;
                    cursor: pointer;
                }

                .graph-detail h3 {
                    margin: 0 0 4px;
                    font-size: 1rem;
                }

                .graph-detail-kind {
                    margin: 0 0 12px;
                    color: var(--text-muted);
                    font-size: 0.75rem;
                    text-transform: uppercase;
                    letter-spacing: 0.05em;
                }

                .graph-detail-content {
                    display: flex;
                    flex-direction: column;
                    gap: 12px;
                }

                .graph-detail-content p {
                    color: var(--text-secondary);
                    font-size: 0.88rem;
                    line-height: 1.5;
                    white-space: pre-wrap;
                }

                /* Note editor modal */
                .note-editor-modal {
                    position: fixed;
                    inset: 0;
                    background: rgba(1, 4, 9, 0.8);
                    z-index: 900;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    padding: 24px;
                }

                .note-editor-modal[hidden] {
                    display: none;
                }

                .note-editor-card {
                    width: 100%;
                    max-width: 640px;
                    max-height: 90vh;
                    padding: 20px;
                    background: var(--bg-surface);
                    border: 1px solid var(--border-default);
                    border-radius: 12px;
                    display: flex;
                    flex-direction: column;
                    gap: 12px;
                    overflow-y: auto;
                }

                .note-editor-header {
                    display: flex;
                    align-items: center;
                    justify-content: space-between;
                }

                .note-editor-text {
                    min-height: 200px;
                    resize: vertical;
                }

                .note-editor-actions {
                    display: flex;
                    justify-content: flex-end;
                    gap: 8px;
                    margin-top: 8px;
                }

                .danger-button {
                    padding: 0 16px;
                    min-height: 36px;
                    border: 1px solid #da3633;
                    border-radius: 8px;
                    background: #2b0f10;
                    color: #f85149;
                    font-weight: 600;
                    cursor: pointer;
                }

                .danger-button:hover:not(:disabled) {
                    background: #4a1515;
                }

                /* Live caption workspace */
                .live-workspace {
                    flex-direction: row;
                }

                .live-header-meta {
                    display: flex;
                    align-items: center;
                    gap: 12px;
                    margin-left: auto;
                    margin-right: 16px;
                }

                .live-timer {
                    font-family: "Consolas", "Monaco", monospace;
                    font-size: 1rem;
                    color: var(--text-primary);
                    font-weight: 700;
                    letter-spacing: 0.04em;
                }

                .live-rec-badge {
                    display: inline-flex;
                    align-items: center;
                    padding: 4px 10px;
                    background: #da3633;
                    color: #ffffff;
                    font-size: 0.72rem;
                    font-weight: 800;
                    border-radius: 12px;
                    letter-spacing: 0.08em;
                    animation: record-pulse 1.4s ease-in-out infinite;
                }

                .live-rec-badge[hidden] { display: none; }

                .live-sidebar {
                    width: 320px;
                    min-width: 260px;
                    padding: 16px;
                    border-right: 1px solid var(--bg-surface-3);
                    overflow-y: auto;
                    display: flex;
                    flex-direction: column;
                    gap: 12px;
                    background: var(--bg-surface);
                }

                .live-sidebar-list {
                    list-style: none;
                    padding: 0;
                    margin: 0;
                    display: flex;
                    flex-direction: column;
                    gap: 8px;
                }

                .live-sidebar-item {
                    padding: 10px 12px;
                    border: 1px solid var(--border-default);
                    border-radius: 10px;
                    background: var(--bg-surface-2);
                    cursor: pointer;
                    display: flex;
                    flex-direction: column;
                    gap: 6px;
                    transition: all 0.15s ease;
                }

                .live-sidebar-item:hover {
                    border-color: #1f6feb;
                    background: #1a2332;
                }

                .live-sidebar-item-header {
                    display: flex;
                    justify-content: space-between;
                    align-items: baseline;
                    gap: 8px;
                }

                .live-sidebar-item-header strong {
                    font-size: 0.9rem;
                    color: var(--text-primary);
                    overflow: hidden;
                    text-overflow: ellipsis;
                    white-space: nowrap;
                }

                .live-sidebar-summary {
                    margin: 0;
                    color: var(--text-muted);
                    font-size: 0.78rem;
                }

                .live-sidebar-actions {
                    display: flex;
                    gap: 4px;
                    justify-content: flex-end;
                }

                .live-sidebar-actions .text-button {
                    min-height: 28px;
                    padding: 0 10px;
                    font-size: 0.8rem;
                }

                .live-main {
                    flex: 1;
                    display: flex;
                    flex-direction: column;
                    padding: 20px;
                    gap: 16px;
                    overflow: hidden;
                    min-height: 0;
                }

                .live-config-row {
                    display: flex;
                    align-items: center;
                    gap: 10px;
                    padding: 10px 14px;
                    border: 1px solid var(--border-default);
                    border-radius: 10px;
                    background: var(--bg-surface-2);
                    flex-wrap: wrap;
                }

                .live-config-row.is-locked {
                    opacity: 0.6;
                    pointer-events: none;
                }

                .live-model-row {
                    display: flex;
                    align-items: center;
                    gap: 10px;
                    flex-wrap: wrap;
                    color: var(--text-muted);
                    font-size: 0.84rem;
                }

                .live-model-row.is-locked {
                    opacity: 0.6;
                    pointer-events: none;
                }

                .live-model-select {
                    max-width: 240px;
                    min-width: 170px;
                }

                .live-current {
                    display: grid;
                    grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
                    gap: 12px;
                    padding: 12px;
                    border: 1px solid var(--border-default);
                    border-radius: 8px;
                    background: var(--bg-surface-2);
                }

                .live-current-block {
                    min-width: 0;
                    display: flex;
                    flex-direction: column;
                    gap: 6px;
                }

                .live-current-label {
                    color: var(--text-muted);
                    font-size: 0.78rem;
                    font-weight: 700;
                    letter-spacing: 0;
                }

                .live-current-text {
                    min-height: 56px;
                    max-height: 128px;
                    overflow-y: auto;
                    margin: 0;
                    padding: 10px 12px;
                    border: 1px solid var(--border-default);
                    border-radius: 8px;
                    background: var(--bg-surface);
                    color: var(--text-primary);
                    font-size: 1rem;
                    line-height: 1.55;
                    white-space: pre-wrap;
                    overflow-wrap: anywhere;
                }

                .live-current-translation {
                    color: var(--text-primary);
                }

                .live-toggle {
                    margin: 0;
                    font-weight: 600;
                }

                .field-label-inline {
                    color: var(--text-muted);
                    font-size: 0.8rem;
                    font-weight: 600;
                    text-transform: uppercase;
                    letter-spacing: 0.04em;
                }

                .live-captions {
                    flex: 1;
                    overflow-y: auto;
                    padding: 16px 18px;
                    border: 1px solid var(--border-default);
                    border-radius: 12px;
                    background: var(--bg-surface);
                    display: flex;
                    flex-direction: column;
                    gap: 12px;
                    min-height: 0;
                }

                .live-empty {
                    margin: auto;
                    text-align: center;
                    color: var(--text-muted);
                    font-size: 0.95rem;
                    line-height: 1.6;
                    padding: 24px;
                }

                .live-caption-row {
                    display: grid;
                    grid-template-columns: 72px 1fr;
                    gap: 14px;
                    align-items: flex-start;
                    padding: 10px 12px;
                    border-radius: 10px;
                    background: var(--bg-surface-2);
                    border-left: 3px solid #1f6feb;
                    animation: live-fade-in 0.2s ease-out;
                }

                @keyframes live-fade-in {
                    from { opacity: 0; transform: translateY(6px); }
                    to { opacity: 1; transform: translateY(0); }
                }

                .live-caption-time {
                    color: var(--text-muted);
                    font-family: "Consolas", "Monaco", monospace;
                    font-size: 0.82rem;
                    padding-top: 2px;
                }

                .live-caption-body {
                    min-width: 0;
                }

                .live-caption-pairs {
                    display: flex;
                    flex-direction: column;
                    gap: 8px;
                }

                .live-caption-pair {
                    display: grid;
                    grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
                    gap: 12px;
                    align-items: start;
                    padding-bottom: 8px;
                    border-bottom: 1px solid var(--bg-surface-3);
                }

                .live-caption-pair:last-child {
                    padding-bottom: 0;
                    border-bottom: 0;
                }

                .live-caption-source {
                    margin: 0;
                    color: var(--text-secondary);
                    font-size: 0.95rem;
                    line-height: 1.5;
                    word-break: break-word;
                }

                .live-caption-translated {
                    margin: 0;
                    color: var(--text-primary);
                    font-size: 1.05rem;
                    font-weight: 600;
                    line-height: 1.55;
                    word-break: break-word;
                }

                .live-controls {
                    display: flex;
                    align-items: center;
                    gap: 12px;
                    padding: 12px 14px;
                    border: 1px solid var(--border-default);
                    border-radius: 12px;
                    background: var(--bg-surface-2);
                }

                .live-action-button {
                    padding: 0 20px;
                    min-height: 44px;
                    border-radius: 22px;
                    font-size: 0.95rem;
                }

                .live-hint {
                    color: var(--text-muted);
                    font-size: 0.82rem;
                    margin-left: auto;
                }

                @media (max-width: 960px) {
                    .live-workspace { flex-direction: column; }
                    .live-current { grid-template-columns: 1fr; }
                    .live-caption-pair { grid-template-columns: 1fr; }
                    .live-sidebar {
                        width: 100%;
                        max-height: 200px;
                        border-right: 0;
                        border-bottom: 1px solid var(--bg-surface-3);
                    }
                    .notes-workspace,
                    .main-notes-workspace { flex-direction: column; }
                    .notes-sidebar {
                        width: 100%;
                        max-width: none;
                        max-height: 36vh;
                        border-right: 0;
                        border-bottom: 1px solid var(--bg-surface-3);
                    }
                    .work-pane {
                        max-height: 60vh;
                    }
                }

                /* Translation workspace (legacy styles kept for compatibility) */
                .translation-workspace {
                    flex-direction: row;
                }

                .translation-sidebar {
                    width: 340px;
                    min-width: 280px;
                    padding: 20px;
                    border-right: 1px solid var(--bg-surface-3);
                    overflow-y: auto;
                    display: flex;
                    flex-direction: column;
                    gap: 12px;
                }

                .sidebar-title {
                    margin: 0;
                    font-size: 1rem;
                    color: var(--text-primary);
                }

                .translation-sidebar-list {
                    list-style: none;
                    padding: 0;
                    margin: 0;
                    display: flex;
                    flex-direction: column;
                    gap: 8px;
                }

                .translation-sidebar-item {
                    padding: 10px;
                    border: 1px solid var(--border-default);
                    border-radius: 8px;
                    background: var(--bg-surface-2);
                    cursor: pointer;
                    display: flex;
                    flex-direction: column;
                    gap: 4px;
                    transition: all 0.15s ease;
                }

                .translation-sidebar-item:hover {
                    border-color: #1f6feb;
                }

                .translation-sidebar-top {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                }

                .translation-sidebar-source,
                .translation-sidebar-result {
                    margin: 0;
                    color: var(--text-secondary);
                    font-size: 0.82rem;
                    line-height: 1.4;
                    display: -webkit-box;
                    -webkit-line-clamp: 2;
                    -webkit-box-orient: vertical;
                    overflow: hidden;
                }

                .translation-sidebar-result {
                    color: var(--text-primary);
                    font-weight: 600;
                }

                .translation-sidebar-arrow {
                    margin: 0;
                    color: var(--text-muted);
                    font-size: 0.8rem;
                    text-align: center;
                }

                .translation-main {
                    flex: 1;
                    display: flex;
                    flex-direction: column;
                    padding: 20px;
                    gap: 16px;
                    overflow-y: auto;
                }

                .translation-input-row {
                    display: grid;
                    grid-template-columns: 1fr auto 1fr;
                    gap: 12px;
                    align-items: stretch;
                }

                .translation-column {
                    display: flex;
                    flex-direction: column;
                    gap: 8px;
                    min-width: 0;
                }

                .translation-column-header {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    gap: 8px;
                }

                .language-inline {
                    width: auto;
                    min-width: 120px;
                    padding: 6px 8px;
                    font-size: 0.85rem;
                }

                .translation-arrow {
                    display: grid;
                    place-items: center;
                    font-size: 2rem;
                    color: var(--text-muted);
                }

                .translation-textarea {
                    flex: 1;
                    min-height: 240px;
                    resize: vertical;
                }

                .translation-result-actions {
                    display: flex;
                    justify-content: flex-end;
                    gap: 8px;
                }

                .translation-controls {
                    display: grid;
                    grid-template-columns: auto 1fr auto;
                    gap: 12px;
                    align-items: center;
                }

                @media (max-width: 960px) {
                    .translation-workspace {
                        flex-direction: column;
                    }

                    .translation-sidebar {
                        width: 100%;
                        max-height: 240px;
                        border-right: 0;
                        border-bottom: 1px solid var(--bg-surface-3);
                    }

                    .translation-input-row {
                        grid-template-columns: 1fr;
                    }

                    .translation-arrow {
                        transform: rotate(90deg);
                    }
                }

                /* Current model badge */
                .model-badge {
                    display: inline-flex;
                    align-items: center;
                    gap: 6px;
                    padding: 6px 12px;
                    border: 1px solid #1f6feb;
                    border-radius: 20px;
                    background: #0c2d6b;
                    color: #79c0ff;
                    font-size: 0.82rem;
                    font-weight: 600;
                }

                .model-badge[hidden] {
                    display: none;
                }

                .model-badge-icon {
                    font-size: 0.9rem;
                }

                .language-badge {
                    padding: 0 4px 0 10px;
                    gap: 4px;
                }

                .topbar-language-select {
                    width: auto;
                    min-width: 104px;
                    max-width: 150px;
                    padding: 6px 22px 6px 6px;
                    border: 0;
                    border-radius: 14px;
                    background: transparent;
                    color: #c9e3ff;
                    font-size: 0.8rem;
                    font-weight: 700;
                }

                .topbar-language-select:focus {
                    outline: none;
                    box-shadow: 0 0 0 2px rgba(121, 192, 255, 0.32);
                    background: rgba(121, 192, 255, 0.12);
                }

                .topbar-language-select option {
                    color: var(--text-primary);
                    background: var(--bg-surface);
                }

                .model-badge-label {
                    font-family: "Consolas", "Monaco", monospace;
                    font-size: 0.8rem;
                }

                /* Note selection (for draft composition) */
                .notes-selection-bar {
                    display: flex;
                    align-items: center;
                    gap: 8px;
                    padding: 8px 12px;
                    border-top: 1px solid var(--bg-surface-3);
                    border-bottom: 1px solid var(--bg-surface-3);
                    background: #0c2d6b;
                    color: var(--text-primary);
                    font-size: 0.82rem;
                }

                .notes-selection-bar[hidden] { display: none; }

                .notes-selection-count {
                    font-weight: 600;
                }

                .notes-selection-bar .text-button {
                    min-height: 28px;
                    padding: 0 10px;
                    font-size: 0.8rem;
                }

                #notesSelectToggle[data-active="true"] {
                    background: #0c2d6b;
                    border-color: #1f6feb;
                    color: #79c0ff;
                }

                .note-card.is-selectable {
                    position: relative;
                    padding-left: 36px;
                }

                .note-card-checkbox {
                    position: absolute;
                    top: 10px;
                    left: 10px;
                    width: 16px;
                    height: 16px;
                    accent-color: #1f6feb;
                    cursor: pointer;
                }

                .note-card[data-picked="true"] {
                    border-color: #1f6feb;
                    background: #0c2d6b;
                }

                /* Drafts workspace */
                .drafts-workspace {
                    flex-direction: row;
                }

                .drafts-sidebar {
                    width: 300px;
                    min-width: 240px;
                    padding: 14px;
                    border-right: 1px solid var(--bg-surface-3);
                    background: var(--bg-surface);
                    display: flex;
                    flex-direction: column;
                    gap: 10px;
                    overflow-y: auto;
                }

                .drafts-list {
                    list-style: none;
                    padding: 0;
                    margin: 0;
                    display: flex;
                    flex-direction: column;
                    gap: 6px;
                }

                .drafts-list-item {
                    padding: 10px 12px;
                    border: 1px solid var(--border-default);
                    border-radius: 8px;
                    background: var(--bg-surface-2);
                    cursor: pointer;
                    transition: all 0.15s ease;
                }

                .drafts-list-item:hover {
                    border-color: #1f6feb;
                    background: #1a2332;
                }

                .drafts-list-item[data-active="true"] {
                    border-color: #1f6feb;
                    background: #0c2d6b;
                    box-shadow: inset 3px 0 0 #79c0ff;
                }

                .drafts-list-item strong {
                    display: block;
                    color: var(--text-primary);
                    font-size: 0.9rem;
                    margin-bottom: 4px;
                }

                .drafts-list-meta {
                    margin: 0;
                    color: var(--text-muted);
                    font-size: 0.75rem;
                }

                .drafts-main {
                    flex: 1;
                    display: flex;
                    flex-direction: column;
                    padding: 16px 20px;
                    gap: 12px;
                    min-width: 0;
                    min-height: 0;
                }

                .drafts-editor-toolbar {
                    display: flex;
                    align-items: center;
                    gap: 10px;
                    flex-wrap: wrap;
                }

                .draft-title-input {
                    flex: 1;
                    min-width: 240px;
                    font-size: 1rem;
                    font-weight: 700;
                }

                .drafts-editor-actions {
                    display: flex;
                    align-items: center;
                    gap: 6px;
                }

                .drafts-main-body {
                    flex: 1;
                    display: grid;
                    grid-template-columns: minmax(0, 1fr) 260px;
                    gap: 12px;
                    min-height: 0;
                }

                .draft-editor-content {
                    width: 100%;
                    height: 100%;
                    min-height: 240px;
                    padding: 18px 20px;
                    border: 1px solid var(--border-default);
                    border-radius: 10px;
                    background: var(--bg-inset);
                    color: var(--text-primary);
                    font-family: "Consolas", "Monaco", ui-monospace, monospace;
                    font-size: 0.95rem;
                    line-height: 1.6;
                    resize: none;
                }

                .draft-references {
                    padding: 12px 14px;
                    border: 1px solid var(--border-default);
                    border-radius: 10px;
                    background: var(--bg-surface);
                    overflow-y: auto;
                }

                .draft-references[hidden] { display: none; }

                .draft-references h4 {
                    margin: 0 0 10px;
                    color: var(--text-muted);
                    font-size: 0.78rem;
                    text-transform: uppercase;
                    letter-spacing: 0.04em;
                }

                .draft-references ol {
                    padding-left: 20px;
                    margin: 0;
                    display: flex;
                    flex-direction: column;
                    gap: 4px;
                }

                .draft-references li {
                    color: var(--text-secondary);
                    font-size: 0.82rem;
                    line-height: 1.45;
                }

                @media (max-width: 960px) {
                    .drafts-workspace { flex-direction: column; }
                    .drafts-sidebar {
                        width: 100%;
                        max-height: 30vh;
                        border-right: 0;
                        border-bottom: 1px solid var(--bg-surface-3);
                    }
                    .drafts-main-body {
                        grid-template-columns: 1fr;
                    }
                }

                /* Quick toggles near the record button */
                .quick-toggles {
                    display: flex;
                    align-items: center;
                    gap: 10px;
                    flex-wrap: wrap;
                }

                /* Grouping container for the 3 post-processing toggles. A subtle
                   outline plus a small caption makes it obvious that these
                   belong together and are distinct from the "Save to note" pill. */
                .quick-toggle-group {
                    display: inline-flex;
                    align-items: center;
                    gap: 6px;
                    padding: 4px 8px 4px 10px;
                    border: 1px dashed var(--border-default);
                    border-radius: 14px;
                    background: var(--bg-surface);
                }

                .quick-toggle-group-label {
                    color: var(--text-muted);
                    font-size: 0.68rem;
                    font-weight: 700;
                    letter-spacing: 0.04em;
                    text-transform: uppercase;
                    padding-right: 2px;
                }

                .quick-toggle {
                    display: inline-flex;
                    align-items: center;
                    gap: 4px;
                    padding: 4px 10px;
                    border: 1px solid var(--border-default);
                    border-radius: 999px;
                    background: var(--bg-surface-2);
                    color: var(--text-muted);
                    font-size: 0.78rem;
                    font-weight: 600;
                    cursor: pointer;
                    transition: all 0.15s ease;
                }

                .quick-toggle:hover {
                    border-color: var(--border-strong);
                    color: var(--text-secondary);
                }

                .quick-toggle input {
                    display: none;
                }

                .quick-toggle:has(input:checked) {
                    border-color: #1f6feb;
                    background: #0c2d6b;
                    color: #79c0ff;
                }

                .quick-toggle-icon {
                    font-size: 0.85rem;
                }

                /* Custom step management */
                .postprocess-section-header {
                    display: flex;
                    align-items: center;
                    justify-content: space-between;
                    gap: 8px;
                }

                .postprocess-section-header .secondary-button {
                    padding: 0 12px;
                    min-height: 30px;
                    font-size: 0.8rem;
                }

                .postprocess-section-header-actions {
                    display: inline-flex;
                    gap: 6px;
                }

                /* Preset picker inside the custom-steps modal. Shares the
                   note-editor-modal chrome for consistency. */
                .custom-preset-list {
                    display: flex;
                    flex-direction: column;
                    gap: 8px;
                    max-height: 50vh;
                    overflow-y: auto;
                    padding: 4px;
                }
                .custom-preset-row {
                    display: grid;
                    grid-template-columns: 1fr auto;
                    gap: 10px;
                    align-items: center;
                    padding: 10px 12px;
                    border: 1px solid var(--border-default);
                    border-radius: 8px;
                    background: var(--bg-surface-2);
                }
                .custom-preset-body p {
                    margin: 2px 0 0 0;
                }

                .custom-step-list {
                    display: flex;
                    flex-direction: column;
                    gap: 6px;
                }

                .custom-step-row {
                    display: grid;
                    grid-template-columns: 28px 1fr auto;
                    align-items: center;
                    gap: 10px;
                    padding: 8px 10px;
                    border: 1px solid var(--border-default);
                    border-radius: 8px;
                    background: var(--bg-surface-2);
                }

                .custom-step-order {
                    display: grid;
                    place-items: center;
                    width: 24px;
                    height: 24px;
                    border-radius: 50%;
                    background: #0c2d6b;
                    color: #79c0ff;
                    font-size: 0.78rem;
                    font-weight: 800;
                    font-family: "Consolas", "Monaco", monospace;
                }

                .custom-step-body {
                    min-width: 0;
                }

                .custom-step-body strong {
                    color: var(--text-primary);
                    font-size: 0.9rem;
                    display: block;
                }

                .custom-step-meta {
                    margin: 0;
                    color: var(--text-muted);
                    font-size: 0.76rem;
                    line-height: 1.4;
                }

                .custom-step-actions {
                    display: flex;
                    gap: 4px;
                }

                .custom-step-actions .icon-button {
                    min-height: 28px;
                    width: 28px;
                    padding: 0;
                }

                .custom-step-actions .text-button {
                    min-height: 28px;
                    padding: 0 10px;
                    font-size: 0.8rem;
                }

                /* Enable / disable pill on each custom step row */
                .custom-step-enable {
                    min-height: 26px;
                    padding: 0 10px;
                    border-radius: 12px;
                    border: 1px solid var(--border-default);
                    background: var(--bg-surface-3);
                    color: var(--text-muted);
                    font-size: 0.72rem;
                    font-weight: 700;
                    letter-spacing: 0.03em;
                    cursor: pointer;
                }

                .custom-step-enable[data-enabled="true"] {
                    border-color: #2ea043;
                    background: #0f2817;
                    color: #4ac26b;
                }

                body[data-theme="light"] .custom-step-enable[data-enabled="true"] {
                    background: #dcf5e0;
                    color: #166534;
                }

                .custom-step-row[data-enabled="false"] {
                    opacity: 0.55;
                }

                .custom-step-row[data-enabled="false"] .custom-step-body strong {
                    text-decoration: line-through dotted var(--text-muted);
                }

                /* Transcription tab: single "current model" summary card */
                .current-model-card {
                    display: flex;
                    flex-direction: column;
                    gap: 10px;
                    padding: 14px;
                    border: 1px solid var(--border-default);
                    border-radius: 10px;
                    background: var(--bg-surface-2);
                }

                .current-model-card-head {
                    display: grid;
                    grid-template-columns: 32px 1fr auto;
                    gap: 12px;
                    align-items: center;
                }

                .current-model-icon {
                    display: grid;
                    place-items: center;
                    width: 32px;
                    height: 32px;
                    border-radius: 50%;
                    background: #0c2d6b;
                    color: #79c0ff;
                    font-size: 1rem;
                    font-weight: 800;
                }

                .current-model-icon.is-spinning {
                    animation: spin 1.2s linear infinite;
                    background: #302608;
                    color: #f0c96f;
                }

                .current-model-info {
                    min-width: 0;
                }

                .current-model-alias {
                    display: block;
                    font-family: "Consolas", "Monaco", monospace;
                    font-size: 0.95rem;
                    color: var(--text-primary);
                    font-weight: 700;
                    margin-bottom: 2px;
                }

                .current-model-desc {
                    margin: 0;
                    color: var(--text-secondary);
                    font-size: 0.82rem;
                    line-height: 1.4;
                }

                .current-model-hint {
                    margin: 0;
                    color: var(--text-muted);
                    font-size: 0.8rem;
                    line-height: 1.5;
                }

                .current-model-card .secondary-button {
                    align-self: flex-start;
                    padding: 0 14px;
                    min-height: 34px;
                    font-size: 0.85rem;
                }

                /* Unified models panel (settings → Models) */
                .models-summary {
                    display: grid;
                    gap: 8px;
                    padding: 12px 14px;
                    border: 1px solid var(--border-default);
                    border-radius: 10px;
                    background: var(--bg-surface-2);
                }

                .models-summary-row {
                    display: flex;
                    align-items: center;
                    gap: 10px;
                }

                .models-summary-label {
                    color: var(--text-muted);
                    font-size: 0.78rem;
                    text-transform: uppercase;
                    letter-spacing: 0.04em;
                    font-weight: 700;
                    min-width: 130px;
                }

                .models-summary-value {
                    flex: 1;
                    color: var(--text-primary);
                    font-size: 0.88rem;
                    overflow: hidden;
                    text-overflow: ellipsis;
                    white-space: nowrap;
                    font-family: "Consolas", "Monaco", monospace;
                }

                code.models-summary-value {
                    padding: 4px 8px;
                    border-radius: 6px;
                    background: var(--bg-inset);
                    border: 1px solid var(--bg-surface-3);
                }

                .models-section {
                    display: flex;
                    flex-direction: column;
                    gap: 10px;
                    padding: 14px;
                    border: 1px solid var(--border-default);
                    border-radius: 10px;
                    background: var(--bg-surface);
                }

                .models-section-header {
                    display: flex;
                    align-items: flex-start;
                    gap: 12px;
                }

                .models-section-icon {
                    font-size: 1.35rem;
                    line-height: 1.1;
                    padding-top: 2px;
                }

                .models-section-header h3 {
                    margin: 0 0 2px;
                    font-size: 0.98rem;
                }

                .models-section-desc {
                    margin: 0;
                    color: var(--text-muted);
                    font-size: 0.82rem;
                    line-height: 1.45;
                }

                .models-group-list {
                    display: flex;
                    flex-direction: column;
                    gap: 6px;
                }

                .models-empty {
                    margin: 0;
                    padding: 12px;
                    border: 1px dashed var(--border-default);
                    border-radius: 8px;
                    color: var(--text-muted);
                    font-size: 0.85rem;
                    text-align: center;
                }

                .model-row {
                    display: grid;
                    grid-template-columns: 28px 1fr auto;
                    align-items: center;
                    gap: 12px;
                    padding: 10px 12px;
                    border: 1px solid var(--border-default);
                    border-radius: 8px;
                    background: var(--bg-surface-2);
                }

                .model-row[data-active="true"] {
                    border-color: #1f6feb;
                    background: #0c2d6b;
                }

                .model-row[data-downloading="true"] {
                    border-color: #bb8009;
                    background: #302608;
                }

                .model-row-icon {
                    display: grid;
                    place-items: center;
                    width: 28px;
                    height: 28px;
                    border-radius: 50%;
                    background: var(--bg-surface-3);
                    color: var(--text-muted);
                    font-size: 0.95rem;
                    font-weight: 800;
                }

                .model-row[data-active="true"] .model-row-icon {
                    background: #1f6feb;
                    color: #ffffff;
                }

                .model-row:not([data-active="true"]):not([data-downloading="true"]) .model-row-icon:where(.is-cached) {
                    background: #0f2817;
                    color: #4ac26b;
                }

                .model-row-icon.is-spinning {
                    animation: spin 1.2s linear infinite;
                    background: #302608;
                    color: #f0c96f;
                }

                .model-row-body {
                    display: grid;
                    gap: 4px;
                    min-width: 0;
                }

                .model-row-name-row {
                    display: flex;
                    align-items: center;
                    gap: 8px;
                    flex-wrap: wrap;
                }

                .model-row-name {
                    font-family: "Consolas", "Monaco", monospace;
                    font-size: 0.9rem;
                    color: var(--text-primary);
                    font-weight: 700;
                }

                .model-row-status {
                    padding: 2px 8px;
                    border-radius: 10px;
                    font-size: 0.68rem;
                    font-weight: 700;
                    letter-spacing: 0.04em;
                    text-transform: uppercase;
                }

                .model-row-status[data-variant="active"] {
                    background: #0c2d6b;
                    color: #79c0ff;
                }

                .model-row-status[data-variant="cached"] {
                    background: #0f2817;
                    color: #4ac26b;
                }

                .model-row-status[data-variant="missing"] {
                    background: var(--bg-surface-3);
                    color: var(--text-muted);
                }

                .model-row-status[data-variant="downloading"] {
                    background: #302608;
                    color: #f0c96f;
                }

                .model-row-status[data-variant="incompatible"] {
                    background: #3a0f0f;
                    color: #ffa198;
                }

                /* Dim model rows that can't run on this host so the eye goes
                   to the usable entries first. Actions remain available (with
                   a confirm prompt) in case the user wants to try anyway. */
                .model-row[data-incompatible="true"] {
                    opacity: 0.58;
                }
                .model-row[data-incompatible="true"]:hover {
                    opacity: 0.85;
                }

                /* Execution-provider chips in the Models tab summary. */
                .models-ep-list {
                    display: flex;
                    flex-wrap: wrap;
                    gap: 4px;
                    align-items: center;
                }
                .models-ep-chip {
                    display: inline-flex;
                    align-items: center;
                    gap: 4px;
                    padding: 2px 8px;
                    border-radius: 10px;
                    font-size: 0.72rem;
                    font-weight: 600;
                    font-family: inherit;
                    border: 1px solid var(--border-default);
                }
                .models-ep-chip[data-registered="true"] {
                    background: #0f2817;
                    color: #4ac26b;
                    border-color: #0f2817;
                }
                .models-ep-chip[data-registered="true"]::before {
                    content: "●";
                    font-size: 0.65rem;
                }
                .models-ep-chip[data-registered="false"] {
                    background: var(--bg-surface-3);
                    color: var(--text-muted);
                }
                .models-ep-chip[data-registered="false"]::before {
                    content: "○";
                    font-size: 0.65rem;
                }

                .model-row-desc {
                    margin: 0;
                    color: var(--text-secondary);
                    font-size: 0.82rem;
                    line-height: 1.4;
                }

                .model-test-result {
                    display: flex;
                    align-items: center;
                    gap: 6px;
                    flex-wrap: wrap;
                    margin-top: 4px;
                    font-size: 0.78rem;
                }

                .model-test-title {
                    color: var(--text-muted);
                    font-weight: 700;
                }

                .model-test-chip {
                    display: inline-flex;
                    align-items: center;
                    min-height: 24px;
                    padding: 2px 8px;
                    border-radius: 999px;
                    border: 1px solid var(--border-default);
                    background: var(--bg-surface-3);
                    color: var(--text-muted);
                    font-weight: 700;
                }

                .model-test-chip[data-ok="true"] {
                    border-color: rgba(61, 220, 151, 0.35);
                    background: rgba(61, 220, 151, 0.08);
                    color: #7ee7b7;
                }

                .model-test-chip[data-ok="false"] {
                    border-color: rgba(248, 81, 73, 0.35);
                    background: rgba(248, 81, 73, 0.08);
                    color: #ff9a92;
                }

                .model-row-actions {
                    display: flex;
                    gap: 6px;
                }

                .model-row-delete {
                    width: 30px;
                    min-height: 30px;
                    padding: 0;
                    color: var(--text-muted);
                }

                .model-row-delete:hover:not(:disabled) {
                    border-color: #da3633;
                    background: #2b0f10;
                    color: #f85149;
                }

                /* Model list in settings */
                .model-list {
                    display: grid;
                    gap: 8px;
                }

                .model-item {
                    display: grid;
                    grid-template-columns: minmax(0, 1fr) auto;
                    align-items: center;
                    gap: 8px;
                    padding: 12px 14px;
                    border: 1px solid var(--border-default);
                    border-radius: 8px;
                    background: var(--bg-surface);
                    transition: all 0.15s ease;
                }

                .model-item:hover {
                    border-color: var(--border-strong);
                    background: var(--bg-surface-2);
                }

                .model-item[data-active="true"] {
                    border-color: #1f6feb;
                    background: #0c2d6b;
                }

                .model-item[data-downloading="true"] {
                    border-color: #bb8009;
                    background: #302608;
                }

                .model-item-select {
                    display: grid;
                    grid-template-columns: auto auto minmax(0, 1fr);
                    align-items: center;
                    gap: 12px;
                    cursor: pointer;
                    min-width: 0;
                }

                .model-item input[type="radio"] {
                    width: 16px;
                    height: 16px;
                    accent-color: #1f6feb;
                    cursor: pointer;
                }

                .model-item-delete {
                    display: grid;
                    place-items: center;
                    width: 36px;
                    height: 36px;
                    min-height: 36px;
                    padding: 0;
                    border: 1px solid var(--border-default);
                    border-radius: 8px;
                    background: var(--bg-surface-3);
                    color: var(--text-muted);
                    font-size: 1rem;
                    cursor: pointer;
                    transition: all 0.15s ease;
                }

                .model-item-delete:hover {
                    border-color: #da3633;
                    background: #2b0f10;
                    color: #f85149;
                }

                .model-list-empty {
                    padding: 16px;
                    border: 1px dashed var(--border-default);
                    border-radius: 8px;
                    background: var(--bg-surface);
                    color: var(--text-muted);
                    font-size: 0.9rem;
                    text-align: center;
                }

                .model-item-icon {
                    display: grid;
                    width: 28px;
                    height: 28px;
                    place-items: center;
                    border-radius: 50%;
                    font-size: 0.95rem;
                    font-weight: 800;
                    background: var(--bg-surface-3);
                    color: var(--text-muted);
                }

                .model-item[data-active="true"] .model-item-icon {
                    background: #1f6feb;
                    color: #ffffff;
                }

                .model-item[data-downloaded="true"]:not([data-active="true"]):not([data-downloading="true"]) .model-item-icon {
                    background: #0f2817;
                    color: #4ac26b;
                }

                .model-item[data-downloading="true"] .model-item-icon {
                    background: #302608;
                    color: #f0c96f;
                    animation: spin 1.2s linear infinite;
                }

                .model-item-body {
                    display: grid;
                    gap: 2px;
                    min-width: 0;
                }

                .model-item-name {
                    font-family: "Consolas", "Monaco", monospace;
                    font-size: 0.9rem;
                    font-weight: 700;
                    color: var(--text-primary);
                }

                .model-item-description {
                    font-size: 0.82rem;
                    color: var(--text-secondary);
                }

                .model-item-status {
                    margin-top: 2px;
                    font-size: 0.72rem;
                    font-weight: 700;
                    letter-spacing: 0.03em;
                    text-transform: uppercase;
                }

                .model-item-status[data-variant="active"] {
                    color: #79c0ff;
                }

                .model-item-status[data-variant="downloaded"] {
                    color: #4ac26b;
                }

                .model-item-status[data-variant="missing"] {
                    color: var(--text-muted);
                }

                .model-item-status[data-variant="downloading"] {
                    color: #f0c96f;
                }

                /* Status badge */
                .status-badge {
                    min-width: 100px;
                    border: 1px solid var(--border-default);
                    border-radius: 20px;
                    padding: 6px 14px;
                    background: var(--bg-surface-2);
                    color: var(--text-muted);
                    text-align: center;
                    font-size: 0.85rem;
                    font-weight: 600;
                }

                .status-badge[data-mode="busy"] {
                    border-color: #bb8009;
                    background: #302608;
                    color: #f0c96f;
                }

                .status-badge[data-mode="ready"] {
                    border-color: #2ea043;
                    background: #0f2817;
                    color: #4ac26b;
                }

                .status-badge[data-mode="error"] {
                    border-color: #da3633;
                    background: #2b0f10;
                    color: #f85149;
                }

                /* Buttons */
                button {
                    border: 0;
                    border-radius: 8px;
                    padding: 0 16px;
                    min-height: 36px;
                    font-weight: 600;
                    cursor: pointer;
                    transition: all 0.15s ease;
                }

                button:disabled {
                    cursor: not-allowed;
                    opacity: 0.5;
                }

                .icon-button, .text-button {
                    border: 1px solid var(--border-default);
                    background: var(--bg-surface-3);
                    color: var(--text-primary);
                    padding: 0 12px;
                    min-height: 32px;
                    font-size: 0.9rem;
                }

                .icon-button:hover:not(:disabled), .text-button:hover:not(:disabled) {
                    background: var(--border-default);
                    border-color: var(--border-strong);
                }

                .settings-button {
                    font-size: 1.05rem;
                    width: 32px;
                    min-height: 32px;
                    padding: 0;
                }

                .secondary-button {
                    border: 1px solid var(--border-default);
                    background: var(--bg-surface-3);
                    color: var(--text-primary);
                }

                .secondary-button:hover:not(:disabled) {
                    background: var(--border-default);
                    border-color: var(--border-strong);
                }

                .primary-button {
                    background: #238636;
                    color: #ffffff;
                    font-weight: 700;
                }

                .primary-button:hover:not(:disabled) {
                    background: #2ea043;
                }

                /* Toast notifications */
                .toast-container {
                    position: fixed;
                    top: 72px;
                    right: 24px;
                    z-index: 1500;
                    display: flex;
                    flex-direction: column;
                    gap: 8px;
                    pointer-events: none;
                    max-width: calc(100vw - 48px);
                }

                .toast {
                    display: flex;
                    align-items: center;
                    gap: 10px;
                    padding: 10px 14px;
                    border-radius: 10px;
                    background: rgba(22, 27, 34, 0.92);
                    border: 1px solid var(--border-default);
                    color: var(--text-primary);
                    font-size: 0.88rem;
                    font-weight: 500;
                    box-shadow: 0 8px 24px rgba(1, 4, 9, 0.4);
                    backdrop-filter: blur(8px);
                    pointer-events: auto;
                    min-width: 200px;
                    max-width: 380px;
                    animation: toast-in 0.2s ease-out;
                    transform-origin: top right;
                }

                .toast.is-leaving {
                    animation: toast-out 0.25s ease-in forwards;
                }

                .toast-icon {
                    flex: 0 0 auto;
                    font-size: 1rem;
                }

                .toast-message {
                    flex: 1;
                    line-height: 1.4;
                    word-break: break-word;
                }

                .toast[data-variant="success"] {
                    border-color: #2ea043;
                    background: rgba(15, 40, 23, 0.92);
                }

                .toast[data-variant="success"] .toast-icon {
                    color: #4ac26b;
                }

                .toast[data-variant="info"] {
                    border-color: #1f6feb;
                    background: rgba(12, 45, 107, 0.85);
                }

                .toast[data-variant="info"] .toast-icon {
                    color: #79c0ff;
                }

                .toast[data-variant="warning"] {
                    border-color: #bb8009;
                    background: rgba(48, 38, 8, 0.92);
                }

                .toast[data-variant="warning"] .toast-icon {
                    color: #f0c96f;
                }

                @keyframes toast-in {
                    from {
                        opacity: 0;
                        transform: translateY(-8px) scale(0.96);
                    }
                    to {
                        opacity: 1;
                        transform: translateY(0) scale(1);
                    }
                }

                @keyframes toast-out {
                    to {
                        opacity: 0;
                        transform: translateY(-6px) scale(0.97);
                    }
                }

                @media (prefers-reduced-motion: reduce) {
                    .toast {
                        animation: none;
                    }
                    .toast.is-leaving {
                        opacity: 0;
                    }
                }

                /* Error banner */
                .error-banner {
                    position: fixed;
                    top: 72px;
                    left: 50%;
                    transform: translateX(-50%);
                    z-index: 1450;
                    width: min(720px, calc(100vw - 48px));
                    border: 1px solid #da3633;
                    border-radius: 10px;
                    background: #2b0f10;
                    color: #f85149;
                    padding: 12px 16px;
                    box-shadow: 0 12px 32px rgba(1, 4, 9, 0.36);
                }

                .error-banner[hidden] {
                    display: none;
                }

                .error-banner-content {
                    display: grid;
                    grid-template-columns: auto 1fr auto;
                    gap: 12px;
                    align-items: start;
                }

                .error-banner-icon {
                    font-size: 1.25rem;
                    padding-top: 2px;
                }

                .error-banner-body {
                    min-width: 0;
                }

                .error-banner-body strong {
                    display: block;
                    margin-bottom: 2px;
                    color: #f85149;
                    font-size: 0.9rem;
                }

                .error-banner-body p {
                    margin: 0;
                    color: #ffd6d6;
                    font-size: 0.85rem;
                    white-space: pre-wrap;
                    word-break: break-word;
                }

                .error-banner-close {
                    min-height: 28px;
                    padding: 0 8px;
                    border: 0;
                    background: transparent;
                    color: #f85149;
                    cursor: pointer;
                    font-size: 1rem;
                }

                .requirements-banner {
                    position: fixed;
                    top: 72px;
                    left: 50%;
                    transform: translateX(-50%);
                    z-index: 1460;
                    width: min(760px, calc(100vw - 48px));
                    border: 1px solid #bb8009;
                    border-radius: 10px;
                    background: #302608;
                    color: #f0c96f;
                    padding: 12px 16px;
                    box-shadow: 0 12px 32px rgba(1, 4, 9, 0.36);
                }

                .requirements-banner[hidden] {
                    display: none;
                }

                .requirements-banner-content {
                    display: grid;
                    grid-template-columns: auto 1fr auto;
                    gap: 12px;
                    align-items: start;
                }

                .requirements-banner-icon {
                    display: inline-flex;
                    align-items: center;
                    justify-content: center;
                    width: 24px;
                    height: 24px;
                    border: 1px solid currentColor;
                    border-radius: 999px;
                    font-size: 0.9rem;
                    font-weight: 800;
                }

                .requirements-banner-body {
                    min-width: 0;
                }

                .requirements-banner-body strong {
                    display: block;
                    margin-bottom: 2px;
                    color: currentColor;
                    font-size: 0.9rem;
                }

                .requirements-banner-body p {
                    margin: 0;
                    color: #f4d88f;
                    font-size: 0.85rem;
                    line-height: 1.45;
                    white-space: pre-wrap;
                    word-break: break-word;
                }

                .requirements-command {
                    display: inline-block;
                    margin-top: 8px;
                    padding: 6px 8px;
                    border: 1px solid rgba(240, 201, 111, 0.45);
                    border-radius: 6px;
                    background: rgba(1, 4, 9, 0.22);
                    color: #ffe7a3;
                    font-family: "Consolas", "Monaco", monospace;
                    font-size: 0.82rem;
                    word-break: break-all;
                }

                .requirements-banner-actions {
                    display: flex;
                    align-items: center;
                    justify-content: flex-end;
                    flex-wrap: wrap;
                    gap: 8px;
                }

                .requirements-banner-actions .text-button,
                .requirements-banner-actions .icon-button {
                    min-height: 30px;
                    padding: 0 10px;
                    white-space: nowrap;
                }

                @media (max-width: 760px) {
                    .requirements-banner-content {
                        grid-template-columns: auto 1fr;
                    }
                    .requirements-banner-actions {
                        grid-column: 1 / -1;
                        justify-content: flex-start;
                    }
                }

                /* Drop overlay */
                .drop-overlay {
                    display: none;
                    position: fixed;
                    inset: 0;
                    background: rgba(1, 4, 9, 0.95);
                    z-index: 9999;
                    align-items: center;
                    justify-content: center;
                }

                .drop-prompt {
                    text-align: center;
                    padding: 48px;
                    border: 3px dashed var(--border-strong);
                    border-radius: 16px;
                    background: var(--bg-surface-2);
                }

                .drop-icon {
                    font-size: 4rem;
                    margin-bottom: 16px;
                }

                .drop-prompt p {
                    font-size: 1.5rem;
                    font-weight: 700;
                    color: var(--text-primary);
                    margin-bottom: 8px;
                }

                .drop-prompt small {
                    font-size: 0.9rem;
                    color: var(--text-muted);
                }

                /* Caption display */
                .caption-display {
                    flex: 1;
                    display: flex;
                    flex-direction: column;
                    border: 1px solid var(--border-default);
                    border-radius: 12px;
                    background: var(--bg-surface);
                    overflow: hidden;
                }

                .caption-header {
                    display: flex;
                    align-items: center;
                    justify-content: space-between;
                    padding: 12px 16px;
                    border-bottom: 1px solid var(--bg-surface-3);
                    background: var(--bg-surface-2);
                    gap: 12px;
                }

                .caption-header-left {
                    display: flex;
                    align-items: center;
                    gap: 10px;
                    min-width: 0;
                }

                .caption-label {
                    font-size: 0.78rem;
                    font-weight: 700;
                    color: var(--text-muted);
                    text-transform: uppercase;
                    letter-spacing: 0.05em;
                    white-space: nowrap;
                }

                .caption-actions {
                    display: flex;
                    gap: 6px;
                    align-items: center;
                }

                .caption-actions .icon-button {
                    min-height: 32px;
                    width: 32px;
                    padding: 0;
                }

                .caption-content {
                    flex: 1;
                    padding: 32px;
                    font-size: 1.5rem;
                    line-height: 1.7;
                    color: var(--text-primary);
                    overflow-y: auto;
                    white-space: pre-wrap;
                    word-wrap: break-word;
                    width: 100%;
                    border: 0;
                    background: transparent;
                    resize: none;
                    font-family: inherit;
                    outline: none;
                    transition: background 0.15s ease;
                }

                .caption-content::placeholder {
                    color: var(--text-subtle);
                }

                .caption-content:focus {
                    background: rgba(31, 111, 235, 0.05);
                }

                .caption-content[data-readonly="true"] {
                    background: transparent;
                    cursor: not-allowed;
                    color: var(--text-secondary);
                }

                .caption-dirty {
                    padding: 4px 8px;
                    border-radius: 12px;
                    background: #302608;
                    color: #f0c96f;
                    font-size: 0.72rem;
                    font-weight: 700;
                    letter-spacing: 0.03em;
                    text-transform: uppercase;
                }

                .caption-stats {
                    display: flex;
                    gap: 16px;
                    padding: 10px 20px;
                    border-top: 1px solid var(--bg-surface-3);
                    background: var(--bg-surface);
                    color: var(--text-muted);
                    flex-wrap: wrap;
                }

                .caption-stats[hidden] {
                    display: none;
                }

                .caption-stat {
                    display: inline-flex;
                    align-items: baseline;
                    gap: 4px;
                    font-size: 0.78rem;
                }

                .caption-stat[hidden] {
                    display: none;
                }

                .caption-stat-value {
                    color: var(--text-primary);
                    font-weight: 700;
                    font-size: 0.88rem;
                }

                .caption-stat-label {
                    color: var(--text-muted);
                    text-transform: uppercase;
                    letter-spacing: 0.03em;
                }

                .note-analysis {
                    display: grid;
                    gap: 10px;
                    padding: 14px;
                    border: 1px solid var(--border-default);
                    border-radius: 8px;
                    background: var(--bg-surface-2);
                }

                .note-analysis[hidden] {
                    display: none;
                }

                .note-analysis-row {
                    display: grid;
                    grid-template-columns: 90px 1fr;
                    gap: 10px;
                    align-items: start;
                }

                .note-analysis-label {
                    color: var(--text-muted);
                    font-size: 0.72rem;
                    font-weight: 700;
                    text-transform: uppercase;
                    letter-spacing: 0.04em;
                    padding-top: 3px;
                }

                .note-analysis-tone {
                    display: inline-block;
                    padding: 3px 10px;
                    border-radius: 12px;
                    background: #0c2d6b;
                    color: #79c0ff;
                    font-weight: 700;
                    font-size: 0.82rem;
                }

                .note-analysis-summary {
                    margin: 0;
                    color: var(--text-primary);
                    font-size: 0.88rem;
                    line-height: 1.5;
                    white-space: pre-wrap;
                }

                .note-analysis-keywords {
                    display: flex;
                    flex-wrap: wrap;
                    gap: 4px;
                }

                .note-analysis-keywords .tag-chip {
                    cursor: pointer;
                }

                .note-analysis-keywords .tag-chip:hover {
                    background: #2ea043;
                }

                .caption-dirty[hidden] {
                    display: none;
                }

                .primary-inline {
                    background: #238636;
                    color: #ffffff;
                    border: 1px solid #2ea043;
                }

                .primary-inline:hover:not(:disabled) {
                    background: #2ea043;
                }

                /* Control bar - center-aligned record CTA */
                .control-bar {
                    display: grid;
                    grid-template-columns: 1fr auto 1fr;
                    align-items: center;
                    gap: 16px;
                    padding: 14px 20px;
                    border: 1px solid var(--border-default);
                    border-radius: 12px;
                    background: var(--bg-surface-2);
                }

                /* Wrap the record button so the progress ring (which sits
                   behind it) can be anchored to the same centre. */
                .record-button-wrap {
                    position: relative;
                    display: inline-flex;
                    justify-self: center;
                }

                /* Countdown ring that depletes as the recording approaches the
                   configured max duration. Driven from JS via the custom
                   property `--record-progress` (0 → fully expired, 1 → full).
                   Hidden until `data-active="true"` so it doesn't disturb the
                   button's drop shadow when idle. */
                .record-progress-ring {
                    position: absolute;
                    inset: -6px;
                    border-radius: 999px;
                    pointer-events: none;
                    opacity: 0;
                    transition: opacity 0.25s ease;
                    --record-progress: 1;
                    background:
                        conic-gradient(
                            #ffd33d calc(var(--record-progress) * 360deg),
                            rgba(255, 255, 255, 0.08) 0
                        );
                    padding: 4px;
                    mask: radial-gradient(closest-side, transparent calc(100% - 5px), #000 calc(100% - 4px));
                    -webkit-mask: radial-gradient(closest-side, transparent calc(100% - 5px), #000 calc(100% - 4px));
                }
                .record-progress-ring[data-active="true"] {
                    opacity: 1;
                }
                /* Turn the ring red as time runs out so the urgency is obvious. */
                .record-progress-ring[data-warning="true"] {
                    background:
                        conic-gradient(
                            #f85149 calc(var(--record-progress) * 360deg),
                            rgba(255, 255, 255, 0.08) 0
                        );
                }

                .record-button {
                    position: relative;
                    display: inline-flex;
                    align-items: center;
                    gap: 10px;
                    padding: 0 28px;
                    min-height: 56px;
                    border-radius: 28px;
                    background: #238636;
                    color: #ffffff;
                    font-size: 1.05rem;
                    font-weight: 700;
                    box-shadow: 0 6px 18px rgba(46, 160, 67, 0.24);
                    transition: all 0.15s ease;
                }

                .record-button:hover:not(:disabled) {
                    background: #2ea043;
                    transform: translateY(-1px);
                    box-shadow: 0 8px 22px rgba(46, 160, 67, 0.32);
                }

                .record-button[data-recording="true"] {
                    background: #da3633;
                    box-shadow: 0 6px 18px rgba(218, 54, 51, 0.32);
                    animation: record-pulse 1.4s ease-in-out infinite;
                }

                .record-button[data-recording="true"]:hover:not(:disabled) {
                    background: #f85149;
                }

                @keyframes record-pulse {
                    0%, 100% { box-shadow: 0 6px 18px rgba(218, 54, 51, 0.32); }
                    50% { box-shadow: 0 6px 24px rgba(218, 54, 51, 0.55); }
                }

                .record-countdown {
                    margin-left: 4px;
                    font-variant-numeric: tabular-nums;
                    font-size: 0.9rem;
                    opacity: 0.95;
                }
                .record-countdown[hidden] {
                    display: none;
                }

                .record-icon {
                    font-size: 1.1rem;
                }

                .record-info {
                    color: var(--text-muted);
                    font-size: 0.9rem;
                    overflow: hidden;
                    text-overflow: ellipsis;
                    white-space: nowrap;
                    justify-self: start;
                }

                .control-hint {
                    color: var(--text-subtle);
                    font-size: 0.8rem;
                    text-align: right;
                    justify-self: end;
                }

                /* Download overlay */
                .download-overlay {
                    position: fixed;
                    inset: 0;
                    background: rgba(1, 4, 9, 0.85);
                    z-index: 2000;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    padding: 24px;
                    backdrop-filter: blur(4px);
                }

                .download-overlay[hidden] {
                    display: none;
                }

                .download-card {
                    width: 100%;
                    max-width: 480px;
                    padding: 32px;
                    background: var(--bg-surface);
                    border: 1px solid var(--border-default);
                    border-radius: 12px;
                    text-align: center;
                }

                .download-spinner {
                    width: 48px;
                    height: 48px;
                    margin: 0 auto 20px;
                    border: 4px solid var(--bg-surface-3);
                    border-top-color: #238636;
                    border-radius: 50%;
                    animation: spin 1s linear infinite;
                }

                @keyframes spin {
                    to { transform: rotate(360deg); }
                }

                .download-card h3 {
                    margin-bottom: 8px;
                    font-size: 1.25rem;
                    color: var(--text-primary);
                }

                .download-card p {
                    margin-bottom: 20px;
                    color: var(--text-muted);
                    font-size: 0.9rem;
                }

                .progress-bar {
                    width: 100%;
                    height: 10px;
                    margin-bottom: 12px;
                    background: var(--bg-surface-3);
                    border-radius: 5px;
                    overflow: hidden;
                }

                .progress-fill {
                    height: 100%;
                    background: linear-gradient(90deg, #238636, #2ea043);
                    border-radius: 5px;
                    transition: width 0.3s ease;
                }

                .progress-text {
                    display: flex;
                    justify-content: space-between;
                    color: var(--text-secondary);
                    font-size: 0.9rem;
                    font-weight: 600;
                }

                #progressAlias {
                    color: var(--text-muted);
                    font-weight: 400;
                    font-family: "Consolas", "Monaco", monospace;
                    font-size: 0.85rem;
                }

                .download-actions {
                    margin-top: 18px;
                }

                .download-cancel-hint {
                    margin-top: 10px;
                    font-size: 0.78rem;
                    color: var(--text-muted);
                }

                /* Download diagnostic panel — shown when the native core
                   returns a download failure. Lists the available variants so
                   the user can identify CPU/GPU-specific entries. */
                .download-diagnostic {
                    position: fixed;
                    right: 16px;
                    bottom: 16px;
                    width: 420px;
                    max-width: calc(100vw - 32px);
                    max-height: 60vh;
                    overflow: auto;
                    padding: 14px 16px;
                    background: var(--bg-surface);
                    border: 1px solid var(--border-default);
                    border-radius: 10px;
                    z-index: 2100;
                    box-shadow: 0 10px 30px rgba(0, 0, 0, 0.35);
                }

                .download-diagnostic[hidden] {
                    display: none;
                }

                .download-diagnostic-head {
                    display: flex;
                    align-items: center;
                    justify-content: space-between;
                    margin-bottom: 8px;
                }

                .download-diagnostic-head strong {
                    color: #f85149;
                }

                .download-diagnostic-alias {
                    margin: 0 0 6px 0;
                    font-family: "Consolas", "Monaco", monospace;
                    font-size: 0.85rem;
                    color: var(--text-secondary);
                }

                .download-diagnostic-error {
                    margin: 0 0 12px 0;
                    padding: 8px 10px;
                    background: var(--bg-surface-3);
                    border-radius: 6px;
                    color: var(--text-primary);
                    font-size: 0.8rem;
                    white-space: pre-wrap;
                    word-break: break-word;
                }

                .download-diagnostic-variants-head {
                    margin-bottom: 6px;
                    font-size: 0.8rem;
                    color: var(--text-muted);
                }

                .download-diagnostic-variant {
                    padding: 6px 8px;
                    border: 1px solid var(--border-default);
                    border-radius: 6px;
                    margin-bottom: 4px;
                    font-size: 0.8rem;
                }

                .download-diagnostic-variant code {
                    font-family: "Consolas", "Monaco", monospace;
                    color: var(--text-primary);
                    font-size: 0.78rem;
                    word-break: break-all;
                }

                .download-diagnostic-variant-meta {
                    color: var(--text-muted);
                    font-size: 0.72rem;
                }

                /* Settings modal */
                .settings-modal {
                    position: fixed;
                    inset: 0;
                    background: rgba(1, 4, 9, 0.8);
                    z-index: 1000;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    padding: 24px;
                }

                .settings-modal[hidden] {
                    display: none;
                }

                .settings-container {
                    width: 100%;
                    max-width: 800px;
                    max-height: 90vh;
                    background: var(--bg-surface);
                    border: 1px solid var(--border-default);
                    border-radius: 12px;
                    display: flex;
                    flex-direction: column;
                    overflow: hidden;
                }

                .settings-header {
                    display: flex;
                    align-items: center;
                    justify-content: space-between;
                    padding: 20px 24px;
                    border-bottom: 1px solid var(--bg-surface-3);
                    background: var(--bg-surface-2);
                }

                .settings-header-actions {
                    display: flex;
                    align-items: center;
                    gap: 8px;
                }

                .settings-header-actions .text-button {
                    min-height: 30px;
                    padding: 0 10px;
                    font-size: 0.8rem;
                }

                .settings-tabs {
                    display: flex;
                    gap: 4px;
                    padding: 12px 24px;
                    border-bottom: 1px solid var(--bg-surface-3);
                    background: var(--bg-surface-2);
                }

                .settings-tab-button {
                    padding: 8px 16px;
                    border: 1px solid transparent;
                    border-radius: 8px;
                    background: transparent;
                    color: var(--text-muted);
                    font-size: 0.9rem;
                    font-weight: 600;
                }

                .settings-tab-button[aria-selected="true"] {
                    background: var(--bg-surface);
                    border-color: var(--border-default);
                    color: var(--text-primary);
                }

                .settings-content {
                    flex: 1;
                    overflow-y: auto;
                    padding: 24px;
                }

                .settings-panel {
                    display: grid;
                    gap: 16px;
                }

                .settings-panel[hidden] {
                    display: none;
                }

                .postprocess-section {
                    padding: 16px;
                    background: var(--bg-surface-2);
                    border: 1px solid var(--border-default);
                    border-radius: 8px;
                    display: grid;
                    gap: 12px;
                }

                .section-title {
                    margin: 0 0 8px;
                    padding-bottom: 8px;
                    border-bottom: 1px solid var(--bg-surface-3);
                    color: var(--text-primary);
                    font-size: 0.95rem;
                    font-weight: 700;
                }

                .preset-buttons {
                    display: grid;
                    grid-template-columns: 1fr 1fr;
                    gap: 8px;
                }

                .field-label {
                    color: var(--text-primary);
                    font-size: 0.9rem;
                    font-weight: 600;
                }

                /* Shortcuts tab: one row per action with a recordable key input. */
                .shortcut-list {
                    display: flex;
                    flex-direction: column;
                    gap: 6px;
                    margin-top: 8px;
                }
                .shortcut-row {
                    display: grid;
                    grid-template-columns: 1fr auto;
                    align-items: center;
                    gap: 12px;
                    padding: 6px 10px;
                    border: 1px solid var(--border-default);
                    border-radius: 8px;
                    background: var(--bg-surface-2);
                }
                .shortcut-row-label {
                    color: var(--text-primary);
                    font-size: 0.9rem;
                }
                .shortcut-input {
                    width: 180px;
                    padding: 6px 10px;
                    border: 1px solid var(--border-default);
                    border-radius: 6px;
                    background: var(--bg-surface);
                    color: var(--text-primary);
                    font-family: "Consolas", "Monaco", monospace;
                    font-size: 0.85rem;
                    text-align: center;
                    cursor: pointer;
                }
                .shortcut-input:focus {
                    outline: none;
                    border-color: #1f6feb;
                    box-shadow: 0 0 0 2px rgba(31, 111, 235, 0.25);
                }
                .shortcut-input[data-recording="true"] {
                    background: #302608;
                    color: #f0c96f;
                }
                .shortcut-actions {
                    margin-top: 14px;
                    display: flex;
                    justify-content: flex-end;
                }

                .field-hint {
                    color: var(--text-muted);
                    font-size: 0.8rem;
                    line-height: 1.5;
                    margin-top: 4px;
                }

                /* Tighter hint paragraph used directly under a toggle row.
                   Dedents slightly so the eye connects it to the option
                   above it instead of floating on its own. */
                .field-hint-sub {
                    margin-top: 2px;
                    margin-bottom: 6px;
                    font-size: 0.76rem;
                    padding-left: 22px;
                }

                .folder-row,
                .input-action-row,
                .output-button-row,
                .history-actions {
                    display: flex;
                    gap: 8px;
                }

                .input-action-row select,
                .folder-row input {
                    min-width: 0;
                    flex: 1;
                }

                .mic-monitor {
                    display: flex;
                    flex-direction: column;
                    gap: 8px;
                    padding: 10px;
                    border: 1px solid var(--border-default);
                    border-radius: 8px;
                    background: var(--bg-surface-2);
                }

                .mic-monitor-head {
                    display: flex;
                    align-items: center;
                    justify-content: space-between;
                    gap: 10px;
                }

                .mic-monitor-label {
                    color: var(--text-primary);
                    font-size: 0.86rem;
                    font-weight: 700;
                }

                .mic-monitor-canvas {
                    width: 100%;
                    height: 72px;
                    display: block;
                    border: 1px solid var(--border-default);
                    border-radius: 8px;
                    background: #0d1117;
                }

                .mic-monitor-status {
                    color: var(--text-muted);
                    font-size: 0.78rem;
                    line-height: 1.4;
                }

                select {
                    width: 100%;
                    border: 1px solid var(--border-default);
                    border-radius: 8px;
                    padding: 10px 12px;
                    background: var(--bg-surface);
                    color: var(--text-primary);
                }

                textarea {
                    width: 100%;
                    resize: vertical;
                    border: 1px solid var(--border-default);
                    border-radius: 8px;
                    padding: 12px;
                    background: var(--bg-surface);
                    color: var(--text-primary);
                    line-height: 1.6;
                    font-family: inherit;
                }

                .small-textarea {
                    min-height: 72px;
                }

                .toggle-row {
                    display: flex;
                    align-items: center;
                    gap: 10px;
                    color: var(--text-primary);
                    font-size: 0.9rem;
                    font-weight: 600;
                    line-height: 1.35;
                }

                .toggle-row input[type="checkbox"] {
                    width: 18px;
                    height: 18px;
                    flex: 0 0 auto;
                    accent-color: #238636;
                }

                /* History lists */
                .transcription-list,
                .translation-list {
                    display: flex;
                    max-height: 300px;
                    flex-direction: column;
                    gap: 8px;
                    margin: 0;
                    overflow-y: auto;
                    padding: 0;
                    list-style: none;
                }

                .empty-history {
                    border: 1px dashed var(--border-default);
                    border-radius: 8px;
                    padding: 16px;
                    color: var(--text-muted);
                    font-size: 0.9rem;
                    text-align: center;
                }

                .transcription-item,
                .translation-item {
                    list-style: none;
                }

                .history-button {
                    display: grid;
                    width: 100%;
                    min-height: 80px;
                    grid-template-columns: 32px minmax(0, 1fr);
                    gap: 12px;
                    border: 1px solid var(--border-default);
                    border-radius: 8px;
                    background: var(--bg-surface-2);
                    color: var(--text-primary);
                    padding: 12px;
                    text-align: left;
                    transition: all 0.15s ease;
                }

                .history-button:hover {
                    border-color: var(--border-strong);
                    background: var(--bg-surface-3);
                }

                .history-button[data-selected="true"] {
                    border-color: #238636;
                    background: #0f2817;
                }

                .history-index {
                    display: grid;
                    width: 28px;
                    height: 28px;
                    place-items: center;
                    border-radius: 50%;
                    background: #238636;
                    color: #ffffff;
                    font-size: 0.8rem;
                    font-weight: 700;
                }

                .history-body {
                    display: grid;
                    min-width: 0;
                    gap: 6px;
                }

                .history-title {
                    overflow: hidden;
                    color: var(--text-primary);
                    font-size: 0.85rem;
                    font-weight: 700;
                    text-overflow: ellipsis;
                    white-space: nowrap;
                }

                .history-preview {
                    display: -webkit-box;
                    overflow: hidden;
                    color: var(--text-secondary);
                    font-size: 0.9rem;
                    font-weight: 400;
                    line-height: 1.4;
                    -webkit-box-orient: vertical;
                    -webkit-line-clamp: 2;
                }

                .history-meta {
                    overflow: hidden;
                    color: var(--text-muted);
                    font-size: 0.75rem;
                    font-weight: 500;
                    text-overflow: ellipsis;
                    white-space: nowrap;
                }

                /* Responsive */
                @media (max-width: 768px) {
                    .main-workspace {
                        padding: 12px;
                        gap: 12px;
                    }

                    .topbar {
                        flex-wrap: wrap;
                        gap: 8px;
                    }

                    .topbar-left,
                    .topbar-right {
                        flex-wrap: wrap;
                    }

                    .topbar-nav {
                        font-size: 0.8rem;
                    }

                    .caption-content {
                        padding: 20px;
                        font-size: 1.25rem;
                    }

                    .control-bar {
                        grid-template-columns: 1fr;
                        gap: 8px;
                        text-align: center;
                    }

                    .record-info,
                    .control-hint {
                        justify-self: center;
                        text-align: center;
                    }

                    .settings-container {
                        max-height: 95vh;
                    }

                    .settings-tabs {
                        overflow-x: auto;
                    }
                }
            </style>
        </html>
        "##;
    Html(html_text)
}

async fn app_icon_handler() -> Response {
    (
        [
            (header::CONTENT_TYPE, "image/x-icon"),
            (header::CACHE_CONTROL, "public, max-age=86400"),
        ],
        Bytes::from_static(APP_ICON_ICO),
    )
        .into_response()
}

async fn transcribe_handler(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> AppResult<Json<TranscriptionResponse>> {
    let mut audio_bytes = None;
    let mut audio_extension = None;
    let mut audio_name: Option<String> = None;
    let mut language = None;
    let mut speech_model = None;
    let mut transcription_prompt = None;
    let mut project_id: Option<String> = None;
    let mut parent_id: Option<String> = None;
    // When true, persist the transcription as a note. The frontend's quick
    // "自動追加" toggle controls this so a user who only wants the text on
    // screen won't accumulate unwanted notes.
    let mut save_as_note = false;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(AppError::bad_request)?
    {
        let name = field.name().map(str::to_owned);
        let file_name = field.file_name().map(str::to_owned);
        let content_type = field.content_type().map(str::to_owned);

        match name.as_deref() {
            Some("audio") => {
                audio_extension = Some(resolve_audio_extension(
                    file_name.as_deref(),
                    content_type.as_deref(),
                ));
                audio_name = file_name.clone();
                audio_bytes = Some(field.bytes().await.map_err(AppError::bad_request)?);
            }
            Some("language") => {
                let value = field.text().await.map_err(AppError::bad_request)?;
                language = Some(value.trim().to_string());
            }
            Some("speech_model") => {
                let value = field.text().await.map_err(AppError::bad_request)?;
                speech_model = Some(value.trim().to_string());
            }
            Some("transcription_prompt") => {
                let value = field.text().await.map_err(AppError::bad_request)?;
                transcription_prompt = Some(value.trim().to_string());
            }
            Some("project_id") => {
                let value = field.text().await.map_err(AppError::bad_request)?;
                let trimmed = value.trim().to_string();
                if !trimmed.is_empty() {
                    project_id = Some(trimmed);
                }
            }
            Some("parent_id") => {
                let value = field.text().await.map_err(AppError::bad_request)?;
                let trimmed = value.trim().to_string();
                if !trimmed.is_empty() {
                    parent_id = Some(trimmed);
                }
            }
            Some("save_as_note") => {
                let value = field.text().await.map_err(AppError::bad_request)?;
                save_as_note = matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                );
            }
            _ => {}
        }
    }

    let audio_bytes =
        audio_bytes.ok_or_else(|| AppError::bad_request("音声ファイルがありません"))?;
    if audio_bytes.is_empty() {
        return Err(AppError::bad_request("音声ファイルが空です"));
    }

    let upload_path = write_temp_audio(&audio_bytes, audio_extension.as_deref().unwrap_or("webm"))
        .await
        .map_err(AppError::internal)?;
    let transcription_path =
        prepare_audio_for_transcription(&upload_path, audio_extension.as_deref().unwrap_or("webm"))
            .await
            .map_err(AppError::internal)?;

    let transcription = state
        .voice_to_text
        .transcribe(&transcription_path, language, speech_model)
        .await;

    if let Err(error) = tokio::fs::remove_file(&upload_path).await {
        eprintln!(
            "failed to remove temporary audio file {}: {error}",
            upload_path.display()
        );
    }
    if transcription_path != upload_path {
        if let Err(error) = tokio::fs::remove_file(&transcription_path).await {
            eprintln!(
                "failed to remove normalized audio file {}: {error}",
                transcription_path.display()
            );
        }
    }

    let transcription = transcription?;
    let text = if let Some(prompt) = transcription_prompt
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        state
            .voice_to_text
            .correct_transcription_with_context(&transcription.text, prompt)
            .await?
    } else {
        transcription.text
    };

    // Persist the transcription as a note only when the caller opted in.
    // Otherwise the transcription is returned in the response and the user
    // can still save it manually via the "📥 ノートに追加" button.
    let resolved_project_id = match project_id {
        Some(id) => id,
        None => state.storage.default_project_id()?,
    };
    let (note_id, project_id_for_response) = if save_as_note {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let source_name_display = audio_name
            .as_deref()
            .map(|s| s.to_string())
            .unwrap_or_else(|| "audio".to_string());
        // Derive the title from the transcription body so the sidebar shows a
        // meaningful label instead of the upload file name like "recording".
        let title = derive_title_from_text(&text).unwrap_or_else(|| {
            source_name_display
                .rsplit('.')
                .nth(1)
                .map(|s| s.to_string())
                .unwrap_or_else(|| source_name_display.clone())
        });
        let note = Note {
            id: uuid::Uuid::new_v4().to_string(),
            project_id: resolved_project_id.clone(),
            title,
            text: text.clone(),
            source_name: Some(source_name_display),
            language: transcription.language.clone(),
            duration: transcription.duration,
            meta: None,
            parent_id,
            position: 0,
            created_at: now,
            updated_at: now,
        };
        if let Err(error) = state.storage.add_note(&note) {
            eprintln!("failed to persist note: {}", error);
            (None, Some(resolved_project_id.clone()))
        } else {
            (Some(note.id), Some(resolved_project_id.clone()))
        }
    } else {
        (None, Some(resolved_project_id.clone()))
    };

    Ok(Json(TranscriptionResponse {
        text,
        language: transcription.language,
        duration: transcription.duration,
        note_id,
        project_id: project_id_for_response,
    }))
}

async fn refine_handler(
    State(state): State<AppState>,
    Json(request): Json<RefinementRequest>,
) -> AppResult<Json<RefinementResponse>> {
    let ctx = linked_context_for(
        &state.storage,
        request.note_id.as_deref(),
        request.use_linked_context,
    );
    let text = state.voice_to_text.refine(request, ctx).await?;
    Ok(Json(RefinementResponse { text }))
}

async fn translate_handler(
    State(state): State<AppState>,
    Json(request): Json<TranslationRequest>,
) -> AppResult<Json<TranslationResponse>> {
    let ctx = linked_context_for(
        &state.storage,
        request.note_id.as_deref(),
        request.use_linked_context,
    );
    let text = state.voice_to_text.translate(request, ctx).await?;
    Ok(Json(TranslationResponse { text }))
}

async fn analyze_handler(
    State(state): State<AppState>,
    Json(request): Json<AnalysisRequest>,
) -> AppResult<Json<AnalysisResult>> {
    let ctx = linked_context_for(
        &state.storage,
        request.note_id.as_deref(),
        request.use_linked_context,
    );
    let result = state
        .voice_to_text
        .analyze(&request.text, request.model.as_deref(), ctx)
        .await?;
    Ok(Json(result))
}

async fn complete_note_handler(
    State(state): State<AppState>,
    Json(request): Json<CompleteNoteRequest>,
) -> AppResult<Json<CompleteNoteResponse>> {
    let ctx = linked_context_for(
        &state.storage,
        request.note_id.as_deref(),
        request.use_linked_context,
    );
    let added = state
        .voice_to_text
        .complete_note(&request.text, request.model.as_deref(), ctx)
        .await?;
    Ok(Json(CompleteNoteResponse { added }))
}

async fn download_status_handler(State(state): State<AppState>) -> Json<DownloadEvent> {
    let latest = state.voice_to_text.latest_download.lock().await.clone();
    Json(latest)
}

async fn speech_models_handler(
    State(state): State<AppState>,
) -> AppResult<Json<SpeechModelsResponse>> {
    let models = state.voice_to_text.list_speech_models().await?;
    Ok(Json(SpeechModelsResponse { models }))
}

#[derive(Deserialize)]
struct SpeechModelWarmupRequest {
    alias: Option<String>,
}

#[derive(Serialize)]
struct SpeechModelWarmupResponse {
    ok: bool,
    alias: String,
}

async fn warmup_speech_model_handler(
    State(state): State<AppState>,
    Json(request): Json<SpeechModelWarmupRequest>,
) -> AppResult<Json<SpeechModelWarmupResponse>> {
    let alias = state
        .voice_to_text
        .warm_up_speech_model(request.alias)
        .await?;
    Ok(Json(SpeechModelWarmupResponse { ok: true, alias }))
}

#[derive(Deserialize)]
struct DeleteSpeechModelRequest {
    alias: String,
}

async fn delete_speech_model_handler(
    State(state): State<AppState>,
    Json(request): Json<DeleteSpeechModelRequest>,
) -> AppResult<Json<SpeechModelsResponse>> {
    state
        .voice_to_text
        .delete_speech_model(&request.alias)
        .await?;
    let models = state.voice_to_text.list_speech_models().await?;
    Ok(Json(SpeechModelsResponse { models }))
}

async fn all_models_handler(
    State(state): State<AppState>,
) -> AppResult<Json<ModelsCatalogResponse>> {
    let (models, cache_dir, execution_providers) = state.voice_to_text.list_all_models().await?;
    Ok(Json(ModelsCatalogResponse {
        models,
        cache_dir,
        execution_providers,
    }))
}

#[derive(Deserialize)]
struct DeleteModelRequest {
    alias: String,
}

async fn delete_model_handler(
    State(state): State<AppState>,
    Json(request): Json<DeleteModelRequest>,
) -> AppResult<Json<ModelsCatalogResponse>> {
    state
        .voice_to_text
        .delete_model_by_alias(&request.alias)
        .await?;
    let (models, cache_dir, execution_providers) = state.voice_to_text.list_all_models().await?;
    Ok(Json(ModelsCatalogResponse {
        models,
        cache_dir,
        execution_providers,
    }))
}

#[derive(Deserialize)]
struct ModelVariantsQuery {
    alias: String,
}

async fn model_variants_handler(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<ModelVariantsQuery>,
) -> AppResult<Json<ModelVariantsResponse>> {
    let alias = query.alias.trim().to_string();
    if alias.is_empty() {
        return Err(AppError::bad_request("alias is required"));
    }
    let variants = state.voice_to_text.list_variants_for_alias(&alias).await?;
    Ok(Json(ModelVariantsResponse { alias, variants }))
}

async fn model_test_handler(
    State(state): State<AppState>,
    Json(request): Json<DownloadModelRequest>,
) -> AppResult<Json<ModelTestResponse>> {
    let alias = request.alias.trim().to_string();
    if alias.is_empty() {
        return Err(AppError::bad_request("alias is required"));
    }

    let started = std::time::Instant::now();
    if let Err(error) = state
        .voice_to_text
        .ensure_chat_model_by_alias(Some(alias.as_str()))
        .await
    {
        let message = error.message;
        let results = ["refine", "translate", "extract"]
            .into_iter()
            .map(|capability| ModelCapabilityTestResult {
                capability: capability.to_string(),
                ok: false,
                output: None,
                error: Some(message.clone()),
                elapsed_ms: 0,
            })
            .collect::<Vec<_>>();
        return Ok(Json(ModelTestResponse {
            alias,
            ok: false,
            results,
            elapsed_ms: started.elapsed().as_millis(),
        }));
    }

    let mut results = Vec::new();

    let step_started = std::time::Instant::now();
    let refine = state
        .voice_to_text
        .refine(
            RefinementRequest {
                text: "えー、本日は、あー、新しい機能について説明します。".to_string(),
                custom_instruction: None,
                custom_terms: None,
                remove_fillers: true,
                style: None,
                voice_commands: false,
                model: Some(alias.clone()),
                note_id: None,
                use_linked_context: false,
            },
            None,
        )
        .await;
    results.push(model_test_result("refine", refine, step_started));

    let step_started = std::time::Instant::now();
    let translate = state
        .voice_to_text
        .translate(
            TranslationRequest {
                text: "これは翻訳モデルの動作確認です。".to_string(),
                source_language: Some("ja".to_string()),
                target_language: Some("en".to_string()),
                custom_instruction: Some("Output only the translated text.".to_string()),
                custom_terms: None,
                model: Some(alias.clone()),
                note_id: None,
                use_linked_context: false,
            },
            None,
        )
        .await;
    results.push(model_test_result("translate", translate, step_started));

    let step_started = std::time::Instant::now();
    let extract = state
        .voice_to_text
        .analyze(
            "今日の会議では、ライブ字幕の翻訳精度とモデル選択について確認しました。",
            Some(alias.as_str()),
            None,
        )
        .await
        .map(|analysis| {
            format!(
                "{} / {} / {}",
                analysis.tone,
                analysis.summary,
                analysis.keywords.join(", ")
            )
        });
    results.push(model_test_result("extract", extract, step_started));

    let ok = results.iter().any(|result| result.ok);
    Ok(Json(ModelTestResponse {
        alias,
        ok,
        results,
        elapsed_ms: started.elapsed().as_millis(),
    }))
}

fn model_test_result(
    capability: &str,
    result: AppResult<String>,
    started: std::time::Instant,
) -> ModelCapabilityTestResult {
    match result {
        Ok(output) => {
            let output = output.trim().to_string();
            if output.is_empty() {
                ModelCapabilityTestResult {
                    capability: capability.to_string(),
                    ok: false,
                    output: None,
                    error: Some("empty output".to_string()),
                    elapsed_ms: started.elapsed().as_millis(),
                }
            } else {
                ModelCapabilityTestResult {
                    capability: capability.to_string(),
                    ok: true,
                    output: Some(short_model_test_output(&output)),
                    error: None,
                    elapsed_ms: started.elapsed().as_millis(),
                }
            }
        }
        Err(error) => ModelCapabilityTestResult {
            capability: capability.to_string(),
            ok: false,
            output: None,
            error: Some(error.message),
            elapsed_ms: started.elapsed().as_millis(),
        },
    }
}

fn short_model_test_output(text: &str) -> String {
    const MAX_CHARS: usize = 80;
    let mut out = String::new();
    for (index, ch) in text.chars().enumerate() {
        if index >= MAX_CHARS {
            out.push('…');
            return out;
        }
        out.push(ch);
    }
    out
}

const FOUNDRY_LOCAL_INSTALL_COMMAND: &str = "winget install Microsoft.FoundryLocal";

fn hidden_command(program: &str) -> std::process::Command {
    let mut command = std::process::Command::new(program);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x08000000;
        command.creation_flags(CREATE_NO_WINDOW);
    }
    command
}

fn first_non_empty_line(text: &str) -> Option<String> {
    text.lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(str::to_string)
}

fn probe_foundry_cli() -> RuntimeRequirementInfo {
    let output = hidden_command("foundry").arg("--version").output();
    match output {
        Ok(output) if output.status.success() => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            let version = first_non_empty_line(&stdout).or_else(|| first_non_empty_line(&stderr));
            RuntimeRequirementInfo {
                name: "Foundry Local CLI".to_string(),
                ok: true,
                detail: "foundry command is available".to_string(),
                version,
                install_command: None,
            }
        }
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            let detail = first_non_empty_line(&stderr)
                .or_else(|| first_non_empty_line(&stdout))
                .unwrap_or_else(|| format!("foundry --version exited with {}", output.status));
            RuntimeRequirementInfo {
                name: "Foundry Local CLI".to_string(),
                ok: false,
                detail,
                version: None,
                install_command: Some(FOUNDRY_LOCAL_INSTALL_COMMAND.to_string()),
            }
        }
        Err(error) => RuntimeRequirementInfo {
            name: "Foundry Local CLI".to_string(),
            ok: false,
            detail: if error.kind() == ErrorKind::NotFound {
                "foundry command was not found on PATH".to_string()
            } else {
                error.to_string()
            },
            version: None,
            install_command: Some(FOUNDRY_LOCAL_INSTALL_COMMAND.to_string()),
        },
    }
}

fn sdk_runtime_ok() -> (
    RuntimeRequirementInfo,
    bool,
    Vec<ExecutionProviderInfo>,
    Vec<String>,
) {
    match FoundryLocalManager::create(foundry_config()) {
        Ok(manager) => {
            let mut summary = Vec::new();
            let execution_providers = match manager.discover_eps() {
                Ok(eps) => eps
                    .into_iter()
                    .map(|ep| ExecutionProviderInfo {
                        name: ep.name,
                        registered: ep.is_registered,
                    })
                    .collect(),
                Err(error) => {
                    summary.push(format!("Execution provider check failed: {error}"));
                    Vec::new()
                }
            };
            (
                RuntimeRequirementInfo {
                    name: "Foundry Local SDK Runtime".to_string(),
                    ok: true,
                    detail: "SDK runtime initialized successfully".to_string(),
                    version: None,
                    install_command: None,
                },
                true,
                execution_providers,
                summary,
            )
        }
        Err(error) => {
            let message = error.to_string();
            (
                RuntimeRequirementInfo {
                    name: "Foundry Local SDK Runtime".to_string(),
                    ok: false,
                    detail: message.clone(),
                    version: None,
                    install_command: Some(FOUNDRY_LOCAL_INSTALL_COMMAND.to_string()),
                },
                false,
                Vec::new(),
                vec![format!("Foundry Local runtime: {message}")],
            )
        }
    }
}

async fn app_requirements_handler(
    State(state): State<AppState>,
) -> AppResult<Json<AppRequirementsResponse>> {
    let foundry_cli = tokio::task::spawn_blocking(probe_foundry_cli)
        .await
        .map_err(AppError::internal)?;
    let (sdk_runtime, sdk_runtime_ready, execution_providers, mut missing_summary) =
        sdk_runtime_ok();
    if !foundry_cli.ok {
        missing_summary.push(format!("{}: {}", foundry_cli.name, foundry_cli.detail));
    }
    let runtime_ready = sdk_runtime_ready && foundry_cli.ok;
    let required_models = if sdk_runtime_ready {
        state.voice_to_text.required_model_requirements().await
    } else {
        Vec::new()
    };

    for model in &required_models {
        if !model.downloaded {
            missing_summary.push(format!("Model {} is not downloaded", model.alias));
        }
    }

    let ok = runtime_ready && required_models.iter().all(|model| model.downloaded);

    Ok(Json(AppRequirementsResponse {
        ok,
        runtime_ready,
        foundry_cli,
        sdk_runtime,
        execution_providers,
        required_models,
        install_command: FOUNDRY_LOCAL_INSTALL_COMMAND.to_string(),
        missing_summary,
    }))
}

fn app_settings_response(settings: AppSettings) -> AppSettingsResponse {
    let configured_locales_dir = normalize_settings_dir(settings.locales_dir);
    AppSettingsResponse {
        configured_locales_dir: configured_locales_dir.clone(),
        locales_dir: locales_dir().display().to_string(),
        default_locales_dir: default_locales_dir().display().to_string(),
    }
}

async fn app_settings_handler() -> AppResult<Json<AppSettingsResponse>> {
    Ok(Json(app_settings_response(load_app_settings())))
}

async fn update_app_settings_handler(
    State(_state): State<AppState>,
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

async fn pick_folder_handler(
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

#[derive(Deserialize)]
struct DownloadModelRequest {
    alias: String,
}

async fn download_model_handler(
    State(state): State<AppState>,
    Json(request): Json<DownloadModelRequest>,
) -> AppResult<Json<serde_json::Value>> {
    let alias = request.alias.trim().to_string();
    if alias.is_empty() {
        return Err(AppError::bad_request("alias is required"));
    }

    // Run the download in the background so the request returns immediately;
    // the UI listens on the SSE stream for progress + completion.
    let service = Arc::clone(&state.voice_to_text);
    tauri::async_runtime::spawn(async move {
        let manager = match FoundryLocalManager::create(foundry_config()) {
            Ok(m) => m,
            Err(error) => {
                service
                    .emit_download_event(DownloadEvent::Failed {
                        alias: alias.clone(),
                        message: error.to_string(),
                    })
                    .await;
                return;
            }
        };
        let model = match manager.catalog().get_model(&alias).await {
            Ok(m) => m,
            Err(error) => {
                service
                    .emit_download_event(DownloadEvent::Failed {
                        alias: alias.clone(),
                        message: error.to_string(),
                    })
                    .await;
                return;
            }
        };
        if let Err(error) = service.download_model_with_progress(&model, &alias).await {
            service
                .emit_download_event(DownloadEvent::Failed {
                    alias: alias.clone(),
                    message: error.message,
                })
                .await;
        }
    });

    Ok(Json(serde_json::json!({ "ok": true })))
}

// ========== Projects / Notes / Translations API ==========

#[derive(Serialize)]
struct ProjectListResponse {
    projects: Vec<Project>,
}

#[derive(Deserialize)]
struct ProjectInput {
    name: String,
    description: Option<String>,
}

async fn list_projects_handler(
    State(state): State<AppState>,
) -> AppResult<Json<ProjectListResponse>> {
    let projects = state.storage.list_projects()?;
    Ok(Json(ProjectListResponse { projects }))
}

async fn create_project_handler(
    State(state): State<AppState>,
    Json(input): Json<ProjectInput>,
) -> AppResult<Json<Project>> {
    let name = input.name.trim();
    if name.is_empty() {
        return Err(AppError::bad_request("project name is required"));
    }
    let project = state
        .storage
        .create_project(name, input.description.as_deref())?;
    Ok(Json(project))
}

async fn update_project_handler(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(input): Json<ProjectInput>,
) -> AppResult<Json<serde_json::Value>> {
    let name = input.name.trim();
    if name.is_empty() {
        return Err(AppError::bad_request("project name is required"));
    }
    state
        .storage
        .update_project(&id, name, input.description.as_deref())?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

async fn delete_project_handler(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> AppResult<Json<serde_json::Value>> {
    state.storage.delete_project(&id)?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

#[derive(Serialize)]
struct NoteListResponse {
    notes: Vec<NoteWithTags>,
}

#[derive(Deserialize)]
struct NoteListQuery {
    project_id: String,
}

async fn list_notes_handler(
    State(state): State<AppState>,
    Query(query): Query<NoteListQuery>,
) -> AppResult<Json<NoteListResponse>> {
    let notes = state.storage.list_notes(&query.project_id)?;
    Ok(Json(NoteListResponse { notes }))
}

#[derive(Deserialize)]
struct CreateNoteInput {
    project_id: String,
    title: String,
    text: String,
    source_name: Option<String>,
    language: Option<String>,
    duration: Option<f64>,
    meta: Option<String>,
    tags: Option<Vec<String>>,
    parent_id: Option<String>,
}

async fn create_note_handler(
    State(state): State<AppState>,
    Json(input): Json<CreateNoteInput>,
) -> AppResult<Json<Note>> {
    let text = input.text.trim().to_string();
    if text.is_empty() {
        return Err(AppError::bad_request("text is required"));
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let title = if input.title.trim().is_empty() {
        derive_title_from_text(&text).unwrap_or_else(|| text.chars().take(40).collect::<String>())
    } else {
        input.title.trim().to_string()
    };
    let note = Note {
        id: uuid::Uuid::new_v4().to_string(),
        project_id: input.project_id,
        title,
        text,
        source_name: input.source_name,
        language: input.language,
        duration: input.duration,
        meta: input.meta,
        parent_id: input
            .parent_id
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        position: 0,
        created_at: now,
        updated_at: now,
    };
    state.storage.add_note(&note)?;
    if let Some(tags) = input.tags {
        state.storage.set_note_tags(&note.id, &tags)?;
    }
    Ok(Json(note))
}

#[derive(Deserialize)]
struct UpdateNoteInput {
    title: String,
    text: String,
    meta: Option<String>,
    tags: Option<Vec<String>>,
}

async fn update_note_handler(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(input): Json<UpdateNoteInput>,
) -> AppResult<Json<serde_json::Value>> {
    // When the client sends an empty title (e.g. the caption-save flow that
    // wants the server to pick a good label), derive one from the body so the
    // sidebar stays readable.
    let resolved_title = if input.title.trim().is_empty() {
        derive_title_from_text(&input.text).unwrap_or_default()
    } else {
        input.title.clone()
    };
    state
        .storage
        .update_note(&id, &resolved_title, &input.text, input.meta.as_deref())?;
    if let Some(tags) = input.tags {
        state.storage.set_note_tags(&id, &tags)?;
    }
    Ok(Json(serde_json::json!({ "ok": true })))
}

#[derive(Deserialize)]
struct SetNoteParentInput {
    parent_id: Option<String>,
}

#[derive(Deserialize)]
struct ReorderNotesInput {
    project_id: String,
    note_ids: Vec<String>,
}

async fn reorder_notes_handler(
    State(state): State<AppState>,
    Json(input): Json<ReorderNotesInput>,
) -> AppResult<Json<serde_json::Value>> {
    if input.note_ids.is_empty() {
        return Err(AppError::bad_request("note_ids is required"));
    }
    state
        .storage
        .reorder_notes(&input.project_id, &input.note_ids)?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

async fn set_note_parent_handler(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(input): Json<SetNoteParentInput>,
) -> AppResult<Json<serde_json::Value>> {
    let parent = input
        .parent_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    state
        .storage
        .set_note_parent(&id, parent)
        .map_err(|e| AppError::bad_request(e))?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

async fn delete_note_handler(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> AppResult<Json<serde_json::Value>> {
    state.storage.delete_note(&id)?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

#[derive(Deserialize)]
struct SetTagsInput {
    tags: Vec<String>,
}

async fn set_note_tags_handler(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(input): Json<SetTagsInput>,
) -> AppResult<Json<serde_json::Value>> {
    state.storage.set_note_tags(&id, &input.tags)?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

async fn graph_handler(
    State(state): State<AppState>,
    Query(query): Query<NoteListQuery>,
) -> AppResult<Json<GraphData>> {
    let graph = state.storage.graph(&query.project_id)?;
    Ok(Json(graph))
}

#[derive(Deserialize)]
struct CreateLinkInput {
    from_note_id: String,
    to_note_id: String,
    label: Option<String>,
}

async fn create_note_link_handler(
    State(state): State<AppState>,
    Json(input): Json<CreateLinkInput>,
) -> AppResult<Json<NoteLink>> {
    let from = input.from_note_id.trim();
    let to = input.to_note_id.trim();
    if from.is_empty() || to.is_empty() {
        return Err(AppError::bad_request("note ids are required"));
    }
    if from == to {
        return Err(AppError::bad_request("cannot link a note to itself"));
    }
    let label = input
        .label
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let link = state.storage.add_note_link(from, to, label)?;
    Ok(Json(link))
}

async fn delete_note_link_handler(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> AppResult<Json<serde_json::Value>> {
    state.storage.delete_note_link(&id)?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

// ========== Drafts (aggregated notes → single document) ==========

#[derive(Serialize)]
struct DraftListResponse {
    drafts: Vec<DraftWithNotes>,
}

async fn list_drafts_handler(
    State(state): State<AppState>,
    Query(query): Query<NoteListQuery>,
) -> AppResult<Json<DraftListResponse>> {
    let drafts = state.storage.list_drafts(&query.project_id)?;
    Ok(Json(DraftListResponse { drafts }))
}

async fn get_draft_handler(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> AppResult<Json<DraftWithNotes>> {
    let draft = state
        .storage
        .get_draft(&id)?
        .ok_or_else(|| AppError::bad_request("draft not found"))?;
    Ok(Json(draft))
}

#[derive(Deserialize)]
struct ComposeDraftRequest {
    project_id: String,
    title: Option<String>,
    note_ids: Vec<String>,
    mode: Option<String>,        // "concat" | "llm"
    instruction: Option<String>, // used only for llm mode
}

async fn compose_draft_handler(
    State(state): State<AppState>,
    Json(input): Json<ComposeDraftRequest>,
) -> AppResult<Json<DraftWithNotes>> {
    if input.note_ids.is_empty() {
        return Err(AppError::bad_request("note_ids is required"));
    }
    let ordered_notes = state.storage.get_notes_by_ids(&input.note_ids)?;
    if ordered_notes.is_empty() {
        return Err(AppError::bad_request("no matching notes found"));
    }
    let mode = input.mode.as_deref().unwrap_or("concat");
    let pairs: Vec<(String, String)> = ordered_notes
        .iter()
        .map(|n| (n.title.clone(), n.text.clone()))
        .collect();
    let content = state
        .voice_to_text
        .compose_draft(&pairs, mode, input.instruction.as_deref())
        .await?;
    let title = input
        .title
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| {
            ordered_notes
                .first()
                .map(|n| {
                    if n.title.trim().is_empty() {
                        "Draft".to_string()
                    } else {
                        format!("Draft: {}", n.title)
                    }
                })
                .unwrap_or_else(|| "Draft".to_string())
        });
    let draft = state
        .storage
        .create_draft(&input.project_id, &title, &content, &input.note_ids)?;
    Ok(Json(DraftWithNotes {
        draft,
        note_ids: input.note_ids,
    }))
}

#[derive(Deserialize)]
struct UpdateDraftInput {
    title: String,
    content: String,
    note_ids: Option<Vec<String>>,
}

async fn update_draft_handler(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(input): Json<UpdateDraftInput>,
) -> AppResult<Json<serde_json::Value>> {
    state
        .storage
        .update_draft(&id, &input.title, &input.content)?;
    if let Some(note_ids) = input.note_ids {
        state.storage.set_draft_notes(&id, &note_ids)?;
    }
    Ok(Json(serde_json::json!({ "ok": true })))
}

async fn delete_draft_handler(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> AppResult<Json<serde_json::Value>> {
    state.storage.delete_draft(&id)?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

#[derive(Serialize)]
struct TranslationListResponse {
    translations: Vec<Translation>,
}

async fn list_translations_handler(
    State(state): State<AppState>,
    Query(query): Query<NoteListQuery>,
) -> AppResult<Json<TranslationListResponse>> {
    let translations = state.storage.list_translations(&query.project_id)?;
    Ok(Json(TranslationListResponse { translations }))
}

#[derive(Deserialize)]
struct CreateTranslationInput {
    note_id: Option<String>,
    source_text: String,
    target_language: String,
    source_language: Option<String>,
    custom_instruction: Option<String>,
    custom_terms: Option<String>,
}

async fn create_translation_handler(
    State(state): State<AppState>,
    Json(input): Json<CreateTranslationInput>,
) -> AppResult<Json<Translation>> {
    let source_text = input.source_text.trim().to_string();
    if source_text.is_empty() {
        return Err(AppError::bad_request("source text is required"));
    }
    // Use linked-note context when the translation is tied to a specific note.
    let ctx = linked_context_for(&state.storage, input.note_id.as_deref(), true);
    let translated = state
        .voice_to_text
        .translate(
            TranslationRequest {
                text: source_text.clone(),
                source_language: input.source_language.clone(),
                target_language: Some(input.target_language.clone()),
                custom_instruction: input.custom_instruction,
                custom_terms: input.custom_terms,
                model: None,
                note_id: input.note_id.clone(),
                use_linked_context: false, // already resolved above
            },
            ctx,
        )
        .await?;

    let id = uuid::Uuid::new_v4().to_string();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);

    let translation = Translation {
        id,
        note_id: input.note_id,
        source_text,
        translated_text: translated,
        source_language: input.source_language,
        target_language: input.target_language,
        created_at: now,
    };
    state.storage.add_translation(&translation)?;
    Ok(Json(translation))
}

async fn delete_translation_handler(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> AppResult<Json<serde_json::Value>> {
    state.storage.delete_translation(&id)?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

async fn clear_translations_handler(
    State(state): State<AppState>,
    Query(query): Query<NoteListQuery>,
) -> AppResult<Json<serde_json::Value>> {
    state.storage.clear_translations(&query.project_id)?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

// ========== Live captioning ==========

struct LiveSessionState {
    id: String,
    title: String,
    started_at: i64,
    source_language: Option<String>,
    target_language: Option<String>,
    speech_model: Option<String>,
    translate_model: Option<String>,
    translate: bool,
    sequence: i64,
    segments: Vec<LiveSegment>,
    pending_source_text: String,
    pending_start_ms: Option<i64>,
}

#[derive(Deserialize)]
struct LiveStartRequest {
    title: Option<String>,
    source_language: Option<String>,
    target_language: Option<String>,
    speech_model: Option<String>,
    translate_model: Option<String>,
    translate: Option<bool>,
}

#[derive(Serialize)]
struct LiveStartResponse {
    session_id: String,
    started_at: i64,
}

async fn live_start_handler(
    State(state): State<AppState>,
    Json(request): Json<LiveStartRequest>,
) -> AppResult<Json<LiveStartResponse>> {
    let id = uuid::Uuid::new_v4().to_string();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let title = request
        .title
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "Live session".to_string());

    let session = LiveSessionState {
        id: id.clone(),
        title,
        started_at: now,
        source_language: request
            .source_language
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        target_language: request
            .target_language
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        speech_model: request
            .speech_model
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        translate_model: request
            .translate_model
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        translate: request.translate.unwrap_or(true),
        sequence: 0,
        segments: Vec::new(),
        pending_source_text: String::new(),
        pending_start_ms: None,
    };

    state.live_sessions.lock().await.insert(id.clone(), session);

    Ok(Json(LiveStartResponse {
        session_id: id,
        started_at: now,
    }))
}

#[derive(Serialize)]
struct LiveChunkResponse {
    sequence: i64,
    source_text: String,
    translated_text: Option<String>,
    elapsed_ms: i64,
    segments: Vec<LiveChunkSegmentResponse>,
}

#[derive(Clone, Serialize)]
struct LiveChunkSegmentResponse {
    sequence: i64,
    source_text: String,
    translated_text: Option<String>,
    elapsed_ms: i64,
}

#[derive(Deserialize)]
struct LiveTranslationRequest {
    session_id: String,
    sequence: i64,
    sequences: Option<Vec<i64>>,
    source_text: Option<String>,
    append: Option<bool>,
}

#[derive(Serialize)]
struct LiveTranslationResponse {
    sequence: i64,
    source_text: Option<String>,
    translated_unit: Option<String>,
    translated_text: Option<String>,
}

fn live_sentence_boundary_index(text: &str) -> Option<usize> {
    let source = text.trim_start();
    if source.is_empty() {
        return None;
    }

    const ABBREVIATIONS: &[&str] = &[
        "mr", "mrs", "ms", "dr", "prof", "sr", "jr", "st", "vs", "etc",
    ];

    for (idx, ch) in source.char_indices() {
        if !matches!(ch, '。' | '．' | '.' | '!' | '?' | '！' | '？') {
            continue;
        }

        let next_start = idx + ch.len_utf8();
        let prev = source[..idx].chars().next_back();
        let next = source[next_start..].chars().next();

        if ch == '.'
            && prev.is_some_and(|c| c.is_ascii_digit())
            && next.is_some_and(|c| c.is_ascii_digit())
        {
            continue;
        }

        if ch == '.' {
            let before = source[..idx]
                .chars()
                .rev()
                .take_while(|c| c.is_ascii_alphabetic())
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<String>()
                .to_lowercase();
            if ABBREVIATIONS.contains(&before.as_str()) {
                continue;
            }
        }

        let after = &source[next_start..];
        if after.is_empty()
            || after
                .chars()
                .next()
                .is_some_and(|c| c.is_whitespace() || "\"'”’)]}".contains(c))
        {
            return Some(next_start);
        }
    }

    None
}

fn split_live_source_units(text: &str, include_remainder: bool) -> (Vec<String>, String) {
    let mut units = Vec::new();
    let mut rest = text.trim().to_string();

    while !rest.is_empty() {
        let Some(boundary) = live_sentence_boundary_index(&rest) else {
            if include_remainder {
                units.push(rest.trim().to_string());
                rest.clear();
            }
            break;
        };
        let sentence = rest[..boundary].trim().to_string();
        if !sentence.is_empty() {
            units.push(sentence);
        }
        rest = rest[boundary..].trim().to_string();
    }

    units.retain(|unit| !unit.is_empty());
    (units, rest)
}

fn push_live_source_units(
    session: &mut LiveSessionState,
    units: Vec<String>,
    start_ms: i64,
) -> Vec<LiveChunkSegmentResponse> {
    let mut response_segments = Vec::new();

    for unit in units {
        let seq = session.sequence;
        session.sequence += 1;
        session.segments.push(LiveSegment {
            id: uuid::Uuid::new_v4().to_string(),
            session_id: session.id.clone(),
            sequence: seq,
            start_ms,
            source_text: unit.clone(),
            translated_text: None,
        });
        response_segments.push(LiveChunkSegmentResponse {
            sequence: seq,
            source_text: unit,
            translated_text: None,
            elapsed_ms: start_ms,
        });
    }

    response_segments
}

fn flush_live_pending_source(session: &mut LiveSessionState) {
    let pending = std::mem::take(&mut session.pending_source_text);
    let start_ms = session.pending_start_ms.take().unwrap_or(0);
    let (units, _) = split_live_source_units(&pending, true);
    push_live_source_units(session, units, start_ms);
}

async fn live_chunk_handler(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> AppResult<Json<LiveChunkResponse>> {
    let mut audio_bytes = None;
    let mut audio_extension = None;
    let mut session_id: Option<String> = None;
    let mut chunk_start_ms: Option<i64> = None;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(AppError::bad_request)?
    {
        let name = field.name().map(str::to_owned);
        let file_name = field.file_name().map(str::to_owned);
        let content_type = field.content_type().map(str::to_owned);

        match name.as_deref() {
            Some("audio") => {
                audio_extension = Some(resolve_audio_extension(
                    file_name.as_deref(),
                    content_type.as_deref(),
                ));
                audio_bytes = Some(field.bytes().await.map_err(AppError::bad_request)?);
            }
            Some("session_id") => {
                let value = field.text().await.map_err(AppError::bad_request)?;
                session_id = Some(value.trim().to_string());
            }
            Some("chunk_start_ms") => {
                let value = field.text().await.map_err(AppError::bad_request)?;
                chunk_start_ms = value.trim().parse::<i64>().ok();
            }
            _ => {}
        }
    }

    let session_id = session_id
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AppError::bad_request("session_id is required"))?;
    let audio_bytes =
        audio_bytes.ok_or_else(|| AppError::bad_request("audio chunk is required"))?;
    if audio_bytes.is_empty() {
        return Err(AppError::bad_request("audio chunk is empty"));
    }

    // Snapshot session config without holding the lock across the transcription call.
    let (language, speech_model) = {
        let sessions = state.live_sessions.lock().await;
        let session = sessions
            .get(&session_id)
            .ok_or_else(|| AppError::bad_request("unknown session_id"))?;
        (
            session.source_language.clone(),
            session.speech_model.clone(),
        )
    };

    let upload_path = write_temp_audio(&audio_bytes, audio_extension.as_deref().unwrap_or("wav"))
        .await
        .map_err(AppError::internal)?;
    let transcription_path =
        prepare_audio_for_transcription(&upload_path, audio_extension.as_deref().unwrap_or("wav"))
            .await
            .map_err(AppError::internal)?;

    let transcription = state
        .voice_to_text
        .transcribe(&transcription_path, language.clone(), speech_model)
        .await;

    if let Err(error) = tokio::fs::remove_file(&upload_path).await {
        eprintln!(
            "failed to remove live chunk upload {}: {error}",
            upload_path.display()
        );
    }
    if transcription_path != upload_path {
        if let Err(error) = tokio::fs::remove_file(&transcription_path).await {
            eprintln!(
                "failed to remove normalized live chunk {}: {error}",
                transcription_path.display()
            );
        }
    }

    let transcription = transcription?;
    let source_text = transcription.text.trim().to_string();

    let elapsed_ms = chunk_start_ms.unwrap_or(0);

    let (sequence, segments) = {
        let mut sessions = state.live_sessions.lock().await;
        let session = sessions
            .get_mut(&session_id)
            .ok_or_else(|| AppError::bad_request("unknown session_id"))?;
        let mut response_segments = Vec::new();

        if !source_text.is_empty() {
            let pending_start_ms = session.pending_start_ms.unwrap_or(elapsed_ms);
            let combined = if session.pending_source_text.trim().is_empty() {
                source_text.clone()
            } else {
                format!("{} {}", session.pending_source_text.trim(), source_text)
            };
            let (source_units, remainder) = split_live_source_units(&combined, false);
            response_segments = push_live_source_units(session, source_units, pending_start_ms);
            session.pending_source_text = remainder;
            session.pending_start_ms = if session.pending_source_text.trim().is_empty() {
                None
            } else if response_segments.is_empty() {
                Some(pending_start_ms)
            } else {
                Some(elapsed_ms)
            };
        }

        let seq = response_segments
            .last()
            .map(|seg| seg.sequence)
            .unwrap_or(session.sequence);
        (seq, response_segments)
    };

    let response_source_text = if segments.is_empty() {
        source_text
    } else {
        segments
            .iter()
            .map(|seg| seg.source_text.as_str())
            .collect::<Vec<_>>()
            .join("\n")
    };

    Ok(Json(LiveChunkResponse {
        sequence,
        source_text: response_source_text,
        translated_text: None,
        elapsed_ms,
        segments,
    }))
}

async fn live_translate_handler(
    State(state): State<AppState>,
    Json(request): Json<LiveTranslationRequest>,
) -> AppResult<Json<LiveTranslationResponse>> {
    let session_id = request.session_id.trim().to_string();
    if session_id.is_empty() {
        return Err(AppError::bad_request("session_id is required"));
    }
    let translated_sequence = request
        .sequences
        .as_ref()
        .and_then(|sequences| sequences.last().copied())
        .unwrap_or(request.sequence);

    let (stored_source_text, source_language, target_language, translate_model, translate) = {
        let sessions = state.live_sessions.lock().await;
        let session = sessions
            .get(&session_id)
            .ok_or_else(|| AppError::bad_request("unknown session_id"))?;
        let segment = session
            .segments
            .iter()
            .find(|seg| seg.sequence == translated_sequence)
            .ok_or_else(|| AppError::bad_request("unknown live segment"))?;
        (
            segment.source_text.clone(),
            session.source_language.clone(),
            session.target_language.clone(),
            session.translate_model.clone(),
            session.translate,
        )
    };
    let source_text = request
        .source_text
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(stored_source_text.trim())
        .to_string();

    if !translate || source_text.trim().is_empty() || target_language.is_none() {
        return Ok(Json(LiveTranslationResponse {
            sequence: translated_sequence,
            source_text: None,
            translated_unit: None,
            translated_text: None,
        }));
    }

    let translated_text = match state
        .voice_to_text
        .translate_live_caption(
            &source_text,
            source_language.as_deref(),
            target_language.as_deref().unwrap_or("ja"),
            translate_model.as_deref(),
            true,
        )
        .await
    {
        Ok(text) => {
            let text = text.trim().to_string();
            if text.is_empty() { None } else { Some(text) }
        }
        Err(error) => {
            eprintln!("live translation failed: {error:?}");
            None
        }
    };

    let mut response_text = translated_text.clone();
    let response_unit = translated_text.clone();
    if let Some(text) = translated_text.as_ref() {
        let mut sessions = state.live_sessions.lock().await;
        if let Some(session) = sessions.get_mut(&session_id) {
            if let Some(segment) = session
                .segments
                .iter_mut()
                .find(|seg| seg.sequence == translated_sequence)
            {
                if request.append.unwrap_or(false) {
                    let merged = match segment.translated_text.as_deref() {
                        Some(existing) if !existing.trim().is_empty() => {
                            format!("{}\n{}", existing.trim_end(), text)
                        }
                        _ => text.clone(),
                    };
                    segment.translated_text = Some(merged.clone());
                    response_text = Some(merged);
                } else {
                    segment.translated_text = Some(text.clone());
                }
            }
        }
    }

    Ok(Json(LiveTranslationResponse {
        sequence: translated_sequence,
        source_text: Some(source_text),
        translated_unit: response_unit,
        translated_text: response_text,
    }))
}

#[derive(Deserialize)]
struct LiveStopRequest {
    session_id: String,
    save: bool,
    title: Option<String>,
}

#[derive(Serialize)]
struct LiveStopResponse {
    saved: bool,
    session: Option<LiveSession>,
}

async fn live_stop_handler(
    State(state): State<AppState>,
    Json(request): Json<LiveStopRequest>,
) -> AppResult<Json<LiveStopResponse>> {
    let session = {
        let mut sessions = state.live_sessions.lock().await;
        sessions.remove(&request.session_id)
    };
    let Some(session) = session else {
        return Err(AppError::bad_request("unknown session_id"));
    };
    let mut session = session;
    if request.save {
        flush_live_pending_source(&mut session);
    }

    if !request.save || session.segments.is_empty() {
        return Ok(Json(LiveStopResponse {
            saved: false,
            session: None,
        }));
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let title = request
        .title
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or(session.title);

    let persistable = LiveSession {
        id: session.id.clone(),
        title,
        started_at: session.started_at,
        ended_at: Some(now),
        source_language: session.source_language,
        target_language: session.target_language,
        duration_ms: now - session.started_at,
        segment_count: session.segments.len() as i64,
    };
    state
        .storage
        .save_live_session(&persistable, &session.segments)?;

    Ok(Json(LiveStopResponse {
        saved: true,
        session: Some(persistable),
    }))
}

#[derive(Serialize)]
struct LiveSessionsResponse {
    sessions: Vec<LiveSession>,
}

async fn live_sessions_handler(
    State(state): State<AppState>,
) -> AppResult<Json<LiveSessionsResponse>> {
    let sessions = state.storage.list_live_sessions()?;
    Ok(Json(LiveSessionsResponse { sessions }))
}

async fn live_session_detail_handler(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> AppResult<Json<LiveSessionDetail>> {
    let session = state
        .storage
        .get_live_session(&id)?
        .ok_or_else(|| AppError::bad_request("session not found"))?;
    Ok(Json(session))
}

async fn live_session_delete_handler(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> AppResult<Json<serde_json::Value>> {
    state.storage.delete_live_session(&id)?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

#[derive(Deserialize)]
struct RenameLiveSessionInput {
    title: String,
}

async fn live_session_rename_handler(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(input): Json<RenameLiveSessionInput>,
) -> AppResult<Json<serde_json::Value>> {
    let title = input.title.trim();
    if title.is_empty() {
        return Err(AppError::bad_request("title is required"));
    }
    state.storage.rename_live_session(&id, title)?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

async fn download_events_handler(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.voice_to_text.download_tx.subscribe();
    let latest = state.voice_to_text.latest_download.lock().await.clone();

    let initial = futures::stream::once(async move {
        let data = serde_json::to_string(&latest).unwrap_or_else(|_| "{}".to_string());
        Ok(Event::default().data(data))
    });

    let stream = tokio_stream::wrappers::BroadcastStream::new(rx).filter_map(|result| {
        futures::future::ready(match result {
            Ok(event) => match serde_json::to_string(&event) {
                Ok(data) => Some(Ok(Event::default().data(data))),
                Err(_) => None,
            },
            Err(_) => None,
        })
    });

    Sse::new(initial.chain(stream)).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    )
}

async fn process_pipeline_handler(
    State(state): State<AppState>,
    Json(request): Json<ProcessPipelineRequest>,
) -> AppResult<Json<ProcessPipelineResponse>> {
    let mut current_text = request.text;
    let mut results = Vec::new();

    // Resolve linked-note context once for the whole pipeline. The same block
    // is re-used for every step so the LLM sees consistent background info.
    let shared_ctx = linked_context_for(
        &state.storage,
        request.note_id.as_deref(),
        request.use_linked_context,
    );

    for step in request.pipeline {
        match step {
            PipelineStep::Refine { config } => {
                let refined_request = RefinementRequest {
                    text: current_text.clone(),
                    custom_instruction: config.custom_instruction,
                    custom_terms: config.custom_terms,
                    remove_fillers: config.remove_fillers,
                    style: config.style,
                    voice_commands: config.voice_commands,
                    model: config.model,
                    note_id: None,
                    use_linked_context: false,
                };
                current_text = state
                    .voice_to_text
                    .refine(refined_request, shared_ctx.clone())
                    .await?;
                results.push(PipelineStepResult {
                    step_type: "refine".to_string(),
                    text: current_text.clone(),
                });
            }
            PipelineStep::Translate { config } => {
                let translate_request = TranslationRequest {
                    text: current_text.clone(),
                    source_language: config.source_language,
                    target_language: config.target_language,
                    custom_instruction: config.custom_instruction,
                    custom_terms: config.custom_terms,
                    model: config.model,
                    note_id: None,
                    use_linked_context: false,
                };
                current_text = state
                    .voice_to_text
                    .translate(translate_request, shared_ctx.clone())
                    .await?;
                results.push(PipelineStepResult {
                    step_type: "translate".to_string(),
                    text: current_text.clone(),
                });
            }
            PipelineStep::Custom { config } => {
                let max_tokens = config.max_tokens.unwrap_or(1024);
                current_text = state
                    .voice_to_text
                    .run_custom_prompt(
                        &current_text,
                        &config.instruction,
                        max_tokens,
                        config.model.as_deref(),
                        shared_ctx.clone(),
                    )
                    .await?;
                let step_type = config
                    .label
                    .as_deref()
                    .map(|s| format!("custom:{}", s))
                    .unwrap_or_else(|| "custom".to_string());
                results.push(PipelineStepResult {
                    step_type,
                    text: current_text.clone(),
                });
            }
        }
    }

    Ok(Json(ProcessPipelineResponse { results }))
}

/// Maximum total characters worth of linked-note text we attach to an LLM
/// request. Exists to protect small local models from blowing past their
/// context window when a note is heavily linked.
const MAX_LINKED_CONTEXT_CHARS: usize = 4000;
/// Per-note trim threshold — any single linked note is truncated to this many
/// characters so a long neighbour can't starve the others.
const MAX_PER_LINKED_NOTE_CHARS: usize = 1000;

/// Build a natural-language description of the notes linked to `note_id`, or
/// `None` when there are no links (caller can skip injecting a context block).
///
/// The output is designed to be embedded as a system-prompt fragment, so the
/// LLM can condition its output on related notes — e.g. when refining a
/// follow-up meeting note, the LLM gets a short summary of the previous one.
///
/// Returns early with `None` when `note_id` is empty or the lookup fails, so
/// callers don't need to handle the absence of a note specially.
fn fetch_linked_context(storage: &Storage, note_id: &str) -> Option<String> {
    let id = note_id.trim();
    if id.is_empty() {
        return None;
    }
    let linked = storage.list_linked_notes(id).ok()?;
    if linked.is_empty() {
        return None;
    }

    let mut remaining = MAX_LINKED_CONTEXT_CHARS;
    let mut lines: Vec<String> = Vec::new();
    for link in linked {
        if remaining == 0 {
            break;
        }
        // Trim each neighbour's text to keep any single note from dominating.
        let body = link.text.trim();
        let mut snippet: String = body.chars().take(MAX_PER_LINKED_NOTE_CHARS).collect();
        if snippet.chars().count() < body.chars().count() {
            snippet.push('…');
        }
        // Honour the overall cap even mid-note.
        if snippet.chars().count() > remaining {
            snippet = snippet.chars().take(remaining).collect();
        }

        let relation = match (link.direction.as_str(), link.label.as_deref()) {
            ("out", Some(label)) if !label.trim().is_empty() => {
                format!("このノートから『{}』というリンクで参照", label.trim())
            }
            ("in", Some(label)) if !label.trim().is_empty() => {
                format!("『{}』として被リンク", label.trim())
            }
            ("out", _) => "リンク先".to_string(),
            ("in", _) => "リンク元".to_string(),
            _ => "関連ノート".to_string(),
        };
        let heading = if link.title.trim().is_empty() {
            format!("- [{relation}]")
        } else {
            format!("- [{relation}] {}", link.title.trim())
        };
        let entry = format!("{heading}\n  {snippet}");
        let entry_len = entry.chars().count();
        lines.push(entry);
        remaining = remaining.saturating_sub(entry_len.min(remaining));
    }

    if lines.is_empty() {
        None
    } else {
        let body = lines.join("\n");
        Some(format!(
            "参考: このノートに手動でリンクされている関連ノートの抜粋です。LLM の出力を調整する際の背景情報として利用してください（内容をそのまま引き写さないこと）。\n{body}"
        ))
    }
}

/// Helper: build the linked-note context block for a request, unless the
/// request opted out or no note id was supplied.
fn linked_context_for(storage: &Storage, note_id: Option<&str>, enabled: bool) -> Option<String> {
    if !enabled {
        return None;
    }
    let id = note_id?.trim();
    if id.is_empty() {
        return None;
    }
    fetch_linked_context(storage, id)
}

fn remove_common_fillers(text: &str) -> String {
    let mut cleaned = text.to_string();
    for filler in [
        "あー、",
        "あー",
        "えー、",
        "えー",
        "えっと、",
        "えっと",
        "そのー、",
        "そのー",
        "まあ、",
        "まぁ、",
        "まあ",
        "まぁ",
        "うーん、",
        "うーん",
        "um,",
        "um",
        "uh,",
        "uh",
    ] {
        cleaned = cleaned.replace(filler, "");
    }

    cleaned
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string()
}

fn apply_spoken_commands(text: &str) -> String {
    let replacements = [
        ("改行してください", "\n"),
        ("改行", "\n"),
        ("新しい行", "\n"),
        ("段落", "\n\n"),
        ("句点", "。"),
        ("読点", "、"),
        ("丸", "。"),
        ("まる", "。"),
        ("点", "、"),
        ("てん", "、"),
        ("疑問符", "？"),
        ("はてな", "？"),
        ("びっくり", "！"),
        ("感嘆符", "！"),
        ("new paragraph", "\n\n"),
        ("new line", "\n"),
        ("period", "."),
        ("comma", ","),
        ("question mark", "?"),
        ("exclamation mark", "!"),
    ];
    let mut converted = text.to_string();
    for (from, to) in replacements {
        converted = converted.replace(from, to);
    }

    converted
}

/// Return an i18n key describing the model's purpose. The frontend resolves the
/// key against the active locale so the description is translated too. When a
/// description is unavailable the caller can fall back to the alias itself.
fn describe_model_purpose(
    alias: &str,
    input_mod: &str,
    output_mod: &str,
    is_speech: bool,
) -> String {
    let lower = alias.to_lowercase();
    if is_speech {
        if lower.contains("tiny") {
            "models.desc.whisper.tiny"
        } else if lower.contains("base") {
            "models.desc.whisper.base"
        } else if lower.contains("small") {
            "models.desc.whisper.small"
        } else if lower.contains("medium") {
            "models.desc.whisper.medium"
        } else if lower.contains("large") && lower.contains("turbo") {
            "models.desc.whisper.largeTurbo"
        } else if lower.contains("large") {
            "models.desc.whisper.large"
        } else {
            "models.desc.whisper.generic"
        }
    } else if lower.contains("qwen") {
        "models.desc.llm.qwen"
    } else if lower.contains("phi") {
        "models.desc.llm.phi"
    } else if lower.contains("llama") {
        "models.desc.llm.llama"
    } else if input_mod.to_lowercase().contains("image") {
        "models.desc.llm.image"
    } else if output_mod.to_lowercase().contains("text") {
        "models.desc.llm.text"
    } else {
        "models.desc.llm.generic"
    }
    .to_string()
}

fn extract_json_object(text: &str) -> Option<&str> {
    let start = text.find('{')?;
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escape = false;
    for (i, ch) in text[start..].char_indices() {
        if escape {
            escape = false;
            continue;
        }
        if ch == '\\' && in_string {
            escape = true;
            continue;
        }
        if ch == '"' {
            in_string = !in_string;
            continue;
        }
        if in_string {
            continue;
        }
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    let end = start + i + 1;
                    return Some(&text[start..end]);
                }
            }
            _ => {}
        }
    }
    None
}

/// Derive a human-readable note title from the body text.
///
/// Picks the first non-empty line, strips common leading markers
/// (`# `, `- `, `1. `, `> `, quote marks), and truncates to roughly 40 chars.
/// Returns `None` when the body is empty so callers can fall back to their
/// own default (e.g. the file name or a placeholder).
fn derive_title_from_text(text: &str) -> Option<String> {
    // Walk lines until we find one with content after trimming.
    let raw_line = text
        .lines()
        .map(|line| line.trim())
        .find(|line| !line.is_empty())?;

    // Strip common leading syntax so a Markdown heading or list item still
    // produces a readable plain-text title.
    let mut line = raw_line.to_string();
    let strip_prefixes = [
        "# ", "## ", "### ", "#### ", "##### ", "###### ", "- ", "* ", "> ", "・", "● ", "■ ",
    ];
    loop {
        let stripped = strip_prefixes
            .iter()
            .find_map(|p| line.strip_prefix(p))
            .map(str::to_string);
        // Also strip numbered list prefixes like "1. " / "12. "
        let stripped = stripped.or_else(|| {
            let mut chars = line.char_indices();
            let mut end = 0;
            let mut saw_digit = false;
            while let Some((i, c)) = chars.next() {
                if c.is_ascii_digit() {
                    saw_digit = true;
                    end = i + c.len_utf8();
                } else {
                    break;
                }
            }
            if saw_digit && line[end..].starts_with(". ") {
                Some(line[end + 2..].to_string())
            } else {
                None
            }
        });
        match stripped {
            Some(next) if next != line => line = next,
            _ => break,
        }
    }
    let line = line
        .trim()
        .trim_matches('"')
        .trim_matches('\'')
        .trim_matches('「')
        .trim_matches('」')
        .trim_matches('『')
        .trim_matches('』')
        .trim();
    if line.is_empty() {
        return None;
    }

    // Truncate to ~40 Unicode characters; append an ellipsis if we cut.
    const MAX_TITLE_CHARS: usize = 40;
    let total = line.chars().count();
    let title = if total > MAX_TITLE_CHARS {
        let head: String = line.chars().take(MAX_TITLE_CHARS).collect();
        format!("{head}…")
    } else {
        line.to_string()
    };
    Some(title)
}

/// Remove `<think>...</think>` blocks (and similar chain-of-thought markers)
/// that reasoning-style models like Qwen3 emit before their real answer.
/// Handles:
///   - `<think>...</think>` (case-insensitive, multiline)
///   - `<thinking>...</thinking>`
///   - A dangling opening tag with no closing counterpart — in that case we
///     drop everything up to and including the last newline that precedes the
///     final paragraph, on the assumption that everything before was the
///     model's inner monologue.
///
/// Returns a trimmed copy.
fn strip_think_blocks(text: &str) -> String {
    let mut out = text.to_string();

    // Remove well-formed think/thinking blocks.
    for tag in ["think", "thinking"] {
        loop {
            let lower = out.to_ascii_lowercase();
            let open_tag = format!("<{tag}>");
            let close_tag = format!("</{tag}>");
            let Some(start) = lower.find(&open_tag) else {
                break;
            };
            let Some(rel_end) = lower[start..].find(&close_tag) else {
                break;
            };
            let end = start + rel_end + close_tag.len();
            out.replace_range(start..end, "");
        }
    }

    // Strip a trailing unclosed `<think>...` — some small models forget the
    // closing tag entirely, so the block runs to EOF. Drop everything from the
    // opening tag onward; if the opener is at the start, we end up empty and
    // the caller falls back to whatever came after.
    let lower = out.to_ascii_lowercase();
    for tag in ["<think>", "<thinking>"] {
        if let Some(start) = lower.find(tag) {
            if !lower[start..].contains("</") {
                out.truncate(start);
            }
        }
    }

    // Strip a trailing unclosed `</think>` — model emitted only the closer.
    // Keep everything AFTER it on the assumption that the answer comes last.
    let lower = out.to_ascii_lowercase();
    for tag in ["</think>", "</thinking>"] {
        if let Some(pos) = lower.rfind(tag) {
            if !lower[..pos].contains("<") {
                out = out[pos + tag.len()..].to_string();
            }
        }
    }

    out.trim().to_string()
}

fn strip_llm_preamble(text: &str) -> String {
    // Remove reasoning / chain-of-thought blocks first so downstream heuristics
    // (quote trimming, "Here is..." detection) see only the real answer.
    let text = strip_think_blocks(text);
    let trimmed = text.trim();
    if let Some((_, body)) = trimmed.split_once("\n\n") {
        let first_line = trimmed
            .lines()
            .next()
            .unwrap_or_default()
            .to_ascii_lowercase();
        if first_line.contains("here")
            || first_line.contains("sure")
            || first_line.contains("revised")
        {
            return body.trim().to_string();
        }
    }

    trimmed
        .trim_matches('"')
        .trim_matches('「')
        .trim_matches('」')
        .trim()
        .to_string()
}

fn live_translation_output_is_invalid(text: &str) -> bool {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return true;
    }

    let lower = trimmed.to_ascii_lowercase();
    let leaked_markers = [
        "output only",
        "translation only",
        "translated text only",
        "do not explain",
        "answer with",
        "no notes",
        "translator",
        "translate live captions",
        "translation engine",
        "return only",
        "source language",
        "target language",
        "入力言語",
        "出力言語",
        "翻訳本文のみ",
        "出力は",
        "前置き",
        "見出し",
        "引用符",
        "追加指示",
        "説明は出力",
        "条件:",
        "入力:",
    ];

    leaked_markers
        .iter()
        .any(|marker| lower.contains(&marker.to_ascii_lowercase()))
}

fn live_language_label(code: &str) -> &str {
    match code.trim().to_ascii_lowercase().as_str() {
        "" | "auto" => "the source language",
        "ja" | "jp" | "japanese" => "Japanese",
        "en" | "english" => "English",
        "ko" | "kr" | "korean" => "Korean",
        "zh" | "cn" | "chinese" => "Chinese",
        "fr" | "french" => "French",
        "de" | "german" => "German",
        "es" | "spanish" => "Spanish",
        "it" | "italian" => "Italian",
        "pt" | "portuguese" => "Portuguese",
        _ => code,
    }
}

async fn prepare_audio_for_transcription(
    input_path: &Path,
    extension: &str,
) -> Result<PathBuf, String> {
    if !should_normalize_audio(extension) {
        return Ok(input_path.to_path_buf());
    }

    let input_path = input_path.to_path_buf();
    let output_path = input_path.with_extension("normalized.wav");
    let normalized_path = output_path.clone();

    tokio::task::spawn_blocking(move || normalize_audio_to_wav(&input_path, &output_path))
        .await
        .map_err(|error| format!("音声変換タスクの実行に失敗しました: {error}"))??;

    Ok(normalized_path)
}

fn should_normalize_audio(extension: &str) -> bool {
    matches!(
        extension.to_ascii_lowercase().as_str(),
        "mp3" | "m4a" | "mp4" | "wav" | "flac" | "ogg" | "webm"
    )
}

fn normalize_audio_to_wav(input_path: &Path, output_path: &Path) -> Result<(), String> {
    let source = File::open(input_path)
        .map_err(|error| format!("音声ファイルを開けませんでした: {error}"))?;
    let media_source = MediaSourceStream::new(Box::new(source), Default::default());
    let mut hint = Hint::new();
    if let Some(extension) = input_path.extension().and_then(|value| value.to_str()) {
        hint.with_extension(extension);
    }

    let probed = symphonia::default::get_probe()
        .format(
            &hint,
            media_source,
            &FormatOptions::default(),
            &MetadataOptions::default(),
        )
        .map_err(|error| format!("音声形式を判定できませんでした: {error}"))?;
    let mut format = probed.format;
    let track = format
        .default_track()
        .ok_or_else(|| "音声トラックが見つかりませんでした".to_string())?;
    let track_id = track.id;
    let codec_params = track.codec_params.clone();
    let mut decoder = symphonia::default::get_codecs()
        .make(&codec_params, &DecoderOptions::default())
        .map_err(|error| format!("音声デコーダを作成できませんでした: {error}"))?;

    let mut writer: Option<hound::WavWriter<BufWriter<File>>> = None;
    let mut wrote_samples = 0usize;

    loop {
        let packet = match format.next_packet() {
            Ok(packet) => packet,
            Err(SymphoniaError::IoError(error)) if error.kind() == ErrorKind::UnexpectedEof => {
                break;
            }
            Err(SymphoniaError::ResetRequired) => {
                return Err("音声デコーダのリセットが必要な形式です".to_string());
            }
            Err(error) => return Err(format!("音声パケットを読み込めませんでした: {error}")),
        };

        if packet.track_id() != track_id {
            continue;
        }

        let decoded = match decoder.decode(&packet) {
            Ok(decoded) => decoded,
            Err(SymphoniaError::DecodeError(_)) => continue,
            Err(error) => return Err(format!("音声をデコードできませんでした: {error}")),
        };
        let spec = *decoded.spec();
        let channel_count = spec.channels.count().max(1);

        if writer.is_none() {
            writer = Some(
                hound::WavWriter::create(
                    output_path,
                    hound::WavSpec {
                        channels: 1,
                        sample_rate: spec.rate,
                        bits_per_sample: 16,
                        sample_format: hound::SampleFormat::Int,
                    },
                )
                .map_err(|error| format!("WAVファイルを作成できませんでした: {error}"))?,
            );
        }

        write_decoded_audio_as_mono(
            decoded,
            channel_count,
            writer.as_mut().unwrap(),
            &mut wrote_samples,
        )?;
    }

    let writer = writer.ok_or_else(|| "音声サンプルを読み込めませんでした".to_string())?;
    writer
        .finalize()
        .map_err(|error| format!("WAVファイルを完了できませんでした: {error}"))?;

    if wrote_samples == 0 {
        return Err("音声サンプルが空でした".to_string());
    }

    Ok(())
}

fn write_decoded_audio_as_mono(
    decoded: AudioBufferRef<'_>,
    channel_count: usize,
    writer: &mut hound::WavWriter<BufWriter<File>>,
    wrote_samples: &mut usize,
) -> Result<(), String> {
    let spec = *decoded.spec();
    let mut sample_buffer = SampleBuffer::<f32>::new(decoded.capacity() as u64, spec);
    sample_buffer.copy_interleaved_ref(decoded);

    for frame in sample_buffer.samples().chunks(channel_count) {
        let mono = frame.iter().copied().sum::<f32>() / channel_count as f32;
        writer
            .write_sample(float_sample_to_i16(mono))
            .map_err(|error| format!("WAVサンプルを書き込めませんでした: {error}"))?;
        *wrote_samples += 1;
    }

    Ok(())
}

fn float_sample_to_i16(sample: f32) -> i16 {
    let clamped = sample.clamp(-1.0, 1.0);
    (clamped * i16::MAX as f32).round() as i16
}

async fn write_temp_audio(bytes: &[u8], extension: &str) -> std::io::Result<PathBuf> {
    let upload_dir = std::env::temp_dir().join("seedraft-voice-to-text");
    tokio::fs::create_dir_all(&upload_dir).await?;

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let file_name = format!(
        "upload-{}-{timestamp}.{}",
        std::process::id(),
        sanitize_extension(extension)
    );
    let path = upload_dir.join(file_name);
    tokio::fs::write(&path, bytes).await?;
    Ok(path)
}

fn resolve_audio_extension(file_name: Option<&str>, content_type: Option<&str>) -> String {
    if let Some(extension) = file_name
        .and_then(|name| Path::new(name).extension())
        .and_then(|extension| extension.to_str())
        .filter(|extension| {
            !extension.is_empty()
                && extension.len() <= 8
                && extension.chars().all(|ch| ch.is_ascii_alphanumeric())
        })
    {
        return extension.to_ascii_lowercase();
    }

    match content_type.unwrap_or_default() {
        "audio/mpeg" => "mp3",
        "audio/mp4" | "audio/x-m4a" => "m4a",
        "audio/wav" | "audio/x-wav" => "wav",
        "audio/webm" => "webm",
        "audio/ogg" => "ogg",
        _ => "webm",
    }
    .to_string()
}

fn sanitize_extension(extension: &str) -> String {
    let sanitized: String = extension
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .take(8)
        .collect();

    if sanitized.is_empty() {
        "webm".to_string()
    } else {
        sanitized.to_ascii_lowercase()
    }
}

fn build_router(voice_to_text: Arc<VoiceToTextService>) -> Router {
    let storage =
        Arc::new(Storage::open(storage_db_path()).expect("failed to open storage database"));
    let state = AppState {
        voice_to_text,
        storage,
        live_sessions: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
    };

    Router::new()
        .route("/", get(index_handler))
        .route("/index", get(index_handler))
        .route("/assets/icon.ico", get(app_icon_handler))
        .route("/api/refine", post(refine_handler))
        .route("/api/translate", post(translate_handler))
        .route("/api/analyze", post(analyze_handler))
        .route("/api/complete", post(complete_note_handler))
        .route("/api/process", post(process_pipeline_handler))
        .route("/api/transcribe", post(transcribe_handler))
        .route("/api/download/status", get(download_status_handler))
        .route("/api/download/events", get(download_events_handler))
        .route("/api/models/speech", get(speech_models_handler))
        .route(
            "/api/models/speech/warmup",
            post(warmup_speech_model_handler),
        )
        .route(
            "/api/models/speech/delete",
            post(delete_speech_model_handler),
        )
        .route("/api/models", get(all_models_handler))
        .route("/api/models/delete", post(delete_model_handler))
        .route("/api/models/download", post(download_model_handler))
        .route("/api/models/variants", get(model_variants_handler))
        .route("/api/models/test", post(model_test_handler))
        .route("/api/app/requirements", get(app_requirements_handler))
        .route(
            "/api/app/settings",
            get(app_settings_handler).post(update_app_settings_handler),
        )
        .route("/api/app/pick-folder", post(pick_folder_handler))
        .route("/api/locales", get(locales_handler))
        .route(
            "/api/projects",
            get(list_projects_handler).post(create_project_handler),
        )
        .route(
            "/api/projects/{id}",
            put(update_project_handler).delete(delete_project_handler),
        )
        .route(
            "/api/notes",
            get(list_notes_handler).post(create_note_handler),
        )
        .route(
            "/api/notes/{id}",
            put(update_note_handler).delete(delete_note_handler),
        )
        .route("/api/notes/{id}/tags", post(set_note_tags_handler))
        .route("/api/notes/{id}/parent", post(set_note_parent_handler))
        .route("/api/notes/reorder", post(reorder_notes_handler))
        .route("/api/graph", get(graph_handler))
        .route("/api/note-links", post(create_note_link_handler))
        .route("/api/note-links/{id}", delete(delete_note_link_handler))
        .route("/api/drafts", get(list_drafts_handler))
        .route("/api/drafts/compose", post(compose_draft_handler))
        .route(
            "/api/drafts/{id}",
            get(get_draft_handler)
                .put(update_draft_handler)
                .delete(delete_draft_handler),
        )
        .route(
            "/api/translations",
            get(list_translations_handler).post(create_translation_handler),
        )
        .route("/api/translations/{id}", delete(delete_translation_handler))
        .route("/api/translations/clear", post(clear_translations_handler))
        .route("/api/live/start", post(live_start_handler))
        .route("/api/live/chunk", post(live_chunk_handler))
        .route("/api/live/translate", post(live_translate_handler))
        .route("/api/live/stop", post(live_stop_handler))
        .route("/api/live/sessions", get(live_sessions_handler))
        .route(
            "/api/live/sessions/{id}",
            get(live_session_detail_handler)
                .put(live_session_rename_handler)
                .delete(live_session_delete_handler),
        )
        .layer(DefaultBodyLimit::max(MAX_AUDIO_BYTES))
        .with_state(state)
}

/// Try the preferred port first, then fall back to an OS-assigned ephemeral
/// port if it's already in use. Returns the bound listener along with the
/// actual port so the caller can build the WebView URL from it.
async fn bind_preferred_port() -> std::io::Result<(tokio::net::TcpListener, u16)> {
    let preferred_addr = format!("{LOOPBACK_IP}:{PREFERRED_PORT}");
    match tokio::net::TcpListener::bind(&preferred_addr).await {
        Ok(listener) => Ok((listener, PREFERRED_PORT)),
        Err(preferred_err) => {
            eprintln!(
                "port {PREFERRED_PORT} is unavailable ({preferred_err}); falling back to an ephemeral port",
            );
            let fallback_addr = format!("{LOOPBACK_IP}:0");
            let listener = tokio::net::TcpListener::bind(&fallback_addr).await?;
            let port = listener.local_addr()?.port();
            Ok((listener, port))
        }
    }
}

fn storage_db_path() -> PathBuf {
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
    // Shortcut hint now mentions double-tap support for Ctrl/Alt.
    (
        "settings.shortcuts.hint",
        "各項目の入力欄をクリックして、割り当てたいキーを押してください。Esc で取り消し、Delete / Backspace で解除できます。",
    ),
    (
        "settings.shortcuts.hint",
        "Click a field and press the keys you want to bind. Esc to cancel, Delete / Backspace to clear.",
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
struct LocalesResponse {
    /// Map of locale code → translation dictionary. Each dictionary is a
    /// flat `{ key: string }` object matching what the frontend expects.
    locales: std::collections::BTreeMap<String, serde_json::Value>,
    /// Filesystem path where the locale JSON files live — shown to the user
    /// so they can edit or add new languages.
    dir: String,
}

async fn locales_handler() -> AppResult<Json<LocalesResponse>> {
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

fn run_server_mode(bind_addr: String) {
    println!("Server mode: binding to http://{bind_addr}");

    let runtime = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
    let voice_to_text = Arc::new(VoiceToTextService::new());
    let voice_for_shutdown = Arc::clone(&voice_to_text);

    runtime.block_on(async move {
        let listener = match tokio::net::TcpListener::bind(&bind_addr).await {
            Ok(listener) => listener,
            Err(error) => {
                eprintln!("failed to bind {bind_addr}: {error}");
                eprintln!(
                    "tip: another process may already be using that port; try a different port with `-s {LOOPBACK_IP}:PORT`"
                );
                return;
            }
        };

        let actual = listener
            .local_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| bind_addr.clone());
        println!("Listening on http://{actual}");
        println!("Press Ctrl+C to stop the server.");

        if let Err(error) = axum::serve(listener, build_router(voice_to_text))
            .with_graceful_shutdown(async {
                tokio::signal::ctrl_c()
                    .await
                    .expect("failed to listen for Ctrl+C");
            })
            .await
        {
            eprintln!("axum server stopped unexpectedly: {error}");
        }

        voice_for_shutdown.shutdown().await;
    });
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if let Some(pos) = args.iter().position(|arg| arg == "-s") {
        let ip_arg = args
            .get(pos + 1)
            .cloned()
            .unwrap_or_else(|| "0.0.0.0".to_string());
        let bind_addr = if ip_arg.contains(':') {
            ip_arg
        } else {
            format!("{ip_arg}:8000")
        };

        run_server_mode(bind_addr);
        return;
    }

    let shutdown_inner = Arc::new(Mutex::new(None::<tokio::sync::oneshot::Sender<()>>));
    let voice_to_text = Arc::new(VoiceToTextService::new());
    let shutdown_for_event = Arc::clone(&shutdown_inner);
    let voice_for_event = Arc::clone(&voice_to_text);

    tauri::Builder::default()
        .manage(ShutdownState {
            shutdown_tx: shutdown_inner,
        })
        .setup(move |app| {
            // The server reports back either a successful port binding or the bind error.
            let (ready_tx, ready_rx) = mpsc::channel::<Result<u16, String>>();
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

            {
                let state = app.state::<ShutdownState>();
                let mut guard = state.shutdown_tx.lock().unwrap();
                *guard = Some(shutdown_tx);
            }

            let voice_for_server = Arc::clone(&voice_to_text);
            tauri::async_runtime::spawn(async move {
                let (listener, port) = match bind_preferred_port().await {
                    Ok(pair) => pair,
                    Err(error) => {
                        let _ = ready_tx.send(Err(format!("failed to bind loopback: {error}")));
                        return;
                    }
                };

                let _ = ready_tx.send(Ok(port));

                if let Err(error) = axum::serve(listener, build_router(voice_for_server))
                    .with_graceful_shutdown(async move {
                        shutdown_rx.await.ok();
                    })
                    .await
                {
                    eprintln!("axum server stopped unexpectedly: {error}");
                }
            });

            let port = ready_rx
                .recv_timeout(Duration::from_secs(10))
                .map_err(|error| {
                    std::io::Error::other(format!("server startup timed out: {error}"))
                })?
                .map_err(std::io::Error::other)?;

            let window_url = format!("http://{LOOPBACK_IP}:{port}/index");
            println!("Serving SeeDraft UI at {window_url}");

            tauri::WebviewWindowBuilder::new(
                app,
                "main",
                tauri::WebviewUrl::External(window_url.parse().unwrap()),
            )
            .title("SeeDraft")
            .inner_size(1400.0, 900.0)
            .min_inner_size(1024.0, 720.0)
            .disable_drag_drop_handler()
            .build()?;

            Ok(())
        })
        .on_window_event(move |window, event| {
            if window.label() != "main" {
                return;
            }

            if let tauri::WindowEvent::Destroyed = event {
                // Release OGA-backed models before the process exits so that
                // OnnxRuntime GenAI's leak checker stays quiet.
                let voice = Arc::clone(&voice_for_event);
                tauri::async_runtime::block_on(async move {
                    voice.shutdown().await;
                });

                if let Ok(mut guard) = shutdown_for_event.lock() {
                    if let Some(tx) = guard.take() {
                        let _ = tx.send(());
                    }
                }
            }
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
