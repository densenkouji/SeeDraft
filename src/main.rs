#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod desktop;
mod settings;
mod storage;
mod system_audio;
mod views;

use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Multipart, Path as AxumPath, Query, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response, Sse, sse::Event},
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
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use storage::{
    DraftWithNotes, GraphData, LiveSegment, LiveSession, LiveSessionDetail, Note, NoteLink,
    NoteWithTags, Project, Storage, Theme, Translation,
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
const DEFAULT_DRAFT_PROMPT_TEMPLATE: &str = "Reconstruct the following notes into one coherent document.\n\nRules:\n- Output in the same language as the input.\n- Merge duplicate content.\n- Use appropriate Markdown headings (#, ##), paragraphs, and bullet lists for readability.\n- Do not add information that is not present in the input.\n- Respect the note order while creating a natural flow.\n- Do not output a preface, explanation, or code fence.\n- Output only the Markdown body.\n{instruction}\n\nInput notes:\n{notes}";
const MAX_AUDIO_BYTES: usize = 200 * 1024 * 1024;
const MIN_TRANSCRIBABLE_AUDIO_SECONDS: f64 = 0.25;
const SHORT_AUDIO_PEAK_THRESHOLD: f64 = 0.08;
const SILENCE_RMS_THRESHOLD: f64 = 0.0035;
const SILENCE_PEAK_THRESHOLD: f64 = 0.018;
const SILENCE_ACTIVE_SAMPLE_THRESHOLD: f64 = 0.02;
const SILENCE_ACTIVE_RATIO_THRESHOLD: f64 = 0.01;
static FOUNDRY_NATIVE_DIR: OnceLock<PathBuf> = OnceLock::new();

fn is_required_model_alias(alias: &str) -> bool {
    alias == DEFAULT_SPEECH_MODEL_ALIAS || alias == CHAT_MODEL_ALIAS
}

fn foundry_config() -> FoundryLocalConfig {
    let mut config = FoundryLocalConfig::new("seedraft");
    if let Some(dir) = FOUNDRY_NATIVE_DIR.get() {
        config = config.library_path(dir.display().to_string());
    }
    config
}

fn set_foundry_native_dir(path: PathBuf) {
    if path.join("Microsoft.AI.Foundry.Local.Core.dll").is_file() {
        let _ = FOUNDRY_NATIVE_DIR.set(path);
    }
}

fn bundled_foundry_native_dir_from_exe() -> Option<PathBuf> {
    std::env::current_exe().ok()?.parent().and_then(|dir| {
        let direct = dir.join("foundry-local");
        if direct.join("Microsoft.AI.Foundry.Local.Core.dll").is_file() {
            return Some(direct);
        }
        let resources = dir.join("resources").join("foundry-local");
        if resources
            .join("Microsoft.AI.Foundry.Local.Core.dll")
            .is_file()
        {
            return Some(resources);
        }
        None
    })
}

fn shutdown_app_resources(
    voice: &Arc<VoiceToTextService>,
    shutdown_tx: &Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    cleanup_done: &Arc<AtomicBool>,
) {
    if cleanup_done.swap(true, Ordering::SeqCst) {
        return;
    }

    if let Ok(mut guard) = shutdown_tx.lock() {
        if let Some(tx) = guard.take() {
            let _ = tx.send(());
        }
    }

    let voice = Arc::clone(voice);
    tauri::async_runtime::block_on(async move {
        voice.shutdown().await;
    });
}

struct ShutdownState {
    shutdown_tx: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
}

#[derive(Clone)]
struct AppState {
    voice_to_text: Arc<VoiceToTextService>,
    storage: Arc<Storage>,
    live_sessions: Arc<tokio::sync::Mutex<HashMap<String, LiveSessionState>>>,
    system_audio: Arc<system_audio::SystemAudioCaptureManager>,
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
    required: bool,
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
    /// Foundry Local reports progress per download, while the UI shows a
    /// single progress overlay. Keep model downloads sequential so progress
    /// events never interleave across aliases.
    download_lock: tokio::sync::Mutex<()>,
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
            download_lock: tokio::sync::Mutex::new(()),
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
            "Common fillers have already been removed. Remove only any remaining unnecessary filler or hesitation."
        } else {
            "Keep hesitations that carry meaning."
        };
        let custom_terms = request
            .custom_terms
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| {
                format!("\n- Preserve the spelling of these terms and proper nouns: {value}")
            })
            .unwrap_or_default();
        let custom_instruction = request
            .custom_instruction
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| format!("\n- Additional instruction: {value}"))
            .unwrap_or_default();
        let language_guard = refinement_language_guard_instruction(&source_text);

        // Refine = meaning-preserving cleanup ONLY. Rewriting style, tone, or
        // register is explicitly out of scope — users who want those changes
        // should configure a Custom post-processing step instead.
        let user_prompt = format!(
            "Proofread the following transcription without translating it.\n\nStrict rules:\n- Preserve the exact language and script of the input. Do not translate.\n- {language_guard}\n- If the input is English, output English only. If the input is Japanese, output Japanese only.\n- Do not output Chinese unless the input itself is Chinese.\n- Do not change meaning, content, amount of information, wording style, or register.\n- Keep the original vocabulary and phrasing as much as possible.\n- {filler_instruction}\n- Only fix obvious speech-recognition errors, punctuation, spacing, and notation inconsistencies.\n- Do not summarize.\n- Do not remove sentences or information.\n- Keep the original sentence order.\n- Do not add facts, subjects, objects, or terms.\n- Output only the proofread body. No preface, explanation, heading, or quotes.{custom_terms}{custom_instruction}\n\nInput:\n{source_text}"
        );
        let mut messages: Vec<ChatCompletionRequestMessage> = Vec::new();
        messages.push(
            ChatCompletionRequestSystemMessage::from(
                "You are a transcription proofreading engine. Never translate the input. Always keep the same language and script as the input. Only correct obvious recognition errors, filler remnants, punctuation, spacing, and notation. Output the body text only.",
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

        let refined = strip_llm_preamble(&refined);
        if refinement_language_changed(&source_text, &refined) {
            return Err(AppError::internal(
                "整形結果の言語が入力と異なるため破棄しました",
            ));
        }

        Ok(refined)
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
                format!("\n- Preserve these proper nouns and terms as much as possible: {value}")
            })
            .unwrap_or_default();
        let custom_instruction = request
            .custom_instruction
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| format!("\n- Additional instruction: {value}"))
            .unwrap_or_default();

        let model = self.ensure_chat_model_by_alias(model_alias).await?;
        let client = model.create_chat_client().temperature(0.0).max_tokens(1024);
        let user_prompt = format!(
            "Translate the following transcription.\n\nRules:\n- Source language: {source_language}\n- Target language: {target_language}\n- Preserve the meaning.\n- Do not summarize.\n- Do not remove or add information.\n- Make the result natural and readable as live-caption subtitles.\n- Do not output a preface, explanation, heading, or quotes.\n- Output only the translated body text.{custom_terms}{custom_instruction}\n\nInput:\n{source_text}"
        );
        let mut messages: Vec<ChatCompletionRequestMessage> = Vec::new();
        messages.push(
            ChatCompletionRequestSystemMessage::from(
                "You are a translator for live captions. Preserve the input meaning and order, and output only the translated body text.",
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
        model_alias: Option<&str>,
        prompt_template: Option<&str>,
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
        let model = self.ensure_chat_model_by_alias(model_alias).await?;
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
        let instruction_block = instruction
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| format!("\n- Additional instruction: {s}"))
            .unwrap_or_default();
        let user_prompt =
            build_draft_prompt(prompt_template, &bodies, instruction, &instruction_block);
        let messages: Vec<ChatCompletionRequestMessage> = vec![
            ChatCompletionRequestSystemMessage::from(
                "You are an editor who turns multiple notes into one coherent article. Output only the Markdown body.",
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
        let mut cleaned = clean_draft_llm_output(&text);
        if cleaned.is_empty() {
            return Err(AppError::internal("合成結果が空でした"));
        }

        if draft_output_looks_like_prompt_echo(&cleaned) {
            let retry_prompt = format!(
                "Create the final Markdown article from the source notes below.\n\nRequirements:\n- Return only the finished article body.\n- Do not repeat the task, requirements, source labels, or source notes.\n- Keep the same language as the source notes.\n- Merge overlapping ideas and preserve only facts present in the source.\n- Use clear Markdown headings and paragraphs.{instruction_block}\n\nSOURCE NOTES START\n{bodies}\nSOURCE NOTES END\n\nFINAL ARTICLE:"
            );
            let retry_messages: Vec<ChatCompletionRequestMessage> = vec![
                ChatCompletionRequestSystemMessage::from(
                    "You write final articles from source notes. The next answer must be only the final article body, never the prompt or source labels.",
                )
                .into(),
                ChatCompletionRequestUserMessage::from(retry_prompt.as_str()).into(),
            ];
            let retry_response = client
                .complete_chat(&retry_messages, None)
                .await
                .map_err(AppError::internal)?;
            let retry_text = retry_response.choices[0]
                .message
                .content
                .as_deref()
                .unwrap_or("");
            cleaned = clean_draft_llm_output(retry_text);
            if cleaned.is_empty() {
                return Err(AppError::internal("合成結果が空でした"));
            }
            if draft_output_looks_like_prompt_echo(&cleaned) {
                return Err(AppError::internal(
                    "LLMが再構成結果ではなくプロンプトを返しました。入力を短くするか、追加指示を簡潔にして再実行してください。",
                ));
            }
        }

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
            "Process the following text according to the instruction.\n\nInstruction:\n{instruction}\n\nRules:\n- Do not output a preface, explanation, or code fence.\n- Output only the processed body text.\n\nInput:\n{source}"
        );
        let mut messages: Vec<ChatCompletionRequestMessage> = Vec::new();
        messages.push(
            ChatCompletionRequestSystemMessage::from(
                "You are a text-processing engine that follows the instruction. Output only the processed body text.",
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
            "Extract information from the following text and output strict JSON only.\n\nRequirements:\n- Do not output anything outside JSON: no preface, explanation, or code fence.\n- Use exactly these three keys: \"tone\", \"summary\", \"keywords\".\n- tone: one short label for the speaker's overall tone, in the same language as the input.\n- summary: 1-2 sentences summarizing the key points, in the same language as the input.\n- keywords: an array of 3-6 important terms, in the same language as the input where appropriate.\n\nInput:\n{source}"
        );
        let mut messages: Vec<ChatCompletionRequestMessage> = Vec::new();
        messages.push(
            ChatCompletionRequestSystemMessage::from(
                "You extract tone, key points, and keywords from text. Follow the specified JSON schema exactly and output JSON only.",
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
            "Continue the following note body.\n\nRules:\n- Continue the style, language, and topic of the input body.\n- Never repeat the existing body. Output only the continuation text.\n- Do not output a preface such as \"I will continue\" or \"Sure\", a heading, quotes, or a code fence.\n- Be conservative with facts. Do not invent proper nouns or numbers that are not in the input.\n- If related notes are attached as system context, use them only as background and do not copy them directly.\n- Wrap up the topic naturally in about 3-6 sentences.\n\nExisting note body:\n{source_text}"
        );
        let mut messages: Vec<ChatCompletionRequestMessage> = Vec::new();
        messages.push(
            ChatCompletionRequestSystemMessage::from(
                "You are an assistant that continues notes. Preserve the existing body's style and language, output only the continuation body, and do not include a preface or explanation.",
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
            "Correct the following transcription using the provided context.\n\nRules:\n- Correct only obvious recognition errors.\n- Preserve the meaning.\n- Do not summarize.\n- Do not add or remove information.\n- Do not output a preface, explanation, heading, or quotes.\n- Output only the corrected body text.\n\nContext and instruction:\n{context_prompt}\n\nTranscription:\n{source_text}"
        );
        let messages: Vec<ChatCompletionRequestMessage> = vec![
            ChatCompletionRequestSystemMessage::from(
                "You are an editor who corrects transcription wording and notation. Use the context as a clue, but do not add content that is not in the source.",
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
        // Primary (default) chat model — normally this is also present in
        // `chat_models`, but keep it in the unload list so cleanup is robust if
        // that relationship changes.
        let primary_chat_model = self.chat_model.lock().await.take();

        // All cached chat models
        let mut chat_models: Vec<Arc<Model>> = {
            let mut guard = self.chat_models.lock().await;
            guard.drain().map(|(_, model)| model).collect()
        };
        if let Some(model) = primary_chat_model {
            chat_models.push(model);
        }
        let mut unloaded_chat_aliases = std::collections::HashSet::new();
        for model in chat_models {
            if unloaded_chat_aliases.insert(model.alias().to_string()) {
                let _ = model.unload().await;
            }
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
        let _download_guard = self.download_lock.lock().await;

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
            let required = is_required_model_alias(&alias);

            results.push(ModelInfo {
                alias,
                category: category.to_string(),
                description,
                downloaded,
                loaded: loaded_in_cache,
                active: is_active,
                required,
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
        if is_required_model_alias(alias) {
            return Err(AppError::bad_request("必須モデルは削除できません"));
        }

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
        if is_required_model_alias(alias) {
            return Err(AppError::bad_request("必須モデルは削除できません"));
        }

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
struct SystemAudioStartRequest {
    max_seconds: Option<u32>,
}

#[derive(Serialize)]
struct SystemAudioCancelResponse {
    cancelled: bool,
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
    #[serde(default)]
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

    let audio_activity = read_wav_audio_activity_if_available(&transcription_path)
        .await
        .map_err(AppError::internal)?;
    if audio_activity
        .as_ref()
        .is_some_and(audio_activity_is_effectively_silent)
    {
        let project_id_for_response = match project_id.clone() {
            Some(id) => Some(id),
            None => Some(state.storage.default_project_id()?),
        };
        cleanup_transcription_files(&upload_path, &transcription_path).await;
        return Ok(Json(TranscriptionResponse {
            text: String::new(),
            language: None,
            duration: audio_activity.map(|activity| activity.duration_seconds),
            note_id: None,
            project_id: project_id_for_response,
        }));
    }

    let transcription = state
        .voice_to_text
        .transcribe(&transcription_path, language, speech_model)
        .await;

    cleanup_transcription_files(&upload_path, &transcription_path).await;

    let transcription = transcription?;
    let mut text = if let Some(prompt) = transcription_prompt
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
    if transcription_looks_like_no_speech_hallucination(&text)
        && audio_activity
            .as_ref()
            .is_some_and(audio_activity_is_low_confidence_speech)
    {
        text.clear();
    }

    // Persist the transcription as a note only when the caller opted in.
    // Otherwise the transcription is returned in the response and the user
    // can still save it manually with the create-note button.
    let resolved_project_id = match project_id {
        Some(id) => id,
        None => state.storage.default_project_id()?,
    };
    let (note_id, project_id_for_response) = if save_as_note && !text.trim().is_empty() {
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

async fn system_audio_status_handler(
    State(state): State<AppState>,
) -> AppResult<Json<system_audio::SystemAudioCaptureStatus>> {
    Ok(Json(state.system_audio.status().await))
}

async fn system_audio_start_handler(
    State(state): State<AppState>,
    Json(request): Json<SystemAudioStartRequest>,
) -> AppResult<Json<system_audio::SystemAudioStartResponse>> {
    let max_seconds = request.max_seconds.map(|seconds| seconds.clamp(1, 7200));
    state
        .system_audio
        .start(max_seconds)
        .await
        .map(Json)
        .map_err(AppError::internal)
}

async fn system_audio_stop_handler(State(state): State<AppState>) -> AppResult<Response> {
    let wav = state
        .system_audio
        .stop()
        .await
        .map_err(AppError::bad_request)?;
    Response::builder()
        .header(header::CONTENT_TYPE, "audio/wav")
        .header(
            header::CONTENT_DISPOSITION,
            "attachment; filename=\"system-audio.wav\"",
        )
        .header("X-SeeDraft-Sample-Rate", wav.sample_rate.to_string())
        .header("X-SeeDraft-Sample-Count", wav.sample_count.to_string())
        .header(
            "X-SeeDraft-Duration-Seconds",
            format!("{:.3}", wav.duration_seconds),
        )
        .body(Body::from(wav.bytes))
        .map_err(AppError::internal)
}

async fn system_audio_drain_handler(State(state): State<AppState>) -> AppResult<Response> {
    let chunk = state
        .system_audio
        .drain()
        .await
        .map_err(AppError::bad_request)?;
    Response::builder()
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header("X-SeeDraft-Sample-Rate", chunk.sample_rate.to_string())
        .header("X-SeeDraft-Sample-Count", chunk.sample_count.to_string())
        .body(Body::from(chunk.bytes))
        .map_err(AppError::internal)
}

async fn system_audio_cancel_handler(
    State(state): State<AppState>,
) -> AppResult<Json<SystemAudioCancelResponse>> {
    Ok(Json(SystemAudioCancelResponse {
        cancelled: state.system_audio.cancel().await,
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
    let runtime_ready = sdk_runtime_ready;
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
struct ThemeListResponse {
    themes: Vec<Theme>,
}

#[derive(Deserialize)]
struct ThemeInput {
    name: String,
    description: Option<String>,
    transcription_prompt: Option<String>,
    custom_terms: Option<String>,
    custom_instruction: Option<String>,
}

fn normalize_theme_field(value: Option<String>) -> Option<String> {
    value
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

async fn list_themes_handler(
    State(state): State<AppState>,
) -> AppResult<Json<ThemeListResponse>> {
    let themes = state.storage.list_themes()?;
    Ok(Json(ThemeListResponse { themes }))
}

async fn create_theme_handler(
    State(state): State<AppState>,
    Json(input): Json<ThemeInput>,
) -> AppResult<Json<Theme>> {
    let name = input.name.trim();
    if name.is_empty() {
        return Err(AppError::bad_request("theme name is required"));
    }
    let description = normalize_theme_field(input.description);
    let transcription_prompt = normalize_theme_field(input.transcription_prompt);
    let custom_terms = normalize_theme_field(input.custom_terms);
    let custom_instruction = normalize_theme_field(input.custom_instruction);
    let theme = state.storage.create_theme(
        name,
        description.as_deref(),
        transcription_prompt.as_deref(),
        custom_terms.as_deref(),
        custom_instruction.as_deref(),
    )?;
    Ok(Json(theme))
}

async fn update_theme_handler(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(input): Json<ThemeInput>,
) -> AppResult<Json<Theme>> {
    let name = input.name.trim();
    if name.is_empty() {
        return Err(AppError::bad_request("theme name is required"));
    }
    let description = normalize_theme_field(input.description);
    let transcription_prompt = normalize_theme_field(input.transcription_prompt);
    let custom_terms = normalize_theme_field(input.custom_terms);
    let custom_instruction = normalize_theme_field(input.custom_instruction);
    state.storage.update_theme(
        &id,
        name,
        description.as_deref(),
        transcription_prompt.as_deref(),
        custom_terms.as_deref(),
        custom_instruction.as_deref(),
    )?;
    let theme = state
        .storage
        .get_theme(&id)?
        .ok_or_else(|| AppError::bad_request("theme not found"))?;
    Ok(Json(theme))
}

async fn delete_theme_handler(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> AppResult<Json<serde_json::Value>> {
    state.storage.delete_theme(&id)?;
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
    mode: Option<String>,            // "concat" | "llm"
    instruction: Option<String>,     // per-draft instruction, used only for llm mode
    model: Option<String>,           // LLM alias, used only for llm mode
    prompt_template: Option<String>, // draft prompt template, used only for llm mode
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
        .compose_draft(
            &pairs,
            mode,
            input.instruction.as_deref(),
            input.model.as_deref(),
            input.prompt_template.as_deref(),
        )
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
    model: Option<String>,
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
                model: input.model,
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
                format!("Referenced from this note with label \"{}\"", label.trim())
            }
            ("in", Some(label)) if !label.trim().is_empty() => {
                format!("Backlink to this note with label \"{}\"", label.trim())
            }
            ("out", _) => "Linked note".to_string(),
            ("in", _) => "Backlink source".to_string(),
            _ => "Related note".to_string(),
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
            "Reference: excerpts from notes manually linked to this note. Use them as background when adjusting the LLM output, but do not copy their content directly.\n{body}"
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TextScript {
    Latin,
    Japanese,
    Cjk,
    Hangul,
    Cyrillic,
    Arabic,
    Devanagari,
    Thai,
}

#[derive(Default)]
struct TextScriptCounts {
    latin: usize,
    cjk: usize,
    kana: usize,
    hangul: usize,
    cyrillic: usize,
    arabic: usize,
    devanagari: usize,
    thai: usize,
}

impl TextScriptCounts {
    fn total(&self) -> usize {
        self.latin
            + self.cjk
            + self.kana
            + self.hangul
            + self.cyrillic
            + self.arabic
            + self.devanagari
            + self.thai
    }
}

fn count_text_scripts(text: &str) -> TextScriptCounts {
    let mut counts = TextScriptCounts::default();
    for ch in text.chars() {
        if ch.is_ascii_alphabetic() {
            counts.latin += 1;
        } else if ('\u{00C0}'..='\u{024F}').contains(&ch) || ('\u{1E00}'..='\u{1EFF}').contains(&ch)
        {
            counts.latin += 1;
        } else if ('\u{3040}'..='\u{30FF}').contains(&ch) {
            counts.kana += 1;
        } else if ('\u{4E00}'..='\u{9FFF}').contains(&ch) || ('\u{3400}'..='\u{4DBF}').contains(&ch)
        {
            counts.cjk += 1;
        } else if ('\u{AC00}'..='\u{D7AF}').contains(&ch) || ('\u{1100}'..='\u{11FF}').contains(&ch)
        {
            counts.hangul += 1;
        } else if ('\u{0400}'..='\u{04FF}').contains(&ch) {
            counts.cyrillic += 1;
        } else if ('\u{0600}'..='\u{06FF}').contains(&ch) {
            counts.arabic += 1;
        } else if ('\u{0900}'..='\u{097F}').contains(&ch) {
            counts.devanagari += 1;
        } else if ('\u{0E00}'..='\u{0E7F}').contains(&ch) {
            counts.thai += 1;
        }
    }
    counts
}

fn dominant_text_script(text: &str) -> Option<TextScript> {
    let counts = count_text_scripts(text);
    if counts.total() < 3 {
        return None;
    }
    if counts.kana > 0 && counts.kana + counts.cjk >= 3 {
        return Some(TextScript::Japanese);
    }

    let candidates = [
        (TextScript::Latin, counts.latin),
        (TextScript::Cjk, counts.cjk),
        (TextScript::Hangul, counts.hangul),
        (TextScript::Cyrillic, counts.cyrillic),
        (TextScript::Arabic, counts.arabic),
        (TextScript::Devanagari, counts.devanagari),
        (TextScript::Thai, counts.thai),
    ];
    candidates
        .into_iter()
        .filter(|(_, count)| *count >= 3)
        .max_by_key(|(_, count)| *count)
        .map(|(script, _)| script)
}

fn refinement_language_guard_instruction(source_text: &str) -> &'static str {
    match dominant_text_script(source_text) {
        Some(TextScript::Latin) => {
            "Detected input script: Latin. The output must remain in the same Latin-script language as the input. Do not output Chinese, Japanese, Korean, Cyrillic, Arabic, or another script."
        }
        Some(TextScript::Japanese) => {
            "Detected input language/script: Japanese. The output must remain Japanese. Do not translate it into Chinese, English, Korean, or any other language."
        }
        Some(TextScript::Cjk) => {
            "Detected input script: CJK ideographs. Preserve the same source language and script. Do not switch between Chinese, Japanese, Korean, or another language."
        }
        Some(TextScript::Hangul) => {
            "Detected input script: Korean Hangul. The output must remain Korean Hangul."
        }
        Some(TextScript::Cyrillic) => {
            "Detected input script: Cyrillic. The output must remain in the same Cyrillic-script language."
        }
        Some(TextScript::Arabic) => {
            "Detected input script: Arabic. The output must remain in the same Arabic-script language."
        }
        Some(TextScript::Devanagari) => {
            "Detected input script: Devanagari. The output must remain in the same Devanagari-script language."
        }
        Some(TextScript::Thai) => "Detected input script: Thai. The output must remain Thai.",
        None => "Preserve the input language and script exactly. Do not translate.",
    }
}

fn refinement_language_changed(source_text: &str, refined_text: &str) -> bool {
    let Some(source_script) = dominant_text_script(source_text) else {
        return false;
    };
    let Some(refined_script) = dominant_text_script(refined_text) else {
        return false;
    };
    if source_script == refined_script {
        return false;
    }

    let source_counts = count_text_scripts(source_text);
    let refined_counts = count_text_scripts(refined_text);
    let refined_total = refined_counts.total().max(1);
    let cjk_like = refined_counts.cjk + refined_counts.kana + refined_counts.hangul;

    // A few proper nouns in another script are fine. A dominant script switch is not.
    (match source_script {
        TextScript::Latin => {
            cjk_like * 100 / refined_total >= 20 || refined_script != TextScript::Latin
        }
        TextScript::Japanese => refined_script != TextScript::Japanese,
        TextScript::Cjk => refined_script != TextScript::Cjk,
        TextScript::Hangul => refined_script != TextScript::Hangul,
        TextScript::Cyrillic => refined_script != TextScript::Cyrillic,
        TextScript::Arabic => refined_script != TextScript::Arabic,
        TextScript::Devanagari => refined_script != TextScript::Devanagari,
        TextScript::Thai => refined_script != TextScript::Thai,
    }) && source_counts.total() >= 3
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

fn build_draft_prompt(
    prompt_template: Option<&str>,
    bodies: &str,
    instruction: Option<&str>,
    instruction_block: &str,
) -> String {
    let template = prompt_template
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(DEFAULT_DRAFT_PROMPT_TEMPLATE);
    let instruction_text = instruction
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("");

    let mut prompt = template
        .replace("{notes}", bodies)
        .replace("{input_notes}", bodies)
        .replace("{instruction}", instruction_block.trim())
        .replace("{additional_instruction}", instruction_text);

    if !template.contains("{notes}") && !template.contains("{input_notes}") {
        prompt.push_str("\n\nInput notes:\n");
        prompt.push_str(bodies);
    }

    if !instruction_text.is_empty()
        && !template.contains("{instruction}")
        && !template.contains("{additional_instruction}")
    {
        prompt.push_str("\n\nAdditional instruction:\n");
        prompt.push_str(instruction_text);
    }

    prompt.trim().to_string()
}

fn clean_draft_llm_output(text: &str) -> String {
    let preamble_stripped = strip_llm_preamble(text);
    let mut cleaned = preamble_stripped.trim();

    if cleaned.starts_with("```") {
        if let Some((_, body)) = cleaned.split_once('\n') {
            cleaned = body.trim();
        }
        if let Some(body) = cleaned.strip_suffix("```") {
            cleaned = body.trim_end();
        }
    }

    let labels = [
        "final article:",
        "article:",
        "draft:",
        "markdown:",
        "output:",
        "本文:",
        "最終記事:",
        "ドラフト:",
    ];
    loop {
        let Some(first_line) = cleaned.lines().next() else {
            break;
        };
        let first_line_lower = first_line.trim().to_ascii_lowercase();
        if !labels.iter().any(|label| first_line_lower == *label) {
            break;
        }
        cleaned = cleaned[first_line.len()..].trim_start();
    }

    cleaned.trim().to_string()
}

fn draft_output_looks_like_prompt_echo(text: &str) -> bool {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return false;
    }

    let lower = trimmed.to_ascii_lowercase();
    if lower.starts_with("reconstruct the following notes")
        || lower.starts_with("create the final markdown article")
    {
        return true;
    }

    let prompt_markers = [
        "input notes:",
        "source notes start",
        "source notes end",
        "requirements:",
        "rules:",
        "additional instruction:",
        "output only the markdown body",
        "return only the finished article",
        "do not repeat the task",
        "do not add information",
        "merge duplicate content",
        "respect the note order",
    ];
    let marker_count = prompt_markers
        .iter()
        .filter(|marker| lower.contains(**marker))
        .count();
    if marker_count >= 2 {
        return true;
    }

    let source_label_count = trimmed
        .lines()
        .filter(|line| {
            let line = line.trim_start();
            line.starts_with("### [") || line.starts_with("## [")
        })
        .count();
    source_label_count >= 2 || (source_label_count >= 1 && marker_count >= 1)
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
        "ru" | "russian" => "Russian",
        "ar" | "arabic" => "Arabic",
        "hi" | "hindi" => "Hindi",
        "id" | "indonesian" => "Indonesian",
        "vi" | "vietnamese" => "Vietnamese",
        "th" | "thai" => "Thai",
        _ => code,
    }
}

#[derive(Clone, Copy, Debug)]
struct AudioActivity {
    duration_seconds: f64,
    rms: f64,
    peak: f64,
    active_ratio: f64,
}

fn audio_activity_is_effectively_silent(activity: &AudioActivity) -> bool {
    (activity.duration_seconds < MIN_TRANSCRIBABLE_AUDIO_SECONDS
        && activity.peak <= SHORT_AUDIO_PEAK_THRESHOLD)
        || activity.peak <= SILENCE_PEAK_THRESHOLD
        || (activity.rms <= SILENCE_RMS_THRESHOLD
            && activity.active_ratio <= SILENCE_ACTIVE_RATIO_THRESHOLD)
}

fn audio_activity_is_low_confidence_speech(activity: &AudioActivity) -> bool {
    activity.duration_seconds < 1.0
        || activity.peak <= 0.06
        || (activity.rms <= 0.01 && activity.active_ratio <= 0.04)
}

fn transcription_looks_like_no_speech_hallucination(text: &str) -> bool {
    let normalized = text
        .trim()
        .chars()
        .filter(|ch| {
            !ch.is_whitespace()
                && !matches!(
                    ch,
                    '。' | '、' | '.' | ',' | '!' | '?' | '！' | '？' | '"' | '\'' | '「' | '」'
                )
        })
        .collect::<String>()
        .to_ascii_lowercase();

    matches!(
        normalized.as_str(),
        "ご視聴ありがとうございました"
            | "ご清聴ありがとうございました"
            | "ご視聴ありがとうございます"
            | "thankyouforwatching"
            | "thanksforwatching"
    )
}

async fn read_wav_audio_activity_if_available(
    path: &Path,
) -> Result<Option<AudioActivity>, String> {
    if path.extension().and_then(|value| value.to_str()) != Some("wav") {
        return Ok(None);
    }

    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || read_wav_audio_activity(&path))
        .await
        .map_err(|error| format!("音声解析タスクの実行に失敗しました: {error}"))?
        .map(Some)
}

fn read_wav_audio_activity(path: &Path) -> Result<AudioActivity, String> {
    let mut reader =
        hound::WavReader::open(path).map_err(|error| format!("WAVを開けませんでした: {error}"))?;
    let spec = reader.spec();
    let channels = u64::from(spec.channels.max(1));
    let sample_rate = f64::from(spec.sample_rate.max(1));
    let mut sample_count = 0u64;
    let mut active_count = 0u64;
    let mut peak = 0.0f64;
    let mut sum_squares = 0.0f64;

    let mut accumulate = |sample: f64| {
        let abs = sample.abs();
        peak = peak.max(abs);
        sum_squares += sample * sample;
        sample_count += 1;
        if abs >= SILENCE_ACTIVE_SAMPLE_THRESHOLD {
            active_count += 1;
        }
    };

    match spec.sample_format {
        hound::SampleFormat::Float => {
            for sample in reader.samples::<f32>() {
                accumulate(f64::from(sample.map_err(|error| {
                    format!("WAVサンプルを読めませんでした: {error}")
                })?));
            }
        }
        hound::SampleFormat::Int if spec.bits_per_sample <= 16 => {
            for sample in reader.samples::<i16>() {
                accumulate(
                    f64::from(
                        sample
                            .map_err(|error| format!("WAVサンプルを読めませんでした: {error}"))?,
                    ) / f64::from(i16::MAX),
                );
            }
        }
        hound::SampleFormat::Int => {
            let max = (1_i64 << (spec.bits_per_sample.saturating_sub(1).min(31))) as f64;
            for sample in reader.samples::<i32>() {
                accumulate(
                    f64::from(
                        sample
                            .map_err(|error| format!("WAVサンプルを読めませんでした: {error}"))?,
                    ) / max,
                );
            }
        }
    }

    if sample_count == 0 {
        return Ok(AudioActivity {
            duration_seconds: 0.0,
            rms: 0.0,
            peak: 0.0,
            active_ratio: 0.0,
        });
    }

    Ok(AudioActivity {
        duration_seconds: sample_count as f64 / channels as f64 / sample_rate,
        rms: (sum_squares / sample_count as f64).sqrt(),
        peak,
        active_ratio: active_count as f64 / sample_count as f64,
    })
}

async fn cleanup_transcription_files(upload_path: &Path, transcription_path: &Path) {
    if let Err(error) = tokio::fs::remove_file(upload_path).await {
        eprintln!(
            "failed to remove temporary audio file {}: {error}",
            upload_path.display()
        );
    }
    if transcription_path != upload_path {
        if let Err(error) = tokio::fs::remove_file(transcription_path).await {
            eprintln!(
                "failed to remove normalized audio file {}: {error}",
                transcription_path.display()
            );
        }
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
    let storage_db_path = settings::storage_db_path();
    if let Some(parent) = storage_db_path.parent() {
        std::fs::create_dir_all(parent).expect("failed to create storage database directory");
    }
    let storage =
        Arc::new(Storage::open(storage_db_path).expect("failed to open storage database"));
    let state = AppState {
        voice_to_text,
        storage,
        live_sessions: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        system_audio: Arc::new(system_audio::SystemAudioCaptureManager::new()),
    };

    Router::new()
        .route("/", get(views::index_handler))
        .route("/index", get(views::index_handler))
        .route("/hold-overlay", get(views::hold_overlay_handler))
        .route("/assets/icon.ico", get(views::app_icon_handler))
        .route("/api/refine", post(refine_handler))
        .route("/api/translate", post(translate_handler))
        .route("/api/analyze", post(analyze_handler))
        .route("/api/complete", post(complete_note_handler))
        .route("/api/process", post(process_pipeline_handler))
        .route("/api/transcribe", post(transcribe_handler))
        .route("/api/system-audio/status", get(system_audio_status_handler))
        .route("/api/system-audio/start", post(system_audio_start_handler))
        .route("/api/system-audio/stop", post(system_audio_stop_handler))
        .route("/api/system-audio/drain", post(system_audio_drain_handler))
        .route(
            "/api/system-audio/cancel",
            post(system_audio_cancel_handler),
        )
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
            get(settings::app_settings_handler).post(settings::update_app_settings_handler),
        )
        .route("/api/app/pick-folder", post(settings::pick_folder_handler))
        .route("/api/locales", get(settings::locales_handler))
        .route(
            "/api/projects",
            get(list_projects_handler).post(create_project_handler),
        )
        .route(
            "/api/projects/{id}",
            put(update_project_handler).delete(delete_project_handler),
        )
        .route(
            "/api/themes",
            get(list_themes_handler).post(create_theme_handler),
        )
        .route(
            "/api/themes/{id}",
            put(update_theme_handler).delete(delete_theme_handler),
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
        if let Some(dir) = bundled_foundry_native_dir_from_exe() {
            set_foundry_native_dir(dir);
        }
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
    let cleanup_done = Arc::new(AtomicBool::new(false));
    let cleanup_for_event = Arc::clone(&cleanup_done);
    let shutdown_for_run = Arc::clone(&shutdown_inner);
    let voice_for_run = Arc::clone(&voice_to_text);
    let cleanup_for_run = Arc::clone(&cleanup_done);
    let quit_requested = Arc::new(AtomicBool::new(false));
    let quit_for_setup = Arc::clone(&quit_requested);
    let quit_for_event = Arc::clone(&quit_requested);
    let quit_for_run = Arc::clone(&quit_requested);

    let app = tauri::Builder::default()
        .manage(ShutdownState {
            shutdown_tx: shutdown_inner,
        })
        .manage(desktop::GlobalShortcutRegistry::default())
        .manage(desktop::AppExitState::new(Arc::clone(&quit_requested)))
        .plugin(
            tauri_plugin_global_shortcut::Builder::new()
                .with_handler(|app, shortcut, event| {
                    desktop::handle_global_shortcut(app, shortcut, event.state)
                })
                .build(),
        )
        .setup(move |app| {
            if let Some(dir) = app
                .path()
                .resolve("foundry-local", tauri::path::BaseDirectory::Resource)
                .ok()
            {
                set_foundry_native_dir(dir);
            } else if let Some(dir) = bundled_foundry_native_dir_from_exe() {
                set_foundry_native_dir(dir);
            }

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
            let start_minimized = settings::load_app_settings().start_minimized;

            tauri::WebviewWindowBuilder::new(
                app,
                "main",
                tauri::WebviewUrl::External(window_url.parse().unwrap()),
            )
            .title("SeeDraft")
            .inner_size(1400.0, 900.0)
            .min_inner_size(1024.0, 720.0)
            .visible(!start_minimized)
            .disable_drag_drop_handler()
            .build()?;

            let hold_overlay_url = format!("http://{LOOPBACK_IP}:{port}/hold-overlay");
            let hold_overlay = tauri::WebviewWindowBuilder::new(
                app,
                desktop::HOLD_OVERLAY_WINDOW_LABEL,
                tauri::WebviewUrl::External(hold_overlay_url.parse().unwrap()),
            )
            .title("SeeDraft Recording")
            .inner_size(
                desktop::HOLD_OVERLAY_WIDTH as f64,
                desktop::HOLD_OVERLAY_HEIGHT as f64,
            )
            .min_inner_size(
                desktop::HOLD_OVERLAY_WIDTH as f64,
                desktop::HOLD_OVERLAY_HEIGHT as f64,
            )
            .max_inner_size(
                desktop::HOLD_OVERLAY_WIDTH as f64,
                desktop::HOLD_OVERLAY_HEIGHT as f64,
            )
            .resizable(false)
            .decorations(false)
            .transparent(true)
            .always_on_top(true)
            .skip_taskbar(true)
            .focusable(false)
            .focused(false)
            .visible(false)
            .disable_drag_drop_handler()
            .build()?;
            if let Err(error) = hold_overlay.set_ignore_cursor_events(true) {
                eprintln!("failed to make hold overlay click-through: {error}");
            }

            desktop::start_native_hold_key_hook(app.handle().clone());
            desktop::build_system_tray(app, Arc::clone(&quit_for_setup))?;

            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            desktop::close_main_window,
            desktop::quit_app,
            desktop::register_global_shortcuts,
            desktop::trigger_hold_recording_shortcut,
            desktop::show_hold_recording_overlay,
            desktop::hide_hold_recording_overlay,
            desktop::complete_hold_recording
        ])
        .on_window_event(move |window, event| {
            if window.label() != "main" {
                return;
            }

            match event {
                tauri::WindowEvent::CloseRequested { api, .. } => {
                    if !quit_for_event.load(Ordering::SeqCst) {
                        api.prevent_close();
                        if let Err(error) = window.hide() {
                            eprintln!("failed to hide main window: {error}");
                        }
                    }
                }
                tauri::WindowEvent::Destroyed => {
                    // Release OGA-backed models before the process exits so that
                    // OnnxRuntime GenAI's leak checker stays quiet.
                    shutdown_app_resources(
                        &voice_for_event,
                        &shutdown_for_event,
                        &cleanup_for_event,
                    );
                }
                _ => {}
            }
        })
        .build(tauri::generate_context!())
        .expect("error while building tauri application");

    app.run(move |app, event| match event {
        tauri::RunEvent::ExitRequested { .. } => {
            quit_for_run.store(true, Ordering::SeqCst);
            desktop::close_webview_windows_for_exit(app);
            shutdown_app_resources(&voice_for_run, &shutdown_for_run, &cleanup_for_run);
        }
        tauri::RunEvent::Exit => {
            shutdown_app_resources(&voice_for_run, &shutdown_for_run, &cleanup_for_run);
        }
        _ => {}
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refinement_guard_rejects_english_to_cjk() {
        assert!(refinement_language_changed(
            "This meeting starts at nine and covers the quarterly roadmap.",
            "这次会议从九点开始，讨论季度路线图。"
        ));
    }

    #[test]
    fn refinement_guard_allows_english_to_english() {
        assert!(!refinement_language_changed(
            "This meeting starts at nine and covers the quarterly roadmap.",
            "This meeting starts at 9 and covers the quarterly roadmap."
        ));
    }

    #[test]
    fn refinement_guard_allows_japanese_to_japanese() {
        assert!(!refinement_language_changed(
            "今日は新しい機能について説明します。",
            "今日は新しい機能について説明します。"
        ));
    }

    #[test]
    fn draft_echo_guard_rejects_repeated_prompt() {
        let echoed = "Reconstruct the following notes into one coherent document.\n\nRules:\n- Output only the Markdown body.\n\nInput notes:\n### [1] Memo\nBody";
        assert!(draft_output_looks_like_prompt_echo(echoed));
    }

    #[test]
    fn draft_echo_guard_allows_article_body() {
        let article = "# 週次まとめ\n\n今週は入力体験を改善し、初回起動時の案内を整理しました。";
        assert!(!draft_output_looks_like_prompt_echo(article));
    }

    #[test]
    fn draft_cleaner_strips_fences_and_answer_label() {
        let raw = "```markdown\nFinal article:\n# 週次まとめ\n\n本文です。\n```";
        assert_eq!(
            clean_draft_llm_output(raw),
            "# 週次まとめ\n\n本文です。".to_string()
        );
    }

    #[test]
    fn draft_prompt_template_fills_placeholders() {
        let prompt = build_draft_prompt(
            Some("Write it.\n{instruction}\n\n{notes}"),
            "### [1] Note\nBody",
            Some("Use polite style."),
            "\n- Additional instruction: Use polite style.",
        );
        assert!(prompt.contains("Additional instruction: Use polite style."));
        assert!(prompt.contains("### [1] Note\nBody"));
    }

    #[test]
    fn draft_prompt_template_appends_notes_when_placeholder_missing() {
        let prompt = build_draft_prompt(
            Some("Write one article."),
            "### [1] Note\nBody",
            Some("Start with decisions."),
            "\n- Additional instruction: Start with decisions.",
        );
        assert!(prompt.contains("Write one article."));
        assert!(prompt.contains("Input notes:\n### [1] Note\nBody"));
        assert!(prompt.contains("Additional instruction:\nStart with decisions."));
    }
}
