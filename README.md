
<p align="center">
    <img src="images/SeeDraft-icon_128x128.png" alt="logo">
</p>
<p align="center">Local-first, privacy-friendly voice-to-text for Windows. Record, transcribe, refine, translate, connect, and draft with on-device AI.</p>

# SeeDraft

**Languages:** **English** · [日本語](README.ja.md)

---
[![SeeDraft Main Windows](images\screen01.png)](https://github.com/densenkouji/SeeDraft)


## Overview

SeeDraft is a desktop application that turns speech and audio files into editable, connected notes without sending your data to the cloud. It combines:

- **Whisper speech-to-text** through Foundry Local
- **Local LLM post-processing** for refinement, translation, extraction, completion, and draft composition
- **SQLite-backed projects, notes, tags, links, drafts, translations, and live-caption sessions**
- **axum + Tauri 2** in one local Windows desktop app

No API keys are required for the app itself. Model execution is local through the sibling Foundry Local SDK checkout.

## Features

### Recording And Transcription

- Record from the microphone or drag-and-drop audio files such as `wav`, `mp3`, `m4a`, `webm`, `ogg`, `flac`, `aac`, and `opus`.
- Choose a Whisper model from the Models tab (`whisper-tiny` through larger Whisper variants).
- Track model downloads with Server-Sent Events, including progress, failure diagnostics, and variant/runtime details.
- Warm up the selected speech model at startup so the first transcription request does not pay the model-load cost.
- Add a transcription prompt with meeting topics, speaker names, terminology, or spelling rules.
- Normalize uploaded audio on the backend with Symphonia when needed; microphone recordings are converted in the browser to WAV.

### Post-Processing Pipeline

- Run post-processing after transcription with quick toggles: save to note, refine, translate, extract, and custom steps.
- **Refine** fixes transcription artifacts while preserving meaning: fillers, obvious misrecognitions, punctuation, and spoken commands such as "new line", "period", `改行`, and `句点`.
- **Translate** source or refined text with optional terminology and translation instructions.
- **Extract** tone, summary, and keywords from text.
- **Custom steps** let users define ordered LLM instructions, with built-in presets for polite tone, business memo, meeting minutes, casual rewrite, summaries, and bullet organization.
- Linked-note context can be injected into refinement, translation, extraction, completion, and custom steps.

### Notes, Graph, And Drafts

- Organize notes by project with persistent SQLite storage.
- Search, edit, delete, reorder, tag, and nest notes with parent-child relationships.
- Connect notes manually in the graph view; linked notes can become LLM context.
- View a project graph containing project, note, tag, manual-link, and parent-child edges.
- Select notes and compose a draft by simple concatenation or by LLM rewrite into a single article.
- Edit, save, delete, and export drafts as Markdown.
- Complete a note with an LLM continuation from the note editor.

### Live Caption And Translation

- Start a live-caption session from the top bar.
- Buffer microphone audio continuously and transcribe each utterance after a short pause.
- Optionally translate live captions while the session is running.
- Save, rename, view, and delete live-caption sessions.

### Daily Use

- Light, dark, or system theme.
- Japanese and English UI, with locale files seeded into the user data directory for extension.
- Persistent settings for project, speech model, language, quick toggles, custom steps, post-processing models, theme, locale, output filename prefix, and shortcuts.
- Keyboard shortcuts, including double-tap `Ctrl` / `Alt` bindings.
- Toasts for normal feedback and a persistent error banner for serious failures.

## Architecture

```
┌────────────────────────────────────────────────────────┐
│ Tauri 2 WebView                                        │
│ HTML / CSS / JS served by the local axum server        │
└──────────────────────────┬─────────────────────────────┘
                           │ HTTP on 127.0.0.1
┌──────────────────────────▼─────────────────────────────┐
│ axum 0.8 server                                        │
│  ├─ /api/transcribe              Whisper STT           │
│  ├─ /api/process                 pipeline steps        │
│  ├─ /api/refine, /api/translate  single-step APIs      │
│  ├─ /api/analyze, /api/complete  extraction/completion │
│  ├─ /api/projects, /api/notes    SQLite notes          │
│  ├─ /api/note-links, /api/graph  graph relationships   │
│  ├─ /api/drafts                  draft workspace       │
│  ├─ /api/translations            translation history   │
│  ├─ /api/live/*                  live caption sessions │
│  ├─ /api/models/*                model catalog/cache   │
│  └─ /api/download/events         SSE progress stream   │
└──────────────────────────┬─────────────────────────────┘
                           │
┌──────────────────────────▼─────────────────────────────┐
│ foundry-local-sdk                                      │
│ ONNX Runtime GenAI / WinML on Windows                  │
└────────────────────────────────────────────────────────┘
```

- Desktop mode starts the axum server in the Tauri `setup` hook, then opens the WebView after the server is ready.
- The preferred loopback port is `127.0.0.1:38713`; if unavailable, the app falls back to an OS-assigned port.
- Server-only mode binds to the address passed with `-s`.
- User data is stored in the OS data directory as `seedraft.sqlite`; locale JSON files live beside it under `locales/`.
- Temporary uploaded audio is written under the system temp directory in `seedraft-voice-to-text/`.

## Requirements

- Windows 11 x64
- Rust toolchain, edition 2024
- Microsoft C++ Build Tools / MSVC
- Foundry Local SDK checked out as a sibling directory at `../Foundry-Local/sdk/rust` for development and installer builds
- Tauri CLI for installer builds

Packaged installers include the Foundry Local native runtime DLLs staged from the SDK build output, so end-user PCs do not need a separate Foundry Local app or CLI installation.

## Getting Started

### 1. Install Rust

Install Rust with `rustup`, then confirm the toolchain:

```powershell
rustc --version
cargo --version
```

### 2. Install Tauri CLI

```powershell
cargo install tauri-cli --version "^2.0.0" --locked
cargo tauri --version
```

### 3. Clone With Foundry Local As A Sibling

```powershell
git clone https://github.com/microsoft/Foundry-Local
git clone https://github.com/densenkouji/SeeDraft.git
cd SeeDraft
```

Expected layout:

```
/
├─ Foundry-Local/
│  └─ sdk/rust/
└─ SeeDraft/
   └─ Cargo.toml
```

### 4. Run

```powershell
# Desktop application
cargo run

# Server-only mode for browser debugging
cargo run -- -s 127.0.0.1:38713
```

On first use, the selected speech model and the default LLM (`qwen2.5-coder-0.5b`) are downloaded through Foundry Local if they are not already cached. SeeDraft uses Foundry Local's default model cache location and does not override it. The selected speech model is warmed up automatically after startup.

### 5. Build Installers

```powershell
cargo tauri build
```

Installer artifacts are written under `target/release/bundle/`.
During the build, SeeDraft stages the required Foundry Local native binaries into `native/foundry-local/win-x64` and bundles them as the Tauri `foundry-local` resource. The staged DLLs are ignored by Git.
The default bundle target is the NSIS `.exe` installer.

## Usage Cheatsheet

| Action | Where |
|---|---|
| Start / stop recording | Record button at the bottom |
| Transcribe an audio file | Drag and drop onto the app window |
| Save current text as a note | `Save as note` / `ノートに追加` button |
| Auto-run post-processing | Quick toggles beside the record button |
| Change speech model or download models | Settings -> Models |
| Add terminology or transcription hints | Settings -> Transcription |
| Configure refine, translation, custom steps, linked context, extraction, completion | Settings -> Post-processing |
| Search and organize notes | Left notes sidebar |
| Link notes visually | Graph toolbar -> Connect notes |
| Compose a draft | Select notes -> Compose draft |
| Use live captions | Top bar -> Live |
| Export a draft | Drafts workspace -> Export as Markdown |

## Project Structure

```
/
├─ AGENTS.md               # Project context and coding workflow
├─ CLAUDE.md               # Claude-oriented project context
├─ Cargo.toml              # Rust dependencies
├─ Cargo.lock
├─ build.rs                # tauri_build::build()
├─ tauri.conf.json         # Tauri config (productName: SeeDraft)
├─ dist/
│  └─ index.html           # Tauri frontendDist placeholder
├─ src/
│  ├─ main.rs              # axum + Tauri entry point, UI, APIs, model service
│  ├─ storage.rs           # SQLite data layer
│  └─ locales/             # Bundled ja/en locale JSON
├─ res/
│  └─ icon.ico
├─ icons/
│  └─ icon.png
└─ docs/                   # API, database, screen specs, changelog
```

## Data Model

SQLite tables are created and migrated by `src/storage.rs`:

- `projects`
- `notes`
- `tags`
- `note_tags`
- `note_links`
- `drafts`
- `draft_notes`
- `translations`
- `live_sessions`
- `live_segments`

## Key Design Decisions

- **Local HTTP app shell.** axum serves all HTML/API routes; Tauri displays the local URL and manages the desktop window.
- **Readiness before WebView.** The Tauri window is created only after the HTTP server binds successfully.
- **Preferred port with fallback.** The app tries `38713` first, then falls back to an ephemeral loopback port.
- **Model cache and warmup.** Loaded speech/chat models are cached in memory; the selected speech model is warmed up at startup and unloaded during shutdown.
- **SSE download progress.** Model download events are broadcast to the UI via `/api/download/events`.
- **Structured local storage.** Projects, notes, links, drafts, translations, and live sessions are persisted in SQLite with additive migrations.
- **Meaning-preserving refinement.** Built-in refinement is intentionally conservative; stylistic rewrites are handled by custom post-processing steps.
- **Graceful model shutdown.** Cached Foundry Local models are explicitly unloaded before the process exits.

## Roadmap

- [ ] More robust live streaming transcription
- [ ] Packaged release workflow
- [ ] macOS / Linux support when the local runtime stack allows it

## License

MIT.

## Acknowledgements

- [Microsoft Foundry Local](https://github.com/microsoft/Foundry-Local) — on-device model runtime
- [Tauri](https://tauri.app/) — desktop shell
- [axum](https://github.com/tokio-rs/axum) — HTTP server
- [symphonia](https://github.com/pdeljanov/Symphonia) — audio decoding
- [rusqlite](https://github.com/rusqlite/rusqlite) — SQLite binding
