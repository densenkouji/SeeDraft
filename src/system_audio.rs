use serde::Serialize;
use std::{
    sync::{
        Arc, Mutex,
        mpsc::{self, Sender},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Default)]
pub(crate) struct SystemAudioCaptureManager {
    session: tokio::sync::Mutex<Option<CaptureThreadHandle>>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct SystemAudioCaptureStatus {
    pub supported: bool,
    pub active: bool,
    pub device_name: Option<String>,
    pub sample_rate: Option<u32>,
    pub started_at: Option<i64>,
    pub elapsed_ms: Option<u128>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct SystemAudioStartResponse {
    pub supported: bool,
    pub active: bool,
    pub device_name: String,
    pub sample_rate: u32,
    pub started_at: i64,
}

pub(crate) struct SystemAudioWav {
    pub bytes: Vec<u8>,
    pub sample_rate: u32,
    pub sample_count: usize,
    pub duration_seconds: f64,
}

pub(crate) struct SystemAudioPcmChunk {
    pub bytes: Vec<u8>,
    pub sample_rate: u32,
    pub sample_count: usize,
}

struct SystemAudioCaptureSession {
    #[cfg(windows)]
    stream: cpal::Stream,
    buffer: Arc<Mutex<CaptureBuffer>>,
    device_name: String,
    sample_rate: u32,
    started_at: i64,
    started_instant: Instant,
}

struct CaptureThreadHandle {
    command_tx: Sender<CaptureCommand>,
    join_handle: thread::JoinHandle<()>,
    device_name: String,
    sample_rate: u32,
    started_at: i64,
    started_instant: Instant,
}

enum CaptureCommand {
    Drain(Sender<Result<SystemAudioPcmChunk, String>>),
    Stop(Sender<Result<SystemAudioWav, String>>),
    Cancel,
}

#[derive(Debug)]
struct CaptureBuffer {
    samples: Vec<i16>,
    max_samples: Option<usize>,
    read_cursor: usize,
}

impl CaptureBuffer {
    fn new(sample_rate: u32, max_seconds: Option<u32>) -> Self {
        let max_samples = max_seconds
            .filter(|seconds| *seconds > 0)
            .map(|seconds| sample_rate as usize * seconds as usize);
        Self {
            samples: Vec::new(),
            max_samples,
            read_cursor: 0,
        }
    }

    fn push(&mut self, sample: i16) {
        if self
            .max_samples
            .is_some_and(|max_samples| self.samples.len() >= max_samples)
        {
            return;
        }
        self.samples.push(sample);
    }

    fn drain_since_last_read(&mut self) -> Vec<i16> {
        let cursor = self.read_cursor.min(self.samples.len());
        let chunk = self.samples[cursor..].to_vec();
        self.read_cursor = self.samples.len();

        if self.read_cursor > 262_144 {
            self.samples.drain(..self.read_cursor);
            self.read_cursor = 0;
        }

        chunk
    }
}

impl SystemAudioCaptureManager {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) async fn status(&self) -> SystemAudioCaptureStatus {
        let guard = self.session.lock().await;
        match guard.as_ref() {
            Some(session) => SystemAudioCaptureStatus {
                supported: system_audio_supported(),
                active: true,
                device_name: Some(session.device_name.clone()),
                sample_rate: Some(session.sample_rate),
                started_at: Some(session.started_at),
                elapsed_ms: Some(session.started_instant.elapsed().as_millis()),
            },
            None => SystemAudioCaptureStatus {
                supported: system_audio_supported(),
                active: false,
                device_name: None,
                sample_rate: None,
                started_at: None,
                elapsed_ms: None,
            },
        }
    }

    pub(crate) async fn start(
        &self,
        max_seconds: Option<u32>,
    ) -> Result<SystemAudioStartResponse, String> {
        let mut guard = self.session.lock().await;
        if guard.is_some() {
            return Err("システム音声キャプチャは既に実行中です".to_string());
        }

        let session = start_capture_thread(max_seconds)?;
        let response = SystemAudioStartResponse {
            supported: true,
            active: true,
            device_name: session.device_name.clone(),
            sample_rate: session.sample_rate,
            started_at: session.started_at,
        };
        *guard = Some(session);
        Ok(response)
    }

    pub(crate) async fn stop(&self) -> Result<SystemAudioWav, String> {
        let session = {
            let mut guard = self.session.lock().await;
            guard
                .take()
                .ok_or_else(|| "システム音声キャプチャは開始されていません".to_string())?
        };

        tokio::task::spawn_blocking(move || session.stop())
            .await
            .map_err(|error| format!("キャプチャ停止タスクの実行に失敗しました: {error}"))?
    }

    pub(crate) async fn cancel(&self) -> bool {
        let mut guard = self.session.lock().await;
        let Some(session) = guard.take() else {
            return false;
        };
        tokio::task::spawn_blocking(move || session.cancel())
            .await
            .ok();
        true
    }

    pub(crate) async fn drain(&self) -> Result<SystemAudioPcmChunk, String> {
        let command_tx = {
            let guard = self.session.lock().await;
            guard
                .as_ref()
                .map(|session| session.command_tx.clone())
                .ok_or_else(|| "システム音声キャプチャは開始されていません".to_string())?
        };

        tokio::task::spawn_blocking(move || {
            let (reply_tx, reply_rx) = mpsc::channel();
            command_tx
                .send(CaptureCommand::Drain(reply_tx))
                .map_err(|_| "PC音声キャプチャスレッドが停止しています".to_string())?;
            reply_rx
                .recv()
                .map_err(|_| "PC音声キャプチャ結果を受信できませんでした".to_string())?
        })
        .await
        .map_err(|error| format!("キャプチャ読み取りタスクの実行に失敗しました: {error}"))?
    }
}

impl CaptureThreadHandle {
    fn stop(self) -> Result<SystemAudioWav, String> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.command_tx
            .send(CaptureCommand::Stop(reply_tx))
            .map_err(|_| "PC音声キャプチャスレッドが停止しています".to_string())?;
        let result = reply_rx
            .recv()
            .map_err(|_| "PC音声キャプチャ結果を受信できませんでした".to_string())?;
        self.join_handle
            .join()
            .map_err(|_| "PC音声キャプチャスレッドの終了に失敗しました".to_string())?;
        result
    }

    fn cancel(self) {
        let _ = self.command_tx.send(CaptureCommand::Cancel);
        let _ = self.join_handle.join();
    }
}

impl SystemAudioCaptureSession {
    fn drain_pcm(&self) -> Result<SystemAudioPcmChunk, String> {
        let samples = {
            let mut guard = self
                .buffer
                .lock()
                .map_err(|_| "キャプチャバッファを読み取れませんでした".to_string())?;
            guard.drain_since_last_read()
        };
        Ok(SystemAudioPcmChunk {
            bytes: encode_pcm16_bytes(&samples),
            sample_rate: self.sample_rate,
            sample_count: samples.len(),
        })
    }

    fn into_wav(self) -> Result<SystemAudioWav, String> {
        #[cfg(windows)]
        drop(self.stream);

        let samples = {
            let guard = self
                .buffer
                .lock()
                .map_err(|_| "キャプチャバッファを読み取れませんでした".to_string())?;
            guard.samples.clone()
        };

        if samples.is_empty() {
            return Err(
                "PC音声を取得できませんでした。再生中の音声があるか確認してください".to_string(),
            );
        }

        let duration_seconds = samples.len() as f64 / self.sample_rate.max(1) as f64;
        let bytes = encode_mono_pcm16_wav(self.sample_rate, &samples)?;
        Ok(SystemAudioWav {
            bytes,
            sample_rate: self.sample_rate,
            sample_count: samples.len(),
            duration_seconds,
        })
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

fn encode_mono_pcm16_wav(sample_rate: u32, samples: &[i16]) -> Result<Vec<u8>, String> {
    let data_size = samples
        .len()
        .checked_mul(2)
        .ok_or_else(|| "WAVデータが大きすぎます".to_string())?;
    let riff_size = 36usize
        .checked_add(data_size)
        .ok_or_else(|| "WAVデータが大きすぎます".to_string())?;
    if riff_size > u32::MAX as usize {
        return Err("WAVデータが大きすぎます".to_string());
    }

    let mut bytes = Vec::with_capacity(44 + data_size);
    bytes.extend_from_slice(b"RIFF");
    bytes.extend_from_slice(&(riff_size as u32).to_le_bytes());
    bytes.extend_from_slice(b"WAVE");
    bytes.extend_from_slice(b"fmt ");
    bytes.extend_from_slice(&16u32.to_le_bytes());
    bytes.extend_from_slice(&1u16.to_le_bytes());
    bytes.extend_from_slice(&1u16.to_le_bytes());
    bytes.extend_from_slice(&sample_rate.to_le_bytes());
    bytes.extend_from_slice(&(sample_rate * 2).to_le_bytes());
    bytes.extend_from_slice(&2u16.to_le_bytes());
    bytes.extend_from_slice(&16u16.to_le_bytes());
    bytes.extend_from_slice(b"data");
    bytes.extend_from_slice(&(data_size as u32).to_le_bytes());
    bytes.extend_from_slice(&encode_pcm16_bytes(samples));
    Ok(bytes)
}

fn encode_pcm16_bytes(samples: &[i16]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(samples.len() * 2);
    for sample in samples {
        bytes.extend_from_slice(&sample.to_le_bytes());
    }
    bytes
}

#[cfg(windows)]
fn system_audio_supported() -> bool {
    true
}

#[cfg(not(windows))]
fn system_audio_supported() -> bool {
    false
}

#[cfg(not(windows))]
fn start_capture_thread(_max_seconds: Option<u32>) -> Result<CaptureThreadHandle, String> {
    Err("PC音声キャプチャは Windows でのみ利用できます".to_string())
}

#[cfg(windows)]
fn start_capture_thread(max_seconds: Option<u32>) -> Result<CaptureThreadHandle, String> {
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    let (command_tx, command_rx) = mpsc::channel();

    let join_handle = thread::spawn(move || match start_capture_stream(max_seconds) {
        Ok(session) => {
            let _ = ready_tx.send(Ok(CaptureThreadReady {
                device_name: session.device_name.clone(),
                sample_rate: session.sample_rate,
                started_at: session.started_at,
                started_instant: session.started_instant,
            }));
            loop {
                match command_rx.recv() {
                    Ok(CaptureCommand::Drain(reply_tx)) => {
                        let _ = reply_tx.send(session.drain_pcm());
                    }
                    Ok(CaptureCommand::Stop(reply_tx)) => {
                        let _ = reply_tx.send(session.into_wav());
                        break;
                    }
                    Ok(CaptureCommand::Cancel) | Err(_) => break,
                }
            }
        }
        Err(error) => {
            let _ = ready_tx.send(Err(error));
        }
    });

    let ready = ready_rx
        .recv_timeout(Duration::from_secs(5))
        .map_err(|error| format!("PC音声キャプチャの開始待ちに失敗しました: {error}"))??;

    Ok(CaptureThreadHandle {
        command_tx,
        join_handle,
        device_name: ready.device_name,
        sample_rate: ready.sample_rate,
        started_at: ready.started_at,
        started_instant: ready.started_instant,
    })
}

#[cfg(windows)]
struct CaptureThreadReady {
    device_name: String,
    sample_rate: u32,
    started_at: i64,
    started_instant: Instant,
}

#[cfg(windows)]
fn start_capture_stream(max_seconds: Option<u32>) -> Result<SystemAudioCaptureSession, String> {
    use cpal::{
        SampleFormat,
        traits::{DeviceTrait, HostTrait, StreamTrait},
    };

    let host = cpal::default_host();
    let device = host
        .default_output_device()
        .ok_or_else(|| "既定の再生デバイスが見つかりません".to_string())?;
    let device_name = device
        .name()
        .unwrap_or_else(|_| "Default output device".to_string());
    let supported_config = device.default_output_config().map_err(|error| {
        format!("再生デバイスの既定フォーマットを取得できませんでした: {error}")
    })?;
    let sample_rate = supported_config.sample_rate().0;
    let channels = supported_config.channels().max(1) as usize;
    let sample_format = supported_config.sample_format();
    let stream_config: cpal::StreamConfig = supported_config.into();
    let buffer = Arc::new(Mutex::new(CaptureBuffer::new(sample_rate, max_seconds)));

    let err_fn = |error| eprintln!("system audio capture stream error: {error}");
    let stream = match sample_format {
        SampleFormat::F32 => build_input_stream(
            &device,
            &stream_config,
            channels,
            &buffer,
            err_fn,
            append_f32,
        ),
        SampleFormat::F64 => build_input_stream(
            &device,
            &stream_config,
            channels,
            &buffer,
            err_fn,
            append_f64,
        ),
        SampleFormat::I8 => build_input_stream(
            &device,
            &stream_config,
            channels,
            &buffer,
            err_fn,
            append_i8,
        ),
        SampleFormat::I16 => build_input_stream(
            &device,
            &stream_config,
            channels,
            &buffer,
            err_fn,
            append_i16,
        ),
        SampleFormat::I32 => build_input_stream(
            &device,
            &stream_config,
            channels,
            &buffer,
            err_fn,
            append_i32,
        ),
        SampleFormat::I64 => build_input_stream(
            &device,
            &stream_config,
            channels,
            &buffer,
            err_fn,
            append_i64,
        ),
        SampleFormat::U8 => build_input_stream(
            &device,
            &stream_config,
            channels,
            &buffer,
            err_fn,
            append_u8,
        ),
        SampleFormat::U16 => build_input_stream(
            &device,
            &stream_config,
            channels,
            &buffer,
            err_fn,
            append_u16,
        ),
        SampleFormat::U32 => build_input_stream(
            &device,
            &stream_config,
            channels,
            &buffer,
            err_fn,
            append_u32,
        ),
        SampleFormat::U64 => build_input_stream(
            &device,
            &stream_config,
            channels,
            &buffer,
            err_fn,
            append_u64,
        ),
        _ => Err(format!("未対応の音声サンプル形式です: {sample_format:?}")),
    }?;
    stream
        .play()
        .map_err(|error| format!("PC音声キャプチャを開始できませんでした: {error}"))?;

    Ok(SystemAudioCaptureSession {
        stream,
        buffer,
        device_name,
        sample_rate,
        started_at: now_millis(),
        started_instant: Instant::now(),
    })
}

#[cfg(windows)]
fn build_input_stream<T, F>(
    device: &cpal::Device,
    config: &cpal::StreamConfig,
    channels: usize,
    buffer: &Arc<Mutex<CaptureBuffer>>,
    err_fn: impl FnMut(cpal::StreamError) + Send + 'static,
    append: F,
) -> Result<cpal::Stream, String>
where
    T: cpal::SizedSample,
    F: Fn(&[T], usize, &Arc<Mutex<CaptureBuffer>>) + Send + 'static,
{
    use cpal::traits::DeviceTrait;

    let capture_buffer = Arc::clone(buffer);
    device
        .build_input_stream(
            config,
            move |data: &[T], _| append(data, channels, &capture_buffer),
            err_fn,
            None,
        )
        .map_err(|error| format!("PC音声キャプチャストリームを作成できませんでした: {error}"))
}

#[cfg(windows)]
fn append_mono_frames<T, F>(
    data: &[T],
    channels: usize,
    buffer: &Arc<Mutex<CaptureBuffer>>,
    convert: F,
) where
    T: Copy,
    F: Fn(T) -> f64,
{
    let Ok(mut guard) = buffer.lock() else {
        return;
    };
    for frame in data.chunks(channels.max(1)) {
        let sum = frame.iter().copied().map(&convert).sum::<f64>();
        let mono = sum / frame.len().max(1) as f64;
        guard.push(float_to_i16(mono));
    }
}

#[cfg(windows)]
fn append_f32(data: &[f32], channels: usize, buffer: &Arc<Mutex<CaptureBuffer>>) {
    append_mono_frames(data, channels, buffer, f64::from);
}

#[cfg(windows)]
fn append_f64(data: &[f64], channels: usize, buffer: &Arc<Mutex<CaptureBuffer>>) {
    append_mono_frames(data, channels, buffer, |sample| sample);
}

#[cfg(windows)]
fn append_i8(data: &[i8], channels: usize, buffer: &Arc<Mutex<CaptureBuffer>>) {
    append_mono_frames(data, channels, buffer, |sample| {
        sample as f64 / i8::MAX as f64
    });
}

#[cfg(windows)]
fn append_i16(data: &[i16], channels: usize, buffer: &Arc<Mutex<CaptureBuffer>>) {
    append_mono_frames(data, channels, buffer, |sample| {
        sample as f64 / i16::MAX as f64
    });
}

#[cfg(windows)]
fn append_i32(data: &[i32], channels: usize, buffer: &Arc<Mutex<CaptureBuffer>>) {
    append_mono_frames(data, channels, buffer, |sample| {
        sample as f64 / i32::MAX as f64
    });
}

#[cfg(windows)]
fn append_i64(data: &[i64], channels: usize, buffer: &Arc<Mutex<CaptureBuffer>>) {
    append_mono_frames(data, channels, buffer, |sample| {
        sample as f64 / i64::MAX as f64
    });
}

#[cfg(windows)]
fn append_u8(data: &[u8], channels: usize, buffer: &Arc<Mutex<CaptureBuffer>>) {
    append_mono_frames(data, channels, buffer, |sample| {
        unsigned_to_float(sample as f64, u8::MAX as f64)
    });
}

#[cfg(windows)]
fn append_u16(data: &[u16], channels: usize, buffer: &Arc<Mutex<CaptureBuffer>>) {
    append_mono_frames(data, channels, buffer, |sample| {
        unsigned_to_float(sample as f64, u16::MAX as f64)
    });
}

#[cfg(windows)]
fn append_u32(data: &[u32], channels: usize, buffer: &Arc<Mutex<CaptureBuffer>>) {
    append_mono_frames(data, channels, buffer, |sample| {
        unsigned_to_float(sample as f64, u32::MAX as f64)
    });
}

#[cfg(windows)]
fn append_u64(data: &[u64], channels: usize, buffer: &Arc<Mutex<CaptureBuffer>>) {
    append_mono_frames(data, channels, buffer, |sample| {
        unsigned_to_float(sample as f64, u64::MAX as f64)
    });
}

#[cfg(windows)]
fn unsigned_to_float(sample: f64, max: f64) -> f64 {
    (sample / max) * 2.0 - 1.0
}

#[cfg(windows)]
fn float_to_i16(sample: f64) -> i16 {
    let clamped = sample.clamp(-1.0, 1.0);
    (clamped * i16::MAX as f64).round() as i16
}
