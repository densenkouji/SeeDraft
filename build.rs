fn main() {
    ensure_windows_resource_compiler_on_path();
    stage_foundry_local_native_libraries();
    tauri_build::build();
}

#[cfg(windows)]
fn ensure_windows_resource_compiler_on_path() {
    use std::env;

    if find_in_path("rc.exe").is_some() {
        return;
    }

    let Some(rc_dir) = find_windows_sdk_rc_dir() else {
        return;
    };

    let mut paths = env::split_paths(&env::var_os("PATH").unwrap_or_default()).collect::<Vec<_>>();
    if paths.iter().any(|path| path == &rc_dir) {
        return;
    }

    paths.insert(0, rc_dir);
    if let Ok(joined) = env::join_paths(paths) {
        // Build scripts are single-threaded here; this only affects child
        // processes spawned by tauri-build during this build.
        unsafe {
            env::set_var("PATH", joined);
        }
    }
}

#[cfg(not(windows))]
fn ensure_windows_resource_compiler_on_path() {}

#[cfg(windows)]
fn stage_foundry_local_native_libraries() {
    use std::path::PathBuf;

    const REQUIRED_DLLS: &[&str] = &[
        "Microsoft.AI.Foundry.Local.Core.dll",
        "Microsoft.WindowsAppRuntime.Bootstrap.dll",
        "dxcompiler.dll",
        "dxil.dll",
        "onnxruntime.dll",
        "onnxruntime_providers_shared.dll",
        "onnxruntime-genai.dll",
    ];

    println!("cargo:rerun-if-changed=../Foundry-Local/sdk/rust/build.rs");

    let manifest_dir =
        PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap_or_else(|| ".".into()));
    let profile = std::env::var("PROFILE").unwrap_or_else(|_| "debug".to_string());
    let target_root = manifest_dir.join("target");
    let target_dirs = [
        target_root.join(&profile),
        target_root.join("release"),
        target_root.join("debug"),
    ];
    let Some(sdk_out_dir) = target_dirs
        .iter()
        .find_map(|target_dir| find_foundry_sdk_out_dir(target_dir, REQUIRED_DLLS))
    else {
        println!(
            "cargo:warning=Foundry Local native DLLs were not found under {}; run cargo check/build once so foundry-local-sdk can stage them.",
            target_root.display()
        );
        return;
    };

    let staged_dir = manifest_dir
        .join("native")
        .join("foundry-local")
        .join("win-x64");
    if let Err(error) = std::fs::create_dir_all(&staged_dir) {
        println!(
            "cargo:warning=failed to create Foundry Local native staging dir {}: {error}",
            staged_dir.display()
        );
        return;
    }

    for dll in REQUIRED_DLLS {
        let source = sdk_out_dir.join(dll);
        let dest = staged_dir.join(dll);
        println!("cargo:rerun-if-changed={}", source.display());
        if let Err(error) = std::fs::copy(&source, &dest) {
            println!(
                "cargo:warning=failed to stage Foundry Local native DLL {}: {error}",
                source.display()
            );
        }
    }

    println!("cargo:rerun-if-changed={}", staged_dir.display());
}

#[cfg(not(windows))]
fn stage_foundry_local_native_libraries() {}

#[cfg(windows)]
fn find_foundry_sdk_out_dir(
    target_dir: &std::path::Path,
    required: &[&str],
) -> Option<std::path::PathBuf> {
    let build_dir = target_dir.join("build");
    let entries = std::fs::read_dir(build_dir).ok()?;
    let mut candidates = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path().join("out"))
        .filter(|path| path.is_dir())
        .filter(|path| required.iter().all(|dll| path.join(dll).is_file()))
        .collect::<Vec<_>>();

    candidates.sort_by_key(|path| {
        std::fs::metadata(path)
            .and_then(|metadata| metadata.modified())
            .ok()
    });
    candidates.pop()
}

#[cfg(windows)]
fn find_in_path(name: &str) -> Option<std::path::PathBuf> {
    let paths = std::env::var_os("PATH")?;
    std::env::split_paths(&paths)
        .map(|path| path.join(name))
        .find(|candidate| candidate.is_file())
}

#[cfg(windows)]
fn find_windows_sdk_rc_dir() -> Option<std::path::PathBuf> {
    let roots = [
        std::env::var_os("WindowsSdkDir").map(std::path::PathBuf::from),
        std::env::var_os("ProgramFiles(x86)")
            .map(std::path::PathBuf::from)
            .map(|path| path.join("Windows Kits").join("10")),
        std::env::var_os("ProgramFiles")
            .map(std::path::PathBuf::from)
            .map(|path| path.join("Windows Kits").join("10")),
    ];

    roots
        .into_iter()
        .flatten()
        .filter_map(|root| {
            let bin = root.join("bin");
            let entries = std::fs::read_dir(bin).ok()?;
            let mut versions = entries
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .filter(|path| path.join("x64").join("rc.exe").is_file())
                .collect::<Vec<std::path::PathBuf>>();
            versions.sort();
            versions.pop().map(|version| version.join("x64"))
        })
        .next()
}
