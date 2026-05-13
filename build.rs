fn main() {
    ensure_windows_resource_compiler_on_path();
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
