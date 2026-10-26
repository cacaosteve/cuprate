fn main() {
    set_commit_env();
}

/// This sets the git `COMMIT` environment variable.
fn set_commit_env() {
    println!("cargo:rerun-if-env-changed=GITHUB_SHA");
    // A detached checkout changes HEAD, not refs/heads. Resolve Git's paths
    // rather than assuming .git is a directory (it is a file in worktrees).
    if std::env::var_os("GITHUB_SHA").is_none() {
        for name in ["HEAD", "refs/heads", "packed-refs"] {
            let output = std::process::Command::new("git")
                .args(["rev-parse", "--git-path", name])
                .output()
                .expect("git must be available when building Cuprate");
            assert!(output.status.success(), "could not locate Git metadata");
            let path = String::from_utf8(output.stdout).unwrap();
            let path = path.trim();
            // Watching a nonexistent optional packed-refs file would rebuild on
            // every invocation; a later pack also changes the watched refs dir.
            if std::path::Path::new(path).exists() {
                println!("cargo:rerun-if-changed={path}");
            }
        }
    }

    let commit = if let Ok(t) = std::env::var("GITHUB_SHA") {
        t
    } else {
        // FIXME: This could also be `std::fs::read({PATH}/{branch})`
        // so the machine building doesn't need `git`, although:
        // 1. Having `git` as a build dependency is probably ok
        // 2. It causes issues on PRs that aren't the `main` branch
        String::from_utf8(
            std::process::Command::new("git")
                .args(["show", "-s", "--format=%H"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap()
    }
    .trim()
    .to_lowercase();

    assert_eq!(
        commit.len(),
        40,
        "Commit hash should always be 40 bytes long."
    );

    println!("cargo:rustc-env=COMMIT={commit}");
}
