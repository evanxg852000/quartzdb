// build.rs
// use std::process::Command;

fn main() {
    // Optional: Re-run the build script if the HEAD file changes
    // println!("cargo:rerun-if-changed=.git/HEAD");

    // Get the git hash (short version)
    // let output = Command::new("git")
    //     .args(["rev-parse", "--short", "HEAD"])
    //     .output()
    //     .expect("Failed to execute git command");

    // let git_hash = String::from_utf8(output.stdout)
    //     .expect("Invalid UTF-8 from git")
    //     .trim()
    //     .to_string();

    // Set the environment variable for the compiler
    // println!("cargo:rustc-env=GIT_HASH={}", git_hash);

    // let now = chrono::Utc::now().to_rfc3339();
    // println!("cargo:rustc-env=BUILD_TIME={}", now);
}
