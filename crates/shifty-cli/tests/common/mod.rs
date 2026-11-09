use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};

pub struct Fixture {
    dir: std::path::PathBuf,
}

impl Fixture {
    pub fn new(name: &str) -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("shifty-cli-{name}-{}-{id}", std::process::id()));
        std::fs::create_dir(&dir).unwrap();
        Self { dir }
    }

    pub fn write(&self, name: &str, contents: &str) -> String {
        let path = self.path(name);
        std::fs::write(&path, contents).unwrap();
        path
    }

    pub fn path(&self, name: &str) -> String {
        self.dir.join(name).to_str().unwrap().to_string()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

pub fn shifty(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_shifty"))
        .args(args)
        .output()
        .unwrap()
}

pub fn run(args: &[&str]) -> String {
    let output = shifty(args);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}
