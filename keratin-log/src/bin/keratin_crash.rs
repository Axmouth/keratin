use keratin_log::*;
use std::{path::PathBuf, process::Command};

// TODO
fn main() {
    let temp_dir = util::test_dir("keratin-crash");

    // child writer
    let exe = std::env::current_exe().unwrap();

    for _ in 0..20 {
        let _ = Command::new(&exe)
            .arg("child")
            .arg(temp_dir.root.to_str().unwrap())
            .spawn()
            .unwrap()
            .wait();
    }

    // final open & validate monotonicity
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let k = Keratin::open(&temp_dir.root, KeratinConfig::default())
            .await
            .unwrap();
        let r = k.reader();

        let mut last = 0;
        loop {
            match r.fetch(last) {
                Ok(Some(_)) => last += 1,
                _ => break,
            }
        }
        println!("Recovered {} messages", last);
    });
}
