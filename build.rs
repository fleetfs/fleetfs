use std::env;
use std::path::PathBuf;
use std::process::Command;
use std::fs::File;
use std::io::Write;

fn main() {
    let out = PathBuf::from(env::var("OUT_DIR").unwrap());
    Command::new("flatc")
        .args(&["--rust", "-o", out.to_str().unwrap(), "flatbuffers/messages.fbs"])
        .spawn()
        .expect("flatc failed");

    // HACK: workaround for issue #18810/#18849
    // See: https://hclarke.ca/generated-code-in-rust.html

    let wrapper_path = out.join("messages_generated.mod");
    let mod_path = out.join("messages_generated.rs");

    let mut f = File::create(wrapper_path).unwrap();

    write!(f, "#[path = \"{}\"]\npub mod messages_generated;\npub use messages_generated::generated;", mod_path.to_string_lossy()).expect("flatc mod failed");
}
