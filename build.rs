extern crate bindgen;
extern crate pkg_config;

use std::env;
use std::path::PathBuf;

fn main() {
    let yottadb = pkg_config::probe_library("yottadb").unwrap();
    let mut include_path = String::from("-I");
    for path in yottadb.include_paths {
        let s = path.to_str().unwrap();
        include_path.push_str(s);
    }
    println!("cargo:include-path={}", include_path);
    let mut library_path = String::from("");
    for path in yottadb.link_paths {
        let s = path.to_str().unwrap();
        library_path.push_str(s);
    }
    println!("cargo:rust-link-search={}", library_path);

    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.
    let mut bindings = bindgen::Builder::default()
    // The input header we would like to generate
    // bindings for.
        .header("wrapper.h")
        .clang_arg(include_path);
    // Add the path to the Distrib inc. directory
    let build_include_files = vec!("sr_linux", "sr_unix_gnp", "sr_unix_gnp", "sr_unix_cm", "sr_unix", "sr_port_cm", "sr_port");
    for path in build_include_files {
        bindings = bindings.clang_arg(format!("-I/home/chathaway/p/YottaDB/{}", path));
    }
    bindings = bindings.clang_arg("-I/home/chathaway/p/ydb-ng");
    let builder = bindings
    // Finish the builder and generate the bindings.
        .generate()
    // Unwrap the Result and panic on failure.
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    builder
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
