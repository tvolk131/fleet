fn main() {
    // Homebrew's slp/krun tap installs libkrun here.
    println!("cargo:rustc-link-search=native=/opt/homebrew/lib");
    println!("cargo:rustc-link-lib=dylib=krun");
}
