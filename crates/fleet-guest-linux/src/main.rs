fn main() {
    let result = fleet_guest::main_args(std::env::args().skip(1));
    match result {
        Ok(code) => std::process::exit(code),
        Err(error) => {
            eprintln!("{error:#}");
            std::process::exit(1);
        }
    }
}
