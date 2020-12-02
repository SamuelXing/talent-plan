extern crate kvs;
use kvs::KvStore;

use clap::{App, AppSettings, Arg, SubCommand};

// checked: https://docs.rs/structopt/0.3.21/structopt/index.html
fn main() {
    let mut kv_store = KvStore::new();
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .setting(AppSettings::DisableHelpSubcommand)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .setting(AppSettings::VersionlessSubcommands)
        .subcommand(
            SubCommand::with_name("set")
                .about("Set the value of a string key to a string")
                .arg(Arg::with_name("KEY").help("A string key").required(true))
                .arg(
                    Arg::with_name("VALUE")
                        .help("The string value of the key")
                        .required(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("get")
                .about("Get the string value of a given string key")
                .arg(Arg::with_name("KEY").help("A string key").required(true)),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("Remove a given key")
                .arg(Arg::with_name("KEY").help("A string key").required(true)),
        )
        .get_matches();

    match matches.subcommand() {
        ("set", Some(_matches)) => {
            let key = _matches.value_of("KEY").unwrap();
            let val = _matches.value_of("VALUE").unwrap();
            kv_store.set(key.to_string(), val.to_string());
        }
        ("get", Some(_matches)) => {
            let key = _matches.value_of("KEY").unwrap();
            match kv_store.get(key.to_string()) {
                None => println!(""),
                Some(val) => println!("{:?}", val),
            };
        }
        ("rm", Some(_matches)) => {
            let key = _matches.value_of("KEY").unwrap();
            kv_store.remove(key.to_string());
        }
        _ => unreachable!(),
    }
}
