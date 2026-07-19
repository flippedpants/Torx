use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "Torx")]
#[command(about = "A fast and minimal BitTorrent client", long_about = None)]
#[command(version)]
#[command(disable_version_flag = true)] 
pub struct Cli {
    /// Print version information
    #[arg(short = 'v', long = "version", action = clap::ArgAction::Version)]
    pub version: (),
}

pub fn parse_args() -> Cli {
    Cli::parse()
}
