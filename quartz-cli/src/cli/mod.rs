mod handlers;
mod utils;

use std::{env, net::SocketAddr};
use std::path::PathBuf;

use anyhow::Ok;
use clap::{Parser, Subcommand};
use config::Config;

use crate::common::config::QuartzConfig;

const CONFIG_FILE_BASE_NAME: &str = "quartzdb";
const CONFIG_ENV_PREFIX: &str = "QUARTZDB";


#[derive(Subcommand)]
pub enum Commands {
    Run {
        #[arg(short, long)]
        address: Option<SocketAddr>,
    },
    Ingest {
        #[arg(short, long)]
        file: PathBuf 
    },
    Query {
        #[arg(short, long)]
        query: String 
    },
    Index {
        #[command(subcommand)]
        action: IndexSubcommands,
    },
}

#[derive(Subcommand)]
pub enum IndexSubcommands {
    List,
    Create { 
        #[arg(short, long)]
        file: PathBuf 
    },
    Delete {
        #[arg(short, long)]
        name: String 
    },
}

#[derive(Parser)]
#[command(
    name = "quartzdb",
    version, 
    about= "A high-performance time-series database.", 
    long_about = "QuartzDB is a high-performance time-series database built in Rust.",
    arg_required_else_help = true,
)]
pub struct CliApp {
    /// config file
    #[arg(short, long)]
    config: Option<PathBuf>,

    // target server endpoint
    #[arg(short, long)]
    endpoint: Option<String>,

    #[command(subcommand)]
    command: Option<Commands>,
}

pub async fn run_cli() -> anyhow::Result<()> {
    let CliApp{config: overriden_config, endpoint: overriden_endpoint, command} = CliApp::parse();
    let mut config = load_config(overriden_config, overriden_endpoint)?;
    // println!("config: {:?}", config);

    match command {
        Some(Commands::Run {address}) => {
            if let Some(overriden_address) = address {
                config.address = overriden_address;
            }
            handlers::handle_run(config).await?;
        }
        Some(Commands::Ingest { file }) => {
            handlers::handle_ingest(file).await?
        }
        Some(Commands::Query { query }) => {
            handlers::handle_query(&query).await?
        }
        Some(Commands::Index { action }) => {
            match action {
                IndexSubcommands::List => {
                    handlers::handle_index_list(config).await?
                }
                IndexSubcommands::Create { file } => {
                    handlers::handle_index_create(config, file).await?
                }
                IndexSubcommands::Delete { name } => {
                    handlers::handle_index_delete(config, &name).await?
                }
            }
        },
        _ => {
            // Handle no command specified
        }
    }
    Ok(())
}

fn load_config(overriden_config: Option<PathBuf>, overriden_endpoint: Option<String>) -> anyhow::Result<QuartzConfig> {
    // config file from install dir (optional)
    let exe_path = env::current_exe().expect("Failed to find executable path");
    let install_dir = exe_path.parent().expect("Failed to get executable directory");
    let installed_config_file_path = install_dir.join(CONFIG_FILE_BASE_NAME);

    // config file from current working directory or specified by flag (optional)
    let current_dir = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let override_config_file_path = overriden_config.unwrap_or_else(|| current_dir.join(CONFIG_FILE_BASE_NAME));
  
    let settings = Config::builder()
        .add_source(config::Config::try_from(&QuartzConfig::default())?)
        .add_source(config::File::from(installed_config_file_path).required(false))
        .add_source(config::File::from(override_config_file_path).required(false))
        .add_source(config::Environment::with_prefix(CONFIG_ENV_PREFIX).separator("_"))
        .build()?;

    let mut config = settings.try_deserialize::<QuartzConfig>()?;
    if let Some(endpoint) = overriden_endpoint {
        config.endpoint = endpoint
    }
    Ok(config)
}
