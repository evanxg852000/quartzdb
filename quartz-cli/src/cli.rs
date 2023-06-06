use clap::{Command, Arg};



pub fn build_cli_commands() -> Command {
    Command::new("Quartz")
        .arg(
            Arg::new("no-color")
                .long("no-color")
                .help(
                    "Disable ANSI terminal codes (colors, etc...) being injected into the logging \
                     output",
                )
                .env("NO_COLOR")
                .global(true)
                .action(ArgAction::SetTrue),
        )
        .arg(arg!(-y --"yes" "Assume 'yes' as an answer to all prompts and run non-interactively.")
            .global(true)
            .required(false)
        )
        .subcommand(build_run_command().display_order(1))
        .subcommand(build_tool_command().display_order(2))
        // .subcommand(build_source_command().display_order(3))
        // .subcommand(build_split_command().display_order(4))
        // .subcommand(build_tool_command().display_order(5))
        .arg_required_else_help(true)
        .disable_help_subcommand(true)
        .subcommand_required(true)
}

#[derive(Debug, PartialEq)]
pub enum CliCommand {
    Run(RunCliCommand),
    Tool(ToolCliCommand),
}

impl CliCommand {
    pub fn default_log_level(&self) -> Level {
        match self {
            CliCommand::Run(_) => Level::INFO,
            CliCommand::Tool(_) => Level::ERROR,
        }
    }

    pub fn parse_cli_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let (command, submatches) = matches
            .remove_subcommand()
            .context("Failed to parse command.")?;
        match command.as_str() {
            "run" => RunCliCommand::parse_cli_args(submatches).map(CliCommand::Run),
            "tool" => ToolCliCommand::parse_cli_args(submatches).map(CliCommand::Tool),
            _ => bail!("Unknown command `{command}`."),
        }
    }

    pub async fn execute(self) -> anyhow::Result<()> {
        match self {
            CliCommand::Run(command) => subcommand.execute().await,
            CliCommand::Tool(subcommand) => subcommand.execute().await,
        }
    }
}
