// mod cli;

use quartz_common::{RED_COLOR, GREEN_COLOR};
use quartz_common::BuildInfo;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("QuartzDB");
    Ok(())
    // quartz_main().await
    // tokio::runtime::Builder::new_multi_thread()
    //     .enable_all()
    //     .build()
    //     .unwrap()
    //     .block_on(quartz_main())
}

// QuartzDB main entry point.
// async fn quartz_main() -> anyhow::Result<()> {
//     let about_text = about_text();
//     let build_info = BuildInfo::get();
//     let version_text = format!(
//         "{} ({} {})",
//         build_info.version, build_info.commit_short_hash, build_info.build_date
//     );

//     let application = build_cli_commands().about(about_text).version(version_text);
//     let matches = application.get_matches();
//     let with_ansi_color = !matches.get_flag("no-color");

//     let command = match CliCommand::parse_cli_args(matches) {
//         Ok(command) => command,
//         Err(err) => {
//             eprintln!("Failed to parse command arguments: {err:?}");
//             std::process::exit(1);
//         }
//     };

//     setup_logging_and_tracing(command.default_log_level(), with_ansi_color, build_info)?;
//     let command_result = command.execute().await;
//     let return_code = match command_result {
//         Ok(_) => {
//             println!("{} [SUCCESS]\n", "✔".color(GREEN_COLOR), err);
//             0
//         },
//         Err(err) => {
//             eprintln!("{} [FAILED]: {:?}\n", "✘".color(RED_COLOR), err);
//             1
//         }
//     };
//     std::process::exit(return_code)
// }


/// Return the about text.
fn about_text() -> String {
   String::from(
        "An experiemental time series database.",
    )
}

// fn setup_logging_and_tracing(
//     level: Level,
//     ansi: bool,
//     build_info: &BuildInfo,
// ) -> anyhow::Result<()> {
//     #[cfg(feature = "tokio-console")]
//     {
//         if std::env::var_os(quickwit_cli::QW_ENABLE_TOKIO_CONSOLE_ENV_KEY).is_some() {
//             console_subscriber::init();
//             return Ok(());
//         }
//     }
//     let env_filter = env::var("RUST_LOG")
//         .map(|_| EnvFilter::from_default_env())
//         .or_else(|_| EnvFilter::try_new(format!("quartzdb={level}")))
//         .context("Failed to set up tracing env filter.")?;
//     global::set_text_map_propagator(TraceContextPropagator::new());
//     let registry = tracing_subscriber::registry().with(env_filter);
//     let event_format = tracing_subscriber::fmt::format()
//         .with_target(true)
//         .with_ansi(ansi)
//         .with_timer(
//             // We do not rely on the Rfc3339 implementation, because it has a nanosecond precision.
//             // See discussion here: https://github.com/time-rs/time/discussions/418
//             UtcTime::new(
//                 time::format_description::parse(
//                     "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z",
//                 )
//                 .expect("Time format invalid."),
//             ),
//         );

//     registry
//         .with(tracing_subscriber::fmt::layer().event_format(event_format))
//         .try_init()
//         .context("Failed to set up tracing.")?;

//     Ok(())
// }
