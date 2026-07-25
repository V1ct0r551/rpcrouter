use std::{env, net::SocketAddr};

use anyhow::{Context, Result, bail};
use rpcrouter::mock_upstream::{MockBehavior, MockController, router};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("rpcrouter=info")),
        )
        .init();
    let (listen, behavior) = parse_args()?;
    let listener = tokio::net::TcpListener::bind(listen).await?;
    info!(%listen, ?behavior, "mock upstream listening");
    axum::serve(listener, router(MockController::new(behavior))).await?;
    Ok(())
}

fn parse_args() -> Result<(SocketAddr, MockBehavior)> {
    let mut listen: SocketAddr = "127.0.0.1:9545".parse().expect("valid default address");
    let mut behavior = MockBehavior::default();
    let mut args = env::args().skip(1);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--listen" => {
                listen = next(&mut args, "--listen")?
                    .parse()
                    .context("invalid listen")?
            }
            "--chain-id" | "--wrong-chain-id" => {
                behavior.chain_id = parse_u64(&next(&mut args, &argument)?)?
            }
            "--block-number" => behavior.block_number = parse_u64(&next(&mut args, &argument)?)?,
            "--block-lag" => behavior.block_lag = parse_u64(&next(&mut args, &argument)?)?,
            "--rate-limit-after" => {
                behavior.rate_limit_after = Some(parse_u64(&next(&mut args, &argument)?)?)
            }
            "--retry-after-seconds" => {
                behavior.retry_after_seconds = Some(parse_u64(&next(&mut args, &argument)?)?)
            }
            "--rate-limit-message" => {
                behavior.rate_limit_message = Some(next(&mut args, &argument)?)
            }
            "--delay-ms" => behavior.delay_ms = parse_u64(&next(&mut args, &argument)?)?,
            "--status-5xx" => {
                behavior.status_5xx = Some(
                    next(&mut args, &argument)?
                        .parse()
                        .context("invalid status")?,
                )
            }
            "--html" => behavior.html = true,
            "--execution-reverted" => behavior.execution_reverted = true,
            "--help" => {
                println!(
                    "mock-upstream [--listen ADDR] [--chain-id ID|--wrong-chain-id ID] \
                     [--block-number N] [--block-lag N] [--rate-limit-after N] \
                     [--retry-after-seconds N] [--rate-limit-message TEXT] [--html] \
                     [--delay-ms N] [--status-5xx CODE] [--execution-reverted]"
                );
                std::process::exit(0);
            }
            unknown => bail!("unknown argument {unknown}"),
        }
    }
    Ok((listen, behavior))
}

fn next(args: &mut impl Iterator<Item = String>, option: &str) -> Result<String> {
    args.next()
        .with_context(|| format!("missing value for {option}"))
}

fn parse_u64(value: &str) -> Result<u64> {
    if let Some(hex) = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
    {
        u64::from_str_radix(hex, 16).context("invalid hexadecimal integer")
    } else {
        value.parse().context("invalid integer")
    }
}
