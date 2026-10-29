// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use clap::Parser;
use cmd::cli::*;
use common::ShipContext;
use panic_alloc::PanicOnLargeAlloc;
use std::time::Instant;

mod commands;
mod common;
mod panic_alloc;

// External modules
use exec_factory as _;
use gitpond as _;
use hydrovu as _;
use sitegen as _;

#[global_allocator]
static PEAK_ALLOC: PanicOnLargeAlloc = PanicOnLargeAlloc::new(3000);

#[allow(clippy::print_stdout)]
fn print_handler(output: &str) {
    print!("{}", output);
}

#[tokio::main]
async fn main() -> Result<()> {
    // Capture original command line arguments before clap parsing for transaction metadata
    let original_args: Vec<String> = std::env::args().collect();

    // Initialize env_logger from RUST_LOG; default to `info` when RUST_LOG is not set
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    log::debug!("Main function started");
    let cli = Cli::parse();
    log::debug!("CLI parsed successfully");

    // Parse hostmount specs
    let mount_specs: Vec<tinyfs::hostmount::MountSpec> = cli
        .hostmount
        .iter()
        .map(|s| tinyfs::hostmount::MountSpec::parse(s))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("Invalid --hostmount: {}", e))?;

    // Create the ship context
    let ship_context = ShipContext::new(
        cli.pond.as_ref(),
        cli.directory.as_ref(),
        mount_specs,
        original_args.clone(),
    );

    // Capture the path/args of the `Run` subcommand (if any) so we can
    // emit a structured "Run summary" log line at process exit naming
    // the factory and elapsed time.  See commands::run_summary.
    let run_meta: Option<(String, Vec<String>)> = if let Commands::Run { path, args } = &cli.command
    {
        Some((path.clone(), args.clone()))
    } else {
        None
    };
    let started = Instant::now();

    let result = match cli.command {
        Commands::Init { birthplace } => {
            // Init command creates a new empty pond.
            commands::init_command(&ship_context, &birthplace).await
        }
        Commands::Recover => {
            // Recover command works with potentially damaged pond, handle specially
            commands::recover_command(&ship_context).await
        }
        Commands::Maintain {
            compact,
            collapse_versions,
            prune,
            keep_txns,
            allow_no_remote,
            dry_run,
        } => {
            commands::maintain_command(
                &ship_context,
                compact,
                collapse_versions,
                prune,
                keep_txns,
                allow_no_remote,
                dry_run,
            )
            .await
        }

        // Read-only commands that use ShipContext for consistency
        Commands::Show { mode } => {
            commands::show_command(&ship_context, &mode, print_handler).await
        }
        Commands::Log {
            limit,
            txn_seq,
            incomplete,
        } => {
            let control_mode = if let Some(seq) = txn_seq {
                commands::control::ControlMode::Detail { txn_seq: seq }
            } else if incomplete {
                commands::control::ControlMode::Incomplete
            } else {
                commands::control::ControlMode::Recent { limit }
            };
            commands::control_command(&ship_context, control_mode).await
        }
        Commands::Push { name } => commands::push_command(&ship_context, name).await,
        Commands::Pull {
            name,
            rebuild_graft,
        } => {
            if rebuild_graft {
                commands::pull_command_with_rebuild(&ship_context, name, true).await
            } else {
                commands::pull_command(&ship_context, name).await
            }
        }
        Commands::Restore { name, url, options } => {
            commands::restore_command(
                &ship_context,
                &name,
                &url,
                options.region,
                options.access_key_id,
                options.secret_access_key,
                options.endpoint,
                options.allow_http,
            )
            .await
        }
        Commands::Verify { name, exact } => {
            commands::verify_command(&ship_context, name, exact).await
        }
        Commands::Status => commands::status_command(&ship_context).await,
        Commands::Limits { format } => commands::limits_command(&ship_context, &format).await,
        Commands::Tlog { command } => match command {
            TlogCommand::Show => commands::tlog_show_command(&ship_context).await,
            TlogCommand::Verify => commands::tlog_verify_command(&ship_context).await,
        },
        Commands::Fsck { quick, verbose } => {
            commands::fsck_command(&ship_context, quick, verbose).await
        }
        Commands::RebuildControl { force } => {
            commands::rebuild_control_command(&ship_context, force).await
        }
        Commands::Remote { command } => match command {
            RemoteCommand::Add {
                name,
                url,
                path,
                options,
            } => {
                commands::add_remote_command(
                    &ship_context,
                    &name,
                    &url,
                    &path,
                    options.region,
                    options.access_key_id,
                    options.secret_access_key,
                    options.endpoint,
                    options.allow_http,
                    options.overwrite,
                )
                .await
            }
            RemoteCommand::Remove { name, purge } => {
                commands::remove_remote_command(&ship_context, &name, purge).await
            }
            RemoteCommand::List => commands::list_remotes_command(&ship_context, None).await,
        },
        Commands::Backup { command } => match command {
            BackupCommand::Add {
                name,
                url,
                bidirectional,
                options,
            } => {
                commands::add_backup_command(
                    &ship_context,
                    &name,
                    &url,
                    bidirectional,
                    options.region,
                    options.access_key_id,
                    options.secret_access_key,
                    options.endpoint,
                    options.allow_http,
                    options.overwrite,
                )
                .await
            }
            BackupCommand::Remove { name, purge } => {
                commands::remove_remote_command(&ship_context, &name, purge).await
            }
            BackupCommand::CleanupUploads {
                name,
                older_than_seconds,
            } => {
                commands::cleanup_backup_uploads_command(&ship_context, &name, older_than_seconds)
                    .await
            }
            BackupCommand::PublishConsolidated { name } => {
                commands::publish_consolidated_packs_command(&ship_context, &name).await
            }
            BackupCommand::List => {
                commands::list_remotes_command(
                    &ship_context,
                    Some(commands::RemoteListFilter::BackupsOnly),
                )
                .await
            }
        },
        Commands::Capsule { command } => match command {
            CapsuleCommand::Publish { name } => {
                commands::capsule_publish_command(&ship_context, &name).await
            }
            CapsuleCommand::Recipe { command } => {
                let (name, action) = match command {
                    CapsuleRecipeCommand::Publish { name } => {
                        (name, commands::RecoveryRecipeAction::Publish)
                    }
                    CapsuleRecipeCommand::Inspect { name } => {
                        (name, commands::RecoveryRecipeAction::Inspect)
                    }
                };
                commands::capsule_recipe_command(&ship_context, &name, action).await
            }
            CapsuleCommand::Inspect { path } | CapsuleCommand::Verify { path } => {
                commands::capsule_inspect_command(&path)
            }
            CapsuleCommand::Import { path, birthplace } => {
                let target = ship_context.resolve_pond_path()?;
                commands::capsule_import_command(&path, &target, &birthplace).await
            }
            CapsuleCommand::Activate => {
                let target = ship_context.resolve_pond_path()?;
                commands::capsule_activate_command(&target).await
            }
        },
        Commands::Freeze { command } => match command {
            FreezeCommand::Enable { reason } => {
                commands::freeze_writes_command(&ship_context, reason).await
            }
            FreezeCommand::Status => commands::freeze_status_command(&ship_context).await,
            FreezeCommand::Disable => commands::unfreeze_writes_command(&ship_context).await,
        },
        Commands::Config { command } => {
            let control_mode = match command {
                Some(ConfigCommand::Set { key, value }) => {
                    commands::control::ControlMode::SetConfig { key, value }
                }
                None => commands::control::ControlMode::ShowConfig,
            };
            commands::control_command(&ship_context, control_mode).await
        }
        Commands::Control { command } => {
            let control_mode = match command {
                ControlCommand::Recent { limit } => {
                    commands::control::ControlMode::Recent { limit }
                }
                ControlCommand::Detail { txn_seq } => {
                    commands::control::ControlMode::Detail { txn_seq }
                }
                ControlCommand::Incomplete => commands::control::ControlMode::Incomplete,
                ControlCommand::ShowConfig => commands::control::ControlMode::ShowConfig,
                ControlCommand::SetConfig { key, value } => {
                    commands::control::ControlMode::SetConfig { key, value }
                }
                ControlCommand::Prune {
                    keep_txns,
                    dry_run,
                    allow_no_remote,
                } => commands::control::ControlMode::Prune {
                    keep_txns,
                    dry_run,
                    allow_no_remote,
                },
            };
            commands::control_command(&ship_context, control_mode).await
        }
        Commands::List { pattern, all, long } => {
            commands::list_command(&ship_context, &pattern, all, long, print_handler).await
        }
        Commands::Describe { pattern } => {
            commands::describe_command(&ship_context, &pattern, print_handler).await
        }
        Commands::Cat {
            path,
            format,
            time_start,
            time_end,
            query,
            explain,
        } => {
            commands::cat_command(
                &ship_context,
                &path,
                &format,
                None,
                time_start,
                time_end,
                query.as_deref(),
                explain,
            )
            .await
        }

        // Write commands that use scoped transactions
        Commands::Copy {
            sources,
            dest,
            strip_prefix,
        } => {
            let options = commands::CopyOptions { strip_prefix };
            commands::copy_command(&ship_context, &sources, &dest, &options).await
        }
        Commands::Mkdir { path, parents } => {
            commands::mkdir_command(&ship_context, &path, parents).await
        }
        Commands::Mknod {
            factory_type,
            path,
            config_path,
            overwrite,
        } => {
            commands::mknod_command(&ship_context, &factory_type, &path, &config_path, overwrite)
                .await
        }
        Commands::ListFactories => commands::list_factories_command().await,
        Commands::Apply { files } => commands::apply_command(&ship_context, &files).await,
        Commands::Run { path, args } => commands::run_command(&ship_context, &path, args).await,
        Commands::Export {
            pattern,
            dir,
            temporal,
            start_time,
            end_time,
            rebuild,
        } => {
            commands::export_command(
                &ship_context,
                &pattern,
                &dir.to_string_lossy(),
                &temporal,
                start_time,
                end_time,
                rebuild,
            )
            .await
        }
        Commands::Emergency(cmd) => match cmd {
            EmergencyCommand::EraseBucket {
                url,
                endpoint,
                region,
                access_key,
                secret_key,
                allow_http,
                dangerous,
            } => {
                commands::emergency::erase_bucket(
                    &url,
                    &endpoint,
                    &region,
                    &access_key,
                    &secret_key,
                    allow_http,
                    dangerous,
                )
                .await
            }
        },
    };

    // Log peak memory usage
    let peak_mem = PEAK_ALLOC.peak_usage_as_mb();
    log::info!("Peak memory usage: {:.2} MB", peak_mem);

    // For `pond run`, also emit a structured one-line summary that
    // names the resolved factory.  This is what selfmon/observability
    // grep for when correlating ticks with their factories: the bare
    // CLI args ("/system/etc/measure/septic-prod push") don't tell
    // you which factory ran.
    if let Some((path, args)) = run_meta {
        let factory = commands::run_summary::take_factory().unwrap_or_else(|| "-".to_string());
        let elapsed = started.elapsed();
        let outcome = if result.is_ok() { "ok" } else { "err" };
        log::info!(
            "Run summary  path={}  factory={}  args={:?}  elapsed_s={:.3}  peak_mem_mb={:.2}  outcome={}",
            path,
            factory,
            args,
            elapsed.as_secs_f64(),
            peak_mem,
            outcome,
        );
    }

    // Print large allocations report
    PEAK_ALLOC.print_large_allocs();

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_target_format_recipe_commands_at_existing_paths() {
        let capsule = Cli::try_parse_from(["pond", "capsule", "publish", "backup"]).unwrap();
        assert!(matches!(
            capsule.command,
            Commands::Capsule {
                command: CapsuleCommand::Publish { ref name }
            } if name == "backup"
        ));

        let publish =
            Cli::try_parse_from(["pond", "capsule", "recipe", "publish", "backup"]).unwrap();
        assert!(matches!(
            publish.command,
            Commands::Capsule {
                command: CapsuleCommand::Recipe {
                    command: CapsuleRecipeCommand::Publish { ref name }
                }
            } if name == "backup"
        ));

        let inspect =
            Cli::try_parse_from(["pond", "capsule", "recipe", "inspect", "backup"]).unwrap();
        assert!(matches!(
            inspect.command,
            Commands::Capsule {
                command: CapsuleCommand::Recipe {
                    command: CapsuleRecipeCommand::Inspect { ref name }
                }
            } if name == "backup"
        ));
    }

    #[test]
    fn rejects_removed_legacy_recipe_namespace() {
        for namespace in ["legacy-migration", "legacy-migration-v2"] {
            assert!(
                Cli::try_parse_from(["pond", "capsule", "recipe", namespace, "publish", "backup",])
                    .is_err()
            );
        }
    }

    #[test]
    fn parses_explicit_backup_upload_cleanup() {
        let cleanup = Cli::try_parse_from([
            "pond",
            "backup",
            "cleanup-uploads",
            "origin",
            "--older-than-seconds",
            "3600",
        ])
        .expect("cleanup syntax");
        assert!(matches!(
            cleanup.command,
            Commands::Backup {
                command: BackupCommand::CleanupUploads {
                    ref name,
                    older_than_seconds: 3600,
                }
            } if name == "origin"
        ));
    }

    #[test]
    fn parses_explicit_consolidated_pack_publication() {
        let publish = Cli::try_parse_from(["pond", "backup", "publish-consolidated", "origin"])
            .expect("consolidated publication syntax");
        assert!(matches!(
            publish.command,
            Commands::Backup {
                command: BackupCommand::PublishConsolidated { ref name }
            } if name == "origin"
        ));
    }

    #[test]
    fn parses_capsule_import_without_experimental_gate_and_activation() {
        let import = Cli::try_parse_from([
            "pond",
            "--pond",
            "restored",
            "capsule",
            "import",
            "capsule",
            "--birthplace",
            "recovery-host",
        ])
        .expect("production import syntax");
        assert!(matches!(
            import.command,
            Commands::Capsule {
                command: CapsuleCommand::Import { .. }
            }
        ));
        assert!(
            Cli::try_parse_from([
                "pond",
                "--pond",
                "restored",
                "capsule",
                "import",
                "capsule",
                "--birthplace",
                "recovery-host",
                "--experimental",
            ])
            .is_err()
        );

        let activate = Cli::try_parse_from(["pond", "--pond", "restored", "capsule", "activate"])
            .expect("activation syntax");
        assert!(matches!(
            activate.command,
            Commands::Capsule {
                command: CapsuleCommand::Activate
            }
        ));
    }
}
