// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Side-effect-free metadata from the same definitions linked into `pond`.

use clap::CommandFactory;
use serde_json::{Value, json};
use std::collections::BTreeMap;

fn commands(command: &clap::Command, path: &str, output: &mut BTreeMap<String, Value>) {
    let mut arguments: Vec<_> = command
        .get_arguments()
        .filter(|arg| !arg.is_hide_set())
        .collect();
    arguments.sort_by_key(|arg| arg.get_id().as_str());
    let arguments: Vec<_> = arguments.into_iter().map(|arg| json!({
        "id": arg.get_id().as_str(),
        "short": arg.get_short().map(|c| c.to_string()),
        "long": arg.get_long(),
        "aliases": arg.get_all_aliases().unwrap_or_default(),
        "help": arg.get_long_help().or_else(|| arg.get_help()).map(ToString::to_string),
        "required": arg.is_required_set(),
        "global": arg.is_global_set(),
        "positional": arg.get_index(),
        "action": format!("{:?}", arg.get_action()),
        "num_args": arg.get_num_args().map(|range| range.to_string()),
        "defaults": arg.get_default_values().iter().map(|value| value.to_string_lossy()).collect::<Vec<_>>(),
        "values": arg.get_possible_values().into_iter().filter(|value| !value.is_hide_set()).map(|value| value.get_name().to_string()).collect::<Vec<_>>(),
        "conflicts": command.get_arg_conflicts_with(arg).iter().map(|other| other.get_id().as_str()).collect::<Vec<_>>(),
    })).collect();
    _ = output.insert(if path.is_empty() { "cli:global-options".into() } else { format!("cli:{path}") }, json!({
        "path": path,
        "about": command.get_long_about().or_else(|| command.get_about()).map(ToString::to_string),
        "aliases": command.get_all_aliases().collect::<Vec<_>>(),
        "arguments": arguments,
    }));
    for child in command
        .get_subcommands()
        .filter(|child| !child.is_hide_set() && child.get_name() != "help")
    {
        let child_path = if path.is_empty() {
            child.get_name().to_string()
        } else {
            format!("{path}/{}", child.get_name())
        };
        commands(child, &child_path, output);
    }
}

#[must_use]
pub fn inventory() -> Value {
    let mut command = crate::cli::Cli::command();
    command.build();
    let mut cli = BTreeMap::new();
    commands(&command, "", &mut cli);
    let factories: BTreeMap<_, _> = provider::FactoryRegistry::list_factories().iter().map(|factory| (
        format!("factory:{}", factory.name), json!({
            "description": factory.description,
            "directory": factory.create_directory.is_some(),
            "file": factory.create_file.is_some(),
            "initialize": factory.initialize.is_some(),
            "execute": factory.execute.is_some(),
            "table_transform": factory.apply_table_transform.is_some(),
            "raw_validation": factory.validate_raw_config.is_some(),
            "post_commit_access": factory.post_commit_access.map(|access| format!("{access:?}")),
        })
    )).collect();
    let formats: Vec<_> = provider::FormatRegistry::list_providers()
        .into_iter()
        .map(|name| format!("format:{name}"))
        .collect();
    let mut formats = formats;
    formats.sort();
    let mut schemes: Vec<_> = provider::registry::SchemeRegistry::builtin_schemes()
        .iter()
        .map(|name| format!("scheme:{name}"))
        .chain(
            provider::Url::compression_schemes()
                .iter()
                .map(|name| format!("compression:{name}")),
        )
        .collect();
    schemes.sort();
    let mut schemas = provider::documentation_schemas();
    schemas.extend(sitegen::documentation_schemas());
    fn schema<T: schemars::JsonSchema>() -> schemars::Schema {
        schemars::generate::SchemaSettings::draft2020_12()
            .for_deserialize()
            .into_generator()
            .into_root_schema_for::<T>()
    }
    schemas.extend([
        ("factory:hydrovu", schema::<hydrovu::HydroVuConfig>()),
        ("factory:git-ingest", schema::<gitpond::GitIngestConfig>()),
        ("factory:exec", schema::<exec_factory::config::ExecConfig>()),
    ]);
    json!({
        "contract": 1,
        "cli": cli,
        "factories": factories,
        "formats": formats,
        "schemes": schemes,
        "shortcodes": sitegen::documentation_shortcodes().iter().map(|name| format!("shortcode:{name}")).collect::<Vec<_>>(),
        "schemas": schemas,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contract<T: serde::de::DeserializeOwned>(id: &str, value: Value, accepted: bool) {
        let metadata = inventory();
        let validator = jsonschema::validator_for(&metadata["schemas"][id]).unwrap();
        assert_eq!(validator.is_valid(&value), accepted, "{id}: {value}");
        assert_eq!(serde_json::from_value::<T>(value).is_ok(), accepted, "{id}");
    }

    #[test]
    fn deserialization_schemas_preserve_tags_defaults_and_unknown_field_rules() {
        use provider::factory::synthetic_timeseries::SyntheticTimeseriesConfig;
        let value = json!({
            "start": "2024-01-01T00:00:00Z", "end": "2024-01-01T03:00:00Z",
            "interval": "1h", "points": [{"name": "temperature", "components": [{"type": "line"}]}]
        });
        contract::<SyntheticTimeseriesConfig>("factory:synthetic-timeseries", value.clone(), true);
        let parsed: SyntheticTimeseriesConfig = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(
            inventory()["schemas"]["factory:synthetic-timeseries"]["properties"]["time_column"]["default"],
            parsed.time_column
        );
        let mut invalid = value.clone();
        invalid["unexpected"] = json!(true);
        contract::<SyntheticTimeseriesConfig>("factory:synthetic-timeseries", invalid, false);
        let mut invalid = value.clone();
        invalid["points"][0]["components"][0] = json!({"type": "sine"});
        contract::<SyntheticTimeseriesConfig>("factory:synthetic-timeseries", invalid, false);
        let mut permissive = value;
        permissive["points"][0]["components"][0]["extra"] = json!(true);
        contract::<SyntheticTimeseriesConfig>("factory:synthetic-timeseries", permissive, true);
        contract::<provider::CsvOptions>("format:csv", json!({"escape": null, "unknown": 1}), true);
        contract::<provider::CsvOptions>("format:csv", json!({"has_header": "wrong"}), false);
    }

    #[test]
    fn nested_and_renamed_configuration_shapes_match_serde() {
        let value = json!({
            "site": {"title": "Schema fixture", "base_url": "/sample/"},
            "static": [{"pattern": "/files/*"}],
            "footer": null,
            "sidebar": ["Home", {"label": "Reference", "children": [{"label": "Fields", "href": "/fields.html"}]}],
            "routes": [{"name": "home", "type": "static", "slug": "", "page": "/index.md"}]
        });
        contract::<sitegen::SiteConfig>("factory:sitegen", value.clone(), true);
        let mut invalid = value;
        invalid["routes"][0]["type"] = json!("not-a-route");
        contract::<sitegen::SiteConfig>("factory:sitegen", invalid, false);
        contract::<gitpond::GitIngestConfig>(
            "factory:git-ingest",
            json!({"url": "/local/repo", "ref": "main", "prefix": null}),
            true,
        );
        contract::<gitpond::GitIngestConfig>(
            "factory:git-ingest",
            json!({"url": "/local/repo", "git_ref": "main"}),
            false,
        );
    }

    #[test]
    fn exports_shared_cli_and_linked_registries_without_running_factories() {
        let metadata = inventory();
        assert_eq!(metadata, inventory());
        for id in [
            "factory:hydrovu",
            "factory:git-ingest",
            "factory:exec",
            "factory:sitegen",
        ] {
            assert!(metadata["factories"].get(id).is_some(), "{id}");
        }
        assert!(metadata["cli"].get("cli:control").is_none());
        assert!(metadata["cli"].get("cli:capsule/recipe/inspect").is_some());
        assert!(
            metadata["shortcodes"]
                .as_array()
                .unwrap()
                .iter()
                .any(|id| id == "shortcode:source")
        );
        let maintain = &metadata["cli"]["cli:maintain"]["arguments"];
        assert!(
            maintain
                .as_array()
                .unwrap()
                .iter()
                .any(|arg| arg["long"] == "collapse-versions" && arg["defaults"] == json!(["0"]))
        );
    }
}
