//! What the machine is and how it is doing, read from macOS's own tools (`sysctl`, `pmset`, `df`).

use std::{collections::HashSet, path::Path, process::Command};

use crate::worker_data::{Hello, Status};

fn output(program: &str, args: &[&str]) -> Option<String> {
    let out = Command::new(program).args(args).output().ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn sysctl(name: &str) -> Option<String> {
    output("sysctl", &["-n", name])
}

/// The machine's description; it reads `system_profiler`, so it takes about a second.
pub fn hello() -> Hello {
    let gpu = output("system_profiler", &["SPDisplaysDataType"]).and_then(|text| {
        let field = |name: &str| {
            text.lines()
                .find_map(|l| l.trim().strip_prefix(name).map(|v| v.trim().to_string()))
        };
        let chip = field("Chipset Model:")?;
        let cores = field("Total Number of Cores:")
            .map(|c| format!(", {c} cores"))
            .unwrap_or_default();
        let metal = field("Metal Support:")
            .map(|m| format!(", {m}"))
            .unwrap_or_default();
        Some(format!("{chip}{cores}{metal}"))
    });
    let cores = sysctl("hw.ncpu")
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |n| n.get() as u32));
    let memory_bytes = sysctl("hw.memsize")
        .and_then(|n| n.parse().ok())
        .or_else(|| {
            std::fs::read_to_string("/proc/meminfo")
                .ok()?
                .lines()
                .find_map(|l| {
                    l.strip_prefix("MemTotal:")
                        .and_then(|s| s.split_whitespace().next()?.parse::<u64>().ok())
                        .map(|kb| kb * 1024)
                })
        })
        .unwrap_or(0);
    Hello {
        agent_version: env!("CARGO_PKG_VERSION").to_string(),

        hostname: output("hostname", &[]).unwrap_or_default(),
        os: output("sw_vers", &["-productVersion"])
            .map(|v| format!("macOS {v}"))
            .unwrap_or_else(|| std::env::consts::OS.to_string()),
        cpu: sysctl("machdep.cpu.brand_string").unwrap_or_default(),
        cores,
        memory_bytes,
        tags: {
            let mut tags = vec![
                format!("os:{}", std::env::consts::OS),
                format!("arch:{}", std::env::consts::ARCH),
            ];
            if gpu.as_deref().is_some_and(|g| g.contains("Metal")) {
                tags.push("gpu:metal".into());
            }
            tags
        },
        sleep_minutes: output("pmset", &["-g"]).and_then(|s| parse_sleep(&s)),
        cpu_slots: cores.saturating_sub(2).max(1),
        gpu,
    }
}

/// The ` sleep  N` line of `pmset -g`.
fn parse_sleep(text: &str) -> Option<u32> {
    text.lines().find_map(|l| {
        let mut words = l.split_whitespace();
        (words.next() == Some("sleep"))
            .then(|| words.next()?.parse().ok())
            .flatten()
    })
}

/// The first running process whose command line contains one of `patterns`, shortened for display.
pub fn running_process(
    patterns: &[String],
    own_groups: &HashSet<u32>,
    own_folder: &Path,
) -> Option<String> {
    if patterns.is_empty() {
        return None;
    }
    let list = output("ps", &["-axww", "-o", "pgid=,command="])?;
    find_external(&list, patterns, own_groups, &own_folder.to_string_lossy())
}

/// In `ps -o pgid=,command=` output, the first command matching a pattern that is not one of fleet's own jobs: each
/// job runs in its own process group (`own_groups`), and its programs usually run from its folder (`own_folder`).
fn find_external(
    list: &str,
    patterns: &[String],
    own_groups: &HashSet<u32>,
    own_folder: &str,
) -> Option<String> {
    list.lines()
        .filter_map(|line| {
            let (pgid, command) = line.trim().split_once(char::is_whitespace)?;
            Some((pgid.parse::<u32>().ok()?, command.trim()))
        })
        .filter(|(pgid, command)| !own_groups.contains(pgid) && !command.contains(own_folder))
        .find(|(_, command)| patterns.iter().any(|p| command.contains(p.as_str())))
        .map(|(_, command)| command.chars().take(80).collect())
}

pub fn status() -> Status {
    let batt = output("pmset", &["-g", "batt"]);
    let home = std::env::var("HOME").unwrap_or_else(|_| "/".into());
    Status {
        vm_capacity: None,
        instances: Vec::new(),
        load: sysctl("vm.loadavg").and_then(|s| parse_loadavg(&s)),
        on_ac_power: batt.as_deref().map(|b| b.contains("'AC Power'")),
        battery_percent: batt.as_deref().and_then(parse_battery_percent),
        disk_free_bytes: output("df", &["-k", &home]).and_then(|s| parse_df_free(&s)),
        running_jobs: Vec::new(),
        gpu_busy_external: None,
        uptime_seconds: sysctl("kern.boottime")
            .and_then(|s| parse_boottime(&s))
            .and_then(|boot| {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .ok()?
                    .as_secs();
                now.checked_sub(boot)
            }),
    }
}

/// `{ 7.20 29.61 34.19 }`
fn parse_loadavg(text: &str) -> Option<[f64; 3]> {
    let v: Vec<f64> = text
        .trim_matches(|c| c == '{' || c == '}' || char::is_whitespace(c))
        .split_whitespace()
        .filter_map(|x| x.parse().ok())
        .collect();
    (v.len() == 3).then(|| [v[0], v[1], v[2]])
}

/// `... -InternalBattery-0 (id=37814371)<tab>97%; discharging; ...`
fn parse_battery_percent(text: &str) -> Option<u8> {
    let at = text.find('%')?;
    let digits: String = text[..at]
        .chars()
        .rev()
        .take_while(char::is_ascii_digit)
        .collect();
    digits.chars().rev().collect::<String>().parse().ok()
}

/// The available column (in 1 KiB blocks) of `df -k`'s second line.
fn parse_df_free(text: &str) -> Option<u64> {
    let kib: u64 = text
        .lines()
        .nth(1)?
        .split_whitespace()
        .nth(3)?
        .parse()
        .ok()?;
    Some(kib * 1024)
}

/// `{ sec = 1759895600, usec = 412000 } Wed Oct  8 ...`
fn parse_boottime(text: &str) -> Option<u64> {
    let rest = text.split("sec = ").nth(1)?;
    rest.split(',').next()?.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fleets_own_jobs_are_not_outside_gpu_work() {
        let ps = "  101 /Users/me/.fleet/jobs/1-a/work/bin/dominion-ai-gpu train --output pilot
  202 python3 train.py --gpu
  303 /opt/runs/bin/dominion-ai-gpu train --output B
";
        let patterns = vec!["dominion-ai-gpu".to_string(), "train.py".to_string()];
        let folder = "/Users/me/.fleet/jobs";
        // A job's program in its folder, and one in its process group, are fleet's; the third is outside work.
        assert_eq!(
            find_external(ps, &patterns, &HashSet::from([202]), folder).as_deref(),
            Some("/opt/runs/bin/dominion-ai-gpu train --output B")
        );
        assert_eq!(
            find_external(ps, &patterns, &HashSet::from([202, 303]), folder),
            None
        );
        assert_eq!(
            find_external(ps, &patterns, &HashSet::new(), folder).as_deref(),
            Some("python3 train.py --gpu")
        );
    }

    #[test]
    fn the_tools_outputs_parse() {
        assert_eq!(
            parse_loadavg("{ 7.20 29.61 34.19 }"),
            Some([7.20, 29.61, 34.19])
        );
        let batt = "Now drawing from 'AC Power'\n -InternalBattery-0 (id=37814371)\t97%; charging; 0:40 remaining present: true";
        assert_eq!(parse_battery_percent(batt), Some(97));
        let df = "Filesystem 1024-blocks Used Available Capacity iused ifree %iused Mounted on\n\
                  /dev/disk3s5 970000000 900000000 26214400 98% 5 261 2% /System/Volumes/Data";
        assert_eq!(parse_df_free(df), Some(26_214_400 * 1024));
        assert_eq!(
            parse_sleep(" standby  1\n sleep                1 (sleep prevented by powerd)\n"),
            Some(1)
        );
        assert_eq!(
            parse_boottime("{ sec = 1759895600, usec = 412000 } Wed Oct  8"),
            Some(1_759_895_600)
        );
    }
}
