//! Alternative speed limits ("turtle mode"): a second pair of rate limits
//! that apply on a schedule, or when the user switches to them.
//!
//! The config file's `alt-speed-download`, `alt-speed-upload` and
//! `alt-speed-schedule` settings hold the limits and the schedule (see
//! `config`). Which mode is on is a pure function of the clock, the
//! schedule and a manual override (`SpeedControl`), so it is tested without
//! one: the manager asks it, with the time, and applies the limits it gives.
//!
//! The schedule is a list of rules, each a span of the day on some weekdays.
//! A span whose `to` is not after its `from` runs past midnight into the
//! next day, and belongs to the weekday it starts on: `days = ["fri"], from
//! = "22:00", to = "06:00"` is Friday 22:00 until Saturday 06:00. Equal
//! `from` and `to` make a span of 24 hours.
//!
//! Switching by hand holds until the schedule next changes its mind, as in
//! Transmission: turn the mode on in the middle of the night and it stays on
//! until the schedule would have turned it off or on (a boundary), and then
//! the schedule is in charge again. With no schedule there is no boundary,
//! so a manual switch stays until it is switched back.

use anyhow::{Result, bail};
use chrono::{Datelike, NaiveDateTime, NaiveTime, Weekday};
use serde::Deserialize;

/// The weekdays a rule applies to, as a bit for each, Monday first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Days(u8);

impl Days {
    const ALL: Days = Days(0b111_1111);

    fn has(self, day: Weekday) -> bool {
        self.0 & (1 << day.num_days_from_monday()) != 0
    }

    /// The days `names` list: weekday names (`mon` or `monday`), `weekdays`
    /// for Monday to Friday and `weekend` for Saturday and Sunday.
    /// The names `parse` reads back, Monday first.
    fn names(self) -> Vec<&'static str> {
        ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
            .into_iter()
            .enumerate()
            .filter(|(i, _)| self.0 & (1 << i) != 0)
            .map(|(_, name)| name)
            .collect()
    }

    fn parse(names: &[String]) -> Result<Self> {
        let mut days = 0;
        for name in names {
            days |= match name.trim().to_ascii_lowercase().as_str() {
                "weekdays" => 0b001_1111,
                "weekend" | "weekends" => 0b110_0000,
                "daily" | "every-day" | "all" => Self::ALL.0,
                other => match other.parse::<Weekday>() {
                    Ok(day) => 1 << day.num_days_from_monday(),
                    Err(_) => bail!(
                        "{name:?} is not a day: use mon, tue, wed, thu, fri, sat, sun, \
                         weekdays or weekend"
                    ),
                },
            };
        }
        if days == 0 {
            bail!("days is empty: leave it out for every day");
        }
        Ok(Days(days))
    }
}

/// A span of the day on some weekdays.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(try_from = "RuleFile")]
pub struct Rule {
    days: Days,
    from: NaiveTime,
    to: NaiveTime,
}

/// A rule as the file spells it.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuleFile {
    days: Option<Vec<String>>,
    from: String,
    to: String,
}

impl TryFrom<RuleFile> for Rule {
    type Error = anyhow::Error;

    fn try_from(file: RuleFile) -> Result<Self> {
        let time = |text: &str| {
            NaiveTime::parse_from_str(text.trim(), "%H:%M")
                .map_err(|_| anyhow::anyhow!("{text:?} is not a time of day like \"08:30\""))
        };
        Ok(Rule {
            days: match &file.days {
                Some(names) => Days::parse(names)?,
                None => Days::ALL,
            },
            from: time(&file.from)?,
            to: time(&file.to)?,
        })
    }
}

impl Rule {
    /// Whether the rule covers `time` on `day`.
    fn covers(&self, day: Weekday, time: NaiveTime) -> bool {
        if self.from < self.to {
            self.days.has(day) && self.from <= time && time < self.to
        } else {
            // Into the next day: the early hours belong to the day before.
            (self.days.has(day) && time >= self.from)
                || (self.days.has(day.pred()) && time < self.to)
        }
    }
}

/// When the alternative limits apply by themselves: whenever any rule does.
#[derive(Debug, Default, Clone, PartialEq, Eq, Deserialize)]
#[serde(transparent)]
pub struct Schedule {
    rules: Vec<Rule>,
}

impl Schedule {
    /// Whether the schedule has any rule.
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// The schedule as a TOML array, as the config file spells it.
    pub fn to_toml(&self) -> String {
        let rules: Vec<String> = self
            .rules
            .iter()
            .map(|rule| {
                let days = if rule.days == Days::ALL {
                    String::new()
                } else {
                    let names: Vec<String> = rule
                        .days
                        .names()
                        .iter()
                        .map(|name| format!("\"{name}\""))
                        .collect();
                    format!("days = [{}], ", names.join(", "))
                };
                format!(
                    "{{ {days}from = \"{}\", to = \"{}\" }}",
                    rule.from.format("%H:%M"),
                    rule.to.format("%H:%M")
                )
            })
            .collect();
        format!("[{}]", rules.join(", "))
    }

    /// How many rules the schedule has.
    pub fn len(&self) -> usize {
        self.rules.len()
    }

    /// Whether the alternative limits are scheduled at `now`.
    pub fn active_at(&self, now: NaiveDateTime) -> bool {
        let (day, time) = (now.weekday(), now.time());
        self.rules.iter().any(|rule| rule.covers(day, time))
    }
}

/// A switch made by hand, which holds until the schedule next changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Override {
    /// The mode the user chose: true for the alternative limits.
    pub alt: bool,
    /// What the schedule said when the user chose, so that the choice ends
    /// when it says something else.
    pub scheduled: bool,
}

/// Download and upload limits in KiB/s, 0 for none.
pub type Limits = (u64, u64);

/// The two pairs of limits, the schedule and the manual override: which
/// limits apply when.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpeedControl {
    pub normal: Limits,
    pub alt: Limits,
    pub schedule: Schedule,
    pub manual: Option<Override>,
}

impl SpeedControl {
    /// Normal limits `normal`, the alternative ones `alt` and `schedule`,
    /// and no switch made by hand.
    pub fn new(normal: Limits, alt: Limits, schedule: Schedule) -> Self {
        Self {
            normal,
            alt,
            schedule,
            manual: None,
        }
    }

    /// Whether the alternative limits apply at `now`: what the schedule says
    /// unless a switch by hand is still in force.
    pub fn is_alt(&self, now: NaiveDateTime) -> bool {
        let scheduled = self.schedule.active_at(now);
        match self.manual {
            Some(manual) if manual.scheduled == scheduled => manual.alt,
            _ => scheduled,
        }
    }

    /// Drops the switch made by hand once the schedule has changed since,
    /// so that it cannot come back to life when the schedule changes back.
    pub fn expire(&mut self, now: NaiveDateTime) {
        let scheduled = self.schedule.active_at(now);
        if self
            .manual
            .is_some_and(|manual| manual.scheduled != scheduled)
        {
            self.manual = None;
        }
    }

    /// The limits that apply at `now`.
    pub fn limits(&self, now: NaiveDateTime) -> Limits {
        if self.is_alt(now) {
            self.alt
        } else {
            self.normal
        }
    }

    /// Switches to the other mode, until the schedule next changes. Returns
    /// whether the alternative limits are on now.
    pub fn toggle(&mut self, now: NaiveDateTime) -> bool {
        let scheduled = self.schedule.active_at(now);
        let alt = !self.is_alt(now);
        // Back in step with the schedule: nothing left to override.
        self.manual = (alt != scheduled).then_some(Override { alt, scheduled });
        alt
    }

    /// Sets the limits of the mode that applies at `now`, as `d` and `u` do.
    pub fn set_active(&mut self, now: NaiveDateTime, limits: Limits) {
        if self.is_alt(now) {
            self.alt = limits;
        } else {
            self.normal = limits;
        }
    }
}

#[cfg(test)]
mod tests;
