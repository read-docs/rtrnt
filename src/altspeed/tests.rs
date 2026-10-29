use super::*;
use chrono::NaiveDate;

/// A time on the week of Monday 2026-10-26: `day` 0 is Monday.
fn at(day: u32, hour: u32, minute: u32) -> NaiveDateTime {
    (NaiveDate::from_ymd_opt(2026, 10, 26).unwrap() + chrono::Days::new(day.into()))
        .and_hms_opt(hour, minute, 0)
        .unwrap()
}

#[derive(Debug, Deserialize)]
struct Holder {
    schedule: Schedule,
}

fn schedule(toml: &str) -> Schedule {
    toml::from_str::<Holder>(toml).unwrap().schedule
}

fn parse_error(toml: &str) -> String {
    toml::from_str::<Holder>(toml).unwrap_err().to_string()
}

#[test]
fn an_empty_schedule_is_never_on() {
    let schedule = Schedule::default();
    assert!(schedule.is_empty());
    assert!(!schedule.active_at(at(0, 12, 0)));
}

#[test]
fn a_span_includes_its_start_and_not_its_end() {
    let s = schedule(r#"schedule = [{ from = "08:00", to = "17:30" }]"#);
    assert!(!s.active_at(at(2, 7, 59)));
    assert!(s.active_at(at(2, 8, 0)));
    assert!(s.active_at(at(2, 17, 29)));
    assert!(!s.active_at(at(2, 17, 30)));
}

#[test]
fn days_limit_a_rule_and_default_to_every_day() {
    let s =
        schedule(r#"schedule = [{ days = ["mon", "Wednesday"], from = "08:00", to = "09:00" }]"#);
    assert!(s.active_at(at(0, 8, 30)));
    assert!(!s.active_at(at(1, 8, 30)));
    assert!(s.active_at(at(2, 8, 30)));
    let every = schedule(r#"schedule = [{ from = "08:00", to = "09:00" }]"#);
    assert!((0..7).all(|day| every.active_at(at(day, 8, 30))));
}

#[test]
fn weekdays_and_weekend_name_groups() {
    let s = schedule(
        r#"schedule = [{ days = ["weekdays"], from = "00:00", to = "12:00" },
                       { days = ["weekend"], from = "12:00", to = "13:00" }]"#,
    );
    assert!((0..5).all(|day| s.active_at(at(day, 6, 0)) && !s.active_at(at(day, 12, 30))));
    assert!((5..7).all(|day| !s.active_at(at(day, 6, 0)) && s.active_at(at(day, 12, 30))));
}

#[test]
fn a_span_past_midnight_belongs_to_the_day_it_starts_on() {
    let s = schedule(r#"schedule = [{ days = ["fri"], from = "22:00", to = "06:00" }]"#);
    assert!(!s.active_at(at(4, 21, 59)));
    assert!(s.active_at(at(4, 22, 0)));
    assert!(s.active_at(at(4, 23, 59)));
    // Saturday's early hours, but not Friday's.
    assert!(s.active_at(at(5, 0, 0)));
    assert!(s.active_at(at(5, 5, 59)));
    assert!(!s.active_at(at(5, 6, 0)));
    assert!(!s.active_at(at(4, 5, 0)));
    assert!(!s.active_at(at(5, 22, 30)));
}

#[test]
fn a_span_past_midnight_wraps_from_sunday_to_monday() {
    let s = schedule(r#"schedule = [{ days = ["sun"], from = "23:00", to = "02:00" }]"#);
    assert!(s.active_at(at(6, 23, 30)));
    assert!(s.active_at(at(0, 1, 0)));
    assert!(!s.active_at(at(0, 2, 0)));
}

#[test]
fn equal_ends_make_a_span_of_a_day() {
    let s = schedule(r#"schedule = [{ days = ["sat"], from = "10:00", to = "10:00" }]"#);
    assert!(!s.active_at(at(5, 9, 59)));
    assert!(s.active_at(at(5, 10, 0)));
    assert!(s.active_at(at(6, 9, 59)));
    assert!(!s.active_at(at(6, 10, 0)));
}

#[test]
fn any_rule_turns_it_on() {
    let s = schedule(
        r#"schedule = [{ from = "01:00", to = "02:00" }, { from = "05:00", to = "06:00" }]"#,
    );
    assert!(s.active_at(at(0, 1, 30)));
    assert!(!s.active_at(at(0, 3, 0)));
    assert!(s.active_at(at(0, 5, 30)));
}

#[test]
fn bad_rules_are_refused_by_name() {
    assert!(parse_error(r#"schedule = [{ from = "8am", to = "09:00" }]"#).contains("8am"));
    assert!(
        parse_error(r#"schedule = [{ days = ["someday"], from = "08:00", to = "09:00" }]"#)
            .contains("someday")
    );
    assert!(
        parse_error(r#"schedule = [{ days = [], from = "08:00", to = "09:00" }]"#)
            .contains("days is empty")
    );
    assert!(parse_error(r#"schedule = [{ from = "08:00" }]"#).contains("to"));
    assert!(parse_error(r#"schedule = [{ from = "08:00", to = "09:00", on = 1 }]"#).contains("on"));
}

#[test]
fn the_schedule_reads_back_from_its_toml() {
    let s = schedule(
        r#"schedule = [{ days = ["mon", "fri"], from = "22:00", to = "06:00" },
                       { from = "01:00", to = "02:00" }]"#,
    );
    assert_eq!(
        s.to_toml(),
        r#"[{ days = ["mon", "fri"], from = "22:00", to = "06:00" }, { from = "01:00", to = "02:00" }]"#
    );
    assert_eq!(schedule(&format!("schedule = {}", s.to_toml())), s);
    assert_eq!(Schedule::default().to_toml(), "[]");
}

fn control(rules: &str) -> SpeedControl {
    let schedule = if rules.is_empty() {
        Schedule::default()
    } else {
        schedule(&format!("schedule = {rules}"))
    };
    SpeedControl::new((1000, 100), (50, 10), schedule)
}

const NIGHT: &str = r#"[{ from = "22:00", to = "06:00" }]"#;

#[test]
fn the_schedule_picks_the_limits() {
    let c = control(NIGHT);
    assert_eq!(c.limits(at(0, 12, 0)), (1000, 100));
    assert_eq!(c.limits(at(0, 23, 0)), (50, 10));
    assert!(c.is_alt(at(1, 3, 0)));
}

#[test]
fn without_a_schedule_a_switch_by_hand_stays() {
    let mut c = control("");
    assert!(!c.is_alt(at(0, 12, 0)));
    assert!(c.toggle(at(0, 12, 0)));
    assert!(c.is_alt(at(3, 12, 0)));
    assert_eq!(c.limits(at(3, 12, 0)), (50, 10));
    assert!(!c.toggle(at(3, 12, 0)));
    assert_eq!(c.manual, None);
}

#[test]
fn a_switch_by_hand_holds_until_the_schedule_changes() {
    let mut c = control(NIGHT);
    // Turned on at noon, it holds through the afternoon.
    assert!(c.toggle(at(0, 12, 0)));
    assert!(c.is_alt(at(0, 21, 59)));
    // At 22:00 the schedule turns on too, which ends the switch.
    assert!(c.is_alt(at(0, 22, 0)));
    c.expire(at(0, 22, 0));
    assert_eq!(c.manual, None);
    // It does not come back in the next day's afternoon.
    assert!(!c.is_alt(at(1, 12, 0)));
}

#[test]
fn turning_it_off_at_night_holds_until_morning() {
    let mut c = control(NIGHT);
    assert!(!c.toggle(at(0, 23, 0)));
    assert!(!c.is_alt(at(1, 5, 59)));
    assert_eq!(c.limits(at(1, 1, 0)), (1000, 100));
    // At 06:00 the schedule goes off, as the user had it.
    assert!(!c.is_alt(at(1, 6, 0)));
    c.expire(at(1, 6, 0));
    assert_eq!(c.manual, None);
    // And it is on again the next night.
    assert!(c.is_alt(at(1, 23, 0)));
}

#[test]
fn an_expired_switch_does_not_come_back_when_the_schedule_flips_again() {
    let mut c = control(r#"[{ from = "10:00", to = "11:00" }]"#);
    assert!(c.toggle(at(0, 9, 0)));
    assert!(c.is_alt(at(0, 9, 30)));
    // 10:00 passes, the schedule changes, and a look is taken.
    c.expire(at(0, 10, 0));
    // 11:00 passes, the schedule flips back to what it was at the switch.
    c.expire(at(0, 11, 0));
    assert!(!c.is_alt(at(0, 12, 0)));
}

#[test]
fn toggling_back_in_step_with_the_schedule_clears_the_switch() {
    let mut c = control(NIGHT);
    assert!(!c.toggle(at(0, 23, 0)));
    assert!(c.manual.is_some());
    assert!(c.toggle(at(0, 23, 30)));
    assert_eq!(c.manual, None);
}

#[test]
fn set_active_edits_the_limits_of_the_mode_that_is_on() {
    let mut c = control(NIGHT);
    c.set_active(at(0, 12, 0), (2000, 200));
    assert_eq!((c.normal, c.alt), ((2000, 200), (50, 10)));
    c.set_active(at(0, 23, 0), (5, 1));
    assert_eq!((c.normal, c.alt), ((2000, 200), (5, 1)));
}
