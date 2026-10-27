//! The UI's colors: true color where the terminal says it has it, the
//! nearest ANSI colors otherwise. The background is always the terminal's.

use ratatui::style::{Color, Modifier, Style};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Theme {
    truecolor: bool,
}

impl Theme {
    /// The theme for a terminal whose `COLORTERM` is `colorterm`.
    pub fn for_colorterm(colorterm: Option<&str>) -> Self {
        Self {
            truecolor: matches!(colorterm, Some("truecolor" | "24bit")),
        }
    }

    /// The theme for this terminal.
    pub fn detect() -> Self {
        Self::for_colorterm(std::env::var("COLORTERM").ok().as_deref())
    }

    fn pick(self, rgb: u32, ansi: Color) -> Color {
        if self.truecolor {
            Color::from_u32(rgb)
        } else {
            ansi
        }
    }

    /// Download, downloading.
    pub fn down(self) -> Color {
        self.pick(0x56c8e8, Color::Cyan)
    }

    /// Upload, have, seeding.
    pub fn up(self) -> Color {
        self.pick(0x7ad97a, Color::Green)
    }

    /// A partly had cell of the piece map.
    pub fn partial(self) -> Color {
        self.pick(0x3f7a45, Color::DarkGray)
    }

    /// Checking, in flight, warnings, peer flags.
    pub fn warn(self) -> Color {
        self.pick(0xe8c25a, Color::Yellow)
    }

    /// Errors, rare pieces.
    pub fn error(self) -> Color {
        self.pick(0xf07070, Color::Red)
    }

    /// Fetching metadata.
    pub fn metadata(self) -> Color {
        self.pick(0xc792ea, Color::Magenta)
    }

    pub fn dim(self) -> Color {
        self.pick(0x6b7280, Color::DarkGray)
    }

    pub fn border(self) -> Color {
        self.pick(0x3b4252, Color::DarkGray)
    }

    pub fn selection(self) -> Color {
        self.pick(0x1e2a3a, Color::Indexed(236))
    }

    pub fn bright(self) -> Color {
        self.pick(0xf0f3f6, Color::White)
    }

    /// Torrent names in the log: the download color, muted.
    pub fn name(self) -> Color {
        self.pick(0x4a9bb3, Color::Cyan)
    }

    pub fn dim_style(self) -> Style {
        Style::new().fg(self.dim())
    }

    pub fn border_style(self) -> Style {
        Style::new().fg(self.border())
    }

    pub fn title_style(self) -> Style {
        Style::new().fg(self.bright()).add_modifier(Modifier::BOLD)
    }

    /// Column headings.
    pub fn heading_style(self) -> Style {
        Style::new().fg(self.dim()).add_modifier(Modifier::BOLD)
    }

    pub fn key_style(self) -> Style {
        Style::new().fg(self.down()).add_modifier(Modifier::BOLD)
    }
}

impl Default for Theme {
    fn default() -> Self {
        Self { truecolor: true }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn true_color_only_when_the_terminal_says_so() {
        let rgb = Theme::for_colorterm(Some("truecolor"));
        assert_eq!(rgb.down(), Color::Rgb(0x56, 0xc8, 0xe8));
        assert_eq!(Theme::for_colorterm(Some("24bit")), rgb);
        let ansi = Theme::for_colorterm(None);
        assert_eq!(ansi.down(), Color::Cyan);
        assert_eq!(ansi.error(), Color::Red);
        assert_eq!(Theme::for_colorterm(Some("256color")), ansi);
    }
}
