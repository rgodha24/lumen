use base64::Engine;
use std::io::{self, Write};

/// Copy text to clipboard using OSC52 escape sequence.
/// This works through the terminal emulator, which then sets the system clipboard.
pub fn copy_osc52(text: &str) -> io::Result<()> {
    let encoded = base64::engine::general_purpose::STANDARD.encode(text);
    let mut stdout = io::stdout();
    // OSC 52 sequence: \x1b]52;c;<base64-encoded-text>\x07
    // 'c' specifies the clipboard selection
    write!(stdout, "\x1b]52;c;{}\x07", encoded)?;
    stdout.flush()
}
