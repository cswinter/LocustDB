// original source: https://github.com/lintje/lintje/blob/501aab06e19008e787237438a69ac961f38bb4b7
// https://tomdebruijn.com/posts/rust-string-length-width-calculations/
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;
use lazy_static::lazy_static;

const ZERO_WIDTH_JOINER: &str = "\u{200d}";
const VARIATION_SELECTOR_16: &str = "\u{fe0f}";
const SKIN_TONES: [&str; 5] = [
    "\u{1f3fb}", // Light Skin Tone
    "\u{1f3fc}", // Medium-Light Skin Tone
    "\u{1f3fd}", // Medium Skin Tone
    "\u{1f3fe}", // Medium-Dark Skin Tone
    "\u{1f3ff}", // Dark Skin Tone
];

lazy_static! {
    static ref OTHER_PUNCTUATION: Vec<char> = vec!['…', '⋯',];
}

// Return String display width as rendered in a monospace font according to the Unicode
// specification.
//
// This may return some odd results at times where some symbols are counted as more character width
// than they actually are.
//
// This function has exceptions for skin tones and other emoji modifiers to determine a more
// accurate display with.
pub fn display_width(string: &str) -> usize {
    // String expressed as a vec of Unicode characters. Characters with accents and emoji may
    // be multiple characters combined.
    let unicode_chars = string.graphemes(true);
    let mut width = 0;
    for c in unicode_chars.into_iter() {
        width += display_width_char(c);
    }
    width
}

/// Calculate the render width of a single Unicode character. Unicode characters may consist of
/// multiple String characters, which is why the function argument takes a string.
fn display_width_char(string: &str) -> usize {
    // Characters that are used as modifiers on emoji. By themselves they have no width.
    if string == ZERO_WIDTH_JOINER || string == VARIATION_SELECTOR_16 {
        return 0;
    }
    // Emoji that are representations of combined emoji. They are normally calculated as the
    // combined width of the emoji, rather than the actual display width. This check fixes that and
    // returns a width of 2 instead.
    if string.contains(ZERO_WIDTH_JOINER) {
        return 2;
    }
    // Any character with a skin tone is most likely an emoji.
    // Normally it would be counted as as four or more characters, but these emoji should be
    // rendered as having a width of two.
    for skin_tone in SKIN_TONES {
        if string.contains(skin_tone) {
            return 2;
        }
    }

    match string {
        "\t" => {
            // unicode-width returns 0 for tab width, which is not how it's rendered.
            // I choose 4 columns as that's what most applications render a tab as.
            4
        }
        _ => UnicodeWidthStr::width(string),
    }
}