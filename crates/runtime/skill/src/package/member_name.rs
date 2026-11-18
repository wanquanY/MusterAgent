use std::borrow::Cow;

/// ZIP general-purpose flag 11 declares UTF-8. The archive reader also sets it
/// after applying a validated Info-ZIP Unicode Path extra field.
const UTF8_NAME: u16 = 1 << 11;

/// Preserve the filename used to identify and hash imported Skill resources.
/// Some valid CP437 sequences also form valid UTF-8, so the flag decides.
pub(super) fn decode(raw: &[u8], flags: u16) -> Cow<'_, str> {
    if flags & UTF8_NAME != 0 || raw.is_ascii() {
        return String::from_utf8_lossy(raw);
    }
    Cow::Owned(
        raw.iter()
            .map(|&byte| match byte {
                0..=127 => char::from(byte),
                _ => CP437_HIGH[usize::from(byte - 128)],
            })
            .collect(),
    )
}

// IBM code page 437, bytes 0x80 through 0xff in order.
const CP437_HIGH: [char; 128] = [
    'Ç', 'ü', 'é', 'â', 'ä', 'à', 'å', 'ç', 'ê', 'ë', 'è', 'ï', 'î', 'ì', 'Ä', 'Å', 'É', 'æ', 'Æ',
    'ô', 'ö', 'ò', 'û', 'ù', 'ÿ', 'Ö', 'Ü', '¢', '£', '¥', '₧', 'ƒ', 'á', 'í', 'ó', 'ú', 'ñ', 'Ñ',
    'ª', 'º', '¿', '⌐', '¬', '½', '¼', '¡', '«', '»', '░', '▒', '▓', '│', '┤', '╡', '╢', '╖', '╕',
    '╣', '║', '╗', '╝', '╜', '╛', '┐', '└', '┴', '┬', '├', '─', '┼', '╞', '╟', '╚', '╔', '╩', '╦',
    '╠', '═', '╬', '╧', '╨', '╤', '╥', '╙', '╘', '╒', '╓', '╫', '╪', '┘', '┌', '█', '▄', '▌', '▐',
    '▀', 'α', 'ß', 'Γ', 'π', 'Σ', 'σ', 'µ', 'τ', 'Φ', 'Θ', 'Ω', 'δ', '∞', 'φ', 'ε', '∩', '≡', '±',
    '≥', '≤', '⌠', '⌡', '÷', '≈', '°', '∙', '·', '√', 'ⁿ', '²', '■', '\u{00a0}',
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_names_follow_the_declared_zip_encoding() {
        let name = b"references/\xc3\xa9.txt";
        assert_eq!(decode(name, 0), "references/├⌐.txt");
        assert_eq!(decode(name, UTF8_NAME), "references/é.txt");
        assert_eq!(decode(b"caf\x82.txt", 0), "café.txt");
        assert_eq!(decode(b"caf\x82.txt", UTF8_NAME), "caf\u{fffd}.txt");
    }
}
