use tantivy_fst::Automaton;

#[derive(Clone, Debug)]
pub(crate) struct Str<'a> {
    string: &'a [u8],
}

impl<'a> Str<'a> {
    /// Constructs automaton that matches an exact string.
    #[inline]
    pub fn new(string: &'a str) -> Str<'a> {
        Str {
            string: string.as_bytes(),
        }
    }
}

impl<'a> Automaton for Str<'a> {
    type State = Option<usize>;

    #[inline]
    fn start(&self) -> Option<usize> {
        Some(0)
    }

    #[inline]
    fn is_match(&self, pos: &Option<usize>) -> bool {
        *pos == Some(self.string.len())
    }

    #[inline]
    fn can_match(&self, pos: &Option<usize>) -> bool {
        pos.is_some()
    }

    #[inline]
    fn accept(&self, pos: &Option<usize>, byte: u8) -> Option<usize> {
        // if we aren't already past the end...
        if let Some(pos) = *pos {
            // and there is still a matching byte at the current position...
            if self.string.get(pos).cloned() == Some(byte) {
                // then move forward
                return Some(pos + 1);
            }
        }
        // otherwise we're either past the end or didn't match the byte
        None
    }
}
