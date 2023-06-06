
struct Term {
    value: String,
    posting_offset: usize,
}


// A wrapper arround fst
struct TermDict {
    /// a map of label to posting offset
    fst: fst::Map,
}

impl TermDict {

    fn new(iter: &dyn Iterator<Term>) -> Self {}

    fn from_file(path: PathBuf) -> Self {}

    fn to_file(&self, path: PathBuf) {}

}
