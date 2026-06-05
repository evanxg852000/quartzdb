use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
    path::Path,
};

use hashbrown::{HashMap, HashSet};
use memmap2::Mmap;

use roaring::RoaringTreemap;
use tantivy_fst::{Automaton, IntoStreamer, Map, MapBuilder, automaton::AlwaysMatch};

use crate::{
    IndexComponent, ObjectId,
    automaton::{levenshtein::LevenshteinAutomaton, str::Str},
    error::{IndexError, IndexResult},
    query::{Matcher, TermMatcher},
};

/// A postings or inverted index to map term -> sorted_vec<doc_id>.
/// It has two stored components of files (.term, .post)
/// - .post: A file storing the sorted list of doc_id
/// - .term: A file storing the FST and mapping term -> offset in .post file
///
#[derive(Debug)]
pub(crate) struct Postings {
    // file_name: PathBuf,
    term_dict: Map<Vec<u8>>,

    /// Posting List Format: sorted list of doc_id
    /// ┌────────────┬─────┬────┬────┬────┬─────────┐
    /// │# data-size │ ... │ .. │ .. │ .. │  item N │
    /// └────────────┴─────┴────┴────┴────┴─────────┘
    ///
    posting_list: Mmap,
}

impl Postings {
    pub fn create(
        directory: impl AsRef<Path>,
        mut term_dict: HashMap<String, Vec<ObjectId>>,
    ) -> IndexResult<Self> {
        let posting_list_file_path = IndexComponent::path(&directory, IndexComponent::PostingList);
        let posting_list_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(posting_list_file_path)?;
        let mut posting_list_writer = io::BufWriter::new(&posting_list_file);

        let term_dict_file_path = IndexComponent::path(&directory, IndexComponent::TermDict);
        let term_dict_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(term_dict_file_path)?;
        let mut term_dict_writer = io::BufWriter::new(&term_dict_file);

        // Create the fst builder to insert new term->posting_offset pairs.
        let mut fst_data = vec![];
        let mut term_dict_builder = MapBuilder::new(&mut fst_data)?;

        // sort the terms as fst only accepts in-order insertion
        let mut terms: Vec<(&[u8], &[ObjectId])> = term_dict
            .iter_mut()
            .map(|(term, list)| {
                list.sort();
                (term.as_bytes(), list.as_slice())
            })
            .collect();
        terms.sort_unstable_by_key(|(term, _)| *term);

        const U64_NUM_BYTE: usize = std::mem::size_of::<u64>();
        let mut offset: u64 = 0;
        let mut posting_list_bytes = vec![];
        for (term, list) in terms {
            term_dict_builder.insert(term, offset)?;
            posting_list_bytes.clear();
            RoaringTreemap::from_iter(list.iter()).serialize_into(&mut posting_list_bytes)?;
            let posting_list_bytes_size = posting_list_bytes.len() as u64;
            posting_list_writer.write_all(&posting_list_bytes_size.to_le_bytes())?;
            posting_list_writer.write_all(&posting_list_bytes)?;
            offset += (U64_NUM_BYTE + posting_list_bytes.len()) as u64;
        }
        posting_list_writer.flush()?;
        term_dict_builder.finish()?;
        term_dict_writer.write_all(&fst_data)?;
        term_dict_writer.flush()?;

        let term_dict = Map::from_bytes(fst_data)?;
        let posting_list = unsafe { Mmap::map(&posting_list_file)? };
        Ok(Postings {
            term_dict,
            posting_list,
        })
    }

    pub fn open(directory: impl AsRef<Path>) -> IndexResult<Self> {
        let posting_list_file_path = IndexComponent::path(&directory, IndexComponent::PostingList);
        let posting_list = unsafe { Mmap::map(&File::open(posting_list_file_path)?)? };

        let term_dict_file_path = IndexComponent::path(&directory, IndexComponent::TermDict);
        let fst_data = std::fs::read(term_dict_file_path)?;
        let term_dict = Map::from_bytes(fst_data)?;

        Ok(Postings {
            term_dict,
            posting_list,
        })
    }

    pub fn terms(&self, from: &str, to: &str) -> IndexResult<Vec<String>> {
        let terms = self
            .term_dict
            .range()
            .ge(from)
            .lt(to)
            .into_stream()
            .into_str_vec()?
            .into_iter()
            .map(|(term, _)| term)
            .collect::<Vec<_>>();
        Ok(terms)
    }

    pub fn search(&self, matcher: Matcher) -> IndexResult<RoaringTreemap> {
        let matching_terms = self.search_matching_terms(matcher)?;
        // fetch the posting list for each matching term & combine them
        let mut postings = RoaringTreemap::new();
        for (_, posting_offset) in matching_terms.into_iter() {
            let temp_postings = self.postings(posting_offset)?;
            postings = postings | temp_postings;
        }
        // let object_ids = postings.into_iter().collect::<Vec<_>>();
        Ok(postings)
    }

    fn search_matching_terms(&self, matcher: Matcher) -> IndexResult<Vec<(String, u64)>> {
        let complement = matcher.complement;
        let matching_terms = match matcher.term_matcher {
            TermMatcher::All => self.search_automaton(AlwaysMatch, complement)?,
            TermMatcher::Equal(term) => self.search_automaton(Str::new(term), complement)?,
            TermMatcher::StartsWith(term) => {
                self.search_automaton(Str::new(term).starts_with(), complement)?
            }
            TermMatcher::Fuzzy(term, dist) => {
                let levenshtein_dfa = LevenshteinAutomaton::new(term, dist);
                self.search_automaton(levenshtein_dfa, complement)?
            }
            TermMatcher::Regex(pattern) => {
                let regex_dfa = tantivy_fst::Regex::new(pattern).unwrap();
                self.search_automaton(regex_dfa, complement)?
            }
            TermMatcher::And(left, right) => {
                let left_result = self.search_matching_terms(*left)?;
                let right_result = self.search_matching_terms(*right)?;

                let mut result = Vec::new();
                let probe = left_result
                    .iter()
                    .map(|(term, posting_offset)| (term, posting_offset))
                    .collect::<HashMap<_, _>>();
                for (term, posting_offset) in right_result {
                    if probe.get(&term).is_some() {
                        result.push((term, posting_offset));
                    }
                }
                result
            }
            TermMatcher::Or(left, right) => {
                let left_result = self.search_matching_terms(*left)?;
                let right_result = self.search_matching_terms(*right)?;

                let mut result = Vec::with_capacity(left_result.len() + right_result.len());
                let mut probe = HashSet::new();
                for (term, posting_offset) in left_result {
                    probe.insert(term.clone());
                    result.push((term, posting_offset));
                }
                for (term, posting_offset) in right_result {
                    if !probe.contains(&term) {
                        result.push((term, posting_offset));
                    }
                }
                result
            }
        };
        Ok(matching_terms)
    }

    fn search_automaton<A: Automaton>(
        &self,
        aut: A,
        complement: bool,
    ) -> IndexResult<Vec<(String, u64)>> {
        if complement {
            return self
                .term_dict
                .search(aut.complement())
                .into_stream()
                .into_str_vec()
                .map_err(IndexError::from);
        }

        self.term_dict
            .search(aut)
            .into_stream()
            .into_str_vec()
            .map_err(IndexError::from)
    }

    fn postings(&self, offset: u64) -> IndexResult<RoaringTreemap> {
        let offset = offset as usize;
        const U64_NUM_BYTE: usize = std::mem::size_of::<u64>();
        let mut data_size_bytes = [0u8; U64_NUM_BYTE];
        data_size_bytes.clone_from_slice(&self.posting_list[offset..offset + U64_NUM_BYTE]);
        let data_size = u64::from_le_bytes(data_size_bytes) as usize;

        let start_offset = offset + U64_NUM_BYTE;
        let end_offset = start_offset + data_size;
        let posting_list_bytes = &self.posting_list[start_offset..end_offset];

        let posting_list = RoaringTreemap::deserialize_from(posting_list_bytes)?;
        // posting_list_roaring_treemap.in

        // let posting_list = posting_list_roaring_treemap.into_iter().collect::<Vec<_>>();
        Ok(posting_list)
    }
}
