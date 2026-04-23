use crate::error::IndexResult;

// Root node of query ast
#[derive(Debug)]
pub enum Query {
    // A full text search query with phrase, slop, filter, and optional limit
    Fts(String, u32, Filter, Option<usize>),
    // An inverted index query with filter
    InvertedIndex(Filter),
}

impl Query {
    pub fn fts(phrase: &str, slop: u32, filter: Filter, limit: Option<usize>) -> Self {
        Query::Fts(phrase.to_string(), slop, filter, limit)
    }

    pub fn inverted_index(filter: Filter) -> Self {
        Query::InvertedIndex(filter)
    }
}

#[derive(Debug)]
pub enum Filter {
    All,
    Equal(String),
    NotEqual(String),
    StartsWith(String),
    NotStartsWith(String),
    Fuzzy(String, u8),
    NotFuzzy(String, u8),
    Regex(String),
    NotRegex(String),
    Or(Box<Filter>, Box<Filter>),
    And(Box<Filter>, Box<Filter>),
    AnyOf(Vec<String>),
    NoneOf(Vec<String>),
}

impl Filter {
    pub(crate) fn matcher(&self) -> IndexResult<Matcher> {
        match self {
            Filter::All => Ok(Matcher::all(false)),
            Filter::Equal(term) => Ok(Matcher::equal(&term, false)),
            Filter::NotEqual(term) => Ok(Matcher::equal(&term, true)),
            Filter::StartsWith(term) => Ok(Matcher::starts_with(term, false)),
            Filter::NotStartsWith(term) => Ok(Matcher::starts_with(term, true)),
            Filter::Fuzzy(term, distance) => Ok(Matcher::fuzzy(term, *distance, false)),
            Filter::NotFuzzy(term, distance) => Ok(Matcher::fuzzy(term, *distance, true)),
            Filter::Regex(pattern) => Ok(Matcher::regex(pattern, false)),
            Filter::NotRegex(pattern) => Ok(Matcher::regex(pattern, true)),
            Filter::And(left, right) => {
                let left_matcher = left.matcher()?;
                let right_matcher = right.matcher()?;
                Ok(Matcher::and(left_matcher, right_matcher))
            }
            Filter::Or(left, right) => {
                let left_matcher = left.matcher()?;
                let right_matcher = right.matcher()?;
                Ok(Matcher::or(left_matcher, right_matcher))
            }
            Filter::AnyOf(terms) => {
                let mut matcher = match terms.len() {
                    0 => Matcher::all(false),
                    _ => Matcher::equal(&terms[0], false),
                };
                for term in terms[1..].iter() {
                    matcher = Matcher::or(matcher, Matcher::equal(term, false));
                }
                Ok(matcher)
            }
            Filter::NoneOf(terms) => {
                let mut matcher = match terms.len() {
                    0 => Matcher::all(true),
                    _ => Matcher::equal(&terms[0], true),
                };
                for term in terms[1..].iter() {
                    matcher = Matcher::and(matcher, Matcher::equal(term, true));
                }
                Ok(matcher)
            }
        }
    }
}

/// Matcher is the core ast for querying a term dictionary
#[derive(Debug)]
pub(crate) struct Matcher<'a> {
    pub term_matcher: TermMatcher<'a>,
    pub complement: bool,
}

impl<'a> Matcher<'a> {
    pub fn all(complement: bool) -> Self {
        Self {
            term_matcher: TermMatcher::All,
            complement,
        }
    }

    pub fn equal(term: &'a str, complement: bool) -> Self {
        Self {
            term_matcher: TermMatcher::Equal(term),
            complement,
        }
    }

    pub fn starts_with(term: &'a str, complement: bool) -> Self {
        Self {
            term_matcher: TermMatcher::StartsWith(term),
            complement,
        }
    }

    pub fn fuzzy(term: &'a str, distance: u8, complement: bool) -> Self {
        Self {
            term_matcher: TermMatcher::Fuzzy(term, distance),
            complement,
        }
    }

    pub fn regex(pattern: &'a str, complement: bool) -> Self {
        Self {
            term_matcher: TermMatcher::Regex(pattern),
            complement,
        }
    }

    pub fn and(left: Matcher<'a>, right: Matcher<'a>) -> Self {
        Self {
            term_matcher: TermMatcher::And(Box::new(left), Box::new(right)),
            complement: false,
        }
    }

    pub fn or(left: Matcher<'a>, right: Matcher<'a>) -> Self {
        Self {
            term_matcher: TermMatcher::Or(Box::new(left), Box::new(right)),
            complement: false,
        }
    }
}

#[derive(Debug)]
pub(crate) enum TermMatcher<'a> {
    All,
    Equal(&'a str),
    StartsWith(&'a str),
    Fuzzy(&'a str, u8),
    Regex(&'a str),
    And(Box<Matcher<'a>>, Box<Matcher<'a>>),
    Or(Box<Matcher<'a>>, Box<Matcher<'a>>),
}

// #[derive(Debug)]
// pub enum Query {
//     All,
//     Equal(String),
//     NotEqual(String),
//     // AnyOf(Vec<String>),
//     // NoneOf(Vec<String>),
//     StartsWith(String),
//     NotStartsWith(String),
//     Fuzzy(String, u32),
//     NotFuzzy(String, u32),
//     Regex(String),
//     NotRegex(String),
//     Or(Box<Query>, Box<Query>),
//     And(Box<Query>, Box<Query>),
// }
// TODO: add query builder
