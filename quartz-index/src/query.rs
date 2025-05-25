// https://docs.victoriametrics.com/victorialogs/logsql/
// https://github.com/GreptimeTeam/promql-parser

use std::ops::Bound;

use tantivy::{query::{AllQuery, BooleanQuery, Occur, PhraseQuery, Query as TantivyQuery, RangeQuery, RegexQuery, TermQuery}, schema::{Field, IndexRecordOption, Schema}, Term};

use crate::error::IndexError;

#[derive(Debug)]
pub(crate) struct Query{
    pub tantivy_query: Box<dyn TantivyQuery>,
    pub start_timestamp_secs: Option<i64>,
    pub end_timestamp_secs: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct QueryBuilder {
    schema: Schema,
    start_timestamp_secs: Option<i64>,
    end_timestamp_secs: Option<i64>,
    filter: Option<Filter>,
}

impl QueryBuilder {
    pub fn new(schema: Schema) -> Self {
        Self { 
            schema,
            start_timestamp_secs: None,
            end_timestamp_secs: None,
            filter: None,
        }
    }

    pub fn set_filter(&mut self, filter: Filter) {
        self.filter = Some(filter);
    }
    
    pub fn set_start_timestamp(&mut self, start_timestamp_secs: i64){
        self.start_timestamp_secs = Some(start_timestamp_secs);
    }

    pub fn set_end_timestamp(&mut self, end_timestamp_secs: i64) {
        self.end_timestamp_secs = Some(end_timestamp_secs);
    }

    pub fn build(self) -> Result<Query, IndexError> {
        let tantivy_query = match &self.filter {
            Some(filter) => filter.to_tantivy_query(&self.schema)?,
            None => Box::new(AllQuery),
        };

        Ok(Query {
            tantivy_query: tantivy_query,
            start_timestamp_secs: self.start_timestamp_secs,
            end_timestamp_secs: self.end_timestamp_secs,
        })
    }
}


#[derive(Debug, Clone)]
pub enum Filter {
    All, // AllQuery
    Phrase(String, String, u32), // PhraseQuery
    Equal(String, String), // TermQuery
    StartsWith(String, String), // RangeQuery
    Contains(String, String),   // RegexQuery
    EndsWith(String, String), // RangeQuery
    Regex(String, String), // RegexQuery
    Not(Box<Filter>), // BooleanQuery with MustNot
    Or(Box<Filter>, Box<Filter>), // BooleanQuery with Should
    And(Box<Filter>, Box<Filter>), // BooleanQuery with Must
    AnyOf(Vec<Box<Filter>>), // BooleanQuery with Should
    AllOf(Vec<Box<Filter>>), // BooleanQuery with Must
    NoneOf(Vec<Box<Filter>>), // BooleanQuery with MustNot
}

impl Filter {
    pub fn to_tantivy_query(&self, schema: &Schema) -> Result<Box<dyn TantivyQuery>, IndexError> {
        let query: Box<dyn TantivyQuery> = match self {
            Filter::All => Box::new(AllQuery),
            Filter::Phrase(field_name, phrase, slop) => {
                let field = schema.get_field(field_name)?;
                let terms = phrase.split_whitespace()
                    .map(|word| Term::from_field_text(field, &word.to_lowercase()))
                    .collect::<Vec<_>>();
                if terms.len() <= 1 {
                    return Err(IndexError::InvalidQuery("A phrase query is required to have more than one term.".to_string()));
                }
                let mut phrase_query = PhraseQuery::new(terms);
                phrase_query.set_slop(*slop);
                Box::new(phrase_query)
            },
            Filter::Equal(field_name, value) => {
                let field = schema.get_field(field_name)?;
                Box::new(TermQuery::new(Term::from_field_text(field, value), IndexRecordOption::Basic))
            },
            Filter::StartsWith(field_name, prefix) => {
                let field = schema.get_field(field_name)?;
                let(lower_bound, upper_bound) = prefix_to_range(field, prefix);
                Box::new(RangeQuery::new(lower_bound, upper_bound))
            },
            Filter::Contains(field_name, needle) => {
                let field = schema.get_field(field_name)?;
                let regex_pattern = format!(".*{}.*", regex::escape(needle));
                Box::new(RegexQuery::from_pattern(&regex_pattern, field)?)
            },
            Filter::EndsWith(field_name, suffix) => {
                let field = schema.get_field(field_name)?;
                let(lower_bound, upper_bound) = suffix_to_range(field, suffix);
                Box::new(RangeQuery::new(lower_bound, upper_bound))
            },
            Filter::Regex(field_name, regex_pattern) => {
                let field = schema.get_field(field_name)?;
                Box::new(RegexQuery::from_pattern(regex_pattern, field)?)
            },
            Filter::Not(filter) => {
                let inner_query = filter.to_tantivy_query(schema)?;
                let occurs = vec![(Occur::MustNot, inner_query)];
                Box::new(BooleanQuery::from(occurs))
            },
            Filter::Or(left_filter, right_filter) => {
                let left_query = left_filter.to_tantivy_query(schema)?;
                let right_query = right_filter.to_tantivy_query(schema)?;
                let occurs = vec![(Occur::Should, left_query), (Occur::Should, right_query)];
                Box::new(BooleanQuery::from(occurs))
            },
            Filter::And(left_filter, right_filter) => {
                let left_query = left_filter.to_tantivy_query(schema)?;
                let right_query = right_filter.to_tantivy_query(schema)?;
                let occurs = vec![(Occur::Must, left_query), (Occur::Must, right_query)];
                Box::new(BooleanQuery::from(occurs))
            },
            Filter::AnyOf(filters) => {
                let mut occurs = Vec::new();
                for filter in filters {
                    let query = filter.to_tantivy_query(schema)?;
                    occurs.push((Occur::Should, query));
                }
                Box::new(BooleanQuery::from(occurs))    
            },
            Filter::AllOf(filters) => {
                let mut occurs = Vec::new();
                for filter in filters {
                    let query = filter.to_tantivy_query(schema)?;
                    occurs.push((Occur::Must, query));
                }
                Box::new(BooleanQuery::from(occurs))
            },
            Filter::NoneOf(filters) => {
                let mut occurs = Vec::new();
                for filter in filters {
                    let query = filter.to_tantivy_query(schema)?;
                    occurs.push((Occur::MustNot, query));
                }
                Box::new(BooleanQuery::from(occurs))
            },
        };
        Ok(query)
    }
}


pub fn all() -> Filter {
    Filter::All
}

pub fn phrase(field: &str, phrase: &str) -> Filter {
    Filter::Phrase(field.to_string(), phrase.to_string(), 0)
}

pub fn phrase_with_slop(field: &str, phrase: &str, slop: u32) -> Filter {
    Filter::Phrase(field.to_string(), phrase.to_string(), slop)
}

pub fn eq(field: &str, value: &str) -> Filter {
    Filter::Equal(field.to_string(), value.to_string())
}

pub fn starts_with(field: &str, value: &str) -> Filter {
    Filter::StartsWith(field.to_string(), value.to_string())
}

pub fn contains(field: &str, value: &str) -> Filter {
    Filter::Contains(field.to_string(), value.to_string())
}

pub fn ends_with(field: &str, value: &str) -> Filter {
    Filter::EndsWith(field.to_string(), value.to_string())
}

pub fn regex(field: &str, value: &str) -> Filter {
    Filter::Regex(field.to_string(), value.to_string())
}

pub fn not(filter: Filter) -> Filter {
    Filter::Not(Box::new(filter))
}

pub fn or(left: Filter, right: Filter) -> Filter {
    Filter::Or(Box::new(left), Box::new(right))
}

pub fn and(left: Filter, right: Filter) -> Filter {
    Filter::And(Box::new(left), Box::new(right))
}

pub fn any_of(filters: Vec<Filter>) -> Filter {
    Filter::AnyOf(filters.into_iter().map(Box::new).collect())
}

pub fn all_of(filters: Vec<Filter>) -> Filter {
    Filter::AllOf(filters.into_iter().map(Box::new).collect())
}

pub fn none_of(filters: Vec<Filter>) -> Filter {
    Filter::NoneOf(filters.into_iter().map(Box::new).collect())
}


// Calculate the lower bound as the prefix itself.
// Calculate the upper bound by incrementing the last character.
fn prefix_to_range(field: Field, prefix: &str) -> (Bound<Term>, Bound<Term>) {
    let mut upper_bound = prefix.to_string();
    if upper_bound.is_empty() {
        // max unicode char if empty prefix
        upper_bound.push('\u{10FFFF}'); 
    } else {
        let last_char = upper_bound.pop().expect("prefix should not be empty");
        upper_bound.push((last_char as u8 + 1) as char);
    }

    (
        Bound::Included(Term::from_field_text(field, prefix)), 
        Bound::Excluded(Term::from_field_text(field, &upper_bound)),
    )
}

fn suffix_to_range(field: Field, suffix: &str) -> (Bound<Term>, Bound<Term>) {
    let mut lower_bound = suffix.to_string();
    if lower_bound.is_empty() {
        // min unicode char if empty suffix
        lower_bound.push('\u{0}'); 
    } else {
        let last_char = lower_bound.pop().expect("suffix should not be empty");
        lower_bound.push((last_char as u8 - 1) as char);
    }

    (
        Bound::Excluded(Term::from_field_text(field, &lower_bound)), 
        Bound::Included(Term::from_field_text(field, suffix)),
    )
}
