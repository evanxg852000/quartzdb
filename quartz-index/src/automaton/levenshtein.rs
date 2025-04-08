use levenshtein_automata::{DFA, Distance, LevenshteinAutomatonBuilder};
use tantivy_fst::Automaton;

pub(crate) struct LevenshteinAutomaton(pub DFA);

impl LevenshteinAutomaton {
    pub fn new(term: &str, distance: u8) -> Self {
        // TODO: build this once with OneCell
        let dfa = LevenshteinAutomatonBuilder::new(distance, true).build_dfa(term);
        Self(dfa)
    }
}

impl Automaton for LevenshteinAutomaton {
    type State = u32;

    fn start(&self) -> Self::State {
        self.0.initial_state()
    }

    fn is_match(&self, state: &Self::State) -> bool {
        match self.0.distance(*state) {
            Distance::Exact(_) => true,
            Distance::AtLeast(_) => false,
        }
    }

    fn can_match(&self, state: &u32) -> bool {
        *state != levenshtein_automata::SINK_STATE
    }

    fn accept(&self, state: &Self::State, byte: u8) -> Self::State {
        self.0.transition(*state, byte)
    }
}
