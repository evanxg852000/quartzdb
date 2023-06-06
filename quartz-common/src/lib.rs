mod colors;
mod build_info;

pub use crate::colors::{GREEN_COLOR, BLUE_COLOR, RED_COLOR};
pub use crate::build_info::{BuildInfo, RuntimeInfo};



#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
