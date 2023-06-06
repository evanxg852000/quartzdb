
pub struct QuartzConfig;

pub fn start_insert_service(config: QuartzConfig) {
    println!("Starting <quartz-insert> service")
}


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
