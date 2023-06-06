

pub struct  QuartzConfig {
    pub storage: StorageConfig,
    pub insert: InsertConfig,
    pub select: SelectConfig,
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
