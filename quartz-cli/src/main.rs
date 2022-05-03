use polars::prelude::*;
use std::fs::File;


fn main() {
    let s1 = Series::new("names", &["a", "b", "c"]);
    let s2 = Series::new("values", &[Some(1), None, Some(3)]);
    let mut df1 = DataFrame::new(vec![s1, s2]).unwrap();

    let s1 = Series::new("names", &["d"]);
    let s2 = Series::new("values", &[Some(1)]);
    let df2 = DataFrame::new(vec![s1, s2]).unwrap();

    df1.extend(&df2).unwrap();

    
    // create a file
    let file = File::create("data.parquet")
        .expect("could not create file");
    ParquetWriter::new(file)
        .finish(&mut df1).unwrap();

    println!("{:?}", df1);
}
