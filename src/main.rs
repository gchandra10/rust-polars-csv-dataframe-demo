//https://docs.pola.rs/user-guide/concepts/data-structures/

use polars::prelude::*;
use regex::Regex;
use std::path::Path;
use std::fs::File;
use std::io::{Write};

//Download the CSV file from github
fn download_csv(url: &str, destination: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let response = reqwest::blocking::get(url)?;
    let mut file = File::create(destination)?;
    let content = response.bytes()?;
    file.write_all(&content)?;
    Ok(())
}

fn read_csv(filename: &str) -> PolarsResult<DataFrame> {
    CsvReadOptions::default()
        .with_has_header(true)
        .try_into_reader_with_file_path(Some({ filename }.into()))?
        .finish()
}

fn print_dataframe(df: &DataFrame) {
    println!("{:?}\n\n", df.head(None));
    println!("Dataframe Schema\n");
    println!("{:?}\n", df.schema());
    println!("{:?}\n", df.shape());

    let rows = df.height();
    let cols = df.width();
    println!("Number of rows: {}\nNumber of columns: {}\n", rows, cols);
    //let columns: &[Series] = df.get_columns();
    //println!("{:?}", columns);
    println!("Column Names: \n{:?}", df.get_column_names());
}

fn clean_dataframe(df: &mut DataFrame) -> PolarsResult<()> {
    let re = Regex::new(r"\s+").unwrap();
    let new_columns: Vec<String> = df
        .get_column_names()
        .iter()
        .map(|name| re.replace_all(name, "").to_lowercase())
        .collect();

    df.set_column_names(&new_columns)?;
    println!("Cleaned Dataframe Schema");
    println!("{:?}", df.schema());
    Ok(())
}

fn filter_and_select(df: &DataFrame) -> PolarsResult<DataFrame> {
    let df1 = df.filter(&df.column("totalprofit")?.gt(500000)?)?;
    let selected_df = df1.select(&["region", "country", "totalprofit"])?;
    Ok(selected_df)
}

fn group_and_sum(df: &DataFrame) -> PolarsResult<DataFrame> {
    let lazy_df = df.clone().lazy();
    let grouped_df = lazy_df
        .group_by(vec![col("region"), col("country")])
        .agg([col("totalprofit").sum()])
        .collect()?;
    Ok(grouped_df)
}


fn main() -> Result<(), PolarsError> {
    let url = "https://raw.githubusercontent.com/gchandra10/filestorage/main/sales_100.csv";
    let destination = Path::new("sales_100.csv");

    match download_csv(url, destination) {
        Ok(_) => println!("File downloaded successfully."),
        Err(e) => eprintln!("Error downloading file: {}", e),
    }

    let mut df: DataFrame = DataFrame::default();
    println!("{:?}",df);
    
    // Read CSV File
    match read_csv("sales_100.csv") {
        Ok(x) => df = x,
        Err(e) => return Err(e),
    };

    // Print Dataframe along with rows and cols
    print_dataframe(&df);

    // clean the header
    clean_dataframe(&mut df)?;

    // Print it again
    print_dataframe(&df);

    // Check the column order of the received file.
    assert_eq!(
        df.get_column_names(),
        &[
            "region",
            "country",
            "itemtype",
            "saleschannel",
            "orderpriority",
            "orderdate",
            "orderid",
            "shipdate",
            "unitssold",
            "unitprice",
            "unitcost",
            "totalrevenue",
            "totalcost",
            "totalprofit"
        ]
    );

    // Some basic filter and select columns

    let selected_df = filter_and_select(&df)?;
    println!("{:?}", selected_df);

    let mut grouped_df = group_and_sum(&df)?;
    println!(
        "Grouped Dataframe (sum of totalprofit by region and country):\n{:?}",
        grouped_df
    );

    let mut file = std::fs::File::create("grouped_sales.csv").unwrap();
    CsvWriter::new(&mut file).finish(&mut grouped_df).unwrap();
    
    Ok(())
}
