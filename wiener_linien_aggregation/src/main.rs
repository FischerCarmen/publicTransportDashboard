use polars::prelude::*;
use polars_lazy::prelude::*;
use std::collections::HashSet;
use std::env;
use std::fs::File;
use std::io;
use std::vec;

fn configure_the_environment() {
    unsafe {
        env::set_var("POLARS_FMT_TABLE_ROUNDED_CORNERS", "1"); // apply rounded corners to UTF8-styled tables.
        env::set_var("POLARS_FMT_MAX_COLS", "20"); // maximum number of columns shown when formatting DataFrames.
        env::set_var("POLARS_FMT_MAX_ROWS", "10"); // maximum number of rows shown when formatting DataFrames.
        env::set_var("POLARS_FMT_STR_LEN", "50"); // maximum number of characters printed per string value.
    }
}

//TODO: Implement good error handling
//TODO: If direction in fahrwegverlaeufe is null take PatternID
//TODO: delete entry in stops if MeansOfTransport is ptRufBus
// test for git
struct Stop {
    stop_id: i32,
    stop_name: String,
    lines: String,
    directions: String,
    platform: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    configure_the_environment();
    let path = "../files/";
    let url = "https://www.wienerlinien.at/ogd_realtime/doku/ogd/";

    let mut csv = HashSet::new();

    csv.insert("wienerlinien-ogd-haltepunkte.csv".to_string());
    csv.insert("wienerlinien-ogd-haltestellen.csv".to_string());
    csv.insert("wienerlinien-ogd-linien.csv".to_string());
    csv.insert("wienerlinien-ogd-fahrwegverlaeufe.csv".to_string());

    for item in csv {
        let request = reqwest::blocking::get(url.to_owned() + &item).expect("request failed");

        let body = request.text().expect("request failed");

        let mut out = File::create(path.to_owned() + &item).expect("failed to create file");
        io::copy(&mut body.as_bytes(), &mut out).expect("failed to copy content");
    }

    let seperator = ';';

    let mut haltepunkte_df = polars_io::csv::read::CsvReadOptions::default()
        .with_has_header(true)
        .with_parse_options(CsvParseOptions::default().with_separator(seperator as u8))
        .try_into_reader_with_file_path(Some(
            (path.to_owned() + "wienerlinien-ogd-haltepunkte.csv").into(),
        ))
        .unwrap()
        .finish()
        .expect("failed to load haltepunkte");

    let haltestellen_df = polars_io::csv::read::CsvReadOptions::default()
        .with_has_header(true)
        .with_parse_options(CsvParseOptions::default().with_separator(seperator as u8))
        .try_into_reader_with_file_path(Some(
            (path.to_owned() + "wienerlinien-ogd-haltestellen.csv").into(),
        ))
        .unwrap()
        .finish()
        .expect("failed to load haltestellen");

    let mut linien_df = polars_io::csv::read::CsvReadOptions::default()
        .with_has_header(true)
        .with_parse_options(CsvParseOptions::default().with_separator(seperator as u8))
        .try_into_reader_with_file_path(Some(
            (path.to_owned() + "wienerlinien-ogd-linien.csv").into(),
        ))
        .unwrap()
        .finish()
        .expect("failed to load linien");

    let mut fahrwegverlaeufe_df = polars_io::csv::read::CsvReadOptions::default()
        .with_has_header(true)
        .with_parse_options(CsvParseOptions::default().with_separator(seperator as u8))
        .try_into_reader_with_file_path(Some(
            (path.to_owned() + "wienerlinien-ogd-fahrwegverlaeufe.csv").into(),
        ))
        .unwrap()
        .finish()
        .expect("failed to load fahrwegverlaeufe");

    println!("fahrwegverlaeufe_df: {:?}", fahrwegverlaeufe_df);

    haltepunkte_df = haltepunkte_df
        .clone()
        .lazy()
        .filter(col("Municipality").eq(lit("Wien")))
        .collect()
        .expect("failed to filter haltepunkte_df");

    haltepunkte_df = haltepunkte_df
        .lazy()
        .with_column(col("StopID").cast(DataType::Int64).alias("StopID"))
        .collect()
        .expect("failed to alias column StopID haltepunkte_df");

    haltepunkte_df = haltepunkte_df
        .drop_nulls::<String>(Some(&["StopID".to_owned()]))
        .expect("failed to drop null haltepunkte_df");

    linien_df = linien_df
        .lazy()
        .with_column(col("LineID").cast(DataType::Int64).alias("LineID"))
        .collect()
        .expect("failed to cast LineID linien_df");

    linien_df = linien_df
        .drop_nulls::<String>(Some(&["LineID".to_owned()]))
        .expect("failed to drop null linien_df");

    fahrwegverlaeufe_df = fahrwegverlaeufe_df
        .lazy()
        .with_column(col("LineID").cast(DataType::Int64).alias("LineID"))
        .collect()
        .expect("failed to cast column LineID fahrwegverlaeufe_df");

    fahrwegverlaeufe_df = fahrwegverlaeufe_df
        .drop_nulls::<String>(Some(&["LineID".to_owned()]))
        .expect("failed to drop null fahrwegverlaeufe_df");

    let mut stops = haltepunkte_df
        .clone()
        .lazy()
        .join(
            haltestellen_df.clone().lazy(),
            [col("DIVA")],
            [col("DIVA")],
            JoinArgs::default(),
        )
        .collect()
        .expect("failed to join haltepunkte_df and haltestellen_df");

    stops = stops
        .clone()
        .lazy()
        .join(
            fahrwegverlaeufe_df.clone().lazy(),
            [col("StopID")],
            [col("StopID")],
            JoinArgs::default(),
        )
        .collect()
        .expect("failed to join stops and fahrwegverlaeufe_df");

    println!("stops: {:?}", stops);

    stops = stops
        .clone()
        .lazy()
        .join(
            linien_df.clone().lazy(),
            [col("LineID")],
            [col("LineID")],
            JoinArgs::default(),
        )
        .collect()
        .expect("failed to join stops and linien_df");

    let stop_id = PlSmallStr::from_str("StopID");
    let line_text = PlSmallStr::from_str("LineText");
    let direction = PlSmallStr::from_str("Direction");
    let stop_name = PlSmallStr::from_str("StopText");
    let platform = PlSmallStr::from_str("PlatformText");

    let columns = Some(vec![stop_id.clone(), line_text.clone(), direction.clone()]);

    let stops_unique = stops
        .unique_impl(true, columns, UniqueKeepStrategy::First, None)
        .expect("failed to compute non unique values");

    let stops_columns = stops_unique.get_column_names_str();
    let columns_to_drop = stops_columns
        .iter()
        .filter(|&col| {
            col != &stop_id
                && col != &line_text
                && col != &direction
                && col != &stop_name
                && col != &platform
        })
        .cloned()
        .collect::<Vec<_>>();

    let mut stops_finished = DataFrame::empty();
    stops_unique.clone_into(&mut stops_finished);

    for c in columns_to_drop {
        stops_finished = stops_finished.drop(c).expect("failed to drop");
    }

    println!("stops: {:?}", stops);
    println!("stops_unique: {:?}", stops_unique);
    println!("stops_finished: {:?}", stops_finished);
    Ok(())
}

//# --- 9. Prepare final JSON ---
//output = []
//for _, row in grouped.iterrows():
//    output.append({
//        "stopId": int(row['StopID']),
//        "stopName": row['StopText'],
//        "lines": row['LineText'],
//        "directions": row['Direction'],
//        "platform": row['PlatformText']
//    })
//
//debug(f"Total stops in final JSON: {len(output)}")
//
//with open("stops_clean.json", "w", encoding="utf-8") as f:
//    json.dump(output, f, ensure_ascii=False, indent=2)
//debug("Generated stops_clean.json")
