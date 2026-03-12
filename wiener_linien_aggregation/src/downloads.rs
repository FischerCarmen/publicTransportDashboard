use std::collections::HashSet;
use std::fs::File;
use std::io;

fn download_data(path: &str, url: &str){
    let path = path;
    let url = url;

    let mut csv = HashSet::new();

    csv.insert("wienerlinien-ogd-haltepunkte.csv".to_string());
    csv.insert("wienerlinien-ogd-haltestellen.csv".to_string());
    csv.insert("wienerlinien-ogd-linien.csv".to_string());
    csv.insert("wienerlinien-ogd-fahrwegverlaeufe.csv".to_string());


    // Downloads csv from wienerlinien
    for item in csv {
        let request = reqwest::blocking::get(url.to_owned() + &item).expect("request failed");

        let body = request.text().expect("request failed");

        let mut out = File::create(path.to_owned() + &item).expect("failed to create file");
        io::copy(&mut body.as_bytes(), &mut out).expect("failed to copy content");
    }

}
