use std::{error::Error, io, process, fs::File};

use chrono::NaiveDateTime;
use csv::Reader;


#[derive(Debug)]
pub struct RawRow<'a> {
    pub time: Option<&'a str>,
    pub field: Option<&'a str>,
    pub sensor_id: Option<&'a str>,
    pub value: Option<&'a str>,
}


#[derive(Debug)]
pub struct Row {
    pub time: NaiveDateTime,
    pub field: String,
    pub sensor: Sensor,
    pub value: f32,
}

#[derive(Debug)]
pub enum Sensor {
    U4c,
    Jedilnica,
    U4b,
    Hodnik,
    Soba18,
    U11,
    U3a,
    Zbornica,
}

impl Row {
    pub fn from(raw: RawRow) -> Result<Self, Box<dyn Error>> {
        let time_result = match raw.time {
            Some(t) => NaiveDateTime::parse_from_str(t, "%Y-%m-%dT%H:%M:%S%Z"),
            None => return Err("time parse err".to_string().into()),
        };
        let time = match time_result {
            Ok(t) => t,
            Err(e) => return Err(e.into()),
        };

        let field = match raw.field {
            Some(t) => String::from(t),
            None => return Err("field parse err".to_string().into()),
        };

        let sensor =  match raw.sensor_id {
            Some(s) => match s {
                "aj-00" => Sensor::U4c,
                "aj-01" => Sensor::Jedilnica,
                "aj-02" => Sensor::U4b,
                "aj-03" => Sensor::Hodnik,
                "aj-04" => Sensor::Soba18,
                "aj-05" => Sensor::U11,
                "aj-06" => Sensor::U3a,
                "aj-07" => Sensor::Zbornica,
                _ => return Err("Sensor parse err".to_string().into()),
            },
            None => return Err("Sensor parse err 2".to_string().into()),
        };

        let value = match raw.value {
            Some(v) => match v.parse::<f32>() {
                Ok(v) => v,
                Err(e) => return Err(format!("parse err: {:#?}", e).into()),
            },
            None => return Err("value parse err".to_string().into()),
        };
        
        Ok(Self { 
            time,
            field, 
            sensor, 
            value, 
        })
    }
}

fn main() {
    let reader = match read_csv("../data/apr_maj_jun_ajdovscina_iaq.csv") {
        Ok(r) => r,
        Err(e) => return println!("Something went worng reading csv: {:#?}", e),
    };



    for r in reader.into_records() {
        let row = match r {
            Ok(row) => row,
            Err(e) => return println!("Something went worng reading row: {:#?}", e),
        };
        let rawrow = RawRow {
            time: row.get(3),
            field: row.get(4),
            sensor_id: row.get(6),
            value: row.get(7),
        };
        let _structured_row = match Row::from(rawrow) {
            Ok(r) => r,
            Err(e) => {
                println!("error parsing row: {:#?}", e);
                continue;
            },
        }; 
    }
}


fn read_csv(file: &str) -> Result<Reader<File>, Box<dyn Error>> {
    Ok(csv::Reader::from_path(file)?)
}
