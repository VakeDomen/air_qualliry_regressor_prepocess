use std::{error::Error, io, process, fs::File};

use chrono::NaiveDateTime;
use csv::Reader;
use rayon::prelude::*;
use std::time::Instant;

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
    pub fn from(
        time: Option<&str>, 
        field: Option<&str>, 
        sensor_id: Option<&str>,
        value: Option<&str>
    ) -> Result<Self, Box<dyn Error>> {
        let time_result = match time {
            Some(t) => NaiveDateTime::parse_from_str(t, "%Y-%m-%dT%H:%M:%S%Z"),
            None => return Err("time parse err".to_string().into()),
        };
        let time = match time_result {
            Ok(t) => t,
            Err(e) => return Err(e.into()),
        };

        let field = match field {
            Some(t) => String::from(t),
            None => return Err("field parse err".to_string().into()),
        };

        let sensor =  match sensor_id {
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

        let value = match value {
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
    
    let now = Instant::now();
    let reader = match read_csv("../data/apr_maj_jun_ajdovscina_iaq.csv") {
        Ok(r) => r,
        Err(e) => return println!("Something went worng reading csv: {:#?}", e),
    };



    // Assuming `reader` is a Vector
    let records: Vec<_> = reader.into_records().collect();

    records.par_iter().for_each(|r| {
        let row_record = match r {
            Ok(row) => row,
            Err(e) => {
                println!("Something went wrong reading row: {:#?}", e);
                return;
            },
        };
        let row = match Row::from(
            row_record.get(3),
            row_record.get(4),
            row_record.get(6),
            row_record.get(7),
        ) {
            Ok(r) => r,
            Err(e) => {
                println!("error parsing row: {:#?}", e);
                return;
            },
        }; 
    });


    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);
}


fn read_csv(file: &str) -> Result<Reader<File>, Box<dyn Error>> {
    Ok(csv::Reader::from_path(file)?)
}
