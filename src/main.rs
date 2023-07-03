use std::{error::Error, fs::File, sync::Mutex, collections::HashSet};

use chrono::NaiveDateTime;
use csv::Reader;
use once_cell::sync::Lazy;
use rayon::prelude::*;
use std::time::Instant;

static DIFF: Lazy<Mutex<HashSet<String>>> = Lazy::new(|| {
    Mutex::new(HashSet::new())
});

#[derive(Debug)]
pub struct Row {
    pub time: NaiveDateTime,
    pub sensor: Sensor,
    pub value: SensorValue,
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

#[derive(Debug)]
pub enum SensorValue {
    DewPoint(f32),
    Luminance(f32),
    VocIndex(f32),
    Co2(f32),
    AbsHumidity(f32),
    Rh(f32),
    Temperature(f32),
    VecEqCo2(f32),
}

#[derive(Debug)]
pub struct TargetRow {
    jan: bool,
    feb: bool,
    mar: bool,
    apr: bool,
    may: bool,
    jun: bool,
    jul: bool,
    aug: bool,
    sep: bool,
    oct: bool,
    nov: bool,
    dec: bool,
    day: i8,
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
        let value = match map_sensor_value(field, value) {
            Ok(v) => v,
            Err(e) => return Err(format!("sensor value parse err: {:#?}", e).into()),
        };
        Ok(Self { 
            time,
            sensor, 
            value, 
        })
    }
}

fn map_sensor_value(field: String, value: f32) -> Result<SensorValue, Box<dyn Error>> {
    match field.as_str() {
        "dew_point" => Ok(SensorValue::DewPoint(value)),
        "luminance" => Ok(SensorValue::Luminance(value)),
        "voc_index" => Ok(SensorValue::VocIndex(value)),
        "co2" => Ok(SensorValue::Co2(value)),
        "abs_humidity" => Ok(SensorValue::AbsHumidity(value)),
        "RH" => Ok(SensorValue::Rh(value)),
        "temperature" => Ok(SensorValue::Temperature(value)),
        "voc_eq_co2" => Ok(SensorValue::VecEqCo2(value)),
        _ => Err("can't map sensor value".into()),
    }
}

fn main() {
    
    let now = Instant::now();
    let reader = match read_csv("data/apr_maj_jun_ajdovscina_iaq.csv") {
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

    {
        let diff = DIFF.lock().unwrap();
        println!("diff: {:#?}", diff);
    }
    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);
}


fn read_csv(file: &str) -> Result<Reader<File>, Box<dyn Error>> {
    Ok(csv::Reader::from_path(file)?)
}
