use std::{error::Error, fs::File, sync::Mutex, collections::{HashSet, HashMap}, str::FromStr, ops::Add};

use chrono::{NaiveDateTime, Timelike, Duration};
use csv::Reader;
use once_cell::sync::Lazy;
use rayon::prelude::*;
use std::time::Instant;

static LOCATION_DATA: Lazy<Mutex<HashMap<Sensor, Vec<LocationRow>>>> = Lazy::new(|| Mutex::new(HashMap::new()));

static  DATA: Lazy<Mutex<HashMap<Sensor, Vec<TargetRow>>>> = Lazy::new(|| Mutex::new(HashMap::new()));

#[derive(Debug)]
pub struct Row {
    pub time: NaiveDateTime,
    pub sensor: Sensor,
    pub value: SensorValue,
}



#[derive(Debug, Clone)]
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

#[derive(Debug)]
pub struct LocationRow {
    date: NaiveDateTime,
    sensor: Sensor,
    people: i32,
}
impl LocationRow {
    pub fn from(
        date: Option<&str>,
        time: Option<&str>,
        room: Option<&str>,
        people: Option<&str>
    ) -> Result<Vec<LocationRow>, Box<dyn Error>> {
        let times: Vec<NaiveDateTime> = match parse_location_times(date, time) {
            Ok(t) => t,
            Err(e) => return Err(format!("Error paring times for location: {:#?}", e).into()),
        };
        let sensor = match parse_location_sensor(room) {
            Ok(s) => s,
            Err(e) => return Err(format!("Error paring sensors for location: {:#?}", e).into()),
        };
        let people = match people {
            Some(p) => match p.parse::<i32>() {
                Ok(p) => p,
                Err(e) => return Err(format!("Error paring people for location: {:#?}", e).into()),
            },
            None => return Err(format!("Error paring peope for location-missing:").into()),
        };
        Ok(
            times
                .into_iter()
                .map(|t| {
                    LocationRow {
                        date: t,
                        sensor: sensor.clone(),
                        people: people.clone(),
                    }
                })
                .collect::<Vec<LocationRow>>()
        )
    }
}

fn parse_location_sensor(room: Option<&str>) -> Result<Sensor, Box<dyn Error>> {
    match room {
        Some(r) => match r {
            "U11" => Ok(Sensor::U11),
            "U18" => Ok(Sensor::Soba18),
            "U3A" => Ok(Sensor::U3a),
            "U4B" => Ok(Sensor::U4b),
            "U4C" => Ok(Sensor::U4c),
            _ => Err("Error parsing sensor on location".into()),
        },
        None => Err("Error parsing sensor on location - MISSING".into()),
    }
}

fn parse_location_times(date: Option<&str>, time: Option<&str>) -> Result<Vec<NaiveDateTime>, Box<dyn Error>> {
    let start_time = match get_start_time(date, time) {
        Ok(t) => t,
        Err(e) => return Err(format!("Error finding statr time of a time-slot: {}", e).into()),
    };

    let slot_len_in_minutes = if start_time.hour() == 7 {
        25
    } else {
        45
    };
    
    let mut times = vec![];
    for i in 0..slot_len_in_minutes {
        times.push(start_time.clone().add(Duration::minutes(i)))
    }
    Ok(times)
}

fn get_start_time(date: Option<&str>, time: Option<&str>) -> Result<NaiveDateTime, Box<dyn Error>> {
    let t = match time {
        Some(t) => t.parse::<i32>(),
        None => return Err("No time slot defined".into()),
    };

    let date = match date {
        Some(t) => t,
        None => return Err("No time slot defined".into()),
    };
    match t {
        Ok(0)  => Ok(NaiveDateTime::parse_from_str(format!("{}T07:30:00Z", date).as_str(), "%Y-%m-%dT%H:%M:%S%Z")?),
        Ok(1)  => Ok(NaiveDateTime::parse_from_str(format!("{}T08:00:00Z", date).as_str(), "%Y-%m-%dT%H:%M:%S%Z")?),
        Ok(2)  => Ok(NaiveDateTime::parse_from_str(format!("{}T08:50:00Z", date).as_str(), "%Y-%m-%dT%H:%M:%S%Z")?),
        Ok(3)  => Ok(NaiveDateTime::parse_from_str(format!("{}T09:40:00Z", date).as_str(), "%Y-%m-%dT%H:%M:%S%Z")?),
        Ok(4)  => Ok(NaiveDateTime::parse_from_str(format!("{}T10:50:00Z", date).as_str(), "%Y-%m-%dT%H:%M:%S%Z")?),
        Ok(5)  => Ok(NaiveDateTime::parse_from_str(format!("{}T11:40:00Z", date).as_str(), "%Y-%m-%dT%H:%M:%S%Z")?),
        Ok(6)  => Ok(NaiveDateTime::parse_from_str(format!("{}T12:30:00Z", date).as_str(), "%Y-%m-%dT%H:%M:%S%Z")?),
        Ok(7)  => Ok(NaiveDateTime::parse_from_str(format!("{}T13:20:00Z", date).as_str(), "%Y-%m-%dT%H:%M:%S%Z")?),
        Ok(8)  => Ok(NaiveDateTime::parse_from_str(format!("{}T14:10:00Z", date).as_str(), "%Y-%m-%dT%H:%M:%S%Z")?),
        _  => Err("error parsing start time of sime-slot".into()),
    }
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


    let location_data_reader = match read_csv("data/school_data.csv") {
        Ok(r) => r,
        Err(e) => return println!("Something went worng reading csv: {:#?}", e),
    };

    let data = parse_location_data(location_data_reader);

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

fn parse_location_data(reader: Reader<File>) -> Result<Vec<LocationRow>, Box<dyn Error>> {
    Ok(
        reader
            .into_records()
            .collect::<Vec<_>>()
            .into_par_iter()
            .filter_map(|r| {
                let row_record = match r {
                    Ok(row) => row,
                    Err(e) => {
                        println!("Something went wrong reading row: {:#?}", e);
                        return None;
                    },
                };
                let row = match LocationRow::from(
                    row_record.get(1),
                    row_record.get(3),
                    row_record.get(5),
                    row_record.get(9)
                ) {
                    Ok(r) => r,
                    Err(e) => {
                        println!("error parsing row: {:#?}", e);
                        return None;
                    },
                };
                Some(row)
            })
            .flatten()
            .collect::<Vec<LocationRow>>()
    )
}


fn read_csv(file: &str) -> Result<Reader<File>, Box<dyn Error>> {
    Ok(csv::Reader::from_path(file)?)
}
