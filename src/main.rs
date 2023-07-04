use std::{error::Error, fs::File, sync::Mutex, collections::{HashSet, HashMap}, str::FromStr, ops::{Add, SubAssign}};

use chrono::{NaiveDateTime, Timelike, Duration, NaiveDate, NaiveTime, Datelike};
use csv::Reader;
use once_cell::sync::Lazy;
use rayon::prelude::*;
use std::time::Instant;
use dashmap::DashMap;

static LOCATION_DATA: Lazy<Mutex<HashMap<SensorLocation, Vec<SensedPeople>>>> = Lazy::new(|| Mutex::new(HashMap::new()));

static  DATA: Lazy<Mutex<HashMap<SensorLocation, Vec<TargetRow>>>> = Lazy::new(|| Mutex::new(HashMap::new()));

#[derive(Debug)]
pub struct SensorData {
    pub sensor_location: SensorLocation,
    pub value: SensorValue,
}

#[derive(Debug, Clone)]
pub struct Sensor {
    location: SensorLocation,
    dew_point: Option<f32>,
    luminance: Option<f32>,
    voc_index: Option<f32>,
    co2: Option<f32>,
    abs_humidity: Option<f32>,
    rh: Option<f32>,
    temperature: Option<f32>,
    vec_eq_co2: Option<f32>,
}

#[derive(Debug, Clone,PartialEq, Eq, Hash)]
pub enum SensorLocation {
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

#[derive(Debug, Clone)]
pub struct SensedPeople {
    sensor_location: SensorLocation,
    people: i32,
}
impl SensedPeople {
    pub fn from(
        date: Option<&str>,
        time: Option<&str>,
        room: Option<&str>,
        people: Option<&str>
    ) -> Result<Vec<(NaiveDateTime, SensedPeople)>, Box<dyn Error>> {
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
                    (
                        t,
                        SensedPeople {
                            sensor_location: sensor.clone(),
                            people: people.clone(),
                        }
                    )
                })
                .collect::<Vec<(NaiveDateTime, SensedPeople)>>()
        )
    }
}

fn parse_location_sensor(room: Option<&str>) -> Result<SensorLocation, Box<dyn Error>> {
    match room {
        Some(r) => match r {
            "U11" => Ok(SensorLocation::U11),
            "U18" => Ok(SensorLocation::Soba18),
            "U3A" => Ok(SensorLocation::U3a),
            "U4B" => Ok(SensorLocation::U4b),
            "U4C" => Ok(SensorLocation::U4c),
            _ => Err("Error parsing sensor on location".into()),
        },
        None => Err("Error parsing sensor on location - MISSING".into()),
    }
}

fn parse_location_times(date: Option<&str>, time: Option<&str>) -> Result<Vec<NaiveDateTime>, Box<dyn Error>> {
    let (start_time, duration) = match get_slot_time(date, time) {
        Ok(t) => t,
        Err(e) => return Err(format!("Error finding statr time of a time-slot: {}", e).into()),
    };   
    let mut times = vec![];
    for i in 0..duration {
        times.push(start_time.clone().add(Duration::minutes(i)))
    }
    Ok(times)
}

fn get_slot_time(date: Option<&str>, time: Option<&str>) -> Result<(NaiveDateTime, i64), Box<dyn Error>> {
    let t = match time {
        Some(t) => t.parse::<i32>(),
        None => return Err("No time slot defined".into()),
    };

    let date = match date {
        Some(t) => t,
        None => return Err("No time slot defined".into()),
    };
    match t {
        Ok(0)  => Ok((NaiveDateTime::parse_from_str(format!("{}T07:30:00Z", date).as_str(), "%Y-%m-%dT%H:%M:%S%Z")?, 30)),
        Ok(1)  => Ok((NaiveDateTime::parse_from_str(format!("{}T08:00:00Z", date).as_str(), "%Y-%m-%dT%H:%M:%S%Z")?, 50)),
        Ok(2)  => Ok((NaiveDateTime::parse_from_str(format!("{}T08:50:00Z", date).as_str(), "%Y-%m-%dT%H:%M:%S%Z")?, 50)),
        Ok(3)  => Ok((NaiveDateTime::parse_from_str(format!("{}T09:40:00Z", date).as_str(), "%Y-%m-%dT%H:%M:%S%Z")?, 70)),
        Ok(4)  => Ok((NaiveDateTime::parse_from_str(format!("{}T10:50:00Z", date).as_str(), "%Y-%m-%dT%H:%M:%S%Z")?, 50)),
        Ok(5)  => Ok((NaiveDateTime::parse_from_str(format!("{}T11:40:00Z", date).as_str(), "%Y-%m-%dT%H:%M:%S%Z")?, 50)),
        Ok(6)  => Ok((NaiveDateTime::parse_from_str(format!("{}T12:30:00Z", date).as_str(), "%Y-%m-%dT%H:%M:%S%Z")?, 50)),
        Ok(7)  => Ok((NaiveDateTime::parse_from_str(format!("{}T13:20:00Z", date).as_str(), "%Y-%m-%dT%H:%M:%S%Z")?, 50)),
        Ok(8)  => Ok((NaiveDateTime::parse_from_str(format!("{}T14:10:00Z", date).as_str(), "%Y-%m-%dT%H:%M:%S%Z")?, 45)),
        _  => Err("error parsing start time of sime-slot".into()),
    }
}

impl SensorData {
    pub fn from(
        time: Option<&str>, 
        field: Option<&str>, 
        sensor_id: Option<&str>,
        value: Option<&str>
    ) -> Result<(NaiveDateTime, Self), Box<dyn Error>> {
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

        let sensor_location =  match sensor_id {
            Some(s) => match s {
                "aj-00" => SensorLocation::U4c,
                "aj-01" => SensorLocation::Jedilnica,
                "aj-02" => SensorLocation::U4b,
                "aj-03" => SensorLocation::Hodnik,
                "aj-04" => SensorLocation::Soba18,
                "aj-05" => SensorLocation::U11,
                "aj-06" => SensorLocation::U3a,
                "aj-07" => SensorLocation::Zbornica,
                _ => return Err("SensorLocation parse err".to_string().into()),
            },
            None => return Err("SensorLocation parse err 2".to_string().into()),
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
        Ok(
            ( 
                time,
                Self { 
                    sensor_location, 
                    value, 
                }
            )
        )
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



fn parse_sensor_data(reader: Reader<File>) -> Result<DashMap<NaiveDateTime, Vec<Sensor>>, Box<dyn Error>> {
    let records: Vec<_> = reader.into_records().collect();
    let data: DashMap<NaiveDateTime, Vec<Sensor>> = DashMap::new();

    records.par_iter().for_each(|r| {
        let row_record = match r {
            Ok(row) => row,
            Err(e) => {
                println!("Something went wrong reading row: {:#?}", e);
                return;
            },
        };
        let row = match SensorData::from(
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
        let mut sensors = data.entry(row.0).or_insert_with(Vec::new);
        let mut existing_sensor = sensors.iter_mut().find(|s| s.location == row.1.sensor_location);

        if existing_sensor.is_none() {
            sensors.push(Sensor {
                location: row.1.sensor_location.clone(),
                dew_point: None,
                luminance: None,
                voc_index: None,
                co2: None,
                abs_humidity: None,
                rh: None,
                temperature: None,
                vec_eq_co2: None,
            });
            existing_sensor = sensors.last_mut();
        }

        let sensor = existing_sensor.unwrap();
        match row.1.value {
            SensorValue::DewPoint(val) => sensor.dew_point = Some(val),
            SensorValue::Luminance(val) => sensor.luminance = Some(val),
            SensorValue::VocIndex(val) => sensor.voc_index = Some(val),
            SensorValue::Co2(val) => sensor.co2 = Some(val),
            SensorValue::AbsHumidity(val) => sensor.abs_humidity = Some(val),
            SensorValue::Rh(val) => sensor.rh = Some(val),
            SensorValue::Temperature(val) => sensor.temperature = Some(val),
            SensorValue::VecEqCo2(val) => sensor.vec_eq_co2 = Some(val),
        }
    });

    Ok(data)

}

fn parse_location_data(reader: Reader<File>) -> Result<DashMap<NaiveDateTime, Vec<SensedPeople>>, Box<dyn Error>> {
    let data = DashMap::new();
    
    reader
        .into_records()
        .collect::<Vec<_>>()
        .into_par_iter()
        .for_each(|r| {
            let row_record = match r {
                Ok(row) => row,
                Err(e) => {
                    println!("Something went wrong reading row: {:#?}", e);
                    return;
                },
            };
            let row = match SensedPeople::from(
                row_record.get(1),
                row_record.get(3),
                row_record.get(5),
                row_record.get(10)
            ) {
                Ok(r) => r,
                Err(e) => {
                    println!("error parsing row: {:#?}", e);
                    return;
                },
            };
            
            for (datetime, sensed_person) in row {
                data.entry(datetime)
                    .or_insert_with(Vec::new)
                    .push(sensed_person);
            }
        });
    
    Ok(data)
}


fn read_csv(file: &str) -> Result<Reader<File>, Box<dyn Error>> {
    Ok(csv::Reader::from_path(file)?)
}
fn merge_maps(
    people_data: DashMap<NaiveDateTime, Vec<SensedPeople>>,
    sensor_data: DashMap<NaiveDateTime, Vec<Sensor>>,
) -> DashMap<SensorLocation, Vec<(NaiveDateTime, Sensor, SensedPeople)>> {
    let merged: DashMap<SensorLocation, Vec<(NaiveDateTime, Sensor, SensedPeople)>> = DashMap::new();

    // For each minute in the sensor data (not sorted)
    for sensor_ref_multi in sensor_data.iter() {
        // For each sensor record in the current minute
        for sensor in sensor_ref_multi.value() {
            // Get all people records for this minute (might not exist)
            let people_ref_multi = people_data.get(sensor_ref_multi.key());
            // Save the time
            let time = sensor_ref_multi.key();
            // Only collect data from 4am to 4pm
            if time.hour() < 4 || time.hour() >= 16 {
                continue;
            }
            // If there are any people recorded for any sensor at this minute
            if let Some(people_ref_multi) = &people_ref_multi {
                // We'll keep track of whether we've found a SensedPeople struct for the current location
                let mut people_present = false;
                // Go through all the sensed people records
                for sensed_people in people_ref_multi.value() {
                    // If the recording we're looking at matches with the current sensor we're looking at
                    if sensor.location == sensed_people.sensor_location {
                        // Get the target slot in merged hashmap to insert the values
                        // Insert empty vector into merged (if it doesn't already exist) with sensor location as the key
                        let mut entry = merged
                            .entry(sensed_people.sensor_location.clone())
                            .or_insert_with(Vec::new);
                        // Add the merged record to the entry
                        entry.push((
                            *sensor_ref_multi.key(), // time
                            sensor.clone(), // sensor data
                            sensed_people.clone(), // people data
                        ));
                        // We've found a SensedPeople struct for this location
                        people_present = true;
                    }
                }
                // If no SensedPeople struct was found for this location, create one with a count of 0
                if !people_present {
                    // Get the target slot in merged hashmap to insert the values
                    // Insert empty vector into merged (if it doesn't already exist) with sensor location as the key
                    let mut entry = merged
                        .entry(sensor.location.clone())
                        .or_insert_with(Vec::new);
                    // Add the merged record to the entry, creating a new SensedPeople struct with a count of 0
                    entry.push((
                        *sensor_ref_multi.key(),
                        sensor.clone(),
                        SensedPeople {
                            sensor_location: sensor.location.clone(),
                            people: 0,
                        },
                    ));
                }
            } else {
                // If there are no matching people data points, add a new SensedPeople struct with a count of 0
                // Get the target slot in merged hashmap to insert the values
                // Insert empty vector into merged (if it doesn't already exist) with sensor location as the key
                let mut entry = merged
                    .entry(sensor.location.clone())
                    .or_insert_with(Vec::new);
                // Add the merged record to the entry, creating a new SensedPeople struct with a count of 0
                entry.push((
                    *sensor_ref_multi.key(),
                    sensor.clone(),
                    SensedPeople {
                        sensor_location: sensor.location.clone(),
                        people: 0,
                    },
                ));
            }
        }
    }

    merged
}


fn get_sorted_data_for_location(
    data: &DashMap<SensorLocation, Vec<(NaiveDateTime, Sensor, SensedPeople)>>,
    location: &SensorLocation,
) -> Option<Vec<(NaiveDateTime, Sensor, SensedPeople)>> {
    data.get(location).map(|multi_ref| {
        let mut sorted_data = multi_ref.value().clone();
        sorted_data.sort_unstable_by_key(|(date, _, _)| *date);
        sorted_data
    })
}

fn aggregate_by_date(
    data: Vec<(NaiveDateTime, Sensor, SensedPeople)>,
) -> HashMap<NaiveDate, Vec<(NaiveDateTime, Sensor, SensedPeople)>> {
    let mut aggregated: HashMap<NaiveDate, Vec<(NaiveDateTime, Sensor, SensedPeople)>> = HashMap::new();
    
    for tuple in data {
        let date = tuple.0.date();  // extract the date part of the NaiveDateTime
        aggregated.entry(date).or_insert_with(Vec::new).push(tuple);
    }

    aggregated
}

fn find_gaps(
    data: &HashMap<NaiveDate, Vec<(NaiveDateTime, Sensor, SensedPeople)>>,
) -> HashMap<NaiveDate, Vec<(NaiveTime, NaiveTime)>> {
    let mut gaps: HashMap<NaiveDate, Vec<(NaiveTime, NaiveTime)>> = HashMap::new();
    
    let start_time = NaiveTime::from_hms(4, 0, 0);  // start of day at 4am
    let end_time = NaiveTime::from_hms(16, 0, 0);  // end of day at 4pm
    let duration = Duration::minutes(1);  // each time slot is 1 minute

    for (&date, tuples) in data {
        let mut last_time = start_time;
        
        for &(time, _, _) in tuples {
            let expected_time = last_time + duration;
            
            // Check if a gap exists between the expected time and the actual time
            if time.time() > expected_time {
                // A gap exists
                gaps.entry(date)
                    .or_insert_with(Vec::new)
                    .push((last_time, time.time()));
            }

            last_time = time.time();
        }

        // Check for a gap between the last recorded time and the end of day
        if last_time < end_time {
            gaps.entry(date)
                .or_insert_with(Vec::new)
                .push((last_time, end_time));
        }
    }

    gaps
}

fn filter_days_by_gaps(
    mut days: HashMap<NaiveDate, Vec<(NaiveDateTime, Sensor, SensedPeople)>>, 
) -> HashMap<NaiveDate, Vec<(NaiveDateTime, Sensor, SensedPeople)>> {
    let gaps = find_gaps(&days);
    for (date, gaps_day) in gaps.iter() {
        println!("Day: {} - gaps {:#?}", date, gaps_day);
        let big_gaps = gaps_day
            .iter()
            .filter(|(s,e)| e.signed_duration_since(*s).num_minutes() > 2)
            .count();
        if big_gaps > 0 {
            days.remove(date);
        }
    }
    days
}

fn main() {
    
    let now = Instant::now();
    let sensor_reader = match read_csv("data/jan_feb_mar_ajdovscina_iaq.csv") {
        Ok(r) => r,
        Err(e) => return println!("Something went worng reading csv: {:#?}", e),
    };


    let location_data_reader = match read_csv("data/school_data.csv") {
        Ok(r) => r,
        Err(e) => return println!("Something went worng reading csv: {:#?}", e),
    };

    let location_data = match parse_location_data(location_data_reader) {
        Ok(r) => r,
        Err(e) => return println!("Something went worng reading csv: {:#?}", e),
    };

    let sensor_data = match parse_sensor_data(sensor_reader) {
        Ok(r) => r,
        Err(e) => return println!("Something went worng reading csv: {:#?}", e),
    };

    let merged_data = merge_maps(location_data, sensor_data);
    let sorted_data_u3a = match get_sorted_data_for_location(&merged_data, &SensorLocation::U3a) {
        Some(data) => data,
        None => return println!("Something went worng sorting the data"),
    };

    let aggreated_u3a = aggregate_by_date(sorted_data_u3a);
    let aggreated_u3a = filter_days_by_gaps(aggreated_u3a);

    let gaps = find_gaps(&aggreated_u3a);
    
    for (date, gaps_day) in gaps.iter() {
        let big_gaps = gaps_day
            .iter()
            .filter(|(s,e)| e.signed_duration_since(*s).num_minutes() <= 3)
            .count();

        let max_gap = gaps_day
            .iter()
            .map(|(s,e)| 
                e.signed_duration_since(*s).num_minutes()
            ).max();
        println!("weekend: {} {} \t- gaps: {} \t- bigger than 2min: {}\t - max gap: {:?}", date.weekday().num_days_from_monday() > 5, date, gaps_day.len(), big_gaps, max_gap);
    }
    println!("agregated days: {}", aggreated_u3a.keys().len());

    for (date, data) in aggreated_u3a.iter() {
        println!("{:#?} - {:#?}",date,  data.len());
    }
    
    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);
}

