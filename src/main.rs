mod scalers;

use std::{error::Error, fs::{File, self}, sync::Mutex, collections::HashMap, ops::Add, path::Path};
use crate::scalers::robust_scaler::RobustScaler;
use chrono::{NaiveDateTime, Timelike, Duration, NaiveDate, NaiveTime, Datelike};
use csv::Reader;
use once_cell::sync::Lazy;
use rand::{seq::SliceRandom, rngs::StdRng, SeedableRng};
use rayon::prelude::*;
use serde::Serialize;
use std::time::Instant;
use dashmap::DashMap;


static FOLDS: i32 = 10;
static START_HOUR: u32 = 3;
static END_HOUR: u32 = 16;
static WINDOW_SIZE: usize = 180;
const SEED: [u8; 32] = [42; 32];


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

#[derive(Debug, Clone, Serialize)]
pub struct TargetRow {
    window_id: i32,
    jan: f32,
    feb: f32,
    mar: f32,
    apr: f32,
    may: f32,
    jun: f32,
    jul: f32,
    aug: f32,
    sep: f32,
    oct: f32,
    nov: f32,
    dec: f32,
    day: f32,
    time: f32,
    dew_point: f32,
    luminance: f32,
    voc_index: f32,
    co2: f32,
    abs_humidity: f32,
    rh: f32,
    temperature: f32,
    vec_eq_co2: f32,
    outside_temperature: f32,
    avg_temperature: f32,
    min_temperature: f32,
    max_temperature: f32,
    rel_humidity: f32,
    avg_rel_humidity: f32,
    min_rel_humidity: f32,
    max_rel_humidity: f32,
    precipitation: f32,
    wind_speed: f32,
    people: f32,
}

#[derive(Debug, Clone)]
pub struct SensedPeople {
    sensor_location: SensorLocation,
    people: i32,
}

#[derive(Debug, Clone)]
pub struct WeatherPoint {
    pub temperature: f32,
    pub avg_temperature: f32,
    pub min_temperature: f32,
    pub max_temperature: f32,
    pub rel_humidity: f32,
    pub avg_rel_humidity: f32,
    pub min_rel_humidity: f32,
    pub max_rel_humidity: f32,
    pub precipitation: f32,
    pub wind_speed: f32,
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
                println!("error parsing sensor row: {:#?}", e);
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

fn parse_weather_data(reader: Reader<File>) -> Result<DashMap<NaiveDateTime, WeatherPoint>, Box<dyn Error>> {
    let mut records = reader.into_records();
    let data: DashMap<NaiveDateTime, WeatherPoint> = DashMap::new();
    let mut prev_record: Option<(NaiveDateTime, WeatherPoint)> = None;

    for result in records.skip(1) {
        let record = result?;
        let timestamp = NaiveDateTime::parse_from_str(record.get(2).unwrap_or_default(), "%Y-%m-%d %H:%M")?;
        let weather_point = WeatherPoint {
            temperature: record.get(3).unwrap_or_default().parse::<f32>()?,
            avg_temperature: record.get(4).unwrap_or_default().parse::<f32>()?,
            min_temperature: record.get(5).unwrap_or_default().parse::<f32>()?,
            max_temperature: record.get(6).unwrap_or_default().parse::<f32>()?,
            rel_humidity: record.get(7).unwrap_or_default().parse::<f32>()?,
            avg_rel_humidity: record.get(8).unwrap_or_default().parse::<f32>()?,
            min_rel_humidity: record.get(9).unwrap_or_default().parse::<f32>()?,
            max_rel_humidity: record.get(10).unwrap_or_default().parse::<f32>()?,
            precipitation: record.get(11).unwrap_or_default().parse::<f32>()?,
            wind_speed: record.get(12).unwrap_or_default().parse::<f32>()?,
        };
        
        if let Some((prev_time, prev_data)) = &prev_record {
            let duration = timestamp - *prev_time;
            let minutes = duration.num_minutes();
            
            for i in 1..minutes {
                let fraction = i as f32 / minutes as f32;
                let interp_time = *prev_time + chrono::Duration::minutes(i);
                let interp_point = interpolate_weather_points(&prev_data, &weather_point, fraction);
                data.insert(interp_time, interp_point);
            }
        }

        data.insert(timestamp, weather_point.clone());
        prev_record = Some((timestamp, weather_point));
    }

    Ok(data)
}

fn interpolate_weather_points(a: &WeatherPoint, b: &WeatherPoint, fraction: f32) -> WeatherPoint {
    WeatherPoint {
        temperature: a.temperature + fraction * (b.temperature - a.temperature),
        avg_temperature: a.avg_temperature + fraction * (b.avg_temperature - a.avg_temperature),
        min_temperature: a.min_temperature + fraction * (b.min_temperature - a.min_temperature),
        max_temperature: a.max_temperature + fraction * (b.max_temperature - a.max_temperature),
        rel_humidity: a.rel_humidity + fraction * (b.rel_humidity - a.rel_humidity),
        avg_rel_humidity: a.avg_rel_humidity + fraction * (b.avg_rel_humidity - a.avg_rel_humidity),
        min_rel_humidity: a.min_rel_humidity + fraction * (b.min_rel_humidity - a.min_rel_humidity),
        max_rel_humidity: a.max_rel_humidity + fraction * (b.max_rel_humidity - a.max_rel_humidity),
        precipitation: a.precipitation + fraction * (b.precipitation - a.precipitation),
        wind_speed: a.wind_speed + fraction * (b.wind_speed - a.wind_speed),
    }
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


fn merge_maps_updated(
    people_data: DashMap<NaiveDateTime, Vec<SensedPeople>>,
    sensor_data: DashMap<NaiveDateTime, Vec<Sensor>>,
    weather_data: DashMap<NaiveDateTime, WeatherPoint>,
) -> DashMap<SensorLocation, Vec<(NaiveDateTime, Sensor, SensedPeople, WeatherPoint)>> {
    let merged: DashMap<SensorLocation, Vec<(NaiveDateTime, Sensor, SensedPeople, WeatherPoint)>> = DashMap::new();
    
    for sensor_ref in sensor_data.iter() {
        let current_sensors_minute = sensor_ref.key();
        let current_sensors_minute_values = sensor_ref.value();
        let current_minute_sensed_people_ref = people_data.get(current_sensors_minute);
        let current_weather_data = match weather_data.get(current_sensors_minute) {
            Some(d) => d.value().clone(),
            None => continue,
        };
        if current_sensors_minute.hour() < 4 || current_sensors_minute.hour() >= 16 {
            continue;
        }

        for sensor in current_sensors_minute_values.iter() {
            let current_minute_sensed_people = match current_minute_sensed_people_ref {
                Some(ref p) => Some(p.value().clone()),
                // will trigger for first sensor each minute
                None => None,
            };

            let current_minute_sensed_people_for_current_sensor = match current_minute_sensed_people {
                Some(p) => p.iter().filter(|s| s.sensor_location == sensor.location).nth(0).cloned(),
                None => None,
            };

            let people = match current_minute_sensed_people_for_current_sensor {
                Some(p) => p.clone(),
                None => SensedPeople {
                    sensor_location: sensor.location.clone(),
                    people: 0,
                },
            };

            let mut entry = merged
                .entry(people.sensor_location.clone())
                .or_insert_with(Vec::new);
            // Add the merged record to the entry
            entry.push((
                *current_sensors_minute, // time
                sensor.clone(), // sensor data
                people, // people data
                current_weather_data.clone(),
            ));
        }
    }
    merged
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
            if time.hour() < START_HOUR || time.hour() >= END_HOUR {
                continue;
            }
            // If there are any people recorded for this sensor at this minute
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
    data: &DashMap<SensorLocation, Vec<(NaiveDateTime, Sensor, SensedPeople, WeatherPoint)>>,
    location: &SensorLocation,
) -> Option<Vec<(NaiveDateTime, Sensor, SensedPeople, WeatherPoint)>> {
    data.get(location).map(|multi_ref| {
        let mut sorted_data = multi_ref.value().clone();
        sorted_data.sort_unstable_by_key(|(date, _, _, _)| *date);
        sorted_data
    })
}

fn aggregate_by_date(
    data: Vec<(NaiveDateTime, Sensor, SensedPeople, WeatherPoint)>,
) -> HashMap<NaiveDate, Vec<(NaiveDateTime, Sensor, SensedPeople, WeatherPoint)>> {
    let mut aggregated: HashMap<NaiveDate, Vec<(NaiveDateTime, Sensor, SensedPeople, WeatherPoint)>> = HashMap::new();
    
    for tuple in data {
        let date = tuple.0.date();  // extract the date part of the NaiveDateTime
        aggregated.entry(date).or_insert_with(Vec::new).push(tuple);
    }

    aggregated
}

fn find_gaps(
    data: &HashMap<NaiveDate, Vec<(NaiveDateTime, Sensor, SensedPeople, WeatherPoint)>>,
) -> HashMap<NaiveDate, Vec<(NaiveTime, NaiveTime)>> {
    let mut gaps: HashMap<NaiveDate, Vec<(NaiveTime, NaiveTime)>> = HashMap::new();
    
    let start_time = NaiveTime::from_hms(4, 0, 0);  // start of day at 4am
    let end_time = NaiveTime::from_hms(16, 0, 0);  // end of day at 4pm
    let duration = Duration::minutes(1);  // each time slot is 1 minute

    for (&date, tuples) in data {
        let mut last_time = start_time;
        
        for &(time, _, _, _) in tuples {
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
    mut days: HashMap<NaiveDate, Vec<(NaiveDateTime, Sensor, SensedPeople, WeatherPoint)>>, 
) -> HashMap<NaiveDate, Vec<(NaiveDateTime, Sensor, SensedPeople, WeatherPoint)>> {
    let gaps = find_gaps(&days);
    for (date, gaps_day) in gaps.iter() {
        // println!("Day: {} - gaps {:#?}", date, gaps_day);
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

fn generate_windows(
    data: &HashMap<NaiveDate, Vec<(NaiveDateTime, Sensor, SensedPeople, WeatherPoint)>>, 
    window_size: usize
) -> HashMap<NaiveDate, Vec<Vec<(NaiveDateTime, Sensor, SensedPeople, WeatherPoint)>>> {
    let mut windowed_data: HashMap<NaiveDate, Vec<Vec<(NaiveDateTime, Sensor, SensedPeople, WeatherPoint)>>> = HashMap::new();

    for (&date, tuples) in data {
        let mut windows = Vec::new();
        for i in 0..(tuples.len() - window_size) {
            windows.push(tuples[i..(i + window_size)].to_vec());
        }
        windowed_data.insert(date, windows);
    }

    windowed_data
}

fn structure_data(
    merged_data: DashMap<SensorLocation, Vec<(NaiveDateTime, Sensor, SensedPeople, WeatherPoint)>>
) ->  HashMap<SensorLocation, HashMap<NaiveDate, Vec<Vec<(NaiveDateTime, Sensor, SensedPeople, WeatherPoint)>>>> {
    // define a hashmap to hold all the data
    let mut data: HashMap<SensorLocation, HashMap<NaiveDate, Vec<Vec<(NaiveDateTime, Sensor, SensedPeople, WeatherPoint)>>>> = HashMap::new();

    // loop over all locations
    for ref_location in merged_data.iter() {
        let location = ref_location.key();
        // get sorted data for the current location
        let location_data = match get_sorted_data_for_location(&merged_data, location) {
            Some(data) => data,
            None => {
                println!("Something went wrong sorting the data for location: {:?}", location);
                continue;
            },
        };

        // aggregate, filter and generate windows for the data
        let location_data = aggregate_by_date(location_data);
        let location_data = filter_days_by_gaps(location_data);
        let location_data = generate_windows(&location_data, WINDOW_SIZE);

        // store the windowed data in the hashmap
        data.insert(location.clone(), location_data);
    }
    data
}

fn restructure_data_to_output(
    data: HashMap<SensorLocation, HashMap<NaiveDate, Vec<Vec<(NaiveDateTime, Sensor, SensedPeople, WeatherPoint)>>>>,
) -> Vec<Vec<TargetRow>> {
    let mut window_id = 1;
    let mut result: Vec<Vec<TargetRow>> = Vec::new();

    for (_, date_map) in data {
        for (date, windows) in date_map {
            let mut date_rows: Vec<TargetRow> = Vec::new();

            for (_, window) in windows.into_iter().enumerate() {
                let window_rows: Vec<TargetRow> = window
                    .into_iter()
                    .map(|(ndt, sensor, sensed_people, weather)| {
                        TargetRow {
                            window_id: window_id as i32,
                            jan: if date.month() == 1 {1.} else {0.},
                            feb: if date.month() == 2 {1.} else {0.},
                            mar: if date.month() == 3 {1.} else {0.},
                            apr: if date.month() == 4 {1.} else {0.},
                            may: if date.month() == 5 {1.} else {0.},
                            jun: if date.month() == 6 {1.} else {0.},
                            jul: if date.month() == 7 {1.} else {0.},
                            aug: if date.month() == 8 {1.} else {0.},
                            sep: if date.month() == 9 {1.} else {0.},
                            oct: if date.month() == 10 {1.} else {0.},
                            nov: if date.month() == 11 {1.} else {0.},
                            dec: if date.month() == 12 {1.} else {0.},
                            day: date.day() as f32,
                            time: (ndt.num_seconds_from_midnight() / 60) as f32,
                            dew_point: sensor.dew_point.unwrap_or_default(),
                            luminance: sensor.luminance.unwrap_or_default(),
                            voc_index: sensor.voc_index.unwrap_or_default(),
                            co2: sensor.co2.unwrap_or_default(),
                            abs_humidity: sensor.abs_humidity.unwrap_or_default(),
                            rh: sensor.rh.unwrap_or_default(),
                            temperature: sensor.temperature.unwrap_or_default(),
                            vec_eq_co2: sensor.vec_eq_co2.unwrap_or_default(),
                            outside_temperature: weather.temperature,
                            avg_temperature: weather.avg_temperature,
                            min_temperature: weather.min_temperature,
                            max_temperature: weather.max_temperature,
                            rel_humidity: weather.rel_humidity,
                            avg_rel_humidity: weather.avg_rel_humidity,
                            min_rel_humidity: weather.min_rel_humidity,
                            max_rel_humidity: weather.max_rel_humidity,
                            precipitation: weather.precipitation,
                            wind_speed: weather.wind_speed,
                            people: sensed_people.people as f32,
                        }
                    })
                    .collect();
                window_id += 1;
                date_rows.extend(window_rows);
            }

            result.push(date_rows);
        }
    }

    result // Collect all the data into a single vector
}


fn shuffle_and_split_into_folds(mut data: Vec<Vec<TargetRow>>, folds: i32) -> Vec<Vec<Vec<TargetRow>>> {
    // Create a mutable reference to data and shuffle it
    let mut rng = StdRng::from_seed(SEED);
    data.shuffle(&mut rng);

    // Calculate the size of each fold
    let fold_size = data.len() / folds as usize;

    // Create the resulting vector of folds
    let mut result = Vec::new();

    // Create each fold by slicing the shuffled data
    for i in 0..folds {
        let start_index = (i as usize) * fold_size;
        let end_index = if i == folds - 1 {
            data.len() // If this is the last fold, take all the remaining data
        } else {
            start_index + fold_size
        };

        // Add the fold to the result
        result.push(data[start_index..end_index].to_vec());
    }

    result
}

fn export_fold(data: Vec<TargetRow>, file: File) -> std::io::Result<()> {
    let mut writer = csv::Writer::from_writer(file);

    for row in data {
        writer.serialize(row)?;
    }

    writer.flush()
}

fn export_data(folded_data: Vec<Vec<Vec<TargetRow>>>) -> Result<(), Box<String>> {
    let dash_data: DashMap<usize, Vec<Vec<TargetRow>>> = DashMap::new();
    let num_of_folds = folded_data.len();
    for (i, fold) in folded_data.into_iter().enumerate() {
        dash_data.insert(i, fold);
    }

    (0..num_of_folds)
        .collect::<Vec<usize>>()
        .into_par_iter()
        .enumerate()
        .try_for_each(|(fold_index, _data)| {
            // Create fold directory
            let fold_dir = format!("out/fold_{}", fold_index + 1);
            println!("Constructing: {}", fold_dir);
            if let Err(e) = fs::create_dir_all(&fold_dir) {
                return Err(Box::new(e.to_string()));
            };
            
            let mut training_data: Vec<Vec<TargetRow>> = Vec::new();

            for i in 0..num_of_folds {
                if i == fold_index {
                    let value = {

                        let test_data = match dash_data.get(&i) {
                            Some(d) => d,
                            None => todo!(),
                        };
                        test_data.value().clone()
                    };

                    println!("Writing test data {}", fold_dir);
                    // if let Err(e) = pickle::to_writer(&mut test_file, &value, SerOptions::default()) {
                    //     return Err(Box::new(e.to_string()));
                    // };
                    let test_file = match fs::File::create(Path::new(&fold_dir).join("test.csv")) {
                        Ok(d) => d,
                        Err(e) =>  return Err(Box::new(e.to_string())),
                    };
                    if let Err(e) = export_fold(
                        value.into_iter().flatten().collect::<Vec<TargetRow>>(), 
                        test_file
                    ) {
                        println!("Error saving test data {}: {}", fold_dir, e.to_string());
                    };
                } else {
                    let target_fold = (fold_index + i) % num_of_folds;
                    match dash_data.get(&i) {
                        Some(d) => training_data.extend_from_slice(d.value()),
                        None => return Err(Box::new(format!("data not found {} {}", target_fold, fold_index))),
                    }
                }
            }
            println!("Writing train data {}", fold_dir);
            // if let Err(e) = pickle::to_writer(&mut train_file, &training_data, SerOptions::default()) {
            //     return Err(Box::new(e.to_string()));
            // };
            let train_file = match fs::File::create(Path::new(&fold_dir).join("train.csv")) {
                Ok(d) => d,
                Err(e) =>  return Err(Box::new(e.to_string())),
            };

            if let Err(e) = export_fold(
                training_data.into_iter().flatten().collect::<Vec<TargetRow>>(), 
                train_file
            ) {
                println!("Error saving test data {}: {}", fold_dir, e.to_string());
            };

            Ok(())
        })
}

pub fn scale_sensor_data(data: &DashMap<NaiveDateTime, Vec<Sensor>>) -> DashMap<NaiveDateTime, Vec<Sensor>> {
    let mut scaled_data = DashMap::new();
    
    let sensors: Vec<Sensor> = data.iter().flat_map(|item| item.value().clone()).collect();

    let dew_point_scaler = RobustScaler::new(
        sensors.iter().filter_map(|sensor| sensor.dew_point).collect::<Vec<_>>().as_slice()
    );
    let luminance_scaler = RobustScaler::new(
        sensors.iter().filter_map(|sensor| sensor.luminance).collect::<Vec<_>>().as_slice()
    );
    let voc_index_scaler = RobustScaler::new(
        sensors.iter().filter_map(|sensor| sensor.voc_index).collect::<Vec<_>>().as_slice()
    );
    let co2_scaler = RobustScaler::new(
        sensors.iter().filter_map(|sensor| sensor.co2).collect::<Vec<_>>().as_slice()
    );
    let abs_humidity_scaler = RobustScaler::new(
        sensors.iter().filter_map(|sensor| sensor.abs_humidity).collect::<Vec<_>>().as_slice()
    );
    let temperature_scaler = RobustScaler::new(
        sensors.iter().filter_map(|sensor| sensor.temperature).collect::<Vec<_>>().as_slice()
    );
    let vec_eq_co2_scaler = RobustScaler::new(
        sensors.iter().filter_map(|sensor| sensor.vec_eq_co2).collect::<Vec<_>>().as_slice()
    );

    for item in data.iter() {
        let date_time = item.key().clone();
        let sensors = item.value().clone();

        let scaled_sensors = sensors.into_iter().map(|sensor| {
            Sensor {
                location: sensor.location.clone(),
                dew_point: sensor.dew_point.map(|value| dew_point_scaler.transform(value)),
                luminance: sensor.luminance.map(|value| luminance_scaler.transform(value)),
                voc_index: sensor.voc_index.map(|value| voc_index_scaler.transform(value)),
                co2: sensor.co2.map(|value| co2_scaler.transform(value)),
                abs_humidity: sensor.abs_humidity.map(|value| abs_humidity_scaler.transform(value)),
                rh: sensor.rh,  // Not scaling RH as it's already bounded between 0 and 100
                temperature: sensor.temperature.map(|value| temperature_scaler.transform(value)),
                vec_eq_co2: sensor.vec_eq_co2.map(|value| vec_eq_co2_scaler.transform(value)),
            }
        }).collect();

        scaled_data.insert(date_time, scaled_sensors);
    }
    
    scaled_data
}

pub fn scale_weather_data(data: &DashMap<NaiveDateTime, WeatherPoint>) -> DashMap<NaiveDateTime, WeatherPoint> {
    let mut scaled_data = DashMap::new();
    
    let temperature_scaler = RobustScaler::new(
        data.iter().map(|w| w.temperature).collect::<Vec<_>>().as_slice()
    );
    let avg_temperature_scaler = RobustScaler::new(
        data.iter().map(|w| w.avg_temperature).collect::<Vec<_>>().as_slice()
    );
    let min_temperature_scaler = RobustScaler::new(
        data.iter().map(|w| w.min_temperature).collect::<Vec<_>>().as_slice()
    );
    let max_temperature_scaler = RobustScaler::new(
        data.iter().map(|w| w.max_temperature).collect::<Vec<_>>().as_slice()
    );
    let rel_humidity_scaler = RobustScaler::new(
        data.iter().map(|w| w.rel_humidity).collect::<Vec<_>>().as_slice()
    );
    let avg_rel_humidity_scaler = RobustScaler::new(
        data.iter().map(|w| w.avg_rel_humidity).collect::<Vec<_>>().as_slice()
    );
    let min_rel_humidity_scaler = RobustScaler::new(
        data.iter().map(|w| w.min_rel_humidity).collect::<Vec<_>>().as_slice()
    );
    let max_rel_humidity_scaler = RobustScaler::new(
        data.iter().map(|w| w.max_rel_humidity).collect::<Vec<_>>().as_slice()
    );
    let precipitation_scaler = RobustScaler::new(
        data.iter().map(|w| w.precipitation).collect::<Vec<_>>().as_slice()
    );
    let wind_speed_scaler = RobustScaler::new(
        data.iter().map(|w| w.wind_speed).collect::<Vec<_>>().as_slice()
    );
    

    for item in data.iter() {

        let date_time = item.key().clone();
        let sensor = item.value().clone();

        
        let scaled_sensor = WeatherPoint {
            temperature: temperature_scaler.transform(sensor.temperature),
            avg_temperature: avg_temperature_scaler.transform(sensor.avg_temperature),
            min_temperature: min_temperature_scaler.transform(sensor.min_temperature),
            max_temperature: max_temperature_scaler.transform(sensor.max_temperature),
            rel_humidity: rel_humidity_scaler.transform(sensor.rel_humidity),
            avg_rel_humidity: avg_rel_humidity_scaler.transform(sensor.avg_rel_humidity),
            min_rel_humidity: min_rel_humidity_scaler.transform(sensor.min_rel_humidity),
            max_rel_humidity: max_rel_humidity_scaler.transform(sensor.max_rel_humidity),
            precipitation: precipitation_scaler.transform(sensor.precipitation),
            wind_speed: wind_speed_scaler.transform(sensor.wind_speed),
        };

        scaled_data.insert(date_time, scaled_sensor);
    }
    
    scaled_data
}



fn main() {
    
    let now = Instant::now();
    let (sensor_reader_1, sensor_reader_2, location_data_reader, weather_data_reader_1, weather_data_reader_2) = get_readers();
    let (sensor_data, location_data, weather_data) = get_data(sensor_reader_1, sensor_reader_2, location_data_reader, weather_data_reader_1, weather_data_reader_2);

    let sensor_data = scale_sensor_data(&sensor_data);
    let weather_data = scale_weather_data(&weather_data);

    let elapsed = now.elapsed();
    println!("Parsing from file: {:.2?}", elapsed);
    let resturcture = Instant::now();

    let data = merge_maps_updated(location_data, sensor_data, weather_data);
    data.remove(&SensorLocation::Jedilnica);
    data.remove(&SensorLocation::Hodnik);
    data.remove(&SensorLocation::Zbornica);

    let data = structure_data(data);
    let data = restructure_data_to_output(data);
    
    let data: Vec<Vec<Vec<TargetRow>>> = shuffle_and_split_into_folds(data, FOLDS); 
    
    let elapsed = resturcture.elapsed();
    println!("Resturcture: {:.2?}", elapsed);
    let export = Instant::now();


    if let Err(e) = export_data(data) {
        println!("Error when saving folded data: {:#?}", e.to_string());
    }

    let elapsed = export.elapsed();
    println!("Export: {:.2?}", elapsed);
    let elapsed = now.elapsed();
    println!("Total: {:.2?}", elapsed);
}

fn get_readers() -> (Reader<File>, Reader<File>, Reader<File>, Reader<File>, Reader<File>) {
    let sensor_reader_1 = match read_csv("data/jan_feb_mar_ajdovscina_iaq.csv") {
        Ok(r) => r,
        Err(e) => panic!("Something went worng reading csv: {:#?}", e),
    };

    let sensor_reader_2 = match read_csv("data/apr_maj_jun_ajdovscina_iaq.csv") {
        Ok(r) => r,
        Err(e) => panic!("Something went worng reading csv: {:#?}", e),
    };

    let location_data_reader = match read_csv("data/school_data.csv") {
        Ok(r) => r,
        Err(e) => panic!("Something went worng reading csv: {:#?}", e),
    };

    let weather_data_reader_1 = match read_csv("data/vreme_jan_feb_mar.csv") {
        Ok(r) => r,
        Err(e) => panic!("Something went worng reading csv: {:#?}", e),
    };

    let weather_data_reader_2 = match read_csv("data/vreme_apr_maj_jun.csv") {
        Ok(r) => r,
        Err(e) => panic!("Something went worng reading csv: {:#?}", e),
    };
    (sensor_reader_1, sensor_reader_2, location_data_reader, weather_data_reader_1, weather_data_reader_2)
}

fn get_data(
    sensor_reader_1: Reader<File>, 
    sensor_reader_2: Reader<File>, 
    location_data_reader: Reader<File>, 
    weather_data_reader_1: Reader<File>, 
    weather_data_reader_2: Reader<File>,
) -> (
    DashMap<NaiveDateTime, Vec<Sensor>>,
    DashMap<NaiveDateTime, Vec<SensedPeople>>,
    DashMap<NaiveDateTime, WeatherPoint>
) {
    let weather_data = match parse_weather_data(weather_data_reader_1) {
        Ok(r) => r,
        Err(e) => panic!("Something went worng reading weather csv 1: {:#?}", e),
    };

    let weather_data_2 = match parse_weather_data(weather_data_reader_2) {
        Ok(r) => r,
        Err(e) => panic!("Something went worng reading weather csv 2: {:#?}", e),
    };


    let location_data = match parse_location_data(location_data_reader) {
        Ok(r) => r,
        Err(e) => panic!("Something went worng reading location csv: {:#?}", e),
    };

    let sensor_data = match parse_sensor_data(sensor_reader_1) {
        Ok(r) => r,
        Err(e) => panic!("Something went worng reading sensor csv 1: {:#?}", e),
    };

    let sensor_data_2 = match parse_sensor_data(sensor_reader_2) {
        Ok(r) => r,
        Err(e) => panic!("Something went worng reading sensor csv 2: {:#?}", e),
    };

    for val_ref in sensor_data_2.into_iter() {
        sensor_data.insert(val_ref.0, val_ref.1);
    }

    for val_ref in weather_data_2.into_iter() {
        weather_data.insert(val_ref.0, val_ref.1);
    }

    (sensor_data, location_data, weather_data)
}