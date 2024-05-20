use std::env;
use std::fs;
use std::fs::metadata;
use std::fs::File;
use std::path::Path;
use std::collections::HashMap;
use std::sync::Mutex;
use linya::{Bar, Progress};
use serde::{Deserialize, Serialize};
use chrono::{DateTime, TimeZone, offset, FixedOffset, NaiveDate, NaiveDateTime, Utc};
use rayon::prelude::*;
use stopwatch::Stopwatch;

fn asosprocess(datasource: &String, outdir: &String){
    let parts = datasource.split("\\").collect::<Vec<&str>>();
    let outname = &parts[parts.len()-1][0..parts[parts.len()-1].len()-4];
    let stationname: &str = &outname[5..9];

    if !fs::metadata(format!("{}\\{}", outdir, stationname)).is_ok(){
        fs::create_dir(&format!("{}\\{}", outdir, stationname)).expect(&format!("Unable to create directory {}\\{}", outdir, stationname));
    }
    let fulloutdir = &format!("{}\\{}", outdir, stationname);

    //let mut wind_map: HashMap<String, (&str, &str, &str, &str)> = HashMap::new();
    let mut wind_map: HashMap<String, (&str, &str)> = HashMap::new();

    let lines: Vec<String> = fs::read_to_string(datasource).expect("Failed to open file!").lines().map(|s| s.to_string()).collect();
    for i in 0..lines.len(){
        
        let line = &lines[i];
        let parts = line.split(" ").collect::<Vec<&str>>();

        if line.len() < 29{
            continue;
        }
        
        //let callsign = &line[10..13];
        if line[13..17].parse::<i32>().is_err() {
            continue;
        }
        if line[17..19].parse::<i32>().is_err() {
            continue;
        }
        if line[19..21].parse::<i32>().is_err() {
            continue;
        }
        if line[21..23].parse::<i32>().is_err() {
            continue;
        }
        if line[21..23].parse::<i32>().is_err() {
            continue;
        }
        if line[25..27].parse::<i32>().is_err() {
            continue;
        }

        let year: i32 = line[13..17].parse::<i32>().unwrap();
        let month: u32 = line[17..19].parse::<u32>().unwrap();
        let day: u32 = line[19..21].parse::<u32>().unwrap();
        let lochour: u32 = line[21..23].parse::<u32>().unwrap();
        let locmin: u32 = line[23..25].parse::<u32>().unwrap();
        let utchour: u32 = line[25..27].parse::<u32>().unwrap(); //fix error thrown

        let u_utchour:i64 = i64::from(utchour);
        let u_lochour:i64 = i64::from(lochour);

        let offset: i64 = (u_utchour - u_lochour) % 24;
        let trueoffset = if offset < 0 {offset + 24} else {offset};
        let secoffset = trueoffset * 3600;

        let ndt: NaiveDateTime = NaiveDate::from_ymd_opt(year,month,day).unwrap().and_hms_opt(lochour, locmin, 0).unwrap();
        let dt_utc = Utc.from_utc_datetime(&ndt);

        let utc_timestamp = dt_utc.timestamp() + secoffset;

        if utc_timestamp % 120 != 0 {
            continue;
        }

        if line.len() < 79{
            //let data_tuple: (&str, &str, &str, &str) = ("null", "null", "null", "null");
            let data_tuple: (&str, &str) = ("null", "null");
            wind_map.insert(utc_timestamp.to_string(), data_tuple);
            continue;
        }
        
        let winddir = &line[71..74];
        let windspeed = &line[76..79];

        //let gustdir = &line[81..84];
        //let gustspeed = &line[86..89];

        let mut wd: &str = "null";
        let mut ws: &str = "null";
        //let mut gd: &str = "null";
        //let mut gs: &str = "null";
        

        if winddir.trim().parse::<f32>().is_ok(){
            wd = winddir.trim();
        }

        if windspeed.trim().parse::<f32>().is_ok(){
            ws = windspeed.trim();
        }
/*
        if gustdir.trim().parse::<f32>().is_ok(){
            gd = gustdir.trim();
        }

        if gustspeed.trim().parse::<f32>().is_ok(){
            gs = gustspeed.trim();
        }
*/
        //let data_tuple: (&str, &str, &str, &str) = (wd, ws, gd, gs);
        let data_tuple: (&str, &str) = (wd, ws);
        wind_map.insert(utc_timestamp.to_string(), data_tuple);
    }

    let outfile = File::create(format!("{}\\{}.json",fulloutdir, outname)).expect("Unable to create file");
    serde_json::to_writer(outfile, &wind_map).expect("Unable to write JSON");
}

//Usage:
//Arg0 -> processname (.exe)
//Arg1 -> ASOS 1 minute .dat file (Averaged every 2 minutes)
//Arg2 -> JSON output directory
fn main() {
    let args: Vec<_> = env::args().collect();

    let datasource = &args[1];
    let outdir = &args[2];

    let watch = Stopwatch::start_new();

    let md = metadata(datasource).unwrap();
    if md.is_dir() {
        println!("Converting Files in {} to JSON", datasource);
        let paths = fs::read_dir(datasource).unwrap();
        let files =
        paths.filter_map(|entry| {
            entry.ok().and_then(|e|
            e.path().file_name()
            .and_then(|n| n.to_str().map(|s| String::from(s)))
        )
        }).collect::<Vec<String>>();

        let numfiles = files.len();

        let amp = |i: &String| {
            asosprocess(&(datasource.to_string() + "\\" + i), outdir);
        };
        
        let progress = Mutex::new(Progress::with_capacity(numfiles));
        let bar: Bar = progress.lock().unwrap().bar(numfiles, format!("Processing {} files...", numfiles));
        files.par_iter().for_each(|j| {
            
            amp(j);
            progress.lock().unwrap().inc_and_draw(&bar, 1);
            }
        );
        println!("Output JSONs to {}", outdir)
        
    }
    if md.is_file() {
        println!("Converting {} to JSON", datasource);
        asosprocess(datasource, outdir);
        println!("Output JSON to {}", outdir)
    }
    println!("Finished in {}ms", watch.elapsed_ms())
    

}
