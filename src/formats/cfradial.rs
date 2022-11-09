use crate::{ParamDescription, RadarFile, ScanMode, Sweep, Ray};
use chrono::{offset::TimeZone, Duration, Utc};
use netcdf::AttrValue;
use std::{collections::HashMap, path::Path};

pub fn is_cfradial() -> bool {
    true
}

fn to_generic_name(name: &str) -> &str {
    match name {
        "DBZ" | "DBZHC" | "DBZHC_F" => "REF",
        "VEL" | "VEL_F" => "VEL",
        "WIDTH" => "SW",
        "RHOHV" | "RHOHV_F" => "RHO",
        "PHIDP" => "PHI",
        "KDP" => "KDP",
        "ZDR" | "ZDR_F" => "ZDR",
        _ => name,
    }
}

pub fn read_cfradial(path: impl AsRef<Path>) -> RadarFile {
    let data_types = [
        "DBZ", "DBZHC", "DBZHC_F", "VEL", "VEL_F", "WIDTH", "KDP", "KDF_F", "PHIDP", "RHOHV",
        "RHOHV_F", "ZDR", "ZDR_F",
    ];

    let reader = netcdf::open(path.as_ref()).unwrap();
    // reader
    //     .groups()
    //     .unwrap()
    //     .for_each(|x| println!("{:?}", x.name()));

    // let str_start_time = {
    //     if let netcdf::AttrValue::Str(s) = reader
    //         .attribute("time_coverage_start")
    //         .unwrap()
    //         .value()
    //         .unwrap()
    //     {
    //         s
    //     } else {
    //         panic!("No start time provided")
    //     }
    // };

    let name = if let Some(AttrValue::Str(s)) = reader
        .attribute("instrument_name")
        .map(|v| v.value().unwrap())
    {
        s
    } else {
        panic!("Instrument name is not a string")
    };

    let mut radar = RadarFile {
        name,
        sweeps: Vec::new(),
        params: HashMap::new(),
        scan_mode: ScanMode::PPI,
    };

    let range_var = reader.variable("range").unwrap();

    let first_gate = match range_var
        .attribute("meters_to_center_of_first_gate")
        .map(|v| v.value().unwrap())
    {
        Some(AttrValue::Str(s)) => s.parse::<f32>().unwrap(),
        Some(AttrValue::Double(s)) => s as f32,
        Some(AttrValue::Float(s)) => s,
        v => {
            println!(
                "Unknown meters_to_center_of_first_gate: {:?}, defaulting to 0",
                v
            );
            0.0
        }
    };

    let gate_range = match range_var
        .attribute("meters_between_gates")
        .map(|v| v.value().unwrap())
    {
        Some(AttrValue::Str(s)) => s.parse::<f32>().unwrap(),
        Some(AttrValue::Double(s)) => s as f32,
        Some(AttrValue::Float(s)) => s,
        Some(v) => {
            println!("Unknown type for meters_between_gates: {:?}, defaulting to 100", v);
            100.0
        }
        None => {
            range_var.value::<f32>(Some(&[1])).unwrap() - range_var.value::<f32>(Some(&[0])).unwrap()
        }
    };

    dbg!(gate_range);

    for var in data_types {
        let corr_name = to_generic_name(var);

        if reader.variable(var).is_none() {
            continue;
        }

        let new_param = ParamDescription {
            description: String::new(),
            units: String::new(),
            meters_to_first_cell: first_gate,
            meters_between_cells: gate_range,
        };

        radar.params.insert(corr_name.to_string(), new_param);
    }

    for i in 0..reader.dimension("sweep").unwrap().len() {
        let mut sweep = Sweep::default();

        let start_idx = reader
            .variable("sweep_start_ray_index")
            .unwrap()
            .value::<u32>(Some(&[i]))
            .unwrap() as usize;
        let end_idx = reader
            .variable("sweep_end_ray_index")
            .unwrap()
            .value::<u32>(Some(&[i]))
            .unwrap() as usize;

        sweep.elevation = reader
            .variable("elevation")
            .unwrap()
            .value::<f32>(Some(&[start_idx]))
            .unwrap();
        sweep.nyquist_velocity = reader
            .variable("nyquist_velocity")
            .unwrap()
            .value::<f32>(Some(&[start_idx]))
            .unwrap();
        sweep.latitude = reader
            .variable("latitude")
            .unwrap()
            .value::<f32>(None)
            .unwrap();
        sweep.longitude = reader
            .variable("longitude")
            .unwrap()
            .value::<f32>(None)
            .unwrap();

        let times = reader
            .variable("time")
            .unwrap()
            .values::<f64>(Some(&[start_idx]), Some(&[end_idx - start_idx]))
            .unwrap();
        let azims = reader
            .variable("azimuth")
            .unwrap()
            .values::<f32>(Some(&[start_idx]), Some(&[end_idx - start_idx]))
            .unwrap();

        let ngates = reader.dimension("range").unwrap().len();
        // dbg!(ngates);

        for i in 0..(end_idx - start_idx) {
            let time = (times[i] * 1000.0) as i64;

            let mut data = HashMap::<String, Vec<f64>>::new();

            for var in data_types {
                let corr_name = to_generic_name(var);

                let var_opt = reader.variable(var);

                if var_opt.is_none() {
                    continue;
                }

                let scale = match var_opt
                    .as_ref()
                    .unwrap()
                    .attribute("scale_factor")
                    .map(|v| v.value().unwrap())
                {
                    Some(AttrValue::Double(x)) => x,
                    Some(AttrValue::Float(x)) => x as f64,
                    _ => 1.0,
                };

                let offset = match var_opt
                    .as_ref()
                    .unwrap()
                    .attribute("add_offset")
                    .map(|v| v.value().unwrap())
                {
                    Some(AttrValue::Double(x)) => x,
                    Some(AttrValue::Float(x)) => x as f64,
                    _ => 0.0,
                };

                let mut var_data = var_opt
                    .unwrap()
                    .values::<f64>(Some(&[i, 0]), Some(&[1, ngates]))
                    .unwrap();

                let mut var_data = var_data * scale + offset;

                // if corr_name == "REF" {
                //     // var_data = var_data * 1.5 + 15.0;
                //     println!("{i} {:?}", &var_data.as_slice().unwrap());
                // }
                // println!("{}", var);
                // println!("{}, {}", scale, offset);
                // println!("{:?}", ((var_data.clone() + offset) * scale).into_raw_vec());
                data.insert(
                    corr_name.to_string(),
                    var_data.into_raw_vec(),
                );
            }

            // TODO: FIX
            let new_ray = Ray {
                // time: start_time.clone() + Duration::seconds(time),
                azimuth: azims[i],
                data: data,
                ..Default::default()
            };

            sweep.rays.push(new_ray);
        }

        radar.sweeps.push(sweep)
    }

    radar
}
