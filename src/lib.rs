use chrono::{DateTime, Utc};
use clap::{App, AppSettings, Arg};
use glob::glob;
use std::collections::HashMap;
use std::path::Path;

mod formats;
use formats::*;

/// Radar format to conver to
#[derive(Clone, Copy)]
pub enum Format {
    NEXRAD,
    DORADE,
}

impl Format {
    fn format_str(&self) -> &str {
        match self {
            Format::DORADE => "DORADE.%Y%m%d_%H%M%S",
            Format::NEXRAD => "NEXRAD.%Y%m%d_%H%M%S",
        }
    }
}

// macro_rules! value_enum {
//     ($name:ident,
//         $(
//             $field:ident = $val:expr,
//         )*
//     ) => {
//         pub enum $name {
//             $(
//                 $field = $val,
//             )*
//         }

//         impl $name {
//             fn from_num(num: u16) -> $name {
//                 match num {
//                     $($val => $name::$field,)*
//                     _ => panic!("Error converting enum: {}, value: {}", stringify!($name), num)
//                 }
//             }
//         }
//     };
// }

// pub static radar_abreivations: HashMap<&str, &str> = HashMap::from([])

/// Scan mode of a radar
#[derive(Copy, Clone, PartialEq, Eq, Default)]
pub enum ScanMode {
    Calibration,
    #[default]
    PPI,
    Coplane,
    RHI,
    Vertical,
    Stationary,
    Manual,
    Idle,
    Surveillance,
    Airborne,
    Horizontal,
}

/// An individual ray in a sweep
#[derive(Clone, Debug)]
pub struct Ray {
    /// Ray scan time
    pub time: DateTime<Utc>,

    /// Azimuth angle for the ray
    pub azimuth: f32,

    /// Data hashmap
    pub data: HashMap<String, Vec<f64>>,
}

impl Default for Ray {
    fn default() -> Self {
        Ray {
            time: chrono::Utc::now(),
            azimuth: 0.0,
            data: std::collections::HashMap::new(),
        }
    }
}

/// An individual sweep
#[derive(Clone, Default)]
pub struct Sweep {
    /// Vector of rays in the sweep
    pub rays: Vec<Ray>,

    /// Sweep elevation
    pub elevation: f32,

    /// Latitude of the radar
    pub latitude: f32,

    /// Longitude of the radar
    pub longitude: f32,

    /// Scan rate in degrees/sec
    pub scan_rate: Option<f32>,

    /// Nyquist velocity for the sweep
    pub nyquist_velocity: f32,

    /// Scanning mode
    pub scan_mode: ScanMode,
}

impl Sweep {
    pub fn time(&self) -> DateTime<Utc> {
        self.rays[0].time
    }

    pub fn nrays(&self) -> u16 {
        self.rays.len() as u16
    }

    pub fn ngates(&self) -> u16 {
        self.rays[0].data.values().next().unwrap().len() as u16
    }

    pub fn azimuths(&self) -> Vec<f32> {
        self.rays.iter().map(|x| x.azimuth).collect::<Vec<_>>()
    }

    pub fn correct_azimuth(&mut self) {
        for ray in &mut self.rays {
            ray.azimuth = ray.azimuth.rem_euclid(360.0);
        }
    }

    pub fn sort_rays_by_azimuth(&mut self) {
        self.correct_azimuth();

        self.rays.sort_by(|a, b| a.azimuth.partial_cmp(&b.azimuth).unwrap());
    }

    pub fn trim_rays(&mut self) {
        self.correct_azimuth();

        let mut change: f32 = 0.0;
        let mut last_change: f32 = 0.0;
        let direction: f32 = {
            let dir: f32 = self.azimuths()[0..5].iter().sum();
            if dir > 0.0 || dir < -300.0 {
                1.0
            } else {
                -1.0
            }
        };

        for i in 0..self.nrays() as usize {
            if i == 0 {
                change = 0.0;
            } else {
                let mut small_change = self.rays[i].azimuth - last_change;

                if small_change.abs() > 300.0 {
                    small_change = 360.0 - small_change.abs()
                }

                change += direction * small_change;
            }

            last_change = self.azimuths()[i];

            if change >= 360.0 {
                self.rays = self.rays[0..i].to_vec();

                break;
            }
        }
    }

    pub fn get_data(&self, field: &str) -> Vec<Vec<f64>> {
        self.rays
            .iter()
            .map(|x| x.data.get(field).unwrap().clone())
            .collect()
    }
}

/// Description of a parameter
#[derive(Debug, Clone, Default)]
pub struct ParamDescription {
    /// Description on the parameter
    pub description: String,

    /// Units
    pub units: String,

    /// Meters to the first cell in the ray
    pub meters_to_first_cell: f32,

    /// Meters between each cell
    pub meters_between_cells: f32,
}

// An entire file, containing multiple sweeps
#[derive(Clone)]
pub struct RadarFile {
    /// Name of the radar
    pub name: String,

    /// Vector of the sweeps
    pub sweeps: Vec<Sweep>,

    /// Hashmap of the field names and the description of the field
    pub params: HashMap<String, ParamDescription>,
}

impl RadarFile {
    /// Returns the number of sweeps in the the file
    pub fn nsweeps(&self) -> u16 {
        self.sweeps.len() as u16
    }

    /// Sorts all the rays in each sweep by azimuth
    pub fn sort_rays_by_azimuth(&mut self) {
        for sweep in &mut self.sweeps {
            sweep.sort_rays_by_azimuth();
        }
    }

    /// Sorts all sweeps by time
    pub fn sort_sweeps_by_time(&mut self) {
        self.sweeps.sort_by_key(|sweep| sweep.time());
    }

    /// Sorts all sweeps by elevation
    pub fn sort_sweeps_by_elevation(&mut self) {
        self.sweeps.sort_by(|s1, s2| s1.elevation.partial_cmp(&s2.elevation).unwrap());
    }

    /// Deletes excess rays in each sweep
    pub fn trim_rays(&mut self) {
        for sweep in &mut self.sweeps {
            sweep.trim_rays();
        }
    }

    /// Splits overlapping rays into new sweeps
    pub fn split_overlap_rays(&mut self) {
        let mut new_sweeps: Vec<Sweep> = Vec::new();

        for sweep in &mut self.sweeps {
            sweep.correct_azimuth();

            let mut change: f32 = 0.0;
            let mut last_change: f32 = 0.0;
            let direction: f32 = {
                let dir: f32 = sweep.azimuths()[0..5].iter().sum();
                if dir > 0.0 || dir < -300.0 {
                    1.0
                } else {
                    -1.0
                }
            };

            let mut ray_idx: usize = 0;

            for i in 0..sweep.nrays() as usize {
                if i == 0 {
                    change = 0.0;
                } else {
                    let mut small_change = sweep.rays[i].azimuth - last_change;

                    if small_change.abs() > 300.0 {
                        small_change = 360.0 - small_change.abs()
                    }

                    change += direction * small_change;
                }

                last_change = sweep.rays[i].azimuth;

                if change >= 360.0 {
                    let mut new_sweep = sweep.clone();
                    new_sweep.rays = new_sweep.rays[ray_idx..i].to_vec();

                    ray_idx = i;
                    change -= 360.0;

                    new_sweeps.push(new_sweep)
                }
            }

            // If last sweep
            if sweep.nrays() - ray_idx as u16 > 20 {
                let mut new_sweep = sweep.clone();
                new_sweep.rays = new_sweep.rays[ray_idx..].to_vec();

                new_sweeps.push(new_sweep)
            }
        }

        self.sweeps = new_sweeps;
    }

    /// Time of first sweep
    pub fn start_time(&self) -> DateTime<Utc> {
        self.sweeps[0].time()
    }
}

/// Options for conversion
#[derive(Clone)]
pub struct RadyOptions {
    /// Overrides the output radar
    pub override_radar: Option<String>,

    /// Deletes overlapping rays
    pub trim_rays: bool,

    /// Splits overlapping rays into new sweeps
    pub split_overlap_rays: bool,

    /// Sorts rays by azimuth
    pub sort_rays_by_azimuth: bool,

    /// Converts to the specified format
    pub format: Format,

    /// Aggregates and writes all volumes
    pub write_volumes: bool,

    /// Aggregates and writes all volumes
    pub write_separate: bool,

    /// Prints all of the file products and exit
    pub print_products: bool,

    /// Adds a file path to read. To select all files in a directory, use the * wildcard at the end
    pub files: String,

    /// Scales reflectivity
    pub scale: f64,

    /// Offsets reflectivity
    pub offset: f64,

    /// Removes all reflectivity values after scale/offset under this number
    pub remove: f64,

    /// Prints the location in lat, long for each sweep
    pub location: bool,

    /// Sets the directory to make the output folder in. Default is the same as the input
    pub outdir: Option<String>,

    /// Creates files with a given name. Available codes are from the "chrono" library
    pub name_format: Option<String>,
}

impl Default for RadyOptions {
    fn default() -> RadyOptions {
        RadyOptions {
            override_radar: None,
            trim_rays: false,
            split_overlap_rays: false,
            sort_rays_by_azimuth: false,
            format: Format::NEXRAD,
            write_volumes: false,
            write_separate: false,
            print_products: false,
            files: String::new(),
            scale: 1.0,
            offset: 0.0,
            remove: -999.0,
            location: false,
            outdir: None,
            name_format: None,
        }
    }
}

impl RadyOptions {
    pub fn apply_options(&self, radar: &mut RadarFile) {
        if self.override_radar.is_some() {
            radar.name = self.override_radar.clone().unwrap();
        }

        if self.trim_rays {
            radar.trim_rays();
        }

        if self.split_overlap_rays {
            radar.split_overlap_rays();
        }

        if self.sort_rays_by_azimuth {
            radar.sort_rays_by_azimuth();
        }

        if self.location {
            println!(
                "{}: {}, {}",
                radar.name, radar.sweeps[0].latitude, radar.sweeps[0].longitude
            );
        }
    }
}

pub fn read(path: impl AsRef<Path>, options: &RadyOptions) -> RadarFile {
    if dorade::is_dorade(path.as_ref()) {
        dorade::read_dorade(path, options)
    // } else if cfradial::is_cfradial() {
    //     cfradial::read_cfradial(path)
    } else {
        panic!("Unknown file format");
    }
}

fn vol_mode(radar: &RadarFile) -> f32 {
    match radar.sweeps.len() {
        0 | 1 => return 1.0,
        2 => return (radar.sweeps[1].elevation - radar.sweeps[0].elevation).signum(),
        _ => (),
    }

    let min_elev = radar.sweeps.iter()
        .map(|sweep| sweep.elevation)
        .reduce(|elev1, elev2| if elev1 < elev2 { elev1 } else { elev2 })
        .unwrap();

    let ii = radar.sweeps.iter()
        .map(|sweep| sweep.elevation)
        .enumerate()
        .find(|elev| (elev.1 - min_elev).abs() < 0.05)
        .unwrap().0;
        
    if ii > radar.sweeps.len() - 2 {
        return -1.0;
    }
    
    if (radar.sweeps[ii + 2].elevation - radar.sweeps[ii].elevation).abs() < 0.05 {
        return 1.0;
    }
    
    (radar.sweeps[ii + 2].elevation - radar.sweeps[ii + 1].elevation).signum()
}

pub fn write(mut radar: RadarFile, path: impl AsRef<Path>, options: &RadyOptions) {
    radar.sort_sweeps_by_time();

    if options.write_volumes {
        let mut vol = RadarFile {
            name: radar.name.clone(),
            sweeps: Vec::new(),
            params: radar.params.clone(),
        };

        let mut new_ops = (*options).clone();
        new_ops.write_volumes = false;
        new_ops.write_separate = false;

        let vol_mode = vol_mode(&radar);

        let mut last = radar.sweeps[0].elevation;
        for sweep in radar.sweeps {
            let elev = sweep.elevation;

            // if (elev - last).abs() < 0.1 {
            //     write(vol.clone(), path.as_ref(), &new_ops);
            //     vol.sweeps = vec![sweep];
            // }

            let change = if vol_mode == 1.0 { elev - last } else { last - elev };

            // TODO: Double check how to handle this
            if vol.sweeps.is_empty() || change > 0.1 {
                vol.sweeps.push(sweep);
            } else {
                write(vol.clone(), path.as_ref(), &new_ops);
                vol.sweeps = vec![sweep];
            }

            last = elev;
        }
    } else if options.write_separate {
        for sweep in radar.sweeps {
            let new_radar = RadarFile {
                name: radar.name.clone(),
                sweeps: vec![sweep],
                params: radar.params.clone(),
            };

            let mut new_ops = (*options).clone();
            new_ops.write_volumes = false;
            new_ops.write_separate = false;
            write(new_radar, path.as_ref(), &new_ops);
        }
    } else {
        radar.sort_sweeps_by_elevation();
        match options.format {
            Format::NEXRAD => {
                for sweep in &mut radar.sweeps {
                    // println!("{}", sweep.elevation);
                    sweep.rays.iter_mut().for_each(|ray| {
                        ray.data.values_mut().for_each(|val| {
                            while val.len() % 2 != 0 {
                                val.pop().unwrap();
                            }
                        })
                    })
                }

                nexrad::write_nexrad(&radar, path, options);
            }
            _ => panic!("Write format not supported"),
        }
    }
}

pub fn convert(options: &RadyOptions) {
    let in_path = Path::new(&options.files);

    let mut out_path = {
        if options.outdir.is_none() {
            in_path.parent().unwrap()
        } else {
            Path::new(options.outdir.as_ref().unwrap())
        }
    }
    .to_path_buf();

    if options.outdir.is_none() {
        out_path.push("output");
    }

    if !out_path.is_dir() && out_path.exists() {
        panic!("Output file path is not a directory")
    }

    let files;

    if Path::new(in_path).is_file() {
        files = vec![Ok(in_path.to_path_buf())];
    } else {
        files = glob(in_path.to_str().unwrap()).unwrap().collect();
    }

    if files.is_empty() {
        panic!("Path: {:?} does not exist or have any files", in_path);
    }

    for file in files {
        if file.as_ref().unwrap().is_dir() {
            continue;
        }

        let mut radar = read(file.unwrap(), options);
        options.apply_options(&mut radar);
        write(radar, out_path.clone(), options);
    }

    // if options.aggregate_volumes {
    //     let mut volume = read(files[0].as_ref().unwrap(), options);

    //     for file in files.iter() {
    //         if file.as_ref().unwrap().is_dir() {
    //             continue;
    //         }
    //         let new = read(file.as_ref().unwrap(), options);

    //         println!("{:?} {:?}", volume.start_time(), volume.sweeps.len());

    //         // If new sweep has a higher elevation by atleast 0.2 deg
    //         if new.sweeps[0].elevation - volume.sweeps.last().unwrap().elevation > 0.5 {
    //             volume.sweeps.extend(new.sweeps);
    //         } else { // Write volume
    //             options.apply_options(&mut volume);
    //             write(&volume, &out_path, options);

    //             volume = new;
    //         }
    //     }

    // } else {
    //     for file in files {
    //         if file.as_ref().unwrap().is_dir() {
    //             continue;
    //         }

    //         let mut radar = read(file.unwrap(), options);
    //         options.apply_options(&mut radar);
    //         write(&radar, out_path.clone(), options);
    //     }
    // }
}

pub fn arg_parse() -> RadyOptions {
    let mut options = RadyOptions::default();

    let matches = App::new("RadyConvert")
        .version("0.0.1")
        .setting(AppSettings::AllowNegativeNumbers)
        .arg(Arg::new("format").short('F').long("format").takes_value(true).help("Converts to the specified format")
            .possible_values(["nexrad"]).ignore_case(true))
        .arg(Arg::new("override radar").short('R').long("radar").takes_value(true).help("Overrides the output radar"))
        .arg(Arg::new("write volumes").long("vols").help("Aggregates sweeps into volumes and writes them separately."))
        .arg(Arg::new("print products").short('P').long("print_p").help("Prints all of the file products and exit"))
        .arg(Arg::new("files").short('f').long("file").takes_value(true).required(true).help("Adds a file path to read. To select all files in a directory, use the * wildcard at the end"))
        .arg(Arg::new("scale").long("scale").takes_value(true).help("Scales reflectivity"))
        .arg(Arg::new("offset").long("offset").takes_value(true).help("Offsets reflectivity"))
        .arg(Arg::new("remove").long("remove").takes_value(true).help("Removes all reflectivity values after scale/offset under this number"))
        .arg(Arg::new("location").short('l').long("location").help("Prints the location in lat, long for each sweep"))
        .arg(Arg::new("outdir").short('o').long("outdir").takes_value(true).help("Sets the directory to make the output folder in. Default is the same as the input"))
        .arg(Arg::new("name format").long("name").takes_value(true).help("Creates files with a given name. Available codes are from the \"chrono\" library"))
        .get_matches();

    if matches.is_present("format") {
        options.format = match matches.value_of("format").unwrap().to_lowercase().as_str() {
            "nexrad" => Format::NEXRAD,
            _ => panic!("Unknown output format"),
        };
    }

    options.files = matches.value_of("files").unwrap().to_string();

    if matches.is_present("print products") {
        options.print_products = true;
    }

    if matches.is_present("override radar") {
        options.override_radar = Some(matches.value_of("override radar").unwrap().to_string());
    }

    if matches.is_present("write volumes") {
        options.write_volumes = true;
    }

    if matches.is_present("outdir") {
        options.outdir = Some(matches.value_of("outdir").unwrap().to_string());
    }

    if matches.is_present("scale") {
        options.scale = matches.value_of("scale").unwrap().parse::<f64>().unwrap();
    }

    if matches.is_present("offset") {
        options.offset = matches.value_of("offset").unwrap().parse::<f64>().unwrap();
    }

    if matches.is_present("location") {
        options.location = true;
    }

    if matches.is_present("remove") {
        options.remove = matches.value_of("remove").unwrap().parse::<f64>().unwrap();
    }

    if matches.is_present("name format") {
        options.name_format = Some(matches.value_of("name format").unwrap().to_string());
    }

    options
}
