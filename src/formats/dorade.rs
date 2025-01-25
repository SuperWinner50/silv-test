use chrono::{DateTime, Datelike, Duration, TimeZone, Utc};
use std::collections::HashMap;
use std::convert::TryInto;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::mem::size_of;
use std::path::Path;

use crate::{ParamDescription, RadarFile, RadyOptions, Ray, ScanMode, Sweep};

impl ScanMode {
    fn from_num(num: u16) -> ScanMode {
        match num {
            0 => ScanMode::Calibration,
            1 => ScanMode::PPI,
            2 => ScanMode::Coplane,
            3 => ScanMode::RHI,
            4 => ScanMode::Vertical,
            5 => ScanMode::Stationary,
            6 => ScanMode::Manual,
            7 => ScanMode::Idle,
            8 => ScanMode::Surveillance,
            9 => ScanMode::Airborne,
            10 => ScanMode::Horizontal,
            x => panic!("Unkown scan mode format {x}"),
        }
    }
}

// Comment block
#[repr(C)]
#[derive(Debug)]
struct COMM {
    id: [u8; 4],
    nbytes: u32,
    comment: [u8; 500],
}

// Super Sweep Identification Block
#[repr(C, packed)]
struct SSWB {
    id: [u8; 4],
    nbytes: u32,
    last_used: u32,
    start_time: u32,
    stop_time: u32,
    sizeof_file: u32,
    compression_flag: u32,
    volume_time_stamp: u32,
    num_params: u32,
    radar_name: [u8; 8],
    start_time_f: f64,
    stop_time_f: f64,
    version_num: u32,
    num_key_tables: u32,
    status: u32,
    place_holder: [u32; 7],
    key_table_0_offset: u32,
    key_table_0_size: u32,
    key_table_0_type: u32,
    key_table_1_offset: u32,
    key_table_1_size: u32,
    key_table_1_type: u32,
    key_table_2_offset: u32,
    key_table_2_size: u32,
    key_table_2_type: u32,
    key_table_3_offset: u32,
    key_table_3_size: u32,
    key_table_3_type: u32,
    key_table_4_offset: u32,
    key_table_4_size: u32,
    key_table_4_type: u32,
    key_table_5_offset: u32,
    key_table_5_size: u32,
    key_table_5_type: u32,
    key_table_6_offset: u32,
    key_table_6_size: u32,
    key_table_6_type: u32,
    key_table_7_offset: u32,
    key_table_7_size: u32,
    key_table_7_type: u32,
}

// Volume description block
#[repr(C)]
#[derive(Debug)]
struct VOLD {
    id: [u8; 4],
    nbytes: u32,
    format_version: u16,
    volume_num: u16,
    maximim_bytes: u32,
    proj_name: [u8; 20],
    year: u16,
    month: u16,
    day: u16,
    data_set_hour: u16,
    data_set_minute: u16,
    data_set_second: u16,
    flight_number: [u8; 8],
    gen_facility: [u8; 8],
    gen_year: u16,
    gen_month: u16,
    gen_day: u16,
    number_second_des: u16,
}

// // Radar description
// #[repr(C, packed)]
// #[derive(Debug)]
// struct RADD {
//     id: [u8; 4],
//     nbytes: u32,
//     radar_name: [u8; 8],
//     radar_const: f32,
//     peak_power: f32,
//     noise_power: f32,
//     receiver_gain: f32,
//     antenna_gain: f32,
//     system_gain: f32,
//     horz_beam_width: f32,
//     vert_beam_width: f32,
//     radar_type: u16,
//     scan_mode: u16,
//     req_rotate_vel: f32,
//     scan_mode_param0: f32,
//     scan_move_param1: f32,
//     num_parameter_des: u16,
//     total_num_des: u16,
//     data_compress: u16,
//     data_reduction: u16,
//     data_red_param0: f32,
//     data_red_param1: f32,
//     radar_longitude: f32,
//     radar_latitude: f32,
//     radar_altitude: f32,
//     eff_unamb_vel: f32,
//     eff_unamb_range: f32,
//     num_freq_trans: u16,
//     num_ipps_trans: u16,
//     freq1: f32,
//     freq2: f32,
//     freq3: f32,
//     freq4: f32,
//     freq5: f32,
//     interpulse_per1: f32,
//     interpulse_per2: f32,
//     interpulse_per3: f32,
//     interpulse_per4: f32,
//     interpulse_per5: f32,
//     extension_num: u32,
//     config_name: [u8; 8],
//     config_num: u32,
//     aperture_size: f32,
//     field_of_view: f32,
//     aperture_eff: f32,
//     freq: [f32; 11],
//     interpulse_per: [f32; 11],
//     pulse_width: f32,
//     primary_cop_basein: f32,
//     secondary_cop_basein: f32,
//     pc_xmtr_bandwith: f32,
//     pc_waveform_type: u32,
//     site_name: [u8; 20]
// }

// Radar description
#[repr(C, packed)]
struct RADD {
    id: [u8; 4],
    nbytes: u32,
    radar_name: [u8; 8],
    radar_const: f32,
    peak_power: f32,
    noise_power: f32,
    receiver_gain: f32,
    antenna_gain: f32,
    system_gain: f32,
    horz_beam_width: f32,
    vert_beam_width: f32,
    radar_type: u16,
    scan_mode: u16,
    req_rotate_vel: f32,
    scan_mode_param0: f32,
    scan_move_param1: f32,
    num_parameter_des: u16,
    total_num_des: u16,
    data_compress: u16,
    data_reduction: u16,
    data_red_param0: f32,
    data_red_param1: f32,
    radar_longitude: f32,
    radar_latitude: f32,
    radar_altitude: f32,
    eff_unamb_vel: f32,
    eff_unamb_range: f32,
    num_freq_trans: u16,
    num_ipps_trans: u16,
    freq1: f32,
    freq2: f32,
    freq3: f32,
    freq4: f32,
    freq5: f32,
    interpulse_per1: f32,
    interpulse_per2: f32,
    interpulse_per3: f32,
    interpulse_per4: f32,
    interpulse_per5: f32,
}

#[repr(C)]
#[derive(Debug)]
struct LIDR {
    id: [u8; 4],
    nbytes: u32,
    lidar_name: [u8; 8],
    lidar_const: f32,
    pulse_energy: f32,
    peak_power: f32,
    pulsewidth: f32,
    aperature_size: f32,
    field_of_view: f32,
    aperatute_eff: f32,
    beam_divergence: f32,
    lidar_type: u16,
    scan_mode: u16,
    req_rotat_vel: f32,
    scan_mode_pram0: f32,
    scan_mode_pram1: f32,
    num_parameter_des: u16,
    total_number_des: u16,
    data_compress: u16,
    data_reduction: u16,
    data_red_parm0: f32,
    data_red_parm1: f32,
    lidar_longitude: f32,
    lidar_latitude: f32,
    lidar_altitude: f32,
    eff_unamb_vel: f32,
    eff_unamb_range: f32,
    num_wvlen_trans: u32,
    prf: u32,
    wavelength: [f32; 10],
}

// Correction factor
#[repr(C)]
#[derive(Debug)]
pub struct CFAC {
    id: [u8; 4],
    nbytes: u32,
    azimuth_corr: f32,
    elevation_curr: f32,
    range_delay_corr: f32,
    longitude_corr: f32,
    latitude_corr: f32,
    pressure_alt_corr: f32,
    radar_alt_corr: f32,
    ew_gndspd_corr: f32,
    ns_gndspd_corr: f32,
    vert_vel_corr: f32,
    heading_corr: f32,
    roll_corr: f32,
    pitch_corr: f32,
    drift_corr: f32,
    rot_angle_corr: f32,
    tilt_corr: f32,
}

// // Parameter (data field) description
// #[repr(C)]
// #[derive(Debug)]
// pub struct PARM {
//     id: [u8; 4],
//     nbytes: u32,
//     parameter_name: [u8; 8],
//     param_description: [u8; 40],
//     param_units: [u8; 8],
//     interpulse_time: u16,
//     xmitted_freq: u16,
//     recvr_bandwidth: f32,
//     pulse_width: u16,
//     polarization: u16,
//     num_samples: u16,
//     binary_format: u16,
//     threshold_field: [u8; 8],
//     threshold_value: f32,
//     parameter_scale: f32,
//     parameter_bias: f32,
//     bad_data: u32,
//     extension_num: u32,
//     config_name: [u8; 8],
//     config_num: u32,
//     offset_to_data: u32,
//     mks_conversion: f32,
//     num_qnames: u32,
//     qdata_names: [u8; 32],
//     num_criteria: u32,
//     criteria_names: [u8; 32],
//     number_cells: u32,
//     meters_to_first_cell: f32,
//     meters_between_cells: f32,
//     eff_unamb_vel: f32
// }

// Parameter (data field) description
#[repr(C)]
#[derive(Debug)]
pub struct PARM {
    id: [u8; 4],
    nbytes: u32,
    parameter_name: [u8; 8],
    param_description: [u8; 40],
    param_units: [u8; 8],
    interpulse_time: u16,
    xmitted_freq: u16,
    recvr_bandwidth: f32,
    pulse_width: u16,
    polarization: u16,
    num_samples: u16,
    binary_format: u16,
    threshold_field: [u8; 8],
    threshold_value: f32,
    parameter_scale: f32,
    parameter_bias: f32,
    bad_data: u32,
}

// Cell vector block
#[repr(C)]
#[derive(Debug)]
pub struct CELV {
    id: [u8; 4],
    nbytes: u32,
    number_cells: u32,
    dist_cells: [f32; 1500],
}

// Cell spacing table
#[repr(C)]
#[derive(Debug)]
pub struct CSFD {
    id: [u8; 4],
    nbytes: u32,
    num_segments: u32,
    dist_to_first: f32,
    spacing: [f32; 8],
    num_cells: [u16; 8],
}

// Sweep information table
#[repr(C)]
#[derive(Debug)]
pub struct SWIB {
    id: [u8; 4],
    nbytes: u32,
    radar_name: [u8; 8],
    sweep_num: u32,
    num_rays: u32,
    start_angle: f32,
    stop_angle: f32,
    fixed_angle: f32,
    filter_flag: u16,
}

// Platform geo-reference block
#[repr(C)]
#[derive(Debug)]
pub struct ASIB {
    id: [u8; 4],
    nbytes: u32,
    longitude: f32,
    latitude: f32,
    altitude_msl: f32,
    altutide_agl: f32,
    ew_velocity: f32,
    ns_velocity: f32,
    vert_velocity: f32,
    heading: f32,
    roll: f32,
    pitch: f32,
    drift_angle: f32,
    rotation_angle: f32,
    tilt: f32,
    ew_horiz_wind: f32,
    ns_horiz_wind: f32,
    vert_wind: f32,
    heading_change: f32,
    pitch_change: f32,
}

// Ray information block
#[repr(C)]
#[derive(Debug)]
pub struct RYIB {
    id: [u8; 4],
    nbytes: u32,
    sweep_num: u32,
    julian_day: u32,
    hour: u16,
    minute: u16,
    second: u16,
    millisecond: u16,
    azimuth: f32,
    elevation: f32,
    peak_power: f32,
    true_scan_rate: f32,
    ray_status: u32,
}

// Field data block
#[repr(C)]
#[derive(Debug)]
pub struct RDAT {
    id: [u8; 4],
    nbytes: u32,
    pdata_name: [u8; 8],
}

// Extended field data block
#[repr(C)]
#[derive(Debug)]
pub struct QDAT {
    id: [u8; 4],
    nbytes: u32,
    pdata_name: [u8; 8],
    extension_num: u32,
    config_num: u32,
    first_cell: [u16; 4],
    num_cells: [u16; 4],
    criteria_value: [f32; 4],
}

// Extra stuff block
#[repr(C)]
#[derive(Debug)]
pub struct XSTF {
    id: [u8; 4],
    nbytes: u32,
    one: u32,
    source_format: u32,
    offset_to_first_item: u32,
    transition_flag: u32,
}

// Null block
#[repr(C)]
#[derive(Debug)]
pub struct _NULL {
    id: [u8; 4],
    nbytes: u32,
}

// Rotation angle data block
#[repr(C)]
#[derive(Debug)]
pub struct _RKTB {
    id: [u8; 4],
    nbytes: u32,
    angle2ndx: u32,
    ndx_que_size: u32,
    first_key_offset: u32,
    angle_table_offset: u32,
    num_rays: u32,
}

// Radar parameter block
#[repr(C)]
#[derive(Debug)]
pub struct _FRAD {
    id: [u8; 4],
    nbytes: u32,
    data_sys_status: u32,
    radar_name: [u8; 8],
    test_pulse_level: f32,
    test_pulse_dist: f32,
    test_pulse_width: f32,
    test_pulse_freq: f32,
    test_pulse_atten: u16,
    test_pulse_fnum: u16,
    noise_power: f32,
    ray_count: u32,
    first_rec_gate: u16,
    last_rec_gate: u16,
}

// Field radar block
#[repr(C)]
#[derive(Debug)]
struct _FRIB {
    id: [u8; 4],
    nbytes: u32,
    data_sys_id: u32,
    loss_out: f32,
    loss_in: f32,
    loss_rjoint: f32,
    ant_v_dim: f32,
    ant_h_dim: f32,
    ant_noise_temp: f32,
    r_noise_figure: f32,
    xmit_power: [f32; 5],
    x_band_gain: f32,
    receiver_gain: [f32; 5],
    if_gain: [f32; 5],
    conversion_gain: f32,
    scale_factor: [f32; 5],
    processor_const: f32,
    dly_tube_antenna: u32,
    dly_rndtrip_chip_atod: u32,
    dly_timmod_testpulse: u32,
    dly_modulator_on: u32,
    dly_modulator_off: u32,
    peak_power_offset: f32,
    test_pulse_offset: f32,
    e_plane_angle: f32,
    h_plane_angle: f32,
    encoder_antenna_up: f32,
    pitch_antenna_up: f32,
    indepf_times_flg: u16,
    time_series_gate: u16,
    num_base_params: u16,
    file_name: [u8; 80],
}

struct ParmDesc {
    scale: f32,
    bias: f32,
    binary_format: u16,
    nyquist: f32,
    offset: u32,
    bad_data: u32,
}

struct DoradeDesc {
    start_time: DateTime<Utc>,
    parm_desc: HashMap<String, ParmDesc>,
    ngates: u16,
    compress: u16,
    scan_mode: ScanMode,
}

macro_rules! consume_block {
    // Macro to convert bytes into a block

    ($reader:expr, $struc:ty) => {{
        const N: usize = size_of::<$struc>();
        let mut new_struc: $struc = unsafe { std::mem::zeroed() };

        unsafe {
            let slice = std::slice::from_raw_parts_mut(&mut new_struc as *mut _ as *mut u8, N);
            $reader.read_exact(slice).unwrap();
        }

        if new_struc.id.as_str().unwrap() != "RDAT" && new_struc.id.as_str().unwrap() != "QDAT" {
            let seek_bytes = (new_struc.nbytes - N as u32) as i64;

            $reader.seek(SeekFrom::Current(seek_bytes)).unwrap();

            // if new_struc.id.as_str() == "CFAC" {
            //     println!("{:?}, {}", new_struc, size_of::<CFAC>());
            // }

            // assert_eq!(new_struc.nbytes as usize, N, "Struct sizes do not match. Expected: {}, Found: {}", stringify!($struc), struc_name);
        }

        new_struc
    }};
}

trait AsString<'a> {
    fn as_string(self) -> Result<String, core::str::Utf8Error>;
    fn as_str(self) -> Result<&'a str, core::str::Utf8Error>;
}

impl<'a> AsString<'a> for &'a [u8] {
    fn as_string(self) -> Result<String, core::str::Utf8Error> {
        Ok(self.as_str()?.to_string())
    }
    fn as_str(self) -> Result<&'a str, core::str::Utf8Error> {
        Ok(std::str::from_utf8(self)?.trim_matches(char::from(0)))
    }
}

trait NextString<'a> {
    fn next_string(&mut self) -> Result<String, core::str::Utf8Error>;
}

impl<'a> NextString<'a> for File {
    fn next_string(&mut self) -> Result<String, core::str::Utf8Error> {
        let mut tmp = [0u8; 4];
        self.read_exact(&mut tmp).unwrap();
        let next = tmp.as_string()?;
        self.seek(SeekFrom::Current(-4)).unwrap();
        Ok(next)
    }
}

fn dorade_to_generic_name(name: String) -> String {
    // Converts format-specific variable names to the generic names

    match name.as_str() {
        "DBZ" | "DCZ" | "DBZHM" => "REF",
        "VEL" | "VC" => "VEL",
        "WIDTH" => "SW",
        "ZDR" => "ZDR",
        "PHI" => "PHI",
        "KDP" => "KDP",
        "RHOHV" => "RHO",
        _ => name.as_str(),
    }
    .to_string()
}

pub fn is_dorade(path: impl AsRef<Path>) -> bool {
    // Checks if a file is in the dorade format

    let mut file = File::open(path).unwrap();
    let id = file.next_string();

    if let Ok(result) = id {
        if result == "COMM".to_string() || result == "SSWB".to_string() {
            return true;
        }
    }

    false
}

pub fn read_dorade(path: impl AsRef<Path>, options: &RadyOptions) -> RadarFile {
    // Reads a dorade file

    let mut reader = File::open(path).unwrap();

    // Load the first 3 blocks.
    // TODO: Check if they all always present
    if reader.next_string().unwrap().as_str() == "COMM" {
        let _comm = consume_block!(reader, COMM);
    }

    let sswb = consume_block!(reader, SSWB);
    let vold = consume_block!(reader, VOLD);

    assert_eq!(sswb.id.as_str().unwrap(), "SSWB");
    assert_eq!(vold.id.as_str().unwrap(), "VOLD");

    let mut radar = RadarFile {
        name: sswb.radar_name.as_string().unwrap(),
        sweeps: Vec::new(),
        params: HashMap::new(),
    };

    let mut desc = DoradeDesc {
        start_time: Utc.timestamp(sswb.start_time as i64, 0),
        parm_desc: HashMap::new(),
        ngates: 0,
        compress: 0,
        scan_mode: ScanMode::PPI,
    };

    load_sensor(&mut reader, &mut radar, &mut desc);

    if options.print_products {
        println!(
            "Products: {}",
            radar.params.keys().cloned().collect::<Vec<_>>().join(", ")
        )
    }

    load_sweep(&mut reader, &mut radar, &mut desc, options);

    radar
}

/// Loads the sensor (header) part of the data
fn load_sensor(reader: &mut File, radar: &mut RadarFile, desc: &mut DoradeDesc) {
    // Load cell correction block
    // TODO: Look into
    if reader.next_string().unwrap().as_str() == "CFAC" {
        let _cfac = consume_block!(reader, CFAC);
    }

    let radd = consume_block!(reader, RADD);
    desc.scan_mode = ScanMode::from_num(radd.scan_mode);

    desc.compress = radd.data_compress;

    // If LIDR exists read it
    if reader.next_string().unwrap() == "LIDR" {
        let _lidr = consume_block!(reader, LIDR);
    }

    // Read all of the PARM blocks
    while reader.next_string().unwrap() == "PARM" {
        let parm = consume_block!(reader, PARM);

        let new_name = dorade_to_generic_name(parm.parameter_name.as_string().unwrap());

        desc.parm_desc.insert(
            new_name.clone(),
            ParmDesc {
                scale: parm.parameter_scale,
                bias: parm.parameter_bias,
                binary_format: parm.binary_format,
                nyquist: radd.eff_unamb_vel,
                offset: 0,
                bad_data: parm.bad_data,
            },
        );

        radar.params.insert(
            new_name.clone(),
            ParamDescription {
                description: parm.param_description.as_string().unwrap(),
                units: parm.param_units.as_string().unwrap(),
                meters_to_first_cell: 50.0,
                meters_between_cells: 50.0,
            },
        );
    }

    // Load the cell descriptor
    match reader.next_string().unwrap().as_str() {
        "CELV" => {
            let celv = consume_block!(reader, CELV);
            desc.ngates = celv.number_cells as u16;

            let first_gate = if celv.dist_cells[0] < 0.0 {
                0.0
            } else {
                celv.dist_cells[0]
            };
            let width = celv.dist_cells[1] - celv.dist_cells[0];

            // println!("{}, {}", first_gate, width);

            for mut val in &mut radar.params.values_mut() {
                val.meters_to_first_cell = first_gate;
                val.meters_between_cells = width;
            }
        }
        "CSFD" => {
            let csfd = consume_block!(reader, CSFD);

            let mut num_segs = csfd.num_segments;
            if num_segs > 8 {
                num_segs = 8;
            }

            for i in 0..num_segs as usize {
                desc.ngates += csfd.num_cells[i];
            }

            let first_gate = csfd.dist_to_first;
            let width = csfd.spacing[0];

            for mut val in &mut radar.params.values_mut() {
                val.meters_to_first_cell = first_gate;
                val.meters_between_cells = width;
            }
        }
        _ => panic!("Unknown cell block format"),
    };

    // Load cell correction block
    // TODO: Look into
    if reader.next_string().unwrap().as_str() == "CFAC" {
        let _cfac = consume_block!(reader, CFAC);
    }
}

/// Load a new sweep
fn load_sweep(
    reader: &mut File,
    radar: &mut RadarFile,
    desc: &mut DoradeDesc,
    options: &RadyOptions,
) {
    let _swib = consume_block!(reader, SWIB);
    let mut sweep = Sweep::default();
    sweep.scan_mode = desc.scan_mode;

    // sweep.sweep_num = radar.sweeps.len() as u32;

    while reader.next_string().unwrap() != "NULL" {
        load_ray(reader, &mut sweep, desc, options);
    }

    radar.sweeps.push(sweep);
}

/// Function to load a single ray into the sweep
fn load_ray(
    reader: &mut File,
    sweep: &mut Sweep,
    desc: &mut DoradeDesc,
    options: &RadyOptions,
) {
    // Load the first two blocks
    let ryib = consume_block!(reader, RYIB);
    let asib = consume_block!(reader, ASIB);

    // Calculate new time
    let new_time: DateTime<Utc> = {
        let ymd = Utc.ymd(desc.start_time.year(), 1, 1).and_hms(0, 0, 0);
        let julian_day = (desc.start_time - ymd).num_days() + 1;

        let (mut hour, mut min, mut sec, mut milli) = (ryib.hour as u32, ryib.minute as u32, ryib.second as u32, ryib.millisecond as u32);

        if hour * 60 * 60 * 1000 + min * 60 * 1000 + sec * 1000 + milli > 24 * 60 * 60 * 1000 {
            (hour, min, sec, milli) = (0, 0, 0, 0);
        }

        desc.start_time.date().and_hms_milli(hour, min, sec, milli)
             + Duration::days(ryib.julian_day as i64 - julian_day)
    };

    // If first ray in sweep
    if sweep.nrays() == 0 {
        sweep.latitude = asib.latitude;
        sweep.longitude = asib.longitude;
        sweep.elevation = if ryib.elevation > 180.0 {
            ryib.elevation - 360.0
        } else {
            ryib.elevation
        };
        sweep.scan_rate = Some(ryib.true_scan_rate);

        if options.location {
            println!("Location: {}, {}", sweep.latitude, sweep.longitude);
        }

        // Only load nyquist velocity if VEL is present
        if desc.parm_desc.contains_key("VEL") {
            sweep.nyquist_velocity = desc.parm_desc.get("VEL").unwrap().nyquist;
        }
    }

    // Create the new ray
    let mut new_ray = Ray {
        time: new_time,
        azimuth: ryib.azimuth,
        data: HashMap::new(),
    };

    // Loop through each gate
    while !["RYIB", "NULL"].contains(&reader.next_string().unwrap().as_str()) {
        let min_offset: usize;
        let mut data_len: usize;
        let data_type: String;

        // Load each data block
        match reader.next_string().unwrap().as_str() {
            "RDAT" => {
                let rdat = consume_block!(reader, RDAT);
                min_offset = size_of::<RDAT>();
                data_len = rdat.nbytes as usize;
                data_type = dorade_to_generic_name(rdat.pdata_name.as_string().unwrap());
            }
            "QDAT" => {
                let qdat = consume_block!(reader, QDAT);
                min_offset = size_of::<QDAT>();
                data_len = qdat.nbytes as usize;
                data_type = dorade_to_generic_name(qdat.pdata_name.as_string().unwrap());
            }
            "XSTF" => {
                consume_block!(reader, XSTF);
                continue;
            }
            _ => panic!(
                "Unexpected data block format: {}, bytes: {:x?}",
                reader.next_string().unwrap(),
                reader.next_string().unwrap().as_bytes()
            ),
        };

        // Find the data offset and where to start reading
        let struct_size = min_offset.clone();
        let mut data_offset = desc.parm_desc.get(&data_type).unwrap().offset as usize;

        if data_offset > min_offset || data_offset == 0 {
            data_offset = min_offset;
        }

        data_len -= data_offset;

        reader
            .seek(SeekFrom::Current((data_offset - struct_size) as i64))
            .unwrap();

        let param_desc = desc.parm_desc.get_mut(&data_type).unwrap();

        let mut data: Vec<f64>;

        // Match the binary format and get the data
        match param_desc.binary_format {
            1 => data = get_data::<i8>(reader, data_len, param_desc),
            2 => {
                data = if desc.compress == 0 {
                    get_data::<i16>(reader, data_len, param_desc)
                } else {
                    get_compressed_data(reader, &data_type, desc, data_len)
                }
            }
            3 => data = get_data::<i32>(reader, data_len, param_desc),
            4 => data = get_data::<f32>(reader, data_len, param_desc),
            _ => panic!("Unknown binary format"),
        }

        if data_type == "REF" {
            for elem in &mut data {
                let tmp = (*elem * options.scale) + options.offset;
                if tmp < options.remove {
                    *elem = -999.0;
                } else {
                    *elem = tmp;
                }
            }
        }

        new_ray.data.insert(data_type, data);
    }

    sweep.rays.push(new_ray);
}

trait FromBytes {
    fn from_le_bytes(bytes: &[u8]) -> Self;
}

impl FromBytes for i8 {
    fn from_le_bytes(bytes: &[u8]) -> Self {
        i8::from_le_bytes(bytes.try_into().unwrap())
    }
}

impl FromBytes for i16 {
    fn from_le_bytes(bytes: &[u8]) -> Self {
        i16::from_le_bytes(bytes.try_into().unwrap())
    }
}

impl FromBytes for i32 {
    fn from_le_bytes(bytes: &[u8]) -> Self {
        i32::from_le_bytes(bytes.try_into().unwrap())
    }
}

impl FromBytes for f32 {
    fn from_le_bytes(bytes: &[u8]) -> Self {
        f32::from_le_bytes(bytes.try_into().unwrap())
    }
}

/// Function to get non-compressed dorade data
fn get_data<T: FromBytes + Copy>(reader: &mut File, data_len: usize, desc: &ParmDesc) -> Vec<f64>
where
    f64: From<T>,
{
    let mut slice: Vec<u8> = Vec::with_capacity(data_len);
    slice.resize(data_len, 0);
    reader.read_exact(&mut slice).unwrap();

    let mut new_vec: Vec<T> = Vec::new();
    for i in (0..data_len).step_by(size_of::<T>()) {
        new_vec.push(T::from_le_bytes(&slice[i..i + size_of::<T>()]))
    }

    new_vec
        .iter()
        .map(|&x| (f64::from(x) / desc.scale as f64) + desc.bias as f64)
        .collect()
}

/// Function to decompress HRD data
#[allow(non_snake_case)]
fn decompress_HRD(raw: &Vec<u16>, bad_data: u16, ngates: u16) -> Vec<u16> {
    // Decompressed output data
    let mut decomp: Vec<u16> = Vec::with_capacity(ngates as usize);
    decomp.resize(ngates as usize, 0);

    // Variables
    let mut raw_i: usize = 0;
    let mut decomp_i: usize = 0;
    let mut nn;
    let mut wcount = 0u16;

    while raw[raw_i] != 1 {
        nn = raw[raw_i] & 0x7fff;

        if wcount + nn > ngates {
            panic!("Could not decode {} {} {}", wcount, nn, ngates);
        } else {
            wcount += nn;
        }

        if (raw[raw_i] & 0x8000) > 0 {
            raw_i += 1;

            while nn > 0 {
                decomp[decomp_i] = raw[raw_i];
                raw_i += 1;
                decomp_i += 1;
                nn -= 1;
            }
        } else {
            raw_i += 1;

            while nn > 1 {
                decomp[decomp_i] = bad_data;
                decomp_i += 1;
                nn -= 1;
            }
        }
    }

    decomp
}

fn get_compressed_data(
    reader: &mut File,
    field: &String,
    desc: &DoradeDesc,
    data_len: usize,
) -> Vec<f64> {
    // Function to decompress 16 bit dorade data

    let parm_desc = desc.parm_desc.get(field).unwrap();

    let raw: Vec<u16> = {
        let mut slice: Vec<u8> = Vec::with_capacity(data_len);
        slice.resize(data_len, 0);
        reader.read_exact(&mut slice).unwrap();
        let mut new_vec: Vec<u16> = Vec::new();
        for i in (0..data_len).step_by(2) {
            new_vec.push(u16::from_le_bytes(slice[i..i + 2].try_into().unwrap()))
        }

        new_vec
    };

    // Decompressed data
    let decomp = decompress_HRD(&raw, parm_desc.bad_data as u16, desc.ngates);

    // Read the u16 data as i16 and apply scale/offset
    decomp
        .iter()
        .map(|&x| i16::from_ne_bytes(x.to_ne_bytes()))
        .map(|x| (f64::from(x) / parm_desc.scale as f64) + parm_desc.bias as f64)
        .collect()
}
