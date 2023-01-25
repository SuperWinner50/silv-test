use chrono::{DateTime, TimeZone, Utc};
use serde::Serialize;
use std::convert::TryInto;
use std::fs::File;
use std::{io::{Read, Write}, path::Path};

use crate::{Format, RadarFile, RadyOptions, Sweep, Ray};

use bincode::{DefaultOptions, Options};

pub fn serialize<S: serde::Serialize>(t: &S) -> Vec<u8> {
    DefaultOptions::new()
        .with_fixint_encoding()
        .with_big_endian()
        .serialize(t)
        .unwrap()
}

#[repr(C)]
#[derive(Serialize)]
struct VolumeHeader {
    tape: [u8; 9],
    extension: [u8; 3],
    date: u32,
    time: u32,
    icao: [u8; 4],
}

#[repr(C)]
#[derive(Serialize)]
struct MsgHeader {
    size: u16,
    channels: u8,
    f_type: u8,
    seq_id: u16,
    date: u16,
    ms: u32,
    segments: u16,
    seg_num: u16,
}

#[repr(C)]
#[derive(Serialize)]
struct Msg31Header {
    icao: [u8; 4],
    collect_ms: u32,
    collect_date: u16,
    azimuth_number: u16,
    azimuth_angle: f32,
    compress_flag: u8,
    spare_0: u8,
    radial_length: u16,
    azimuth_resolution: u8,
    radial_status: u8,
    elevation_number: u8,
    cut_sector: u8,
    elevation_angle: f32,
    radial_blanking: u8,
    azimuth_mode: u8,
    block_count: u16,
}

#[repr(C)]
#[derive(Serialize)]
struct BlockPtrs {
    pts: Vec<u32>,
}

#[repr(C)]
#[derive(Serialize)]
struct DataBlock {
    block_type: [u8; 1],
    data_name: [u8; 3],
    reserved: u32,
    ngates: u16,
    first_gate: u16,
    gate_spacing: u16,
    thresh: u16,
    snr_thresh: u16,
    flags: u8,
    word_size: u8,
    scale: f32,
    offset: f32,
}

#[repr(C)]
#[derive(Serialize)]
struct VolumeDataBlock {
    block_type: [u8; 1],
    data_name: [u8; 3],
    lrtup: u16,
    version_major: u8,
    version_minor: u8,
    lat: f32,
    lon: f32,
    height: u16,
    feedhorn_height: u16,
    refl_calib: f32,
    power_h: f32,
    power_v: f32,
    diff_refl_calib: f32,
    init_phase: f32,
    vcp: u16,
    spare: u16,
}

#[repr(C)]
#[derive(Serialize)]
struct ElevationDataBlock {
    block_name: [u8; 1],
    data_name: [u8; 3],
    lrtup: u16,
    atmos: i16,
    refl_calib: f32,
}

#[repr(C)]
#[derive(Serialize)]
struct RadialDataBlock {
    block_name: [u8; 1],
    data_name: [u8; 3],
    lrtup: u16,
    unambig_range: u16,
    noise_h: f32,
    noise_v: f32,
    nyquist_vel: u16,
    spare: u16,
}

fn scale_offset(data_type: &str) -> (f32, f32) {
    match data_type {
        "REF" => (2.0, 66.0),
        "VEL" => (2.0, 129.0),
        "SW" => (2.0, 129.9),
        "ZDR" => (16.0, 128.0),
        "PHI" => (2.8261, 2.0),
        "RHO" => (300.0, -60.5),
        "CFP" => (1.0, 8.0),
        _ => panic!("Unknown data type: {}", data_type),
    }
}

trait ToBytes {
    fn to_be_bytes(self) -> Vec<u8>;
}

impl ToBytes for u8 {
    fn to_be_bytes(self) -> Vec<u8> {
        vec![self]
    }
}

impl ToBytes for u16 {
    fn to_be_bytes(self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
}

trait F64ToInt<V> {
    fn f64_to_int(self) -> V;
}

impl F64ToInt<u8> for f64 {
    fn f64_to_int(self) -> u8 {
        self as u8
    }
}

impl F64ToInt<u16> for f64 {
    fn f64_to_int(self) -> u16 {
        self as u16
    }
}

/// Converts to the date and time format NEXRAD uses
fn to_day_ms(datetime: DateTime<Utc>) -> (u32, u32) {
    (
        (datetime.date() - Utc.ymd(1970, 1, 1)).num_days() as u32 + 1,
        (datetime - datetime.date().and_hms(0, 0, 0)).num_milliseconds() as u32,
    )
}

macro_rules! consume_block {
    ($reader:expr, BlockPtrs, $len:expr) => {{
        const N: usize = size_of::<u32>();
        let mut ptrs = vec![u32; N * len];

        unsafe {
            let slice = std::slice::from_raw_parts_mut(&mut new_struc as *mut _ as *mut u8, N);
            $reader.read_exact(slice).unwrap();
        }

        BlockPtrs {
            ptrs
        }
    }};
    
    ($reader:expr, $struc:ty) => {{
        const N: usize = size_of::<$struc>();
        let mut new_struc: $struc = unsafe { std::mem::zeroed() };

        unsafe {
            let slice = std::slice::from_raw_parts_mut(&mut new_struc as *mut _ as *mut u8, N);
            $reader.read_exact(slice).unwrap();
        }

        new_struc
    }};
}

macro_rules! consume {
    ($reader:expr, $len:expr) => {{
        let mut buf = vec![0; len];
        $reader.read_exact(&mut buf).unwrap();

        buf
    }};

    ($reader:expr, $len:expr, $ty:ty) => {{
        let mut buf = vec![0; len * std::mem::size_of::<$ty>()];
        $reader.read_exact(&mut buf).unwrap();
        buf.chunks_exact(std::mem::size_of::<$ty>())
            .map(|v| <$ty>::from_le_bytes(v.try_into().unwrap()))
            .collect()
    }};
}

/*
fn read_nexrad(path: impl AsRef<Path>, options: &RadyOptions) -> RadarFile {
    let mut reader = File::open(path).unwrap();

    let vol_header = consume_block!(reader, VolumeHeader);
    let compression_record = consume!(reader, 12);
 
    let mut buf = Vec::new();

    match compression_record[4..6] {
        b"BZ" => panic!("BZ not supported"),
        b"\x00\x00" | b"\t\x80" => reader.read_exact(&mut buf).unwrap(),
        _ => panic!("Unknown compression record"),
    }

    let reader = buf.as_slice();

    while reader.len() > 0 {
        if let Some(ray) = read_ray(reader, &mut sweep) {

        }
    }
    
    
}

fn read_ray(reader: &[u8], sweep: &mut Sweep) -> Option<Ray> {
    let header = consume_block!(reader, MsgHeader);

    if header.f_type != 31 {
        return None;
    }

    let msg_31_header = consume_block!(reader.clone(), Msg31Header);
    let ptrs = consume_block!(reader, BlockPtrs, msg_31_header.block_count as usize);

    let mut ray = Ray::default();
    ray.azimuth = msg_31_header.azimuth_angle;

    for ptr in ptrs {
        read_data_block(&reader.clone()[ptr..], sweep, &mut ray);
    }

}

fn read_data_block(reader: &[u8], sweep: &mut Sweep, ray: &mut Ray) -> RadarFile {
    match &consume!(reader.clone(), 4).as_slice()[1..4] {
        b"VOL" => {
            let vol = consume_block!(reader, VolumeDataBlock);
            sweep.latitude = vol.lat;
            sweep.longitude = vol.lon;
        }
        b"ELV" => {
            let elv = consume_block!(reader, ElevationDataBlock);
            // sweep.nyquist_vel = elv.nyquist;
        }
        b"RAD" => {
            let rad = consume_block!(reader, RadialDataBlock);
            sweep.nyquist_velocity = rad.nyquist_vel;
        }
        name if [b"REF", b"VEL", b"SW", b"ZDR", b"PHI", b"RHO", b"CFP"].contains(name) => {
            let data_block = consume_block!(reader, DataBlock);

            let data = match data_block.word_size {
                16 => consume!(reader, data_block.ngates as usize, u16).map(|v| v as f64).collect(),
                8 => consume!(reader, data_block.ngates as usize, u8).map(|v| v as f64).collect(),
                size => panic!("Unknown block size: {size}"),
            };

            ray.data.insert(name.to_string(), data);
        }
    }
}
*/

/// Function to write a nexrad file
pub fn write_nexrad(radar: &RadarFile, path: impl AsRef<Path>, options: &RadyOptions) {
    let mut writer = create_new_file(path, radar, 0, options);

    for sweep_index in 0..radar.nsweeps() as usize {
        write_sweep(radar, sweep_index, &mut writer);
    }
}

fn string_to_bytes(string: &str) -> [u8; 4] {
    let mut bytes = [0u8; 4];
    to_padded_string(&mut bytes, string);
    bytes
}

fn to_padded_string(mut bytes: &mut [u8], string: &str) {
    bytes.write(string.to_uppercase().as_bytes()).unwrap();
}

/// Creates and initializes a new nexrad file
fn create_new_file(
    path: impl AsRef<Path>,
    radar: &RadarFile,
    sweep_index: usize,
    options: &RadyOptions,
) -> File {
    let sweep = &radar.sweeps[sweep_index];
    let mut file_name = path.as_ref().to_path_buf();

    // Generate the name for the file
    if let Some(name_format) = &options.name_format {
        let time = sweep.time();

        file_name.push(
            time.format(name_format)
                .to_string()
                .replace("[icao]", &radar.name.as_str()[0..4].to_uppercase())
                .as_str(),
        );
    } else {
        file_name.push(
            sweep.time().format(Format::NEXRAD.format_str()).to_string()
                + format!("_{:.1}", sweep.elevation).as_str(),
        );
    }

    // Creates the directory if it doesnt exist
    std::fs::create_dir_all(file_name.parent().unwrap()).unwrap();

    // Open the new file
    let mut writer = File::create(file_name).unwrap();
    let (date, time) = to_day_ms(sweep.time());
    let icao = string_to_bytes(&radar.name);

    // println!("writing");

    // Write the volume header
    let volume = VolumeHeader {
        tape: *b"AR2V0006.",
        extension: *b"001",
        date,
        time,
        icao,
    };

    let bytes = serialize(&volume);
    writer.write_all(&bytes).unwrap();
    writer.write_all(&[0u8; 12]).unwrap();

    writer
}

/// Writes a sweep to the file
fn write_sweep(radar: &RadarFile, sweep_index: usize, writer: &mut File) {
    let sweep = &radar.sweeps[sweep_index];

    for index in 0..sweep.nrays() as usize {
        let (data, ptrs) = pack_data(radar, sweep_index, index);
        let msg_header = pack_msg_header(sweep, data.len());
        let msg_31_header = pack_msg_31_header(radar, sweep_index, index as u16, &ptrs);

        writer.write_all(&msg_header).unwrap();
        writer.write_all(&msg_31_header).unwrap();
        writer.write_all(&data).unwrap();
    }
}

/// Packs a MSG31 header block
fn pack_msg_31_header(radar: &RadarFile, sweep_index: usize, index: u16, ptrs: &[u32]) -> Vec<u8> {
    let sweep = &radar.sweeps[sweep_index];

    let (date, ms) = to_day_ms(sweep.time());
    let radial_status = {
        if index == 0 && sweep_index == 0 {
            3
        } else if index == sweep.nrays() - 1 && sweep_index as u16 == radar.nsweeps() - 1 {
            4
        } else if index == 0 {
            0
        } else if index == sweep.nrays() - 1 {
            2
        } else {
            1
        }
    } as u8;

    let block = Msg31Header {
        icao: string_to_bytes(&radar.name),
        collect_ms: ms,
        collect_date: date as u16,
        azimuth_number: index + 1,
        azimuth_angle: sweep.rays[index as usize].azimuth,
        compress_flag: 0,
        spare_0: 0,
        radial_length: 0,
        azimuth_resolution: 1,
        radial_status,
        elevation_number: sweep_index as u8 + 1,
        cut_sector: 1, // Check
        elevation_angle: sweep.elevation,
        radial_blanking: 0, // Check
        azimuth_mode: 0,
        block_count: 9,
    };

    let mut bytes = serialize(&block);
    bytes.extend(serialize(&ptrs[0..9].to_vec()));
    bytes
}

/// Packs a msg header block
fn pack_msg_header(sweep: &Sweep, data_len: usize) -> Vec<u8> {
    let (date, ms) = to_day_ms(sweep.time());

    let block = MsgHeader {
        size: (std::mem::size_of::<Msg31Header>() + data_len + 4) as u16 / 2,
        channels: 0,
        f_type: 31,
        seq_id: 0,
        date: date as u16,
        ms,
        segments: 1,
        seg_num: 1,
    };

    serialize(&block)
}

/// Packs the data blocks
fn pack_data(radar: &RadarFile, sweep_index: usize, index: usize) -> (Vec<u8>, Vec<u32>) {
    let mut ptrs: Vec<u32> = vec![0x44, 0x70, 0x7C];
    let mut next_ptr: u32 = 0x90;
    let mut data: Vec<u8> = Vec::new();
    let sweep = &radar.sweeps[sweep_index];

    for data_name in ["VOL", "ELV", "RAD"] {
        let mut new_data = match data_name {
            "VOL" => pack_volume_block(sweep),
            "ELV" => pack_elevation_block(),
            "RAD" => pack_radial_block(sweep),
            _ => unreachable!(),
        };

        data.append(&mut new_data);
    }

    for field in ["REF", "VEL", "SW", "RHO", "PHI", "ZDR"] {
        if !radar.params.keys().cloned().any(|x| x == *field) {
            continue;
        }

        let mut new_data = pack_data_block(sweep, field, radar);
        let mut array_data: Vec<u8>;

        if field == "PHI" {
            array_data = pack_data_array::<u16>(sweep, index, 65535.0, field);
        } else {
            array_data = pack_data_array::<u8>(sweep, index, 255.0, field);
        }

        ptrs.push(next_ptr);
        next_ptr += new_data.len() as u32 + array_data.len() as u32 + 12;

        data.append(&mut new_data);
        data.append(&mut array_data);
        data.append(&mut vec![0u8; 12]);
    }

    ptrs.resize(9, 0);

    (data, ptrs)
}

/// Packs a generic data block
fn pack_data_block(sweep: &Sweep, field: &str, radar: &RadarFile) -> Vec<u8> {
    let (scale, offset) = scale_offset(field);
    let param = radar.params.get(&field.to_string()).unwrap();
    let mut field_name = field.as_bytes().to_vec();
    field_name.resize(3, 0);

    let block = DataBlock {
        block_type: *b"D",
        data_name: field_name.as_slice().try_into().unwrap(),
        reserved: 0,
        ngates: sweep.ngates(),
        first_gate: param.meters_to_first_cell as u16, // Already in meters
        gate_spacing: param.meters_between_cells as u16, // Already in meters
        thresh: 0,
        snr_thresh: 0,
        flags: 0,
        word_size: if field == "PHI" { 16 } else { 8 },
        scale,
        offset,
    };

    serialize(&block)
}

/// Packs a generic data array
fn pack_data_array<T: From<u8> + ToBytes>(
    sweep: &Sweep,
    index: usize,
    max_val: f64,
    field: &str,
) -> Vec<u8>
where
    f64: F64ToInt<T>,
{
    let data = sweep.rays[index].data.get(&field.to_string()).unwrap();
    // if field == "REF" {
    //     println!("{:?}", data);
    // }

    let mut new_data: Vec<T> = Vec::with_capacity(data.len());

    let (scale, offset) = scale_offset(field);

    for i in 0..data.len() {
        let val = (data[i] * scale as f64) + offset as f64;

        if val > max_val || val < 2.0 {
            new_data.push(<f64 as F64ToInt<T>>::f64_to_int(0.0f64));
        } else {
            new_data.push(<f64 as F64ToInt<T>>::f64_to_int(val));
        }
    }

    new_data.into_iter().flat_map(|x| x.to_be_bytes()).collect()
}

fn pack_volume_block(sweep: &Sweep) -> Vec<u8> {
    let block = VolumeDataBlock {
        block_type: *b"R",
        data_name: *b"VOL",
        lrtup: 0x2C,
        version_major: 0,
        version_minor: 0,
        lat: sweep.latitude,
        lon: sweep.longitude,
        height: 0,
        feedhorn_height: 0,
        refl_calib: 0.0,
        power_h: 0.0,
        power_v: 0.0,
        diff_refl_calib: 0.0,
        init_phase: 0.0,
        vcp: 0,
        spare: 0,
    };

    serialize(&block)
}

fn pack_elevation_block() -> Vec<u8> {
    let block = ElevationDataBlock {
        block_name: *b"R",
        data_name: *b"ELV",
        lrtup: 0x0C,
        atmos: 0,
        refl_calib: 0.0,
    };

    serialize(&block)
}

fn pack_radial_block(sweep: &Sweep) -> Vec<u8> {
    let block = RadialDataBlock {
        block_name: *b"R",
        data_name: *b"RAD",
        lrtup: 0x14,
        unambig_range: 0,
        noise_h: 0.0,
        noise_v: 0.0,
        nyquist_vel: (sweep.nyquist_velocity * 100.0) as u16,
        spare: 0,
    };

    serialize(&block)
}
