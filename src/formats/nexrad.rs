use chrono::{DateTime, TimeZone, Utc};
use serde::{Serialize, Deserialize, de::DeserializeOwned};
use std::convert::TryInto;
use std::fs::File;
use std::{io::{Read, Write, Seek, SeekFrom}, path::Path};
use std::collections::HashMap;

use crate::{Format, RadarFile, RadyOptions, Sweep, Ray, ParamDescription};

use bincode::{DefaultOptions, Options};

#[repr(C)]
#[derive(Serialize, Deserialize)]
struct VolumeHeader {
    tape: [u8; 9],
    extension: [u8; 3],
    date: u32,
    time: u32,
    icao: [u8; 4],
}

#[repr(C)]
#[derive(Serialize, Deserialize, Debug)]
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
#[derive(Serialize, Deserialize, Debug)]
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
    ptrs: Vec<u32>,
}

#[repr(C)]
#[derive(Serialize, Deserialize)]
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
#[derive(Serialize, Deserialize, Default)]
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
#[derive(Serialize, Deserialize)]
struct ElevationDataBlock {
    block_name: [u8; 1],
    data_name: [u8; 3],
    lrtup: u16,
    atmos: i16,
    refl_calib: f32,
}

#[repr(C)]
#[derive(Serialize, Deserialize)]
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

/// Attributes that are stored per ray, but we want them to be stored per sweep
#[derive(Debug, Clone, Copy, Default)]
struct RayAttribs {
    elev: f32,
    nyq: f32,
    lat: f32,
    lon: f32,
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
        const N: usize = std::mem::size_of::<u32>();
        let mut ptrs = vec![0; N * $len];

        unsafe {
            let slice = std::slice::from_raw_parts_mut(&mut ptrs as *mut _ as *mut u8, N);
            $reader.read_exact(slice).unwrap();
        }

        BlockPtrs {
            ptrs
        }
    }};
    
    ($reader:expr, $struc:ty) => {{
        const N: usize = std::mem::size_of::<$struc>();
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
        let mut buf = vec![0; $len];
        $reader.read_exact(&mut buf).unwrap();

        buf
    }};

    ($reader:expr, $len:expr, $ty:ty) => {{
        let mut buf = vec![0; $len * std::mem::size_of::<$ty>()];
        $reader.read_exact(&mut buf).unwrap();
        buf.chunks_exact(std::mem::size_of::<$ty>())
            .map(|v| <$ty>::from_be_bytes(v.try_into().unwrap()))
            .collect::<Vec<_>>()
    }};
}

pub fn serialize<S: Serialize>(t: &S) -> Vec<u8> {
    DefaultOptions::new()
        .with_fixint_encoding()
        .with_big_endian()
        .serialize(t)
        .unwrap()
}

pub fn deserialize<R: Read, S: DeserializeOwned>(t: R) -> S {
    DefaultOptions::new()
        .with_fixint_encoding()
        .with_big_endian()
        .deserialize_from(t)
        .unwrap()
}

pub fn deserialize_block<S: DeserializeOwned>(mut t: &mut &[u8]) -> S {
    let lrtup = u16::from_be_bytes(t[4..6].try_into().unwrap()) as usize;

    if lrtup > std::mem::size_of::<S>() {
        let s: S = deserialize(&mut t);
        consume!(t, lrtup - std::mem::size_of::<S>());
        s
    } else {
        deserialize(&mut t)
    }
}

pub fn is_nexrad(path: impl AsRef<Path>) -> bool {
    // Checks if a file is in the nexrad format

    let mut file = File::open(path).unwrap();

    consume!(file, 4) == b"AR2V"
}

pub fn read_nexrad(path: impl AsRef<Path>, options: &RadyOptions) -> RadarFile {
    let mut reader = File::open(path).unwrap();

    let vol_header: VolumeHeader = deserialize(&mut reader);
    let compression_record = consume!(reader, 12);
 
    let mut buf = Vec::new();

    match &compression_record[4..6] {
        b"BZ" => buf = decompress_records(reader),
        b"\x00\x00" | b"\t\x80" => reader.read_exact(&mut buf).unwrap(),
        _ => panic!("Unknown compression record"),
    }

    let mut reader = buf.as_slice();  

    let mut params = HashMap::new();
    let mut sweeps = Vec::new();
    let mut sweep = Sweep::default();
    let mut atts = RayAttribs::default();

    while reader.len() > 0 {
        if let Some((ray, end)) = read_ray(&mut reader, &mut atts, &mut params) {
            sweep.rays.push(ray);

            if end {
                sweep.latitude = atts.lat / sweep.rays.len() as f32;
                sweep.longitude = atts.lon / sweep.rays.len() as f32;
                sweep.nyquist_velocity = atts.nyq / sweep.rays.len() as f32;
                sweep.elevation = atts.elev / sweep.rays.len() as f32;

                sweeps.push(sweep);

                atts = RayAttribs::default();
                sweep = Sweep::default();
            }
        }
    }

    RadarFile {
        name: String::from_utf8(vol_header.icao.to_vec()).unwrap(),
        sweeps,
        params
    }
}

fn read_ray(mut reader: &mut &[u8], atts: &mut RayAttribs, params: &mut HashMap<String, ParamDescription>) -> Option<(Ray, bool)> {
    let header: MsgHeader = deserialize(&mut reader);

    if header.f_type != 31 {
        consume!(reader, 2432 - std::mem::size_of::<MsgHeader>());
        return None;
    }

    let msg_31_header: Msg31Header = deserialize(&mut reader);
    let ptrs = consume!(reader, msg_31_header.block_count as usize, u32);

    let mut ray = Ray::default();
    ray.azimuth = msg_31_header.azimuth_angle;
    atts.elev += msg_31_header.elevation_angle;

    // let mut skip = 0;

    for ptr in ptrs.into_iter().filter(|&p| p > 0) {
        let ptr = ptr as usize - std::mem::size_of::<Msg31Header>() - msg_31_header.block_count as usize * std::mem::size_of::<u32>();
        let _ = read_data_block(&mut reader.split_at(ptr).1, atts, &mut ray, params);
    }

    let skip = header.size as usize * 2 - 4 - std::mem::size_of::<Msg31Header>() - msg_31_header.block_count as usize * 4;

    *reader = reader.split_at(std::cmp::min(skip, reader.len())).1;

    Some((ray, msg_31_header.radial_status == 2 || msg_31_header.radial_status == 4))
}

fn read_data_block(mut reader: &mut &[u8], atts: &mut RayAttribs, ray: &mut Ray, params: &mut HashMap<String, ParamDescription>) -> usize {
    match std::str::from_utf8(&consume!(reader.clone(), 4)[1..4]).unwrap() {
        "VOL" => {
            let vol: VolumeDataBlock = deserialize_block(reader);
            atts.lat += vol.lat;
            atts.lon += vol.lon;
            return std::mem::size_of::<VolumeDataBlock>();
        }
        "ELV" => {
            let elv: ElevationDataBlock = deserialize_block(reader);
            return std::mem::size_of::<ElevationDataBlock>();
        }
        "RAD" => {
            let rad: RadialDataBlock = deserialize_block(reader);
            atts.nyq += rad.nyquist_vel as f32 / 100.0;
            return std::mem::size_of::<RadialDataBlock>();
        }
        name if ["REF", "VEL", "SW ", "ZDR", "PHI", "RHO", "CFP"].contains(&name) => {
            let name = name.trim().to_string();
            
            let data_block: DataBlock = deserialize(&mut reader);

            if !params.contains_key(&name) {
                params.insert(name.clone(), ParamDescription {
                    meters_to_first_cell: data_block.first_gate as f32,
                    meters_between_cells: data_block.gate_spacing as f32,
                    ..Default::default()
                });
            }

            let (scale, offset) = scale_offset(&name);

            let data = match data_block.word_size {
                16 => consume!(reader, data_block.ngates as usize, u16).into_iter().map(|v| if v < 2 { f64::MIN } else { ((v as f32 - offset) / scale) as f64 }).collect(),
                8 => consume!(reader, data_block.ngates as usize, u8).into_iter().map(|v| if v < 2 { f64::MIN } else { ((v as f32 - offset) / scale) as f64 }).collect(),
                size => panic!("Unknown word size {size}"),
            };

            ray.data.insert(name, data);
            return std::mem::size_of::<DataBlock>() + data_block.ngates as usize * data_block.word_size as usize / 8;
        }
        name => panic!("Unknown product {name}"),
    }
}

fn decompress_records(mut reader: File) -> Vec<u8> {
    reader.seek(SeekFrom::Current(-12)).unwrap();

    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).unwrap();
    let mut reader = buf.as_slice();

    let mut decompressed_buf = Vec::new();

    loop {
        reader = reader.split_at(4).1;

        let mut new_buf = Vec::new();
        let mut decoder = bzip2::read::BzDecoder::new(reader);

        decoder.read_to_end(&mut new_buf).unwrap();

        reader = reader.split_at(decoder.total_in() as usize).1;

        decompressed_buf.extend(new_buf);

        if reader.len() == 0 {
            break;
        }
    }

    decompressed_buf[12..].to_vec()
}

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
        block_count: ptrs.len() as u16,
    };

    let mut bytes = serialize(&block);

    let extra = 10 - ptrs.len();

    bytes.extend(ptrs.iter().flat_map(|ptr| ptr.to_be_bytes().into_iter()).chain(std::iter::once(0).cycle().take(extra * 4)));
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
    let mut ptrs = Vec::new();;
    // let mut next_ptr: u32 = 0x90;
    let mut next_ptr = std::mem::size_of::<Msg31Header>() as u32 + 10 * 4;
    let mut data: Vec<u8> = Vec::new();
    let sweep = &radar.sweeps[sweep_index];

    for data_name in ["VOL", "ELV", "RAD"] {
        ptrs.push(next_ptr);

        let mut new_data = match data_name {
            "VOL" => { next_ptr += std::mem::size_of::<VolumeDataBlock>() as u32; pack_volume_block(sweep) },
            "ELV" => { next_ptr += std::mem::size_of::<ElevationDataBlock>() as u32; pack_elevation_block() },
            "RAD" => { next_ptr += std::mem::size_of::<RadialDataBlock>() as u32; pack_radial_block(sweep) },
            _ => unreachable!(),
        };

        data.append(&mut new_data);
    }

    for field in ["REF", "VEL", "SW", "RHO", "PHI", "ZDR"] {
        if !sweep.rays[index].data.contains_key(field) {
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

    ptrs.resize(8, 0);

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
        lrtup: std::mem::size_of::<VolumeDataBlock>() as u16,
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
        ..Default::default()
    };

    serialize(&block)
}

fn pack_elevation_block() -> Vec<u8> {
    let block = ElevationDataBlock {
        block_name: *b"R",
        data_name: *b"ELV",
        lrtup: std::mem::size_of::<ElevationDataBlock>() as u16,
        atmos: 0,
        refl_calib: 0.0,
    };

    serialize(&block)
}

fn pack_radial_block(sweep: &Sweep) -> Vec<u8> {
    let block = RadialDataBlock {
        block_name: *b"R",
        data_name: *b"RAD",
        lrtup: std::mem::size_of::<RadialDataBlock>() as u16,
        unambig_range: 0,
        noise_h: 0.0,
        noise_v: 0.0,
        nyquist_vel: (sweep.nyquist_velocity * 100.0) as u16,
        spare: 0,
    };

    serialize(&block)
}
