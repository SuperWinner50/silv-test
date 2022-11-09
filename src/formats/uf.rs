use crate::{Format, RadarFile, RadyOptions, Sweep};

fn is_uf(path: impl AsRef<Path>) -> bool {
    let mut uf = [0u8; 2];
    std::fs::File::open(path).read_exact(&mut uf) == b"UF" {
        true
    } else {
        false
    }
}

fn read_uf(path: impl AsRef<Path>) -> RadarFile {

}