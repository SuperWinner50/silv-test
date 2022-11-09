fn main() {
    let mut args = silv::arg_parse();
    args.sort_rays_by_azimuth = true;

    silv::convert(&args);
}
