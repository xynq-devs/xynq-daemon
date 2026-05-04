use tracing_subscriber::{
    EnvFilter,
    fmt,
};

// pub fn init_logging() {
//     let filter = EnvFilter::from_env("XYNC_LOG_LEVEL");

//     fmt().with_env_filter(filter).with_target(false).init();
// }
//
pub fn init_logging() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));

    fmt().with_env_filter(filter).with_target(false).init();
}
