use std::io::Write; 

pub fn log(msg: &str) { 
    if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open("torx_debug.log") {
         let _ = writeln!(f, "{}", msg);
    } 
}