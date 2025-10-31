// This code would be in a separate Rust program, e.g., `src/bin/generate_protos.rs`
use std::io::Result;
use std::path::PathBuf;

fn main() -> Result<()> {
     // Define the output directory where the generated .rs files will be saved
    let out_dir = PathBuf::from("/mmap/generated_rust");
    
    // Create the output directory if it doesn't exist
    std::fs::create_dir_all(&out_dir)?;

    // Configure prost-build
    let mut config = prost_build::Config::new();

    // Set the output directory explicitly
    config.out_dir(&out_dir);

    // Specify the .proto files and the directories to search for imports
    config.compile_protos(
        &["/mmap/mmap.proto"],
        &["/mmap"]
    )?;

    println!("Proto files compiled successfully to {:?}", out_dir);
    Ok(())
}