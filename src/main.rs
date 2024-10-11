use std::{
    fs::{File, OpenOptions},
    io::{BufRead, Read, Write},
    thread,
};

use memmap2::Mmap;
use rayon::iter::{ParallelBridge, ParallelIterator};

fn reverse_component(dna: &mut [u8]) {
    dna.reverse();

    for base in dna {
        match *base {
            b'A' | b'a' => *base = b'T',
            b'T' | b't' => *base = b'A',
            b'C' | b'c' => *base = b'G',
            b'G' | b'g' => *base = b'C',
            _ => {}
        }
    }
}

fn main() -> anyhow::Result<()> {
    // 全量读取测试
    let start = std::time::Instant::now();
    let mut input = File::open("filteredReads.txt")?;
    let mut mmap = vec![];
    input.read_to_end(&mut mmap)?;
    println!("Time to read file all: {}ms", start.elapsed().as_millis());

    // mmap读取测试 速度损耗比全量读取慢了至少300ms，暂时放弃
    // let input = File::open("filteredReads.txt")?;
    // let mmap = unsafe { Mmap::map(&input)? };

    let mut output_file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open("reversedReads.txt")?;

    // // 简单并行方案
    // let start = std::time::Instant::now();
    // for line in mmap.lines() {
    //     let line = line?;
    //     if line.starts_with('@') {
    //         output_file.write_all(line.as_bytes())?;
    //         output_file.write_all(b"\n")?;
    //         continue;
    //     }
    //     let mut dna = line.as_bytes().to_vec();
    //     dna.chunks_mut(1024).par_bridge().for_each(|chunk| {
    //         reverse_component(chunk);
    //     });
    //     output_file.write_all(&dna)?;
    //     output_file.write_all(b"\n")?;
    // }
    // println!("Time to process: {}ms", start.elapsed().as_millis());

    // 计算 IO 分离方案
    let start = std::time::Instant::now();
    let (tx, rx) = std::sync::mpsc::channel::<Vec<u8>>();
    let writer_handle = thread::spawn(move || {
        while let Ok(data) = rx.recv() {
            if data.is_empty() {
                break;
            }
            if let Err(e) = output_file.write_all(&data) {
                println!("Write error: {}", e);
            };
        }
        println!("Write thread finished.")
    });

    let mut calc_timer: u128 = 0;
    for line in mmap.lines() {
        let line = line?;
        if line.starts_with('@') {
            tx.send(line.as_bytes().to_vec())?;
            tx.send(b"\n".to_vec())?;
            continue;
        }
        let mut dna = line.as_bytes().to_vec();
        let calc_start = std::time::Instant::now();
        dna.chunks_mut(1024).par_bridge().for_each(|chunk| {
            reverse_component(chunk);
        });
        calc_timer += calc_start.elapsed().as_micros();
        tx.send(dna)?;
        tx.send(b"\n".to_vec())?;
    }

    tx.send(vec![]).unwrap();
    writer_handle.join().unwrap();
    println!("Time to calculate: {:.3}ms", calc_timer as f64 / 1000.0);
    println!("Time to process: {}ms", start.elapsed().as_millis());

    Ok(())
}
