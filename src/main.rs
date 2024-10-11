use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufRead, Read, Write},
    sync::LazyLock,
    thread,
};

use nohash::BuildNoHashHasher;
use rayon::iter::{ParallelBridge, ParallelIterator};

type ReverseMap = HashMap<u8, u8, BuildNoHashHasher<u8>>;

static BASE_REVERSE: LazyLock<ReverseMap> = LazyLock::new(|| {
    let mut m: ReverseMap = HashMap::with_hasher(BuildNoHashHasher::default());
    m.insert(b'A', b'T');
    m.insert(b'a', b'T');
    m.insert(b'T', b'A');
    m.insert(b't', b'A');
    m.insert(b'C', b'G');
    m.insert(b'c', b'G');
    m.insert(b'G', b'C');
    m.insert(b'g', b'C');
    m
});

fn reverse_component(dna: &mut [u8]) {
    dna.reverse();

    for base in dna {
        // match *base {
        //     b'A' | b'a' => *base = b'T',
        //     b'T' | b't' => *base = b'A',
        //     b'C' | b'c' => *base = b'G',
        //     b'G' | b'g' => *base = b'C',
        //     _ => {}
        // }
        // 使用哈希表降低100ms
        if let Some(reversed_base) = BASE_REVERSE.get(base) {
            *base = *reversed_base;
        }
    }
}

fn main() -> anyhow::Result<()> {
    // 全量读取测试
    let all_start = std::time::Instant::now();
    let mut input = File::open("filteredReads.txt")?;
    let mut mmap = vec![];
    input.read_to_end(&mut mmap)?;
    println!(
        "Time to read file all: {}ms",
        all_start.elapsed().as_millis()
    );

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

    // 并行计算 IO 分离方案
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
    println!("All time spent: {}ms", all_start.elapsed().as_millis());

    Ok(())
}
