
// place data parsing code is copied from https://github.com/adryzz/place-db
use std::{
    fs::File,
    io::{self, BufReader, Write},
    time::{Instant, Duration}, collections::HashMap,
};
use subprocess::Exec;
use itertools::Itertools;
use num_enum::IntoPrimitive;
use num_enum::TryFromPrimitive;
use anyhow::{anyhow, Ok};
use chrono::TimeZone;
use chrono::Timelike;
use chrono::Utc;
use csv::{ReaderBuilder, StringRecord};
use flate2::read::GzDecoder;
use futures::TryStreamExt;
use rayon::prelude::{IntoParallelRefIterator, ParallelIterator};
use sqlx::{sqlite::SqlitePoolOptions, Pool, Sqlite};

#[derive(Debug, Clone, Copy, sqlx::FromRow)]
struct RedditPixel {
    timestamp: i64,
    id: i64,
    x: i32,
    y: i32,
    x1: i32,
    y1: i32,
    color: u32
}

#[tokio::main]
async fn main() {
    //create_table().await.unwrap();
    //create_table_sorted(&"db.sqlite", &"data_2023_sorted.sqlite").await.unwrap();

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(3))
        .connect(&"db.sqlite").await.unwrap();

    render(
        &pool,
        //162, 173, 201, 202, // bad apple frame
        //-1500, -1000, 1499, 999, // whole thing
        148, 172, 148 + 135, 172 + 93, // osu! template
        date_to_timestamp(&"2023-07-20 13:00 UTC").unwrap(),
        date_to_timestamp(&"2023-07-25 22:00 UTC").unwrap(),
        19_500 // bad apple at 60 fps
        //120_000
    ).await.unwrap();
}

fn put_pixel(frame: &mut [u8], canvas_x: i32, canvas_y: i32, color: u32, x1: i32, y1: i32, width: i32, height: i32) {
    let x = canvas_x - x1;
    let y = canvas_y - y1;
    if x < 0 || y < 0 || x >= width || y >= height {
        return;
    }
    let r: u8 = ((color >> 16) & 0xFF) as u8;
    let g: u8 = ((color >> 8) & 0xFF) as u8;
    let b: u8 = (color & 0xFF) as u8;
    frame[(y * width + x) as usize * 4] = r;
    frame[(y * width + x) as usize * 4 + 1] = g;
    frame[(y * width + x) as usize * 4 + 2] = b;
    frame[(y * width + x) as usize * 4 + 3] = 0xFF;
}

fn draw_pixel(frame: &mut [u8], pixel: &RedditPixel, x1: i32, y1: i32, width: i32, height: i32) {
    //apply 25% dim
    //let r: u8 = ((pixel.color >> 16) & 0xFF) as u8;
    //let g: u8 = ((pixel.color >> 8) & 0xFF) as u8;
    //let b: u8 = (pixel.color & 0xFF) as u8;
    //let dimmed_color: u32 = (((r as u32) << 14) & 0xFF0000) | (((g as u32) << 6) & 0xFF00) | (((b as u32) >> 2) & 0xFF);
    put_pixel(frame, pixel.x, pixel.y, pixel.color, x1, y1, width, height);
}

fn draw_rectangle(frame: &mut [u8], pixel: &RedditPixel, x1: i32, y1: i32, width: i32, height: i32) {
    for y in (pixel.y)..=(pixel.y1) {
        for x in (pixel.x)..=(pixel.x1) {
            put_pixel(frame, x, y, pixel.color, x1, y1, width, height);
        }
    }
}

fn draw_circle(frame: &mut [u8], pixel: &RedditPixel, x1: i32, y1: i32, width: i32, height: i32) {
    let r = pixel.x1;
    for y in (pixel.y - r)..=(pixel.y + r) {
        for x in (pixel.x - r)..=(pixel.x + r) {
            if (x - pixel.x).pow(2) + (y - pixel.y).pow(2) <= r.pow(2) {
                put_pixel(frame, x, y, pixel.color, x1, y1, width, height);
            }
        }
    }
}

async fn render(pool: &Pool<Sqlite>, x1: i32, y1: i32, x2: i32, y2: i32, starttime: i64, endtime: i64, frame_interval: usize) -> anyhow::Result<()> {
    println!("start: {}", starttime);
    println!("end: {}", endtime);
    println!("frame interval: {}", frame_interval);

    let width = x2 - x1 + 1;
    let height = y2 - y1 + 1;

    let mut ffmpeg_stdin = Exec::cmd("ffmpeg")
        .args(&["-y",
            "-f", "rawvideo",
            "-vcodec", "rawvideo",
            "-s", format!("{}x{}", width, height).as_str(),
            "-pix_fmt", "rgba",
            "-r", "60",
            "-i", "-",
            "-an",
            "-vf", "scale=-2:1080:flags=neighbor",
            "-profile:v", "high",
            "-preset", "slow",
            "-c:v", "libx264",
            "-crf", "25",
            "-pix_fmt", "yuv420p",
            "-color_range", "1",
            "-colorspace", "1",
            "-color_trc", "1",
            "-color_primaries", "1",
            "-movflags", "+write_colr",
            "output.mp4",
        ]).stream_stdin()?;
    
    let mut pixels = sqlx::query_as::<_, RedditPixel>(r#"
            SELECT
                *
            FROM
                pixels
            WHERE
                timestamp >= $1
                AND timestamp <= $2
                AND x >= $3
                AND y >= $4
                AND x <= $5
                AND y <= $6
        "#)
        .bind(starttime)
        .bind(endtime)
        .bind(x1)
        .bind(y1)
        .bind(x2)
        .bind(y2)
        .fetch(pool);

    let mut frame = vec![0xFF; (width * height * 4) as usize];

    let mut i = 0;
    let next_frame_timestamp = starttime;
    for current_time in (starttime..endtime).step_by(frame_interval) {
        while let Some(pixel) = pixels.try_next().await? {
            if pixel.timestamp >= current_time {
                break;
            }
            match pixel {
                RedditPixel { x1: i32::MAX, y1: i32::MAX, .. } => {
                    // normal pixel
                    if pixel.x < x1 || pixel.x > x2 || pixel.y < y1 || pixel.y > y2 {
                        continue;
                    }
                    draw_pixel(&mut frame, &pixel, x1, y1, width, height);
                    i += 1;
                },
                RedditPixel { y1: i32::MAX, .. } => {
                    // mod circle
                    draw_circle(&mut frame, &pixel, x1, y1, width, height);
                    i += 1;
                },
                _ => {
                    // mod rect
                    draw_rectangle(&mut frame, &pixel, x1, y1, width, height);
                    i += 1;
                },
            }
        }
        ffmpeg_stdin.write(frame.as_slice())?;
    }
    ffmpeg_stdin.write(frame.as_slice())?;
    println!("Placements rendered: {}\n", i);
    
    Ok(())
}

async fn create_table_sorted(original_file: &str, file: &str) -> anyhow::Result<()> {
    std::fs::File::create(file)?;

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(3))
        .connect(file)
        .await?;

    let pool_original = SqlitePoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(3))
        .connect(original_file)
        .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS pixels (
            timestamp INTEGER,
            id INTEGER,
            x INTEGER,
            y INTEGER,
            x1 INTEGER,
            y1 INTEGER,
            color INTEGER
        )
        "#
    )
    .execute(&pool)
    .await?;

    let progress = progress_bar(130_000_000);

    let mut pixels = sqlx::query_as::<_, RedditPixel>(r#"SELECT * FROM pixels ORDER BY timestamp ASC"#)
        .fetch(&pool_original);

    let mut i = 0;
    let mut transaction = pool.begin().await?;
    while let Some(pixel) = pixels.try_next().await? {
        let q = sqlx::query("INSERT INTO pixels (timestamp, id, x, y, x1, y1, color) VALUES ($1, $2, $3, $4, $5, $6, $7)")
            .bind(pixel.timestamp)
            .bind(pixel.id)
            .bind(pixel.x)
            .bind(pixel.y)
            .bind(pixel.x1)
            .bind(pixel.y1)
            .bind(pixel.color)
            .execute(&mut *transaction).await?;
        progress.inc(1);
        if i % 100_000 == 0 {
            transaction.commit().await?;
            transaction = pool.begin().await?;
        }
        i += 1;
    }
    transaction.commit().await?;
    progress.finish();

    Ok(())
}


async fn create_table() -> anyhow::Result<()> {
    let directory = "../data_2023";
    let entries = std::fs::read_dir(directory)?;

    std::fs::File::create("db.sqlite")?;

    let pool = SqlitePoolOptions::new()
    .max_connections(5)
    .acquire_timeout(Duration::from_secs(3))
    .connect("sqlite://db.sqlite")
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS pixels (
            timestamp INTEGER,
            id INTEGER,
            x INTEGER,
            y INTEGER,
            x1 INTEGER,
            y1 INTEGER,
            color INTEGER
        )
        "#
    )
    .execute(&pool)
    .await?;

    let file_paths: Vec<String> = entries
        .filter_map(|e| {
            match e {
                anyhow::Result::Ok(entry) => {
                    if let Some(file_name) = entry.file_name().to_str() {
                        if file_name.ends_with(".csv.gzip") {
                            return Some(file_name.to_string());
                        }
                    }
                }
                _ => (),
            }
            None
        })
        .map(|a| format!("{}/{}", directory, a))
        .collect();

    dbg!(&file_paths);

    let mut map: HashMap<String, i64> = HashMap::with_capacity(500000);
    let mut count = 0i64;
    let mut fc = 0;

    let progress = progress_bar(130_000_000);
    for f in file_paths.iter() {
        println!("file: {}", fc);
        println!("file_path: {}", f);
        read_csv_gzip_file(f, &mut map, &mut count, &pool, &progress).await?;
        fc+= 1;
    }
    progress.finish();

    Ok(())
}

async fn read_csv_gzip_file(file_path: &str, map: &mut HashMap<String, i64>, count: &mut i64, pool: &Pool<Sqlite>, progress: &indicatif::ProgressBar) -> anyhow::Result<()> {
    let file = File::open(file_path)?;
    let gz_decoder = GzDecoder::new(file);
    let reader = BufReader::new(gz_decoder);
    let mut csv_reader = ReaderBuilder::new().from_reader(reader);
    dbg!(csv_reader.headers()?);

    let chunks = csv_reader.records().chunks(100_000);

    for chunk in chunks.into_iter() {
        let mut transaction = pool.begin().await?;
        for record in chunk {
            let r = record?;
            let pix = read_record(r, map, count)?;
            let q = sqlx::query("INSERT INTO pixels (timestamp, id, x, y, x1, y1, color) VALUES ($1, $2, $3, $4, $5, $6, $7)")
            .bind(pix.timestamp)
            .bind(pix.id)
            .bind(pix.x)
            .bind(pix.y)
            .bind(pix.x1)
            .bind(pix.y1)
            .bind(pix.color)
            .execute(&mut *transaction).await?;
            progress.inc(1);
        }
        transaction.commit().await?;
    }

    Ok(())
}

fn read_record(record: StringRecord, map: &mut HashMap<String, i64>, count: &mut i64) -> anyhow::Result<RedditPixel> {
    let a = record.get(0).ok_or(anyhow!("bad format"))?;
    let timestamp = date_components_to_timestamp(read_date(a)?)?;

    let string_id = record.get(1).ok_or(anyhow!("bad format"))?;
    let id;
    if let Some(e) = map.get(string_id) {
        id = *e;
    } else {
        *count += 1;
        id = *count;
        map.insert(string_id.to_string(), id);
    }

    let b = record.get(2).ok_or(anyhow!("bad format"))?;
    let coords = read_coords(b)?;

    let c = record.get(3).ok_or(anyhow!("bad format"))?;
    let color = read_color(c)?;



    Ok(RedditPixel {
        timestamp,
        id,
        x: coords.0,
        y: coords.1,
        x1: coords.2,
        y1: coords.3,
        color
    })
}

fn date_to_timestamp(text: &str) -> anyhow::Result<i64> {
    Ok(date_components_to_timestamp(read_date(text)?)?)
}

fn read_date(text: &str) -> anyhow::Result<[u32; 7]> {
    let mut components = [0u32; 7];
    let mut current = 0usize;
    let mut multiplier = 10000u32;
    for c in text.chars() {
        if c.is_ascii_digit() {
            components[current] += c.to_digit(10).ok_or(anyhow!("bad digit"))? * multiplier;
            multiplier /= 10;
        } else {
            if current >= components.len() {
                return Ok(components);
            }
            components[current] /= multiplier * 10;
            current += 1;
            multiplier = 1000;
        }
    }

    Err(anyhow!("bad date"))
}

fn date_components_to_timestamp(c: [u32; 7]) -> anyhow::Result<i64> {
    let dt = Utc
        .with_ymd_and_hms(c[0].try_into()?, c[1], c[2], c[3], c[4], c[5])
        .earliest()
        .ok_or(anyhow!(""))?;

    Ok(dt.timestamp_millis() + (c[6] as i64))
}

fn read_coords(text: &str) -> anyhow::Result<(i32, i32, i32, i32)> {
    let mut comma_indices = Vec::with_capacity(4);

    for (index, character) in text.chars().enumerate() {
        if character == ',' {
            comma_indices.push(index);
        }
    }

    match comma_indices.len() {
        1 => {
            let first = &text[0..comma_indices[0]];
            let second = &text[comma_indices[0]+1..];
            Ok((i32::from_str_radix(first, 10)?, i32::from_str_radix(second, 10)?, i32::MAX, i32::MAX))
        }
        2 => {
            // these idiots put JSON in my CSV ffs
            // turns out it's not even JSON
            let mut colon_indices = Vec::with_capacity(4);
            for (index, character) in text.chars().enumerate() {
                if character == ':' {
                    colon_indices.push(index);
                }
            }
            let first = &text[colon_indices[0]+2..comma_indices[0]];
            let second = &text[colon_indices[1]+2..comma_indices[1]];
            let third = &text[colon_indices[2]+2..text.len()-1];
            //println!("Mod circle");
            Ok((i32::from_str_radix(first, 10)?, i32::from_str_radix(second, 10)?, i32::from_str_radix(third, 10)?, i32::MAX))
        }
        3 => {
            let first = &text[0..comma_indices[0]];
            let second = &text[comma_indices[0]+1..comma_indices[1]];
            let third = &text[comma_indices[1]+1..comma_indices[2]];
            let fourth = &text[comma_indices[2]+1..];
            //println!("Mod rectangle");
            Ok((i32::from_str_radix(first, 10)?, i32::from_str_radix(second, 10)?, i32::from_str_radix(third, 10)?, i32::from_str_radix(fourth, 10)?))
        }
        _ => Err(anyhow!("formatting error"))
    }
}

fn read_color(text: &str) -> anyhow::Result<u32> {
    Ok(u32::from_str_radix(&text[1..], 16)?)

/*
    // set alpha
    let color = Colors::try_from_primitive((0xFF << 24) | color_value)?;
    Ok(get_color_id(color)) */
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
#[repr(u32)]
enum Colors
{
    Burgundy = 4279894125,
    DarkRed = 4281925822,
    Red = 4278207999,
    Orange = 4278233343,
    Yellow = 4281718527,
    PaleYellow = 4290312447,
    DarkGreen = 4285047552,
    Green = 4286106624,
    LightGreen = 4283886974,
    DarkTeal = 4285494528,
    Teal = 4289371648,
    LightTeal = 4290825216,
    DarkBlue = 4288958500,
    Blue = 4293562422,
    LightBlue = 4294240593,
    Indigo = 4290853449,
    Periwinkle = 4294925418,
    Lavender = 4294947732,
    DarkPurple = 4288618113,
    Purple = 4290792116,
    PalePurple = 4294945764,
    Magenta = 4286517470,
    Pink = 4286684415,
    LightPink = 4289370623,
    DarkBrown = 4281288813,
    Brown = 4280707484,
    Beige = 4285576447,
    Black = 4278190080,
    DarkGray = 4283585105,
    Gray = 4287663497,
    LightGray = 4292466644,
    White = 4294967295
}

fn get_color_id(c: Colors) -> u8
{
    match c {
        Colors::Burgundy => 0,
        Colors::DarkRed => 1,
        Colors::Red => 2,
        Colors::Orange => 3,
        Colors::Yellow => 4,
        Colors::PaleYellow => 5,
        Colors::DarkGreen => 6,
        Colors::Green => 7,
        Colors::LightGreen => 8,
        Colors::DarkTeal => 9,
        Colors::Teal => 10,
        Colors::LightTeal => 11,
        Colors::DarkBlue => 12,
        Colors::Blue => 13,
        Colors::LightBlue => 14,
        Colors::Indigo => 15,
        Colors::Periwinkle => 16,
        Colors::Lavender => 17,
        Colors::DarkPurple => 18,
        Colors::Purple => 19,
        Colors::PalePurple => 20,
        Colors::Magenta => 21,
        Colors::Pink => 22,
        Colors::LightPink => 23,
        Colors::DarkBrown => 24,
        Colors::Brown => 25,
        Colors::Beige => 26,
        Colors::Black => 27,
        Colors::DarkGray => 28,
        Colors::Gray => 29,
        Colors::LightGray => 30,
        Colors::White => 31
    }
}


fn progress_bar(total: u64) -> indicatif::ProgressBar {
    let style = indicatif::style::ProgressStyle::with_template("{human_pos}/{human_len} speed {per_sec} elapsed {elapsed_precise} eta {eta}").unwrap();
    let progress = indicatif::ProgressBar::new(total).with_style(style);
    progress.enable_steady_tick(Duration::from_millis(100));
    progress
}
