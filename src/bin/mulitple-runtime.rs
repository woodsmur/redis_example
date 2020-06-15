use crossbeam_channel::{Sender, TryRecvError};
use redis::RedisError;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::time;
use tokio::time::delay_for;

async fn fetch_a_value_using_cmd(
    con: &redis::aio::MultiplexedConnection,
    key: &str,
) -> redis::RedisResult<String> {
    let mut con = con.clone();
    let r: String = redis::cmd("LPOP").arg(key).query_async(&mut con).await?;
    println!("Got {:?}", r);
    Ok(r)
}

async fn redis_async_fn(tx: Sender<String>, key: &str) -> redis::RedisResult<String> {
    let tick = 2;

    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let con = client.get_multiplexed_async_std_connection().await?;
    let mut interval = time::interval(Duration::from_secs(tick));

    // In a loop, read data from the socket and write the data back.
    loop {
        interval.tick().await;
        // after interval, fetch value from redis

        /////////////////////////////////////////////////////////////////////////
        // Plan A:
        // using `await?` will not handler error when reading value from redis
        /////////////////////////////////////////////////////////////////////////
        // let value = fetch_a_value_using_cmd(&con, key).await?;

        /////////////////////////////////////////////////////////////////////////
        // Plan B:
        // using match, easy to handle error
        /////////////////////////////////////////////////////////////////////////
        let value = match fetch_a_value_using_cmd(&con, key).await {
            Ok(v) => v,
            Err(e) => {
                println!("fetch error: {}", e);
                // delay for sometime
                // we have interval in loop, so we don't need delay
                // just call continue
                // println!("fetch delay for 1s...");
                // delay_for(Duration::from_millis(1000)).await;
                continue;
            }
        };
        println!("# Got from redis: {}", value);
        println!(">> send");
        tx.send(value).unwrap();
    }
}

fn run_multiple_runtime_one_loop_in_async() -> Result<(), Box<dyn std::error::Error>> {
    let tick = 2;
    let (tx, rx) = crossbeam_channel::unbounded::<String>();
    let thread_redis = std::thread::spawn(move || {
        let key = "test-payload";
        let mut rt = Runtime::new().unwrap();
        rt.block_on(redis_async_fn(tx, key));
    });

    let thread_crawler = std::thread::spawn(move || {
        let mut rt = Runtime::new().unwrap();
        rt.block_on(async {
            let mut interval = time::interval(Duration::from_secs(tick));

            // In a loop, read data from the socket and write the data back.
            loop {
                interval.tick().await;

                let value = match rx.try_recv() {
                    Ok(v) => v,
                    Err(e) => {
                        println!("Error {:?}", e);
                        "".into()
                    }
                };
                // after interval print
                println!("-> {}", value);
            }
        })
    });

    thread_redis.join();
    println!("###");
    thread_crawler.join();
    println!("OOO");
    Ok(())
}

fn main() {
    run_multiple_runtime_one_loop_in_async();
}
