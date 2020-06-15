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

async fn crawl() {

}

fn run_single_runtime_one_loop_in_async() -> Result<(), Box<dyn std::error::Error>> {
    let thread_crawler = std::thread::spawn(move || {

        let mut rt = Runtime::new().unwrap();
        rt.block_on(async {
            let tick = 2;
            let key = "test-payload";
            let client = redis::Client::open("redis://127.0.0.1/").unwrap();
            let con = client.get_multiplexed_async_std_connection().await.unwrap();
            let mut interval = time::interval(Duration::from_secs(tick));

            // In a loop, read data from the socket and write the data back.
            loop {
                // interval.tick().await;
                delay_for(Duration::from_millis(1000)).await;

                let value = match fetch_a_value_using_cmd(&con, key).await {
                    Ok(v) => v,
                    Err(e) => {
                        // println!("fetch error: {}", e);
                        continue;
                    }
                };
                println!("-> {}", value);

                // TODO: crawl using payload
                crawl().await;
                // after interval print
            }
        })
    });

    thread_crawler.join();
    Ok(())
}

fn main() {
    run_single_runtime_one_loop_in_async();
}
