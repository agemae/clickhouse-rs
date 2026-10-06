#![cfg(feature = "tokio_io")]

use std::{env, str::FromStr, time::Duration};

use clickhouse_rs::{errors::Result, types::Block, Client, ClientHandle, Options};
use futures_util::{StreamExt, TryStreamExt};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[cfg(not(feature = "tls"))]
fn database_url() -> String {
    env::var("DATABASE_URL").unwrap_or_else(|_| "tcp://localhost:9000?compression=lz4".into())
}

#[cfg(feature = "tls")]
fn database_url() -> String {
    env::var("DATABASE_URL").unwrap_or_else(|_| {
        "tcp://localhost:9440?compression=lz4&secure=true&skip_verify=true".into()
    })
}

async fn connect() -> Result<ClientHandle> {
    Client::connect(Options::from_str(&database_url())?).await
}

async fn drain_stream(client: &mut ClientHandle, sql: &str) -> Result<usize> {
    let mut stream = client.query(sql).stream();
    let mut rows = 0;
    while let Some(row) = stream.next().await {
        row?;
        rows += 1;
    }
    Ok(rows)
}

async fn fetch_u64s(client: &mut ClientHandle, sql: &str) -> Result<Vec<u64>> {
    let block = client.query(sql).fetch_all().await?;
    (0..block.row_count()).map(|i| block.get(i, 0)).collect()
}

/// Reads one row of a `SELECT ... LIMIT 10` over 100 rows, then drops the stream.
async fn drop_stream_after_first_row(client: &mut ClientHandle) -> Result<()> {
    let mut stream = client
        .query("SELECT number FROM numbers(100) ORDER BY number LIMIT 10")
        .stream();
    stream.next().await.expect("first row")?;
    Ok(())
}

/// Minimal native-protocol server: answers the hello, then replies to the first query with a
/// lone `ProfileInfo` packet and never ends the stream.
async fn server_that_stalls_after_profile_info() -> String {
    fn varint(mut v: u64, out: &mut Vec<u8>) {
        while v >= 0x80 {
            out.push(v as u8 | 0x80);
            v >>= 7;
        }
        out.push(v as u8);
    }
    fn string(s: &str, out: &mut Vec<u8>) {
        varint(s.len() as u64, out);
        out.extend_from_slice(s.as_bytes());
    }

    let mut hello = vec![0]; // Hello: name, major, minor, revision, timezone, display name, patch
    string("ClickHouse", &mut hello);
    for v in [26, 3, 54429] {
        varint(v, &mut hello);
    }
    string("UTC", &mut hello);
    string("fake", &mut hello);
    varint(0, &mut hello);
    // ProfileInfo: rows=10 blocks=1 bytes=80 applied_limit=1 rows_before_limit=100 calculated=1
    let profile_info = [6, 10, 1, 80, 1, 100, 1];

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = [0; 4096];
        socket.read(&mut buf).await.unwrap();
        socket.write_all(&hello).await.unwrap();
        socket.read(&mut buf).await.unwrap();
        socket.write_all(&profile_info).await.unwrap();
        while socket.read(&mut buf).await.map_or(false, |n| n > 0) {}
    });
    format!("tcp://{addr}?compression=none")
}

#[tokio::test]
async fn profile_info_is_none_before_any_query() -> Result<()> {
    let client = connect().await?;

    assert_eq!(client.last_profile_info(), None);
    Ok(())
}

#[tokio::test]
async fn stream_captures_rows_before_limit() -> Result<()> {
    let mut client = connect().await?;

    let rows = drain_stream(
        &mut client,
        "SELECT number FROM numbers(100) ORDER BY number LIMIT 10 \
         SETTINGS exact_rows_before_limit = 1",
    )
    .await?;
    let info = client
        .last_profile_info()
        .expect("ProfileInfo after stream");

    assert_eq!(rows, 10);
    assert!(info.applied_limit);
    assert_eq!(info.rows_before_limit, 100);
    assert_eq!(info.rows, 10);
    Ok(())
}

#[tokio::test]
async fn stream_blocks_captures_rows_before_limit() -> Result<()> {
    let mut client = connect().await?;

    let blocks: Vec<_> = client
        .query(
            "SELECT number FROM numbers(100) ORDER BY number LIMIT 10 \
             SETTINGS exact_rows_before_limit = 1",
        )
        .stream_blocks()
        .try_collect()
        .await?;
    let info = client
        .last_profile_info()
        .expect("ProfileInfo after stream_blocks");

    assert_eq!(blocks.iter().map(|b| b.row_count()).sum::<usize>(), 10);
    assert!(info.applied_limit);
    assert_eq!(info.rows_before_limit, 100);
    Ok(())
}

#[tokio::test]
async fn fetch_all_captures_rows_before_limit_for_grouped_query() -> Result<()> {
    let mut client = connect().await?;

    let block = client
        .query(
            "SELECT number % 7 AS k, count() AS c FROM numbers(1000) GROUP BY k ORDER BY k LIMIT 3 \
             SETTINGS exact_rows_before_limit = 1",
        )
        .fetch_all()
        .await?;
    let info = client
        .last_profile_info()
        .expect("ProfileInfo after fetch_all");

    assert_eq!(block.row_count(), 3);
    assert!(info.applied_limit);
    assert_eq!(info.rows_before_limit, 7);
    Ok(())
}

#[tokio::test]
async fn offset_does_not_change_rows_before_limit() -> Result<()> {
    let mut client = connect().await?;

    let rows = drain_stream(
        &mut client,
        "SELECT number FROM numbers(50) ORDER BY number LIMIT 5 OFFSET 45 \
         SETTINGS exact_rows_before_limit = 1",
    )
    .await?;
    let info = client
        .last_profile_info()
        .expect("ProfileInfo after stream");

    assert_eq!(rows, 5);
    assert_eq!(info.rows_before_limit, 50);
    Ok(())
}

#[tokio::test]
async fn query_without_limit_reports_no_applied_limit() -> Result<()> {
    let mut client = connect().await?;

    drain_stream(&mut client, "SELECT number FROM numbers(5)").await?;
    let info = client
        .last_profile_info()
        .expect("ProfileInfo after stream");

    assert!(!info.applied_limit);
    assert_eq!(info.rows, 5);
    Ok(())
}

#[tokio::test]
async fn new_query_replaces_previous_profile_info() -> Result<()> {
    let mut client = connect().await?;

    drain_stream(
        &mut client,
        "SELECT number FROM numbers(100) ORDER BY number LIMIT 10",
    )
    .await?;
    drain_stream(&mut client, "SELECT number FROM numbers(3)").await?;
    let info = client
        .last_profile_info()
        .expect("ProfileInfo after second stream");

    assert!(!info.applied_limit);
    assert_eq!(info.rows, 3);
    Ok(())
}

#[tokio::test]
async fn dropped_stream_does_not_leak_into_next_query() -> Result<()> {
    let mut client = connect().await?;

    drain_stream(&mut client, "SELECT number FROM numbers(5)").await?;
    drop_stream_after_first_row(&mut client).await?;
    assert_eq!(client.last_profile_info(), None);

    let rows = fetch_u64s(&mut client, "SELECT number + 1000 FROM numbers(3)").await?;
    let info = client
        .last_profile_info()
        .expect("ProfileInfo of the new query");
    assert_eq!(rows, [1000, 1001, 1002]);
    assert!(!info.applied_limit);
    assert_eq!(info.rows, 3);

    drop_stream_after_first_row(&mut client).await?;
    let rows = drain_stream(&mut client, "SELECT number FROM numbers(2)").await?;
    assert_eq!(rows, 2);
    assert_eq!(client.last_profile_info().map(|info| info.rows), Some(2));
    Ok(())
}

#[tokio::test]
async fn ping_after_dropped_stream_keeps_handle_usable() -> Result<()> {
    let mut client = connect().await?;

    drop_stream_after_first_row(&mut client).await?;
    client.ping().await?;

    assert_eq!(fetch_u64s(&mut client, "SELECT 42 :: UInt64").await?, [42]);
    Ok(())
}

#[tokio::test]
async fn timeout_after_profile_info_leaves_no_profile_info() -> Result<()> {
    let url = server_that_stalls_after_profile_info().await;
    let mut client =
        Client::connect(Options::from_str(&url)?.query_timeout(Duration::from_millis(200))).await?;

    let result = client.query("SELECT 1").fetch_all().await;

    assert!(result.is_err());
    assert_eq!(client.last_profile_info(), None);
    Ok(())
}

#[tokio::test]
async fn cancel_after_profile_info_leaves_no_profile_info() -> Result<()> {
    let url = server_that_stalls_after_profile_info().await;
    let mut client = Client::connect(Options::from_str(&url)?).await?;

    let result = tokio::time::timeout(
        Duration::from_millis(200),
        drain_stream(&mut client, "SELECT 1"),
    )
    .await;

    assert!(
        result.is_err(),
        "the stream never ends, so the timeout cancels it"
    );
    assert_eq!(client.last_profile_info(), None);
    Ok(())
}

#[tokio::test]
async fn failed_query_leaves_handle_usable_and_no_profile_info() -> Result<()> {
    let mut client = connect().await?;

    for failing in [
        "SELECT throwIf(number = 3) FROM numbers(10)",
        "SELECT number, throwIf(number = 2) FROM numbers(10) SETTINGS max_block_size = 1",
        "SELECT * FROM system.table_that_does_not_exist",
    ] {
        drain_stream(
            &mut client,
            "SELECT number FROM numbers(100) ORDER BY number LIMIT 10",
        )
        .await?;
        let failed = drain_stream(&mut client, failing).await;

        assert!(failed.is_err(), "{failing}");
        assert_eq!(client.last_profile_info(), None, "{failing}");
        assert_eq!(
            fetch_u64s(&mut client, "SELECT 42 :: UInt64").await?,
            [42],
            "{failing}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn dropped_stream_that_later_fails_keeps_handle_usable() -> Result<()> {
    let mut client = connect().await?;

    {
        // One row per block, 100ms apart, failing at the third row.
        let mut stream = client
            .query(
                "SELECT number, throwIf(number = 2) + sleepEachRow(0.1) FROM numbers(10) \
                 SETTINGS max_block_size = 1, max_threads = 1",
            )
            .stream();
        stream.next().await.expect("first row")?;
        // Let the server send the exception before the stream is dropped.
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    assert_eq!(fetch_u64s(&mut client, "SELECT 42 :: UInt64").await?, [42]);
    Ok(())
}

#[tokio::test]
async fn execute_clears_and_does_not_set_profile_info() -> Result<()> {
    let mut client = connect().await?;

    drain_stream(
        &mut client,
        "SELECT number FROM numbers(100) ORDER BY number LIMIT 10",
    )
    .await?;
    client
        .execute("SELECT number FROM numbers(100) ORDER BY number LIMIT 10")
        .await?;

    assert_eq!(client.last_profile_info(), None);
    Ok(())
}

#[tokio::test]
async fn insert_clears_profile_info() -> Result<()> {
    let mut client = connect().await?;
    client
        .execute("CREATE TEMPORARY TABLE profile_info_insert (x UInt64)")
        .await?;

    drain_stream(
        &mut client,
        "SELECT number FROM numbers(100) ORDER BY number LIMIT 10",
    )
    .await?;
    assert!(client.last_profile_info().is_some());
    // The result is ignored: `insert` fails with `UnexpectedPacket` on servers that send
    // `Progress` during an INSERT (26.3 does). The value is cleared either way.
    let _ = client
        .insert(
            "profile_info_insert",
            Block::new().column("x", vec![1_u64, 2, 3]),
        )
        .await;

    assert_eq!(client.last_profile_info(), None);
    Ok(())
}
