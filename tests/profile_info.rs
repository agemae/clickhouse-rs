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

/// Hand-built native-protocol packets for a scripted fake server, so tests can put exact
/// packet sequences on the wire instead of depending on a real server's timing.
mod wire {
    pub const QUERY: u8 = 1;
    pub const CANCEL: u8 = 3;
    pub const PING: u8 = 4;

    pub fn varint(mut v: u64, out: &mut Vec<u8>) {
        while v >= 0x80 {
            out.push(v as u8 | 0x80);
            v >>= 7;
        }
        out.push(v as u8);
    }

    pub fn string(s: &str, out: &mut Vec<u8>) {
        varint(s.len() as u64, out);
        out.extend_from_slice(s.as_bytes());
    }

    /// Hello: name, major, minor, revision, timezone, display name, patch.
    pub fn hello() -> Vec<u8> {
        let mut out = vec![0];
        string("ClickHouse", &mut out);
        for v in [26, 3, 54429] {
            varint(v, &mut out);
        }
        string("UTC", &mut out);
        string("fake", &mut out);
        varint(0, &mut out);
        out
    }

    /// ProfileInfo: rows, blocks, bytes, applied_limit, rows_before_limit, calculated.
    pub fn profile_info(rows: u64, rows_before_limit: u64) -> Vec<u8> {
        let mut out = vec![6];
        varint(rows, &mut out);
        varint(1, &mut out);
        varint(80, &mut out);
        out.push(1);
        varint(rows_before_limit, &mut out);
        out.push(1);
        out
    }

    pub fn end_of_stream() -> Vec<u8> {
        vec![5]
    }

    /// A complete result as a server sends it: the header block, `ProfileInfo`, end of stream.
    pub fn finished_result(rows: u64) -> Vec<u8> {
        let mut out = data_block("n", &[]);
        out.extend(profile_info(rows, rows));
        out.extend(end_of_stream());
        out
    }

    /// Exception: code, name, message, stack trace, has_nested, then each nested exception.
    pub fn exception(code: u32, message: &str, nested: &[(u32, &str)]) -> Vec<u8> {
        let mut out = vec![2];
        let chain = std::iter::once((code, message)).chain(nested.iter().copied());
        let len = nested.len() + 1;
        for (i, (code, message)) in chain.enumerate() {
            out.extend_from_slice(&code.to_le_bytes());
            string("DB::Exception", &mut out);
            string(message, &mut out);
            string("", &mut out);
            out.push(u8::from(i + 1 < len));
        }
        out
    }

    /// A Data packet with one uncompressed `UInt64` column.
    pub fn data_block(column: &str, values: &[u64]) -> Vec<u8> {
        let mut out = vec![1];
        string("", &mut out);
        // Block info: is_overflows = false, bucket_num = -1, end.
        out.extend_from_slice(&[1, 0, 2, 0xff, 0xff, 0xff, 0xff, 0]);
        varint(1, &mut out);
        varint(values.len() as u64, &mut out);
        string(column, &mut out);
        string("UInt64", &mut out);
        for v in values {
            out.extend_from_slice(&v.to_le_bytes());
        }
        out
    }
}

/// Fake server for one connection: answers the hello, then answers each client message
/// (`wire::QUERY`, `wire::CANCEL`, `wire::PING`) with the next scripted reply. An empty reply
/// sends nothing. A message that doesn't match the script closes the connection.
async fn fake_server(script: Vec<(u8, Vec<u8>)>) -> String {
    use wire::{CANCEL, PING};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0; 1 << 16];
        socket.read(&mut buf).await.unwrap();
        socket.write_all(&wire::hello()).await.unwrap();
        let mut script = script.into_iter();
        'connection: while let Ok(n) = socket.read(&mut buf).await {
            if n == 0 {
                break;
            }
            // `Cancel` and `Ping` are one byte each, so one read can hold a `Cancel` followed
            // by the next query; anything else runs to the end of the read.
            let mut offset = 0;
            while offset < n {
                let kind = buf[offset];
                offset = if kind == CANCEL || kind == PING {
                    offset + 1
                } else {
                    n
                };
                match script.next() {
                    Some((expected, reply)) if expected == kind => {
                        socket.write_all(&reply).await.unwrap();
                    }
                    Some(_) => break 'connection,
                    None => {}
                }
            }
        }
    });
    format!("tcp://{addr}?compression=none")
}

/// Answers the first query with a lone `ProfileInfo` packet and never ends the stream.
async fn server_that_stalls_after_profile_info() -> String {
    fake_server(vec![(wire::QUERY, wire::profile_info(10, 100))]).await
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
async fn failed_query_leaves_handle_usable() -> Result<()> {
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
async fn exception_after_profile_info_leaves_none_and_keeps_handle_usable() -> Result<()> {
    let mut reply = wire::data_block("n", &[]);
    reply.extend(wire::profile_info(10, 100));
    reply.extend(wire::exception(395, "boom", &[]));
    let ok = wire::finished_result(3);
    let url = fake_server(vec![(wire::QUERY, reply), (wire::QUERY, ok)]).await;
    let mut client = Client::connect(Options::from_str(&url)?).await?;

    let failed = client.query("SELECT 1").fetch_all().await;
    assert!(failed.is_err());
    assert_eq!(client.last_profile_info(), None);

    client.query("SELECT 1").fetch_all().await?;
    assert_eq!(client.last_profile_info().map(|info| info.rows), Some(3));
    Ok(())
}

#[tokio::test]
async fn dropped_stream_that_later_fails_keeps_handle_usable() -> Result<()> {
    // The first query sends a row and then an Exception; the test drops the stream after the
    // row, so the next query's drain reads the Exception as the end of the abandoned query.
    let mut first = wire::data_block("n", &[]);
    first.extend(wire::data_block("n", &[7]));
    first.extend(wire::exception(395, "boom", &[]));
    let ok = wire::finished_result(1);
    let url = fake_server(vec![
        (wire::QUERY, first),
        (wire::CANCEL, vec![]),
        (wire::QUERY, ok),
    ])
    .await;
    let mut client = Client::connect(Options::from_str(&url)?).await?;

    {
        let mut stream = client.query("SELECT 1").stream();
        stream.next().await.expect("first row")?;
    }

    client.query("SELECT 1").fetch_all().await?;
    assert_eq!(client.last_profile_info().map(|info| info.rows), Some(1));
    Ok(())
}

/// A server that leaves the first query hanging, ignores the first `Cancel`, and ends the
/// abandoned query on the second one. Used to cancel a command while it drains.
async fn server_that_stalls_the_first_drain() -> String {
    let ok = wire::finished_result(2);
    fake_server(vec![
        (wire::QUERY, wire::profile_info(10, 100)),
        (wire::CANCEL, vec![]),
        (wire::CANCEL, wire::end_of_stream()),
        (wire::QUERY, ok),
    ])
    .await
}

async fn abandon_first_query(client: &mut ClientHandle) {
    let abandoned =
        tokio::time::timeout(Duration::from_millis(100), drain_stream(client, "SELECT 1")).await;
    assert!(abandoned.is_err(), "the first query never ends");
}

#[tokio::test]
async fn query_cancelled_while_draining_keeps_handle_usable() -> Result<()> {
    let url = server_that_stalls_the_first_drain().await;
    let mut client = Client::connect(Options::from_str(&url)?).await?;
    abandon_first_query(&mut client).await;

    let cancelled = tokio::time::timeout(
        Duration::from_millis(100),
        client.query("SELECT 1").fetch_all(),
    )
    .await;
    assert!(cancelled.is_err(), "the first drain never ends");

    client.query("SELECT 1").fetch_all().await?;
    assert_eq!(client.last_profile_info().map(|info| info.rows), Some(2));
    Ok(())
}

#[tokio::test]
async fn execute_cancelled_while_draining_keeps_handle_usable() -> Result<()> {
    let url = server_that_stalls_the_first_drain().await;
    let mut client = Client::connect(Options::from_str(&url)?).await?;
    abandon_first_query(&mut client).await;

    let cancelled =
        tokio::time::timeout(Duration::from_millis(100), client.execute("SELECT 1")).await;
    assert!(cancelled.is_err(), "the first drain never ends");

    client.query("SELECT 1").fetch_all().await?;
    assert_eq!(client.last_profile_info().map(|info| info.rows), Some(2));
    Ok(())
}

#[tokio::test]
async fn ping_timing_out_while_draining_keeps_handle_usable() -> Result<()> {
    let url = server_that_stalls_the_first_drain().await;
    let options = Options::from_str(&url)?.ping_timeout(Duration::from_millis(100));
    let mut client = Client::connect(options).await?;
    abandon_first_query(&mut client).await;

    assert!(client.ping().await.is_err(), "the first drain never ends");

    client.query("SELECT 1").fetch_all().await?;
    assert_eq!(client.last_profile_info().map(|info| info.rows), Some(2));
    Ok(())
}

#[tokio::test]
async fn timeouts_while_draining_a_large_result_keep_handle_usable() -> Result<()> {
    let mut client = connect().await?;
    {
        let mut stream = client
            .query("SELECT number, repeat('x', 1000) FROM numbers(5000000)")
            .stream();
        stream.next().await.expect("first row")?;
    }

    // Short enough to fire during the drain; a query cancelled after it puts the connection
    // back through `BlockStream::drop`.
    for _ in 0..5 {
        let _ = tokio::time::timeout(
            Duration::from_millis(1),
            client.query("SELECT 1").fetch_all(),
        )
        .await;
    }

    assert_eq!(fetch_u64s(&mut client, "SELECT 42 :: UInt64").await?, [42]);
    Ok(())
}

#[tokio::test]
async fn failed_execute_keeps_handle_usable() -> Result<()> {
    let mut client = connect().await?;

    for failing in [
        "SELECT * FROM system.table_that_does_not_exist",
        "SELECT number, throwIf(number = 20000) FROM numbers(100000) SETTINGS max_block_size = 1000",
        "CREATE TABLE profile_info_bad (x UInt64) ENGINE = NoSuchEngine",
    ] {
        assert!(client.execute(failing).await.is_err(), "{failing}");
        assert_eq!(
            fetch_u64s(&mut client, "SELECT 42 :: UInt64").await?,
            [42],
            "{failing}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn failed_insert_keeps_handle_usable() -> Result<()> {
    let mut client = connect().await?;
    client
        .execute(
            "CREATE TEMPORARY TABLE profile_info_checked (x UInt64, CONSTRAINT small CHECK x < 10)",
        )
        .await?;

    // Rejected when the query is sent, before any data.
    let unknown_table = client
        .insert(
            "profile_info_no_such_table",
            Block::new().column("x", vec![1_u64]),
        )
        .await;
    assert!(unknown_table.is_err());
    assert_eq!(fetch_u64s(&mut client, "SELECT 42 :: UInt64").await?, [42]);

    // Rejected after the data is sent.
    let constraint = client
        .insert(
            "profile_info_checked",
            Block::new().column("x", vec![1_u64, 20]),
        )
        .await;
    assert!(constraint.is_err());
    assert_eq!(fetch_u64s(&mut client, "SELECT 42 :: UInt64").await?, [42]);
    Ok(())
}

#[tokio::test]
async fn decode_error_drops_the_connection_instead_of_failing_the_next_query() -> Result<()> {
    let mut client = connect().await?;

    let failed = client
        .query("SELECT 1::Variant(UInt8, String)")
        .fetch_all()
        .await;
    assert!(failed.is_err());

    let next = client.query("SELECT 42 :: UInt64").fetch_all().await;
    let message = next.err().map(|e| e.to_string()).unwrap_or_default();
    assert!(message.contains("Connection broken"), "{message}");
    Ok(())
}

#[tokio::test]
async fn nested_exception_messages_are_kept() -> Result<()> {
    let ok = wire::finished_result(1);
    let url = fake_server(vec![
        (
            wire::QUERY,
            wire::exception(1, "outer", &[(2, "middle"), (3, "inner")]),
        ),
        (wire::QUERY, ok),
    ])
    .await;
    let mut client = Client::connect(Options::from_str(&url)?).await?;

    let message = match client.query("SELECT 1").fetch_all().await {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected the server exception"),
    };
    assert!(message.contains("outer"), "{message}");
    assert!(message.contains("Caused by: Code: 2. middle"), "{message}");
    assert!(message.contains("Caused by: Code: 3. inner"), "{message}");

    client.query("SELECT 1").fetch_all().await?;
    assert_eq!(client.last_profile_info().map(|info| info.rows), Some(1));
    Ok(())
}

#[tokio::test]
async fn deeply_nested_exception_is_rejected_without_overflowing_the_stack() -> Result<()> {
    let nested: Vec<(u32, &str)> = (0..100_000).map(|i| (i, "x")).collect();
    let url = fake_server(vec![(wire::QUERY, wire::exception(1, "outer", &nested))]).await;
    let mut client = Client::connect(Options::from_str(&url)?).await?;

    assert!(client.query("SELECT 1").fetch_all().await.is_err());
    Ok(())
}

#[tokio::test]
async fn totals_rows_are_returned_but_not_counted_in_rows() -> Result<()> {
    let mut client = connect().await?;

    let block = client
        .query(
            "SELECT number % 3 AS k, count() AS c FROM numbers(10) GROUP BY k WITH TOTALS \
             ORDER BY k LIMIT 2 SETTINGS exact_rows_before_limit = 1",
        )
        .fetch_all()
        .await?;
    let info = client.last_profile_info().expect("ProfileInfo");

    assert_eq!(block.row_count(), 3, "two rows plus the totals row");
    assert_eq!(info.rows, 2);
    assert_eq!(info.rows_before_limit, 3);
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
async fn insert_succeeds_and_clears_profile_info() -> Result<()> {
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
    client
        .insert(
            "profile_info_insert",
            Block::new().column("x", vec![1_u64, 2, 3]),
        )
        .await?;

    assert_eq!(client.last_profile_info(), None);
    assert_eq!(
        fetch_u64s(&mut client, "SELECT sum(x) FROM profile_info_insert").await?,
        [6]
    );
    Ok(())
}
