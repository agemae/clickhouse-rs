use std::{env, str::FromStr};

use clickhouse_rs::{errors::Result, Client, ClientHandle, Options};
use futures_util::StreamExt;

fn database_url() -> String {
    env::var("DATABASE_URL").unwrap_or_else(|_| "tcp://localhost:9000?compression=lz4".into())
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
        "SELECT number FROM numbers(100) ORDER BY number LIMIT 10",
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
async fn fetch_all_captures_rows_before_limit_for_grouped_query() -> Result<()> {
    let mut client = connect().await?;

    let block = client
        .query(
            "SELECT number % 7 AS k, count() AS c FROM numbers(1000) GROUP BY k ORDER BY k LIMIT 3",
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
        "SELECT number FROM numbers(50) ORDER BY number LIMIT 5 OFFSET 45",
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
async fn query_start_clears_profile_info() -> Result<()> {
    let mut client = connect().await?;

    drain_stream(
        &mut client,
        "SELECT number FROM numbers(100) ORDER BY number LIMIT 10",
    )
    .await?;
    let result = client.query("SELECT 1");
    drop(result);

    assert_eq!(client.last_profile_info(), None);
    Ok(())
}

#[tokio::test]
async fn execute_clears_profile_info() -> Result<()> {
    let mut client = connect().await?;

    drain_stream(
        &mut client,
        "SELECT number FROM numbers(100) ORDER BY number LIMIT 10",
    )
    .await?;
    client.execute("SELECT 1").await?;

    assert_eq!(client.last_profile_info(), None);
    Ok(())
}

#[tokio::test]
async fn failed_query_leaves_no_profile_info() -> Result<()> {
    let mut client = connect().await?;

    drain_stream(
        &mut client,
        "SELECT number FROM numbers(100) ORDER BY number LIMIT 10",
    )
    .await?;
    let failed = drain_stream(&mut client, "SELECT throwIf(number = 3) FROM numbers(10)").await;

    assert!(failed.is_err());
    assert_eq!(client.last_profile_info(), None);
    Ok(())
}
