use mini_redis::{client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let mut client = client::connect("127.0.0.1:1234").await?;
    client.set("hello", "world".into()).await?;

    let result = client.get("hello from the client").await?;

    println!("{:?}", result);

    Ok(())
}
