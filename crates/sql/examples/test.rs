use libsql::Builder;

#[tokio::main]
async fn main() {
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();

    conn.execute("CREATE TABLE IF NOT EXISTS users (email TEXT)", ())
        .await
        .unwrap();
    conn.execute("INSERT INTO users (email) VALUES ('alice@example.org')", ())
        .await
        .unwrap();

    let mut rows = conn.query("SELECT email FROM users", ()).await.unwrap();

    while let Some(row) = rows.next().await.unwrap() {
        let email: String = row.get(0).unwrap();
        println!("email: {}", email);
    }
}
