use clap::{Parser, Subcommand};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::value::Row;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::fs;

#[derive(Parser, Debug)]
#[command(name = "wq")]
#[command(about = "CQL preview tool for zed", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Query {
        #[arg(short, long)]
        query: String,
        preview_dir_path: String,
    },

    Info,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    let _ = match cli.command {
        Command::Query {
            query,
            preview_dir_path,
        } => query_text(&query, &preview_dir_path).await?,
        Command::Info => cmd_info(),
    };

    Ok(())
}

fn cmd_info() {
    todo!()
}

async fn execute_single_statement(
    session: &Session,
    statement: &str,
    output: &mut String,
) -> Result<(), Box<dyn Error>> {
    if statement.trim().to_uppercase().starts_with("USE ") {
        session.query_unpaged(statement, &[]).await?;
        let msg = "Database context switched.";
        println!("{}", msg);
        output.push_str(&format!("*{}*\n\n", msg));
        return Ok(());
    }

    match session.query_unpaged(statement, &[]).await {
        Ok(query_result) => {
            match query_result.into_rows_result() {
                Ok(rows_result) => {
                    let rows = rows_result.rows::<Row>()?;

                    println!();

                    let mut row_count = 0;
                    let mut table_rows = Vec::new();

                    for row in rows {
                        let row = row?;
                        let mut console_line = String::from("|");
                        let mut markdown_cells = Vec::new();

                        for column in &row.columns {
                            let formatted = match column {
                                None => "null".to_owned(),
                                Some(value) => {
                                    let formatted = format!("{value:?}");
                                    // Truncate long values for console
                                    if formatted.len() > 16 {
                                        format!("{}...", &formatted[..13])
                                    } else {
                                        formatted
                                    }
                                }
                            };

                            console_line.push_str(&format!(" {:16} |", formatted));

                            let full_value = match column {
                                None => "null".to_owned(),
                                Some(value) => format!("{value:?}"),
                            };
                            markdown_cells.push(full_value);
                        }

                        println!("{}", console_line);
                        table_rows.push(markdown_cells);
                        row_count += 1;
                    }

                    if !table_rows.is_empty() {
                        let col_count = table_rows[0].len();

                        output.push_str("|");
                        for i in 0..col_count {
                            output.push_str(&format!(" Column {} |", i + 1));
                        }
                        output.push_str("\n|");

                        for _ in 0..col_count {
                            output.push_str(" --- |");
                        }
                        output.push_str("\n");

                        for row_cells in table_rows {
                            output.push_str("|");
                            for cell in row_cells {
                                let escaped = cell.replace("|", "\\|");
                                output.push_str(&format!(" {} |", escaped));
                            }
                            output.push_str("\n");
                        }
                        output.push_str("\n");
                    }

                    let summary = format!("{} row(s) returned.", row_count);
                    println!("\n{}", summary);
                    output.push_str(&format!("*{}*\n\n", summary));
                }
                Err(_) => {
                    let msg = "Statement executed successfully (no rows returned).";
                    println!("{}", msg);
                    output.push_str(&format!("*{}*\n\n", msg));
                }
            }
            Ok(())
        }
        Err(e) => Err(Box::new(e)),
    }
}

fn get_preview_file_path(preview_dir_path: &str) -> PathBuf {
    let preview_dir = Path::new(preview_dir_path);
    preview_dir.join(".pw.cql.md")
}

fn split_cql_statements(query: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current_statement = String::new();
    let mut in_string = false;
    let mut in_comment = false;
    let mut string_delimiter = '\0';

    let chars: Vec<char> = query.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let ch = chars[i];

        if !in_string && i + 1 < chars.len() && chars[i] == '-' && chars[i + 1] == '-' {
            in_comment = true;
            i += 2;
            continue;
        }

        if in_comment {
            if ch == '\n' {
                in_comment = false;
            }
            i += 1;
            continue;
        }

        if !in_string && (ch == '\'' || ch == '"') {
            in_string = true;
            string_delimiter = ch;
            current_statement.push(ch);
        } else if in_string && ch == string_delimiter {
            if i + 1 < chars.len() && chars[i + 1] == string_delimiter {
                current_statement.push(ch);
                current_statement.push(chars[i + 1]);
                i += 2;
                continue;
            }
            in_string = false;
            current_statement.push(ch);
        } else if !in_string && ch == ';' {
            let trimmed = current_statement.trim().to_string();
            if !trimmed.is_empty() {
                statements.push(trimmed);
            }
            current_statement.clear();
        } else {
            current_statement.push(ch);
        }

        i += 1;
    }

    let trimmed = current_statement.trim().to_string();
    if !trimmed.is_empty() {
        statements.push(trimmed);
    }

    statements
}

async fn query_text(query: &str, preview_dir_path: &str) -> Result<(), Box<dyn Error>> {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "172.17.0.2:9042".to_string());
    let session: Session = SessionBuilder::new()
        .known_node(uri)
        .connection_timeout(Duration::from_secs(3))
        .cluster_metadata_refresh_interval(Duration::from_secs(10))
        .build()
        .await?;

    let output_path = get_preview_file_path(preview_dir_path);
    let mut output = String::new();

    let statements = split_cql_statements(query);

    if statements.is_empty() {
        let msg = "No valid CQL statements found.";
        println!("{}", msg);
        output.push_str(&format!("# CQL Query Results\n\n{}\n", msg));
        fs::write(&output_path, output).await?;
        println!("\nResults written to: {}", output_path.display());
        return Ok(());
    }

    output.push_str(&format!("# CQL Query Results\n\n"));
    output.push_str(&format!("Executed {} statement(s)\n\n", statements.len()));
    output.push_str(&format!("---\n\n"));

    println!("Executing {} statement(s)...\n", statements.len());

    for (idx, statement) in statements.iter().enumerate() {
        let header = format!("─────────────────────────────────────────");
        let stmt_info = format!("Statement {}/{}: {}", idx + 1, statements.len(), statement);

        println!("{}", header);
        println!("{}", stmt_info);
        println!("{}", header);

        output.push_str(&format!(
            "## Statement {}/{}\n\n",
            idx + 1,
            statements.len()
        ));
        output.push_str(&format!("```cql\n{}\n```\n\n", statement));

        match execute_single_statement(&session, &statement, &mut output).await {
            Ok(()) => {
                let success_msg = format!("✓ Statement {} executed successfully\n", idx + 1);
                println!("{}", success_msg);
                output.push_str("\n");
            }
            Err(e) => {
                let error_msg = format!("✗ Error executing statement {}: {}\n", idx + 1, e);
                eprintln!("{}", error_msg);
                output.push_str(&format!("**Error:** {}\n\n", e));
                continue;
            }
        }
    }

    fs::write(&output_path, output).await?;
    println!("\n═══════════════════════════════════════════");
    println!("Results written to: {}", output_path.display());
    println!("═══════════════════════════════════════════");

    Ok(())
}
