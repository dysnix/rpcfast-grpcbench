use crate::config::Endpoint;
use crate::stats::{BenchmarkStats, EndpointSummary, PairwiseSummary};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::fmt::Write;
use std::time::Duration;
use terminal_size::{terminal_size, Width};

const DEFAULT_TERMINAL_WIDTH: usize = 120;
const MIN_TERMINAL_WIDTH: usize = 72;

#[derive(Debug, Serialize)]
pub struct ReportMeta {
    pub generated_at: DateTime<Utc>,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub duration_secs: u64,
    pub warmup_secs: u64,
    pub endpoint_count: usize,
    pub account_include: Vec<String>,
}

pub fn render_report(meta: &ReportMeta, endpoints: &[Endpoint], stats: &BenchmarkStats) -> String {
    let payload = serde_json::json!({
        "meta": meta,
        "stats": stats,
    });
    let payload_json = serde_json::to_string_pretty(&payload).unwrap_or_else(|_| "{}".to_string());

    let mut html = String::new();
    html.push_str("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">");
    html.push_str("<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">");
    html.push_str("<title>RPCFast gRPC Stream Benchmark</title>");
    html.push_str(STYLE);
    html.push_str("</head><body>");
    html.push_str("<main class=\"shell\">");
    html.push_str("<section class=\"hero\">");
    html.push_str("<div><p class=\"eyebrow\">RPCFast gRPCBench</p>");
    html.push_str("<h1>gRPC stream speed report</h1>");
    html.push_str("<p class=\"lede\">Local receive-time comparison across shared transaction signatures.</p></div>");
    html.push_str("<div class=\"stamp\">");
    write!(
        html,
        "<span>Generated</span><strong>{}</strong><span>Measured</span><strong>{}</strong>",
        escape(&meta.generated_at.to_rfc3339()),
        fmt_duration(Duration::from_secs(meta.duration_secs))
    )
    .ok();
    html.push_str("</div></section>");

    html.push_str("<section class=\"kpis\">");
    kpi(
        &mut html,
        "Observed signatures",
        stats.total_unique_signatures.to_string(),
    );
    kpi(
        &mut html,
        "Race eligible",
        stats.race_eligible_signatures.to_string(),
    );
    kpi(
        &mut html,
        "Full coverage",
        stats.full_coverage_signatures.to_string(),
    );
    kpi(&mut html, "Endpoints", meta.endpoint_count.to_string());
    html.push_str("</section>");

    html.push_str("<section><h2>Endpoint ranking</h2><div class=\"table-wrap\"><table>");
    html.push_str("<thead><tr><th>Rank</th><th>Endpoint</th><th>Protocol</th><th>Unique txs</th><th>Coverage</th><th>Wins</th><th>Win rate</th><th>Median lag</th><th>p95 lag</th></tr></thead><tbody>");
    for (idx, endpoint) in stats.endpoint_summaries.iter().enumerate() {
        endpoint_row(&mut html, idx + 1, endpoint);
    }
    html.push_str("</tbody></table></div></section>");

    html.push_str("<section><h2>Pairwise winners</h2><div class=\"table-wrap\"><table>");
    html.push_str("<thead><tr><th>Faster endpoint</th><th>Slower endpoint</th><th>Shared txs</th><th>Wins</th><th>Win rate</th><th>Median lead</th><th>p95 lead</th><th>Median lag</th><th>p95 lag</th></tr></thead><tbody>");
    for pair in stats.pairwise.iter().take(30) {
        pair_row(&mut html, pair);
    }
    html.push_str("</tbody></table></div></section>");

    html.push_str("<section><h2>Configured streams</h2><div class=\"endpoint-grid\">");
    for endpoint in endpoints {
        write!(
            html,
            "<div class=\"endpoint\"><strong>{}</strong><span>{}</span><code>{}</code></div>",
            escape(&endpoint.alias),
            escape(endpoint.protocol.label()),
            escape(redact_url(&endpoint.url).as_str())
        )
        .ok();
    }
    html.push_str("</div></section>");

    html.push_str("<section><h2>Filters</h2>");
    if meta.account_include.is_empty() {
        html.push_str(
            "<div class=\"filter-box\"><span>Account include</span><strong>None</strong></div>",
        );
    } else {
        html.push_str(
            "<div class=\"filter-box\"><span>Account include</span><div class=\"filter-list\">",
        );
        for account in &meta.account_include {
            write!(html, "<code>{}</code>", escape(account)).ok();
        }
        html.push_str("</div></div>");
    }
    html.push_str("</section>");

    html.push_str("<section class=\"notes\"><h2>Timing notes</h2>");
    html.push_str("<p>All endpoints are measured by timestamp of receive event.</p>");
    html.push_str("</section>");

    html.push_str("<section><h2>Raw JSON</h2><details><summary>Open report payload</summary><pre>");
    html.push_str(&escape(&payload_json));
    html.push_str("</pre></details></section>");
    html.push_str("</main></body></html>");
    html
}

pub fn render_terminal_report(
    meta: &ReportMeta,
    endpoints: &[Endpoint],
    stats: &BenchmarkStats,
    output_path: &str,
) -> String {
    let terminal_width = current_terminal_width();
    let mut out = String::new();
    writeln!(out).ok();
    writeln!(
        out,
        "{}",
        term_bold_blue(&center_text("RPC Fast gRPCBench report", terminal_width))
    )
    .ok();
    writeln!(out).ok();
    writeln!(
        out,
        "{}  {}",
        term_peach("Measured"),
        term_white(&fmt_duration(Duration::from_secs(meta.duration_secs))),
    )
    .ok();
    writeln!(
        out,
        "{} {}",
        term_peach("Generated"),
        term_white(&meta.generated_at.to_rfc3339())
    )
    .ok();
    writeln!(
        out,
        "{}      {}",
        term_peach("HTML"),
        term_blue(output_path)
    )
    .ok();
    writeln!(out).ok();

    terminal_line(
        &mut out,
        "+--------------------+--------------+--------------+--------------+",
    );
    terminal_header(
        &mut out,
        "| Observed signatures| Race eligible| Full coverage| Endpoints    |",
    );
    terminal_line(
        &mut out,
        "+--------------------+--------------+--------------+--------------+",
    );
    terminal_data(
        &mut out,
        &format!(
            "| {:>18} | {:>12} | {:>12} | {:>12} |",
            stats.total_unique_signatures,
            stats.race_eligible_signatures,
            stats.full_coverage_signatures,
            meta.endpoint_count
        ),
    );
    terminal_line(
        &mut out,
        "+--------------------+--------------+--------------+--------------+",
    );
    writeln!(out).ok();

    let streams_layout = streams_table_layout(endpoints, terminal_width);
    terminal_section(&mut out, "Filters", streams_layout.total_width());
    if meta.account_include.is_empty() {
        writeln!(
            out,
            "  {}  {}",
            term_peach("account_include"),
            term_muted("none")
        )
        .ok();
    } else {
        writeln!(out, "  {}", term_peach("account_include")).ok();
        for account in &meta.account_include {
            writeln!(out, "    {} {}", term_coral("-"), term_violet(account)).ok();
        }
    }
    writeln!(out).ok();

    terminal_section(&mut out, "Configured streams", streams_layout.total_width());
    terminal_header(
        &mut out,
        &format!(
            "  {:<alias_width$} {:<protocol_width$} Endpoint",
            "Alias",
            "Protocol",
            alias_width = streams_layout.alias_width,
            protocol_width = streams_layout.protocol_width,
        ),
    );
    terminal_table_separator(&mut out, streams_layout.total_width());
    for endpoint in endpoints {
        terminal_data(
            &mut out,
            &format!(
                "  {:<alias_width$} {:<protocol_width$} {}",
                truncate(&endpoint.alias, streams_layout.alias_width),
                truncate(endpoint.protocol.label(), streams_layout.protocol_width),
                truncate(&redact_url(&endpoint.url), streams_layout.endpoint_width),
                alias_width = streams_layout.alias_width,
                protocol_width = streams_layout.protocol_width,
            ),
        );
    }
    writeln!(out).ok();

    let ranking_layout = ranking_table_layout(&stats.endpoint_summaries, terminal_width);
    terminal_section(&mut out, "Endpoint ranking", ranking_layout.total_width());
    terminal_header(
        &mut out,
        &format!(
            "  {:<3} {:<endpoint_width$} {:<protocol_width$} {:>10} {:>9} {:>8} {:>9} {:>11} {:>11}",
            "#",
            "Endpoint",
            "Protocol",
            "Unique",
            "Coverage",
            "Wins",
            "Win rate",
            "Med lag",
            "P95 lag",
            endpoint_width = ranking_layout.endpoint_width,
            protocol_width = ranking_layout.protocol_width,
        ),
    );
    terminal_table_separator(&mut out, ranking_layout.total_width());
    for (idx, endpoint) in stats.endpoint_summaries.iter().enumerate() {
        terminal_data(
            &mut out,
            &format!(
                "  {:<3} {:<endpoint_width$} {:<protocol_width$} {:>10} {:>8.1}% {:>8} {:>8.1}% {:>11} {:>11}",
                idx + 1,
                truncate(&endpoint.alias, ranking_layout.endpoint_width),
                truncate(&endpoint.protocol_label, ranking_layout.protocol_width),
                endpoint.unique_signatures,
                endpoint.coverage_pct,
                endpoint.wins,
                endpoint.win_pct,
                fmt_us(endpoint.median_lag_us),
                fmt_us(endpoint.p95_lag_us),
                endpoint_width = ranking_layout.endpoint_width,
                protocol_width = ranking_layout.protocol_width,
            ),
        );
    }
    writeln!(out).ok();

    if !stats.pairwise.is_empty() {
        let pairwise_layout = pairwise_table_layout(&stats.pairwise, terminal_width);
        terminal_section(&mut out, "Pairwise winners", pairwise_layout.total_width());
        terminal_header(
            &mut out,
            &format!(
                "  {:<faster_width$} {:<slower_width$} {:>9} {:>8} {:>9} {:>11} {:>11} {:>11} {:>11}",
                "Faster",
                "Slower",
                "Shared",
                "Wins",
                "Win rate",
                "Med lead",
                "P95 lead",
                "Med lag",
                "P95 lag",
                faster_width = pairwise_layout.faster_width,
                slower_width = pairwise_layout.slower_width,
            ),
        );
        terminal_table_separator(&mut out, pairwise_layout.total_width());
        for pair in stats.pairwise.iter().take(10) {
            terminal_data(
                &mut out,
                &format!(
                    "  {:<faster_width$} {:<slower_width$} {:>9} {:>8} {:>8.1}% {:>11} {:>11} {:>11} {:>11}",
                    truncate(&pair.faster_alias, pairwise_layout.faster_width),
                    truncate(&pair.slower_alias, pairwise_layout.slower_width),
                    pair.shared_signatures,
                    pair.wins,
                    pair.win_pct,
                    fmt_us(pair.median_lag_us),
                    fmt_us(pair.p95_lag_us),
                    fmt_us(pair.median_behind_us),
                    fmt_us(pair.p95_behind_us),
                    faster_width = pairwise_layout.faster_width,
                    slower_width = pairwise_layout.slower_width,
                ),
            );
        }
        writeln!(out).ok();
    }

    writeln!(
        out,
        "{}",
        term_peach("Note: all endpoints are measured by timestamp of receive event.")
    )
    .ok();
    out
}

fn terminal_section(out: &mut String, title: &str, width: usize) {
    let prefix = format!("-- {title} ");
    writeln!(
        out,
        "{}",
        term_coral(&format!(
            "{}{}",
            prefix,
            "-".repeat(width.saturating_sub(prefix.len()))
        ))
    )
    .ok();
}

fn terminal_table_separator(out: &mut String, width: usize) {
    terminal_line(out, &format!("  {}", "-".repeat(width.saturating_sub(2))));
}

fn terminal_line(out: &mut String, line: &str) {
    writeln!(out, "{}", term_coral(line)).ok();
}

fn terminal_header(out: &mut String, line: &str) {
    writeln!(out, "{}", term_peach(line)).ok();
}

fn terminal_data(out: &mut String, line: &str) {
    writeln!(out, "{}", term_white(line)).ok();
}

fn kpi(html: &mut String, label: &str, value: String) {
    write!(
        html,
        "<div class=\"kpi\"><span>{}</span><strong>{}</strong></div>",
        escape(label),
        escape(&value)
    )
    .ok();
}

fn endpoint_row(html: &mut String, rank: usize, endpoint: &EndpointSummary) {
    write!(
        html,
        "<tr><td>{}</td><td><strong>{}</strong></td><td>{}</td><td>{}</td><td>{:.1}%</td><td>{}</td><td>{:.1}%</td><td>{}</td><td>{}</td></tr>",
        rank,
        escape(&endpoint.alias),
        escape(&endpoint.protocol_label),
        endpoint.unique_signatures,
        endpoint.coverage_pct,
        endpoint.wins,
        endpoint.win_pct,
        fmt_us(endpoint.median_lag_us),
        fmt_us(endpoint.p95_lag_us)
    )
    .ok();
}

fn pair_row(html: &mut String, pair: &PairwiseSummary) {
    write!(
        html,
        "<tr><td><strong>{}</strong></td><td><strong>{}</strong></td><td>{}</td><td>{}</td><td>{:.1}%</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
        escape(&pair.faster_alias),
        escape(&pair.slower_alias),
        pair.shared_signatures,
        pair.wins,
        pair.win_pct,
        fmt_us(pair.median_lag_us),
        fmt_us(pair.p95_lag_us),
        fmt_us(pair.median_behind_us),
        fmt_us(pair.p95_behind_us)
    )
    .ok();
}

fn escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn redact_url(url: &str) -> String {
    url.split('?').next().unwrap_or(url).to_string()
}

fn fmt_us(value: Option<i64>) -> String {
    let Some(value) = value else {
        return "n/a".to_string();
    };
    if value.abs() >= 1_000 {
        format!("{:.2} ms", value as f64 / 1_000.0)
    } else {
        format!("{value} us")
    }
}

fn fmt_duration(duration: Duration) -> String {
    humantime::format_duration(duration).to_string()
}

fn truncate(value: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    let mut chars = value.chars();
    let truncated: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        if max_chars == 1 {
            return "~".to_string();
        }
        let keep = max_chars.saturating_sub(1);
        format!("{}~", value.chars().take(keep).collect::<String>())
    } else {
        truncated
    }
}

fn current_terminal_width() -> usize {
    terminal_size()
        .map(|(Width(width), _)| width as usize)
        .or_else(|| {
            std::env::var("COLUMNS")
                .ok()
                .and_then(|value| value.parse().ok())
        })
        .unwrap_or(DEFAULT_TERMINAL_WIDTH)
        .max(MIN_TERMINAL_WIDTH)
}

#[derive(Debug)]
struct StreamsTableLayout {
    alias_width: usize,
    protocol_width: usize,
    endpoint_width: usize,
}

impl StreamsTableLayout {
    fn total_width(&self) -> usize {
        2 + self.alias_width + 1 + self.protocol_width + 1 + self.endpoint_width
    }
}

fn streams_table_layout(endpoints: &[Endpoint], terminal_width: usize) -> StreamsTableLayout {
    let mut widths = [
        endpoints
            .iter()
            .map(|endpoint| visible_len(&endpoint.alias))
            .max()
            .unwrap_or(0)
            .max("Alias".len()),
        endpoints
            .iter()
            .map(|endpoint| visible_len(endpoint.protocol.label()))
            .max()
            .unwrap_or(0)
            .max("Protocol".len()),
        endpoints
            .iter()
            .map(|endpoint| visible_len(&redact_url(&endpoint.url)))
            .max()
            .unwrap_or(0)
            .max("Endpoint".len()),
    ];
    shrink_columns(&mut widths, &[5, 8, 24], terminal_width, 4);
    StreamsTableLayout {
        alias_width: widths[0],
        protocol_width: widths[1],
        endpoint_width: widths[2],
    }
}

#[derive(Debug)]
struct RankingTableLayout {
    endpoint_width: usize,
    protocol_width: usize,
}

impl RankingTableLayout {
    fn total_width(&self) -> usize {
        2 + 3
            + 1
            + self.endpoint_width
            + 1
            + self.protocol_width
            + 1
            + 10
            + 1
            + 9
            + 1
            + 8
            + 1
            + 9
            + 1
            + 11
            + 1
            + 11
    }
}

fn ranking_table_layout(
    endpoints: &[EndpointSummary],
    terminal_width: usize,
) -> RankingTableLayout {
    let mut widths = [
        endpoints
            .iter()
            .map(|endpoint| visible_len(&endpoint.alias))
            .max()
            .unwrap_or(0)
            .max("Endpoint".len()),
        endpoints
            .iter()
            .map(|endpoint| visible_len(&endpoint.protocol_label))
            .max()
            .unwrap_or(0)
            .max("Protocol".len()),
    ];
    shrink_columns(&mut widths, &[8, 8], terminal_width, 71);
    RankingTableLayout {
        endpoint_width: widths[0],
        protocol_width: widths[1],
    }
}

#[derive(Debug)]
struct PairwiseTableLayout {
    faster_width: usize,
    slower_width: usize,
}

impl PairwiseTableLayout {
    fn total_width(&self) -> usize {
        2 + self.faster_width
            + 1
            + self.slower_width
            + 1
            + 9
            + 1
            + 8
            + 1
            + 9
            + 1
            + 11
            + 1
            + 11
            + 1
            + 11
            + 1
            + 11
    }
}

fn pairwise_table_layout(pairs: &[PairwiseSummary], terminal_width: usize) -> PairwiseTableLayout {
    let mut widths = [
        pairs
            .iter()
            .take(10)
            .map(|pair| visible_len(&pair.faster_alias))
            .max()
            .unwrap_or(0)
            .max("Faster".len()),
        pairs
            .iter()
            .take(10)
            .map(|pair| visible_len(&pair.slower_alias))
            .max()
            .unwrap_or(0)
            .max("Slower".len()),
    ];
    shrink_columns(&mut widths, &[12, 12], terminal_width, 80);
    PairwiseTableLayout {
        faster_width: widths[0],
        slower_width: widths[1],
    }
}

fn shrink_columns(
    widths: &mut [usize],
    min_widths: &[usize],
    max_total_width: usize,
    fixed_width: usize,
) {
    while fixed_width + widths.iter().sum::<usize>() > max_total_width {
        let Some((idx, _)) = widths
            .iter()
            .enumerate()
            .filter(|(idx, width)| **width > min_widths[*idx])
            .max_by_key(|(_, width)| **width)
        else {
            break;
        };
        widths[idx] -= 1;
    }
}

fn visible_len(value: &str) -> usize {
    value.chars().count()
}

fn center_text(value: &str, width: usize) -> String {
    let value_width = visible_len(value);
    if value_width >= width {
        value.to_string()
    } else {
        format!("{}{}", " ".repeat((width - value_width) / 2), value)
    }
}

fn term_coral(text: &str) -> String {
    ansi_rgb(text, 255, 105, 40)
}

fn term_peach(text: &str) -> String {
    ansi_rgb(text, 255, 205, 159)
}

fn term_blue(text: &str) -> String {
    ansi_rgb(text, 180, 220, 250)
}

fn term_bold_blue(text: &str) -> String {
    ansi_bold_rgb(text, 180, 220, 250)
}

fn term_violet(text: &str) -> String {
    ansi_rgb(text, 227, 180, 250)
}

fn term_white(text: &str) -> String {
    ansi_rgb(text, 255, 255, 255)
}

fn term_muted(text: &str) -> String {
    ansi_rgb(text, 200, 200, 200)
}

fn ansi_rgb(text: &str, r: u8, g: u8, b: u8) -> String {
    format!("\x1b[38;2;{r};{g};{b}m{text}\x1b[0m")
}

fn ansi_bold_rgb(text: &str, r: u8, g: u8, b: u8) -> String {
    format!("\x1b[1;38;2;{r};{g};{b}m{text}\x1b[0m")
}

const STYLE: &str = r#"
<style>
:root{color-scheme:dark;--ink:#fff;--muted:#c8c8c8;--line:#3a332f;--paper:#111;--panel:#1a1b1f;--panel2:#1f2220;--coral:#ff6928;--coral-hover:#ff8540;--coral-active:#f84b15;--peach:#ffcd9f;--blue:#b4dcfa;--violet:#e3b4fa;--seashell:#fdf2ec}
*{box-sizing:border-box}body{margin:0;background:var(--paper);color:var(--ink);font:14px/1.45 Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
.shell{max-width:1180px;margin:0 auto;padding:32px 22px 48px}.hero{display:flex;align-items:flex-end;justify-content:space-between;gap:24px;padding:34px 0 22px;border-bottom:1px solid var(--line)}
.eyebrow{margin:0 0 8px;color:var(--coral);font-weight:800;text-transform:uppercase;letter-spacing:.08em;font-size:12px}h1{margin:0;font-size:42px;line-height:1.05;letter-spacing:0}h2{margin:34px 0 14px;font-size:20px}
.lede{max-width:760px;margin:12px 0 0;color:var(--muted);font-size:16px}.stamp{display:grid;grid-template-columns:auto;gap:4px;min-width:220px;text-align:right}.stamp span{color:var(--muted);font-size:12px}.stamp strong{font-size:15px}
.kpis{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin:22px 0 10px}.kpi{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:16px}.kpi span{display:block;color:var(--peach);font-size:12px}.kpi strong{display:block;margin-top:4px;color:var(--seashell);font-size:28px}
.table-wrap{overflow:auto;background:var(--panel);border:1px solid var(--line);border-radius:8px}table{width:100%;border-collapse:collapse;min-width:880px}th,td{padding:11px 12px;border-bottom:1px solid var(--line);text-align:left;white-space:nowrap}th{background:var(--panel2);color:var(--peach);font-size:12px;text-transform:uppercase;letter-spacing:.04em}tr:last-child td{border-bottom:0}td small{display:block;color:var(--blue);margin-top:2px}
.endpoint-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:12px}.endpoint{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:14px;min-width:0}.endpoint span{display:block;color:var(--blue);font-weight:700;margin:3px 0}.endpoint code{display:block;overflow:hidden;text-overflow:ellipsis;color:var(--muted);font-size:12px}
.filter-box{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:14px}.filter-box span{display:block;color:var(--peach);font-size:12px;text-transform:uppercase;letter-spacing:.04em}.filter-box strong{display:block;margin-top:4px}.filter-list{display:flex;flex-wrap:wrap;gap:8px;margin-top:8px}.filter-list code{background:var(--panel2);border:1px solid var(--line);border-radius:6px;padding:6px 8px;color:var(--violet);font-size:12px}
.notes{background:#201915;border:1px solid #5b3828;border-radius:8px;padding:16px;margin-top:30px}.notes h2{margin-top:0;color:var(--coral-hover)}.notes p{margin:8px 0;color:var(--peach)}details{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:12px}summary{cursor:pointer;color:var(--coral-hover);font-weight:800}pre{overflow:auto;max-height:520px;font-size:12px;color:var(--seashell)}
@media(max-width:760px){.hero{display:block}.stamp{text-align:left;margin-top:18px}.kpis{grid-template-columns:repeat(2,minmax(0,1fr))}h1{font-size:34px}.shell{padding:22px 14px 36px}}
</style>
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn html_escape_covers_basic_entities() {
        assert_eq!(escape("<a&b>\"'"), "&lt;a&amp;b&gt;&quot;&#39;");
    }

    #[test]
    fn terminal_report_includes_key_sections() {
        let meta = ReportMeta {
            generated_at: Utc::now(),
            started_at: Utc::now(),
            finished_at: Utc::now(),
            duration_secs: 10,
            warmup_secs: 0,
            endpoint_count: 0,
            account_include: vec!["11111111111111111111111111111111".to_string()],
        };
        let stats = BenchmarkStats {
            endpoint_summaries: Vec::new(),
            pairwise: Vec::new(),
            total_unique_signatures: 0,
            race_eligible_signatures: 0,
            full_coverage_signatures: 0,
            configured_endpoint_count: 0,
        };

        let report = render_terminal_report(&meta, &[], &stats, "report.html");

        assert!(report.contains("RPC Fast gRPCBench report"));
        assert!(report.contains("gRPCBench"));
        assert!(report.contains("account_include"));
        assert!(report.contains("all endpoints are measured"));
    }

    #[test]
    fn terminal_section_rules_match_table_separators() {
        let meta = ReportMeta {
            generated_at: Utc::now(),
            started_at: Utc::now(),
            finished_at: Utc::now(),
            duration_secs: 10,
            warmup_secs: 0,
            endpoint_count: 0,
            account_include: Vec::new(),
        };
        let stats = BenchmarkStats {
            endpoint_summaries: Vec::new(),
            pairwise: vec![PairwiseSummary {
                faster_alias: "fast".to_string(),
                faster_protocol: "yellowstone".to_string(),
                slower_alias: "slow".to_string(),
                slower_protocol: "aperture-txstream".to_string(),
                shared_signatures: 1,
                wins: 1,
                win_pct: 100.0,
                median_lag_us: Some(1),
                p95_lag_us: Some(1),
                median_behind_us: Some(2),
                p95_behind_us: Some(2),
            }],
            total_unique_signatures: 0,
            race_eligible_signatures: 0,
            full_coverage_signatures: 0,
            configured_endpoint_count: 0,
        };

        let report = strip_ansi(&render_terminal_report(&meta, &[], &stats, "report.html"));
        let lines: Vec<&str> = report.lines().collect();

        assert_section_matches_separator(&lines, "Configured streams");
        assert_section_matches_separator(&lines, "Endpoint ranking");
        assert_section_matches_separator(&lines, "Pairwise winners");
    }

    #[test]
    fn terminal_layout_keeps_full_values_when_they_fit() {
        let endpoints = vec![Endpoint {
            alias: "rpcfast_shredstream".to_string(),
            protocol: crate::config::Protocol::JitoShredstream,
            url: "https://beta-solana-shredstream-grpc.rpcfast.com:443".to_string(),
            token: String::new(),
            signatures_only: true,
            include_simulation: false,
        }];
        let streams = streams_table_layout(&endpoints, 180);
        assert_eq!(streams.alias_width, "rpcfast_shredstream".len());
        assert_eq!(
            streams.endpoint_width,
            "https://beta-solana-shredstream-grpc.rpcfast.com:443".len()
        );

        let ranking = ranking_table_layout(
            &[EndpointSummary {
                alias: "rpcfast_shredstream".to_string(),
                protocol_label: "Jito ShredStream".to_string(),
                ..EndpointSummary::default()
            }],
            180,
        );
        assert_eq!(ranking.endpoint_width, "rpcfast_shredstream".len());
        assert_eq!(ranking.protocol_width, "Jito ShredStream".len());

        let pairwise = pairwise_table_layout(
            &[PairwiseSummary {
                faster_alias: "rpcfast_shredstream".to_string(),
                faster_protocol: "Jito ShredStream".to_string(),
                slower_alias: "rpcfast_txstream".to_string(),
                slower_protocol: "Aperture txstream".to_string(),
                shared_signatures: 1,
                wins: 1,
                win_pct: 100.0,
                median_lag_us: Some(1),
                p95_lag_us: Some(1),
                median_behind_us: Some(2),
                p95_behind_us: Some(2),
            }],
            180,
        );
        assert_eq!(pairwise.faster_width, "rpcfast_shredstream".len());
        assert_eq!(pairwise.slower_width, "rpcfast_txstream".len());
    }

    #[test]
    fn center_text_adds_left_padding() {
        assert_eq!(center_text("RPC Fast", 12), "  RPC Fast");
        assert_eq!(center_text("RPC Fast", 8), "RPC Fast");
    }

    fn assert_section_matches_separator(lines: &[&str], title: &str) {
        let section_idx = lines
            .iter()
            .position(|line| line.contains(&format!("-- {title} ")))
            .expect("section line exists");
        let separator_idx = lines[section_idx + 1..]
            .iter()
            .position(|line| line.trim_start().starts_with("---"))
            .expect("table separator exists")
            + section_idx
            + 1;

        assert_eq!(lines[section_idx].len(), lines[separator_idx].len());
    }

    fn strip_ansi(value: &str) -> String {
        let mut stripped = String::new();
        let mut chars = value.chars();
        while let Some(ch) = chars.next() {
            if ch == '\x1b' {
                for escape_ch in chars.by_ref() {
                    if escape_ch == 'm' {
                        break;
                    }
                }
            } else {
                stripped.push(ch);
            }
        }
        stripped
    }
}
